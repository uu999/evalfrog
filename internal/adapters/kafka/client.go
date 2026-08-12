package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type Client struct {
	client        *kgo.Client
	configuration config.KafkaConfig
	maxPoll       int
	deliveryMu    sync.Mutex
	pending       []*kgo.Record
	outstanding   bool
}

func Open(value config.KafkaConfig, clientID string) (*Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(value.Brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(int32(value.BatchBytes)),
		kgo.ProducerLinger(value.Linger.Duration()),
		kgo.RequestTimeoutOverhead(value.RequestTimeout.Duration()),
		kgo.SessionTimeout(value.SessionTimeout.Duration()),
		kgo.HeartbeatInterval(value.HeartbeatInterval.Duration()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	return &Client{client: client, configuration: value, maxPoll: 1}, nil
}

func OpenConsumer(value config.KafkaConfig, clientID, group string, topics []config.KafkaTopicConfig, maxPollRecords int) (*Client, error) {
	if group == "" || len(topics) == 0 || maxPollRecords < 1 {
		return nil, fmt.Errorf("Kafka consumer group, topics and poll limit are required")
	}
	names := make([]string, len(topics))
	for index, topic := range topics {
		names[index] = value.TopicPrefix + "." + topic.Name
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(value.Brokers...), kgo.ClientID(clientID),
		kgo.ConsumerGroup(value.TopicPrefix+"."+group), kgo.ConsumeTopics(names...),
		kgo.DisableAutoCommit(),
		// Delivery ACK is also the rebalance release point. This prevents a
		// partition from being revoked between Poll and the authoritative
		// Claim/Inbox transaction whose offset we are about to commit.
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(value.SessionTimeout.Duration()),
		kgo.HeartbeatInterval(value.HeartbeatInterval.Duration()),
		kgo.RebalanceTimeout(value.MaxPollInterval.Duration()),
		kgo.FetchMaxBytes(int32(value.BrokerMaxMessageBytes)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RequestTimeoutOverhead(value.RequestTimeout.Duration()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	return &Client{client: client, configuration: value, maxPoll: maxPollRecords}, nil
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.client.Ping(ctx); err != nil {
		return fmt.Errorf("Kafka ping: %w", err)
	}
	return nil
}

func (client *Client) Close() {
	// Shutdown may cancel a handler between Poll and settlement. No further
	// offset commit is possible once the process is closing, so release any
	// outstanding rebalance gate before leaving the group.
	client.deliveryMu.Lock()
	client.pending = nil
	client.outstanding = false
	client.deliveryMu.Unlock()
	client.client.AllowRebalance()
	client.client.Close()
}

func (client *Client) PublishRuntimeEvent(ctx context.Context, event eventing.RuntimeEvent) error {
	payload, err := event.MarshalJSONMessage()
	if err != nil {
		return err
	}
	return client.publish(ctx, client.configuration.Topics.RuntimeEvent, event.RunID, payload, nil)
}

func (client *Client) PublishTask(ctx context.Context, message eventing.TaskMessage) error {
	payload, err := message.MarshalJSONMessage()
	if err != nil {
		return err
	}
	topic := client.configuration.Topics.BuiltinTask
	if message.ResourceClass == scheduling.ResourceSandbox {
		topic = client.configuration.Topics.SandboxTask
	}
	return client.publish(ctx, topic, message.AttemptID, payload, nil)
}

func (client *Client) publish(ctx context.Context, topic config.KafkaTopicConfig, key string, payload []byte, headers []kgo.RecordHeader) error {
	if len(payload) == 0 || len(payload) > client.configuration.EnvelopeMaxBytes {
		return fmt.Errorf("Kafka envelope size must be in [1,%d]", client.configuration.EnvelopeMaxBytes)
	}
	record := &kgo.Record{Topic: client.configuration.TopicPrefix + "." + topic.Name, Key: []byte(key), Value: payload, Headers: headers, Timestamp: time.Now().UTC()}
	if err := client.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish Kafka record: %w", err)
	}
	return nil
}

type delivery struct {
	owner   *Client
	record  *kgo.Record
	release sync.Once
}

func (client *Client) Receive(ctx context.Context) (eventing.Delivery, error) {
	client.deliveryMu.Lock()
	defer client.deliveryMu.Unlock()
	if client.outstanding {
		return nil, fmt.Errorf("previous Kafka delivery is not settled")
	}
	for {
		if len(client.pending) == 0 {
			fetches := client.client.PollRecords(ctx, client.maxPoll)
			if err := fetches.Err(); err != nil {
				return nil, fmt.Errorf("poll Kafka record: %w", err)
			}
			fetches.EachRecord(func(value *kgo.Record) { client.pending = append(client.pending, value) })
		}
		if len(client.pending) != 0 {
			record := client.pending[0]
			client.pending = client.pending[1:]
			client.outstanding = true
			return &delivery{owner: client, record: record}, nil
		}
	}
}

func (value *delivery) Topic() string   { return value.record.Topic }
func (value *delivery) Key() string     { return string(value.record.Key) }
func (value *delivery) Payload() []byte { return append([]byte(nil), value.record.Value...) }
func (value *delivery) Nack()           { value.settle(false) }

func (value *delivery) Ack(ctx context.Context) error {
	err := value.owner.client.CommitRecords(ctx, value.record)
	if err != nil {
		value.settle(false)
		return fmt.Errorf("commit Kafka record: %w", err)
	}
	value.settle(true)
	return nil
}

func (value *delivery) settle(success bool) {
	value.release.Do(func() { value.owner.finishDelivery(success) })
}

func (client *Client) finishDelivery(success bool) {
	client.deliveryMu.Lock()
	client.outstanding = false
	if !success {
		// The service exits after a retryable handler error. Forget later
		// fetched records so no offset can be committed past the failure.
		client.pending = nil
	}
	release := !success || len(client.pending) == 0
	client.deliveryMu.Unlock()
	if release {
		client.client.AllowRebalance()
	}
}

func (value *delivery) DeadLetter(ctx context.Context, reason string) error {
	metadata, _ := json.Marshal(map[string]any{"source_topic": value.record.Topic, "partition": value.record.Partition, "offset": value.record.Offset, "reason": reason})
	headers := []kgo.RecordHeader{{Key: "evalfrog-dead-letter", Value: metadata}}
	if err := value.owner.publish(ctx, value.owner.configuration.Topics.DLQ, string(value.record.Key), value.record.Value, headers); err != nil {
		return err
	}
	return value.Ack(ctx)
}

var _ eventing.MessagePublisher = (*Client)(nil)
var _ eventing.TaskPublisher = (*Client)(nil)
var _ eventing.Consumer = (*Client)(nil)
