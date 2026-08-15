package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/scheduling"
)

const consumerRetryBackoff = 200 * time.Millisecond

type Client struct {
	client        *kgo.Client
	configuration config.KafkaConfig
	maxPoll       int
	group         string
	topics        []string
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
	return &Client{client: client, configuration: value, maxPoll: maxPollRecords, group: value.TopicPrefix + "." + group, topics: names}, nil
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.client.Ping(ctx); err != nil {
		return fmt.Errorf("Kafka ping: %w", err)
	}
	return nil
}

// ConsumerLag samples the broker end offsets and this Client's committed group
// offsets. It is intentionally available only for consumer Clients; producers
// have no consumer-group progress to report.
func (client *Client) ConsumerLag(ctx context.Context) ([]eventing.ConsumerLag, error) {
	if client.group == "" || len(client.topics) == 0 {
		return nil, nil
	}
	metadata := kmsg.NewMetadataRequest()
	metadata.AllowAutoTopicCreation = false
	for _, name := range client.topics {
		name := name
		metadata.Topics = append(metadata.Topics, kmsg.MetadataRequestTopic{Topic: &name})
	}
	metadataRaw, err := client.client.Request(ctx, &metadata)
	if err != nil {
		return nil, fmt.Errorf("fetch Kafka topic metadata: %w", err)
	}
	metadataResponse, ok := metadataRaw.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected Kafka metadata response %T", metadataRaw)
	}
	latest := kmsg.NewListOffsetsRequest()
	committed := kmsg.NewOffsetFetchRequest()
	committed.Group = client.group
	for _, topic := range metadataResponse.Topics {
		if topic.Topic == nil || topic.ErrorCode != 0 {
			continue
		}
		latestTopic := kmsg.ListOffsetsRequestTopic{Topic: *topic.Topic}
		committedTopic := kmsg.OffsetFetchRequestTopic{Topic: *topic.Topic}
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				continue
			}
			latestTopic.Partitions = append(latestTopic.Partitions, kmsg.ListOffsetsRequestTopicPartition{Partition: partition.Partition, Timestamp: -1})
			committedTopic.Partitions = append(committedTopic.Partitions, partition.Partition)
		}
		if len(latestTopic.Partitions) != 0 {
			latest.Topics = append(latest.Topics, latestTopic)
			committed.Topics = append(committed.Topics, committedTopic)
		}
	}
	if len(latest.Topics) == 0 {
		return nil, nil
	}
	latestRaw, err := client.client.Request(ctx, &latest)
	if err != nil {
		return nil, fmt.Errorf("fetch Kafka end offsets: %w", err)
	}
	latestResponse, ok := latestRaw.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected Kafka list offsets response %T", latestRaw)
	}
	committedRaw, err := client.client.Request(ctx, &committed)
	if err != nil {
		return nil, fmt.Errorf("fetch Kafka committed offsets: %w", err)
	}
	committedResponse, ok := committedRaw.(*kmsg.OffsetFetchResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected Kafka offset fetch response %T", committedRaw)
	}
	commits := make(map[string]map[int32]int64)
	for _, topic := range committedResponse.Topics {
		values := make(map[int32]int64, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			if partition.ErrorCode == 0 {
				values[partition.Partition] = partition.Offset
			}
		}
		commits[topic.Topic] = values
	}
	values := make([]eventing.ConsumerLag, 0, len(latestResponse.Topics))
	for _, topic := range latestResponse.Topics {
		var lag int64
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 || partition.Offset < 0 {
				continue
			}
			offset, exists := commits[topic.Topic][partition.Partition]
			if !exists || offset < 0 {
				offset = 0
			}
			if partition.Offset > offset {
				lag += partition.Offset - offset
			}
		}
		values = append(values, eventing.ConsumerLag{Group: client.group, Topic: topic.Topic, Value: lag})
	}
	return values, nil
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
				if retryErr := waitForConsumerRetry(ctx, err, consumerRetryBackoff); retryErr != nil {
					return nil, fmt.Errorf("poll Kafka record: %w", retryErr)
				}
				continue
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
	for {
		err := value.owner.client.CommitRecords(ctx, value.record)
		if err == nil {
			value.settle(true)
			return nil
		}
		if retryErr := waitForConsumerRetry(ctx, err, consumerRetryBackoff); retryErr != nil {
			value.settle(false)
			return fmt.Errorf("commit Kafka record: %w", retryErr)
		}
	}
}

// Kafka group coordination and broker transport are deliberately contained in
// the adapter. A new coordinator, a temporary broker loss, or a group
// rebalance must not terminate the Engine or a Worker: their durable Inbox,
// Claim and fencing protocols already make a later delivery safe. Contract and
// authorization errors remain terminal to expose deployment mistakes promptly.
func waitForConsumerRetry(ctx context.Context, err error, delay time.Duration) error {
	if !retryableConsumerError(err) {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryableConsumerError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrClientClosed) {
		return false
	}
	// Unknown topic is flagged retriable by Kafka because topic creation can be
	// asynchronous. EvalFrog creates its finite topic set before consumers
	// start, so seeing it here is a deployment/configuration fault, not a
	// reason to conceal a permanently unhealthy consumer in an infinite loop.
	if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.GroupAuthorizationFailed) || errors.Is(err, kerr.SaslAuthenticationFailed) || errors.Is(err, kerr.FencedInstanceID) {
		return false
	}
	// These are explicitly non-retriable at the raw request layer because the
	// caller must join the group again. The franz-go client owns that rejoin, so
	// a Consumer adapter should wait for the next Poll/commit opportunity rather
	// than convert an ordinary rebalance into a process restart.
	if errors.Is(err, kerr.RebalanceInProgress) || errors.Is(err, kerr.IllegalGeneration) || errors.Is(err, kerr.UnknownMemberID) {
		return true
	}
	if kerr.IsRetriable(err) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
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
