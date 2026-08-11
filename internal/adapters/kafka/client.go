package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/uu999/evalfrog/internal/platform/config"
)

type Client struct {
	client *kgo.Client
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
	return &Client{client: client}, nil
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.client.Ping(ctx); err != nil {
		return fmt.Errorf("Kafka ping: %w", err)
	}
	return nil
}

func (client *Client) Close() { client.client.Close() }
