package cacheredis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/uu999/evalfrog/internal/platform/config"
)

type Client struct {
	client *redis.Client
}

func Open(value config.RedisEndpointConfig) *Client {
	return &Client{client: redis.NewClient(&redis.Options{
		Addr: value.Address, Password: value.Password, DB: value.DB,
		DialTimeout: value.OperationTimeout.Duration(), ReadTimeout: value.OperationTimeout.Duration(),
		WriteTimeout: value.OperationTimeout.Duration(), MaxRetries: value.MaxRetries,
	})}
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Cache Redis ping: %w", err)
	}
	return nil
}

func (client *Client) Close() error { return client.client.Close() }
