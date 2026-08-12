package cacheredis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/uu999/evalfrog/internal/platform/config"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

type Client struct {
	client *redis.Client
	prefix string
}

func Open(value config.RedisEndpointConfig) *Client {
	return &Client{client: redis.NewClient(&redis.Options{
		Addr: value.Address, Password: value.Password, DB: value.DB,
		DialTimeout: value.OperationTimeout.Duration(), ReadTimeout: value.OperationTimeout.Duration(),
		WriteTimeout: value.OperationTimeout.Duration(), PoolTimeout: value.OperationTimeout.Duration(),
		// Cache is an optimization, so an unavailable node must fail fast and let
		// the gateway read PostgreSQL. A zero value enables go-redis' five dial
		// attempts; explicitly keep this to one attempt per command retry.
		DialerRetries: 1, DialerRetryTimeout: value.OperationTimeout.Duration(),
		ContextTimeoutEnabled: true, MaxRetries: value.MaxRetries,
	}), prefix: value.KeyPrefix}
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Cache Redis ping: %w", err)
	}
	return nil
}

func (client *Client) Close() error { return client.client.Close() }

func (client *Client) GetSnapshot(ctx context.Context, snapshotID string) (json.RawMessage, bool) {
	return client.get(ctx, client.prefix+"context:snapshot:"+snapshotID)
}
func (client *Client) PutSnapshot(ctx context.Context, snapshotID string, value json.RawMessage, ttl time.Duration) {
	client.put(ctx, client.prefix+"context:snapshot:"+snapshotID, value, ttl)
}
func (client *Client) GetRunInput(ctx context.Context, runID string) (json.RawMessage, bool) {
	return client.get(ctx, client.prefix+"context:run-input:"+runID)
}
func (client *Client) PutRunInput(ctx context.Context, runID string, value json.RawMessage, ttl time.Duration) {
	client.put(ctx, client.prefix+"context:run-input:"+runID, value, ttl)
}
func (client *Client) GetEffectiveOutput(ctx context.Context, runID, nodeID string) (json.RawMessage, bool) {
	value, err := client.client.HGet(ctx, client.prefix+"context:run:"+runID, "output:"+nodeID).Bytes()
	return validJSON(value, err)
}
func (client *Client) PutEffectiveOutput(ctx context.Context, runID, nodeID string, value json.RawMessage, ttl time.Duration) {
	key := client.prefix + "context:run:" + runID
	pipe := client.client.Pipeline()
	pipe.HSet(ctx, key, "output:"+nodeID, []byte(value))
	pipe.Expire(ctx, key, ttl)
	_, _ = pipe.Exec(ctx)
}
func (client *Client) get(ctx context.Context, key string) (json.RawMessage, bool) {
	value, err := client.client.Get(ctx, key).Bytes()
	return validJSON(value, err)
}
func (client *Client) put(ctx context.Context, key string, value json.RawMessage, ttl time.Duration) {
	if json.Valid(value) {
		_ = client.client.Set(ctx, key, []byte(value), ttl).Err()
	}
}
func validJSON(value []byte, err error) (json.RawMessage, bool) {
	if err != nil || !json.Valid(value) {
		return nil, false
	}
	return json.RawMessage(value), true
}

var _ runtimecontext.Cache = (*Client)(nil)
