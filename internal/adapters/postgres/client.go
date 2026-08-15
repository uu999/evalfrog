package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uu999/evalfrog/internal/platform/config"
)

type Client struct {
	pool *pgxpool.Pool
}

// AcquireObserver receives only bounded pool timing metadata. The adapter
// owns the pgx tracer so platform metrics never become a PostgreSQL dependency.
type AcquireObserver interface {
	ObservePostgresPoolAcquire(time.Duration, string)
}

func Open(ctx context.Context, value config.PostgresConfig) (*Client, error) {
	return OpenWithAcquireObserver(ctx, value, nil)
}

func OpenWithAcquireObserver(ctx context.Context, value config.PostgresConfig, observer AcquireObserver) (*Client, error) {
	poolConfig, err := pgxpool.ParseConfig(value.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	poolConfig.MinConns = value.PoolMin
	poolConfig.MaxConns = value.PoolMax
	poolConfig.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{value.Schema}.Sanitize() + ", public"
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = value.StatementTimeout.Duration().String()
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = value.LockTimeout.Duration().String()
	poolConfig.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = value.IdleInTransactionTimeout.Duration().String()
	if observer != nil {
		poolConfig.ConnConfig.Tracer = poolAcquireTracer{observer: observer}
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	return &Client{pool: pool}, nil
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.pool.Ping(ctx); err != nil {
		return fmt.Errorf("PostgreSQL ping: %w", err)
	}
	return nil
}

func (client *Client) Pool() *pgxpool.Pool { return client.pool }

func (client *Client) Close() { client.pool.Close() }

type poolAcquireStartKey struct{}

type poolAcquireTracer struct{ observer AcquireObserver }

func (tracer poolAcquireTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (poolAcquireTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer poolAcquireTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return context.WithValue(ctx, poolAcquireStartKey{}, time.Now())
}

func (tracer poolAcquireTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	started, ok := ctx.Value(poolAcquireStartKey{}).(time.Time)
	if !ok || tracer.observer == nil {
		return
	}
	outcome := "success"
	if data.Err != nil {
		outcome = "error"
	}
	tracer.observer.ObservePostgresPoolAcquire(time.Since(started), outcome)
}
