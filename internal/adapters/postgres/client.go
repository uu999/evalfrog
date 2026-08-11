package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uu999/evalfrog/internal/platform/config"
)

type Client struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, value config.PostgresConfig) (*Client, error) {
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
