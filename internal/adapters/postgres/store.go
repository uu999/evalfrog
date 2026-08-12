package postgres

import "github.com/jackc/pgx/v5/pgxpool"

// Store implements Access, Managed Resource, and Definition repository ports
// over one PostgreSQL pool. Domain packages never import this adapter.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }
