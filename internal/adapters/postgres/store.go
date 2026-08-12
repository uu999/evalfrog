package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uu999/evalfrog/internal/scheduling"
)

// Store implements Access, Managed Resource, and Definition repository ports
// over one PostgreSQL pool. Domain packages never import this adapter.
type Store struct {
	pool   *pgxpool.Pool
	router scheduling.Router
}

func NewStore(pool *pgxpool.Pool) *Store {
	return NewStoreWithRouter(pool, scheduling.BuiltinV1Router())
}

func NewStoreWithRouter(pool *pgxpool.Pool, router scheduling.Router) *Store {
	return &Store{pool: pool, router: router}
}
