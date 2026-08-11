package migrations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var migrationName = regexp.MustCompile(`^(\d{6})_([a-z0-9_]+)\.up\.sql$`)

type Migration struct {
	Version  int64
	Name     string
	SQL      string
	Checksum string
}

type Runner struct {
	Pool        *pgxpool.Pool
	Schema      string
	Directory   string
	LockTimeout time.Duration
}

func (runner Runner) Up(ctx context.Context) error {
	plan, err := LoadPlan(os.DirFS(runner.Directory))
	if err != nil {
		return err
	}
	lockCtx, cancel := context.WithTimeout(ctx, runner.LockTimeout)
	defer cancel()
	connection, err := runner.Pool.Acquire(lockCtx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer connection.Release()
	lockName := "evalfrog:migrations:" + runner.Schema
	if _, err := connection.Exec(lockCtx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", lockName); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		_, _ = connection.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtextextended($1, 0))", lockName)
	}()

	identifier := pgx.Identifier{runner.Schema}.Sanitize()
	if _, err := connection.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+identifier); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	table := identifier + ".schema_migrations"
	if _, err := connection.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+table+" (version BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}
	for _, migration := range plan {
		var checksum string
		err := connection.QueryRow(ctx, "SELECT checksum FROM "+table+" WHERE version=$1", migration.Version).Scan(&checksum)
		switch {
		case err == nil:
			if checksum != migration.Checksum {
				return fmt.Errorf("migration %06d checksum mismatch", migration.Version)
			}
			continue
		case !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("read migration %06d: %w", migration.Version, err)
		}
		transaction, err := connection.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %06d: %w", migration.Version, err)
		}
		if _, err := transaction.Exec(ctx, "SET LOCAL search_path TO "+identifier); err != nil {
			_ = transaction.Rollback(ctx)
			return fmt.Errorf("set migration schema %06d: %w", migration.Version, err)
		}
		if _, err := transaction.Exec(ctx, migration.SQL); err != nil {
			_ = transaction.Rollback(ctx)
			return fmt.Errorf("execute migration %06d_%s: %w", migration.Version, migration.Name, err)
		}
		if _, err := transaction.Exec(ctx, "INSERT INTO "+table+" (version, name, checksum) VALUES ($1, $2, $3)", migration.Version, migration.Name, migration.Checksum); err != nil {
			_ = transaction.Rollback(ctx)
			return fmt.Errorf("record migration %06d: %w", migration.Version, err)
		}
		if err := transaction.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %06d: %w", migration.Version, err)
		}
	}
	return nil
}

func LoadPlan(filesystem fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(filesystem, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	plan := make([]Migration, 0, len(entries))
	versions := make(map[int64]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches := migrationName.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version %s: %w", entry.Name(), err)
		}
		if previous, exists := versions[version]; exists {
			return nil, fmt.Errorf("duplicate migration version %06d in %s and %s", version, previous, entry.Name())
		}
		content, err := fs.ReadFile(filesystem, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		digest := sha256.Sum256(content)
		versions[version] = entry.Name()
		plan = append(plan, Migration{Version: version, Name: matches[2], SQL: string(content), Checksum: hex.EncodeToString(digest[:])})
	}
	sort.Slice(plan, func(left, right int) bool { return plan[left].Version < plan[right].Version })
	return plan, nil
}
