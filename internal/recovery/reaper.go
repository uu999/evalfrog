// Package recovery owns authority-driven detection of executions that can no
// longer make progress. It never infers business state from Kafka offsets.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/uu999/evalfrog/internal/runtime/attempt"
)

type Repository interface {
	ListExpiredAttempts(context.Context, time.Duration, int) ([]attempt.MarkLostCommand, error)
}

type Coordinator interface {
	MarkExpiredLost(context.Context, attempt.MarkLostCommand) (bool, error)
}

type Reaper struct {
	repository  Repository
	coordinator Coordinator
	grace       time.Duration
	interval    time.Duration
	batch       int
	traceID     string
	logger      *slog.Logger
	stop        context.CancelFunc
}

func NewReaper(repository Repository, coordinator Coordinator, grace, interval time.Duration, batch int, traceID string, logger *slog.Logger) (*Reaper, error) {
	if repository == nil || coordinator == nil || grace < 0 || interval <= 0 || batch < 1 || traceID == "" || logger == nil {
		return nil, fmt.Errorf("attempt reaper dependencies and scan settings are required")
	}
	return &Reaper{repository: repository, coordinator: coordinator, grace: grace, interval: interval, batch: batch, traceID: traceID, logger: logger}, nil
}

func (reaper *Reaper) Name() string { return "expired-attempt-reaper" }

func (reaper *Reaper) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	reaper.stop = cancel
	ticker := time.NewTicker(reaper.interval)
	defer ticker.Stop()
	for {
		if err := reaper.ScanOnce(runCtx); err != nil && runCtx.Err() == nil {
			reaper.logger.Warn("expired attempt scan failed", "error", err)
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
	}
}

func (reaper *Reaper) ScanOnce(ctx context.Context) error {
	values, err := reaper.repository.ListExpiredAttempts(ctx, reaper.grace, reaper.batch)
	if err != nil {
		return err
	}
	for _, command := range values {
		command.TraceID = reaper.traceID
		_, err = reaper.coordinator.MarkExpiredLost(ctx, command)
		if err != nil && !errors.Is(err, attempt.ErrStateConflict) && !errors.Is(err, attempt.ErrNotCurrent) && !errors.Is(err, attempt.ErrLeaseMismatch) {
			return err
		}
	}
	return nil
}

func (reaper *Reaper) Shutdown(context.Context) error {
	if reaper.stop != nil {
		reaper.stop()
	}
	return nil
}
