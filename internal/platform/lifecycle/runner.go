package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type Service interface {
	Name() string
	Run(context.Context) error
	Shutdown(context.Context) error
}

type serviceResult struct {
	name string
	err  error
}

func Run(ctx context.Context, timeout time.Duration, logger *slog.Logger, services ...Service) error {
	if len(services) == 0 {
		return fmt.Errorf("at least one lifecycle service is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan serviceResult, len(services))
	var wait sync.WaitGroup
	for _, service := range services {
		service := service
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- serviceResult{name: service.Name(), err: service.Run(runCtx)}
		}()
	}

	var runError error
	select {
	case <-ctx.Done():
	case result := <-results:
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			runError = fmt.Errorf("service %s stopped: %w", result.name, result.err)
		} else if ctx.Err() == nil {
			runError = fmt.Errorf("service %s stopped unexpectedly", result.name)
		}
	}
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
	defer shutdownCancel()
	var shutdownErrors []error
	for index := len(services) - 1; index >= 0; index-- {
		service := services[index]
		logger.Info("stopping service", "component", service.Name())
		if err := service.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("shutdown %s: %w", service.Name(), err))
		}
	}

	done := make(chan struct{})
	go func() { wait.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		shutdownErrors = append(shutdownErrors, fmt.Errorf("services did not stop before deadline: %w", shutdownCtx.Err()))
	}
	return errors.Join(append([]error{runError}, shutdownErrors...)...)
}
