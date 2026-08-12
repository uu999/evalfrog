package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/uu999/evalfrog/internal/scheduling"
)

type WorkerRegistry interface {
	RegisterWorker(context.Context, scheduling.WorkerRegistration) error
}

type RegistrationService struct {
	registry     WorkerRegistry
	registration scheduling.WorkerRegistration
	interval     time.Duration
	logger       *slog.Logger
	stop         context.CancelFunc
}

func NewRegistrationService(registry WorkerRegistry, registration scheduling.WorkerRegistration, interval time.Duration, logger *slog.Logger) (*RegistrationService, error) {
	if registry == nil || interval <= 0 || registration.TTL < interval*2 || logger == nil {
		return nil, fmt.Errorf("worker registry and safe heartbeat timing are required")
	}
	return &RegistrationService{registry: registry, registration: registration, interval: interval, logger: logger}, nil
}

func (service *RegistrationService) Name() string { return "worker-capacity-registration" }
func (service *RegistrationService) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	service.stop = cancel
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	for {
		if err := service.registry.RegisterWorker(runCtx, service.registration); err != nil && runCtx.Err() == nil {
			service.logger.Warn("worker capacity heartbeat failed", "error", err)
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
	}
}
func (service *RegistrationService) Shutdown(context.Context) error {
	if service.stop != nil {
		service.stop()
	}
	return nil
}
