package eventing

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type BatchRelay interface {
	RelayOnce(context.Context) (int, error)
}

type RelayService struct {
	name       string
	relay      BatchRelay
	activePoll time.Duration
	idleMax    time.Duration
	logger     *slog.Logger
	stop       context.CancelFunc
}

func NewRelayService(name string, relay BatchRelay, activePoll, idleMax time.Duration, logger *slog.Logger) (*RelayService, error) {
	if name == "" || relay == nil || activePoll <= 0 || idleMax < activePoll || logger == nil {
		return nil, fmt.Errorf("relay service dependencies and valid polling bounds are required")
	}
	return &RelayService{name: name, relay: relay, activePoll: activePoll, idleMax: idleMax, logger: logger}, nil
}

func (service *RelayService) Name() string { return service.name }

func (service *RelayService) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	service.stop = cancel
	delay := service.activePoll
	for {
		count, err := service.relay.RelayOnce(runCtx)
		if err != nil && runCtx.Err() == nil {
			service.logger.Warn("outbox relay iteration failed", "component", service.name, "error", err)
		}
		if count > 0 {
			delay = service.activePoll
		} else {
			delay = min(delay*2, service.idleMax)
		}
		timer := time.NewTimer(delay)
		select {
		case <-runCtx.Done():
			timer.Stop()
			return runCtx.Err()
		case <-timer.C:
		}
	}
}

func (service *RelayService) Shutdown(context.Context) error {
	if service.stop != nil {
		service.stop()
	}
	return nil
}
