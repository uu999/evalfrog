package eventing

import (
	"context"
	"fmt"
	"log/slog"
)

type Delivery interface {
	Topic() string
	Key() string
	Payload() []byte
	Ack(context.Context) error
	Nack()
	DeadLetter(context.Context, string) error
}

type Consumer interface {
	Receive(context.Context) (Delivery, error)
}

type RuntimeEventHandler interface {
	Consume(context.Context, RuntimeEvent) error
}

type RuntimeConsumerService struct {
	consumer Consumer
	handler  RuntimeEventHandler
	logger   *slog.Logger
}

func NewRuntimeConsumerService(consumer Consumer, handler RuntimeEventHandler, logger *slog.Logger) (*RuntimeConsumerService, error) {
	if consumer == nil || handler == nil || logger == nil {
		return nil, fmt.Errorf("runtime consumer dependencies are required")
	}
	return &RuntimeConsumerService{consumer: consumer, handler: handler, logger: logger}, nil
}

func (service *RuntimeConsumerService) Name() string { return "runtime-event-consumer" }

func (service *RuntimeConsumerService) Run(ctx context.Context) error {
	for {
		delivery, err := service.consumer.Receive(ctx)
		if err != nil {
			return err
		}
		event, err := ParseRuntimeEvent(delivery.Payload())
		if err != nil {
			service.logger.Warn("runtime event sent to dead letter", "error", err)
			if deadErr := delivery.DeadLetter(ctx, "INVALID_RUNTIME_EVENT"); deadErr != nil {
				return deadErr
			}
			continue
		}
		if err = service.handler.Consume(ctx, event); err != nil {
			delivery.Nack()
			return err
		}
		if err = delivery.Ack(ctx); err != nil {
			return err
		}
	}
}

func (service *RuntimeConsumerService) Shutdown(context.Context) error { return nil }
