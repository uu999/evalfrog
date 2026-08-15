package eventing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestOutboxAgeServiceSamplesAuthorityAndStops(t *testing.T) {
	reader := &outboxAgeReaderStub{age: time.Second}
	observer := &outboxAgeObserverStub{}
	service, err := NewOutboxAgeService(reader, observer, time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	deadline := time.After(time.Second)
	for observer.calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("outbox age was never observed")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error=%v", err)
	}
	if _, err = NewOutboxAgeService(nil, nil, 0, nil); err == nil {
		t.Fatal("invalid service accepted")
	}
}

func TestConsumerLagServiceSamplesBoundedTransportMetric(t *testing.T) {
	reader := &lagReaderStub{values: []ConsumerLag{{Group: "runtime-engine-v1", Topic: "runtime-events", Value: 3}}}
	observer := &lagObserverStub{}
	service, err := NewConsumerLagService(reader, observer, time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	deadline := time.After(time.Second)
	for observer.calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("Kafka lag was never observed")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error=%v", err)
	}
	if _, err = NewConsumerLagService(nil, nil, 0, nil); err == nil {
		t.Fatal("invalid service accepted")
	}
}

type outboxAgeReaderStub struct{ age time.Duration }

func (stub *outboxAgeReaderStub) OldestUnpublishedOutboxAge(context.Context) (time.Duration, error) {
	return stub.age, nil
}

type outboxAgeObserverStub struct {
	calls atomic.Int32
}

func (stub *outboxAgeObserverStub) ObserveOutboxOldestAge(time.Duration) { stub.calls.Add(1) }

type lagReaderStub struct{ values []ConsumerLag }

func (stub *lagReaderStub) ConsumerLag(context.Context) ([]ConsumerLag, error) {
	return stub.values, nil
}

type lagObserverStub struct{ calls atomic.Int32 }

func (stub *lagObserverStub) ObserveKafkaConsumerLag(string, string, int64) { stub.calls.Add(1) }
