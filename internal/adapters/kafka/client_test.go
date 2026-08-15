package kafka

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

func TestRetryableConsumerErrorDistinguishesKafkaRecoveryFromTerminalErrors(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		allow bool
	}{
		{name: "coordinator unavailable", err: kerr.CoordinatorNotAvailable, allow: true},
		{name: "stale coordinator", err: kerr.NotCoordinator, allow: true},
		{name: "group rebalance", err: kerr.RebalanceInProgress, allow: true},
		{name: "generation must rejoin", err: kerr.IllegalGeneration, allow: true},
		{name: "member must rejoin", err: kerr.UnknownMemberID, allow: true},
		{name: "network timeout", err: &net.DNSError{IsTimeout: true}, allow: true},
		{name: "context cancelled", err: context.Canceled, allow: false},
		{name: "missing required topic", err: kerr.UnknownTopicOrPartition, allow: false},
		{name: "fenced static member", err: kerr.FencedInstanceID, allow: false},
		{name: "invalid topic", err: kerr.InvalidTopicException, allow: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := retryableConsumerError(test.err); got != test.allow {
				t.Fatalf("retryableConsumerError(%v)=%t want %t", test.err, got, test.allow)
			}
		})
	}
}

func TestWaitForConsumerRetryHonorsRecoveryAndShutdown(t *testing.T) {
	if err := waitForConsumerRetry(context.Background(), kerr.CoordinatorNotAvailable, 0); err != nil {
		t.Fatalf("retryable coordinator error was not released for a later poll: %v", err)
	}
	if err := waitForConsumerRetry(context.Background(), kerr.InvalidTopicException, 0); !errors.Is(err, kerr.InvalidTopicException) {
		t.Fatalf("terminal Kafka error was hidden: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForConsumerRetry(ctx, kerr.NotCoordinator, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown did not interrupt retry wait: %v", err)
	}
}
