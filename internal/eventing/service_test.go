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

func TestRelayServiceContinuesAfterRelayErrorAndResetsActiveDelay(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	relay := &scriptedRelay{after: cancel}
	service, err := NewRelayService("relay-errors", relay, time.Millisecond, 2*time.Millisecond, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error=%v", err)
	}
	if relay.calls.Load() < 3 {
		t.Fatalf("relay calls=%d", relay.calls.Load())
	}
}

type scriptedRelay struct {
	calls atomic.Int32
	after context.CancelFunc
}

func (value *scriptedRelay) RelayOnce(context.Context) (int, error) {
	call := value.calls.Add(1)
	if call == 1 {
		return 0, errors.New("temporary")
	}
	if call == 2 {
		return 1, nil
	}
	value.after()
	return 0, nil
}
