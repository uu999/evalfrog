package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakeService struct {
	name     string
	mu       *sync.Mutex
	shutdown *[]string
}

func (service fakeService) Name() string { return service.name }

func (service fakeService) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (service fakeService) Shutdown(context.Context) error {
	service.mu.Lock()
	defer service.mu.Unlock()
	*service.shutdown = append(*service.shutdown, service.name)
	return nil
}

func TestRunShutsDownInReverseOrder(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var order []string
	services := []Service{
		fakeService{"http-server", &mu, &order},
		fakeService{"kafka-consumer", &mu, &order},
		fakeService{"background-job", &mu, &order},
	}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)), services...)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := []string{"background-job", "kafka-consumer", "http-server"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("shutdown order=%v, want %v", order, want)
	}
}

type deadlineService struct{}

func (deadlineService) Name() string { return "stuck-shutdown" }
func (deadlineService) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (deadlineService) Shutdown(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRunEnforcesShutdownDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := Run(ctx, 30*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)), deadlineService{})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("shutdown exceeded bound: %s", elapsed)
	}
}
