package httpserver

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/platform/health"
	"github.com/uu999/evalfrog/internal/platform/metrics"
)

func TestServerStartsAndShutsDown(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := New("test", "127.0.0.1:0", time.Second, time.Second, logger, health.New(time.Second), metrics.New("test"))
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run(context.Background()) }()
	select {
	case <-server.Ready():
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	response, err := http.Get("http://" + server.Address() + "/health/live")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}
