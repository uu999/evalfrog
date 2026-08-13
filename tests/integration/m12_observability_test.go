//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/adapters/postgres"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/metrics"
)

// Capacity reporting needs an actual pool acquisition sample, not only a unit
// test of the tracer callback. Pinging a real pool exercises PGX's configured
// AcquireTracer and proves the Controller can export the bounded metric.
func TestM12PostgresPoolAcquireIsExported(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	if dsn := os.Getenv("EVALFROG_INTEGRATION_POSTGRES_DSN"); dsn != "" {
		configuration.Postgres.DSN = dsn
	}
	registry := metrics.New("m12-postgres-observability")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := postgres.OpenWithAcquireObserver(ctx, configuration.Postgres, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Check(ctx); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || !strings.Contains(string(body), `evalfrog_postgres_pool_acquire_seconds_count{outcome="success"} 1`) {
		t.Fatalf("PostgreSQL pool metric is missing after a real acquire: status=%d body=%s", response.Code, body)
	}
}
