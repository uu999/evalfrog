package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryExposesBuildMetric(t *testing.T) {
	t.Parallel()
	registry := New("evalfrog-test")
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "evalfrog_build_info") {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}
