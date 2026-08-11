package bootstrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbe(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if err := Probe(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
}

func TestProbeRejectsUnreadyService(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if err := Probe(context.Background(), server.URL); err == nil {
		t.Fatal("expected unhealthy probe to fail")
	}
}
