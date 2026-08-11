package traceid

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type fixedGenerator struct{ value string }

func (generator fixedGenerator) New() (string, error) { return generator.value, nil }

func TestMiddlewarePreservesValidTraceID(t *testing.T) {
	t.Parallel()
	handler := Middleware(fixedGenerator{"generated"}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if value := From(request.Context()); value != "caller-trace" {
			t.Fatalf("context trace=%q", value)
		}
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(Header, "caller-trace")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if value := response.Header().Get(Header); value != "caller-trace" {
		t.Fatalf("response trace=%q", value)
	}
}

func TestMiddlewareReplacesInvalidTraceID(t *testing.T) {
	t.Parallel()
	handler := Middleware(fixedGenerator{"generated"}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(Header, "invalid trace with spaces")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if value := response.Header().Get(Header); value != "generated" {
		t.Fatalf("response trace=%q", value)
	}
}
