package traceid

import (
	"context"
	"net/http"
	"regexp"

	"github.com/uu999/evalfrog/internal/platform/identity"
)

const Header = "X-Trace-ID"

type contextKey struct{}

var validTraceID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func With(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, contextKey{}, value)
}

func From(ctx context.Context) string {
	value, _ := ctx.Value(contextKey{}).(string)
	return value
}

func Middleware(generator identity.Generator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		traceID := request.Header.Get(Header)
		if !validTraceID.MatchString(traceID) {
			generated, err := generator.New()
			if err != nil {
				http.Error(writer, "unable to create trace identity", http.StatusInternalServerError)
				return
			}
			traceID = generated
		}
		writer.Header().Set(Header, traceID)
		next.ServeHTTP(writer, request.WithContext(With(request.Context(), traceID)))
	})
}
