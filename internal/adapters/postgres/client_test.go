package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPoolAcquireTracerReportsSuccessAndError(t *testing.T) {
	t.Parallel()
	observer := &acquireObserver{}
	tracer := poolAcquireTracer{observer: observer}
	ctx := tracer.TraceAcquireStart(context.Background(), nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})
	ctx = tracer.TraceAcquireStart(context.Background(), nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{Err: errors.New("pool unavailable")})
	if len(observer.samples) != 2 || observer.samples[0].outcome != "success" || observer.samples[1].outcome != "error" {
		t.Fatalf("samples=%+v", observer.samples)
	}
	for _, sample := range observer.samples {
		if sample.duration < 0 {
			t.Fatalf("negative acquire duration: %+v", sample)
		}
	}
}

type acquireObserver struct{ samples []acquireSample }
type acquireSample struct {
	duration time.Duration
	outcome  string
}

func (observer *acquireObserver) ObservePostgresPoolAcquire(duration time.Duration, outcome string) {
	observer.samples = append(observer.samples, acquireSample{duration: duration, outcome: outcome})
}
