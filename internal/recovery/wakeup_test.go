package recovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/clock"
)

func TestRetryTimerEmitsOnlyDurableWakeupsAndPreservesTrace(t *testing.T) {
	repository := &wakeupRepositoryStub{retry: []Wakeup{{ProjectID: "project", RunID: "run", AggregateID: "attempt", EventType: eventing.RetryDue, TraceID: "api-trace"}}}
	emitter := testEmitter(t, repository)
	observer := &recoveryObserverStub{}
	scanner, err := NewRetryTimer(repository, emitter, time.Second, 10, "scanner-trace", discardLogger(), observer)
	if err != nil {
		t.Fatal(err)
	}
	if err = scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repository.emissions) != 1 || repository.emissions[0].TraceID != "api-trace" || repository.emissions[0].ActorID != "retry-timer" {
		t.Fatalf("emissions=%+v", repository.emissions)
	}
	if len(observer.wakeups) != 1 || observer.wakeups[0] != "retry-timer:retry.due:emitted" {
		t.Fatalf("metrics=%v", observer.wakeups)
	}
	repository.emit = false
	if err = scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(observer.wakeups) != 2 || observer.wakeups[1] != "retry-timer:retry.due:stale_or_cooled_down" {
		t.Fatalf("metrics=%v", observer.wakeups)
	}
}

func TestManualReplayRequiresProjectAdminAndNeverAllowsArbitraryEvent(t *testing.T) {
	repository := &wakeupRepositoryStub{}
	emitter := testEmitter(t, repository)
	denied, err := NewManualReplayer(repository, emitter, accessControlStub{err: access.ErrPermissionDenied})
	if err != nil {
		t.Fatal(err)
	}
	wakeup := Wakeup{ProjectID: "project", RunID: "run", AggregateID: "attempt", EventType: eventing.AttemptLost}
	if _, err = denied.Replay(context.Background(), wakeup, "operator-trace", "admin"); !errors.Is(err, access.ErrPermissionDenied) || len(repository.emissions) != 0 {
		t.Fatalf("err=%v emissions=%v", err, repository.emissions)
	}
	allowed, err := NewManualReplayer(repository, emitter, accessControlStub{})
	if err != nil {
		t.Fatal(err)
	}
	if emitted, err := allowed.Replay(context.Background(), wakeup, "operator-trace", "admin"); err != nil || !emitted {
		t.Fatalf("emitted=%t err=%v", emitted, err)
	}
	if len(repository.emissions) != 1 || !repository.emissions[0].Manual || repository.emissions[0].ActorType != "principal" || repository.emissions[0].ActorID != "admin" {
		t.Fatalf("emissions=%+v", repository.emissions)
	}
	if _, err = allowed.Replay(context.Background(), Wakeup{ProjectID: "project", RunID: "run", AggregateID: "other", EventType: eventing.RunCreated}, "operator-trace", "admin"); err == nil {
		t.Fatal("invalid replay wakeup accepted")
	}
}

func TestDeadlineAndReconcilerScannersUseOnlyTheirAuthorityQueries(t *testing.T) {
	repository := &wakeupRepositoryStub{
		deadlines: []Wakeup{{ProjectID: "project", RunID: "run", AggregateID: "run", EventType: eventing.RunDeadlineReached}},
		reconcile: []Wakeup{{ProjectID: "project", RunID: "run", AggregateID: "run", EventType: eventing.RunCreated}},
	}
	emitter := testEmitter(t, repository)
	deadline, err := NewDeadlineScanner(repository, emitter, time.Second, 1, "deadline-trace", discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewReconciler(repository, emitter, time.Second, 1, "reconciler-trace", discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err = deadline.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = reconciler.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repository.deadlineCalls != 1 || repository.reconcileCalls != 1 || repository.retryCalls != 0 || len(repository.emissions) != 2 {
		t.Fatalf("deadline=%d reconcile=%d retry=%d emissions=%d", repository.deadlineCalls, repository.reconcileCalls, repository.retryCalls, len(repository.emissions))
	}
}

func testEmitter(t *testing.T, repository *wakeupRepositoryStub) Emitter {
	t.Helper()
	emitter, err := NewEmitter(repository, &sequenceIDs{}, clock.NewFake(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	return emitter
}

type sequenceIDs struct {
	mu    sync.Mutex
	value int
}

func (ids *sequenceIDs) New() (string, error) {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	ids.value++
	return "00000000-0000-4000-8000-00000000000" + string(rune('0'+ids.value)), nil
}

type wakeupRepositoryStub struct {
	retry, deadlines, reconcile               []Wakeup
	emissions                                 []WakeupEmission
	retryCalls, deadlineCalls, reconcileCalls int
	emit                                      bool
}

func (stub *wakeupRepositoryStub) ListRetryDue(context.Context, int) ([]Wakeup, error) {
	stub.retryCalls++
	return stub.retry, nil
}
func (stub *wakeupRepositoryStub) ListDeadlinesDue(context.Context, int) ([]Wakeup, error) {
	stub.deadlineCalls++
	return stub.deadlines, nil
}
func (stub *wakeupRepositoryStub) ListReconciliationWakeups(context.Context, int) ([]Wakeup, error) {
	stub.reconcileCalls++
	return stub.reconcile, nil
}
func (stub *wakeupRepositoryStub) EmitWakeup(_ context.Context, emission WakeupEmission) (bool, error) {
	stub.emissions = append(stub.emissions, emission)
	if !stub.emit && len(stub.emissions) > 1 {
		return false, nil
	}
	return true, nil
}

type accessControlStub struct{ err error }

func (stub accessControlStub) Require(context.Context, string, string, access.Permission) error {
	return stub.err
}

type recoveryObserverStub struct{ wakeups []string }

func (stub *recoveryObserverStub) ObserveRecoveryWakeup(source, eventType, outcome string) {
	stub.wakeups = append(stub.wakeups, source+":"+eventType+":"+outcome)
}
func (*recoveryObserverStub) ObserveLeaseLost(string) {}
