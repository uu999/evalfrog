package scheduling

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/platform/clock"
)

type sequenceIDs struct {
	mu    sync.Mutex
	value int
}

func (ids *sequenceIDs) New() (string, error) {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	ids.value++
	return "id-" + time.Unix(int64(ids.value), 0).UTC().Format("150405"), nil
}

type fakeAuthority struct {
	mu       sync.Mutex
	snapshot AuthoritySnapshot
	dispatch map[string]int
	err      error
	failNode string
}

type failingIDs struct{ err error }

func (ids failingIDs) New() (string, error) { return "", ids.err }

type changingCapacity struct {
	mu       sync.Mutex
	capacity Capacity
	err      error
}

type recordingObserver struct {
	readyLatencies []time.Duration
	rebuilds       []string
}

func (observer *recordingObserver) ObserveReadyToQueued(value time.Duration) {
	observer.readyLatencies = append(observer.readyLatencies, value)
}

func (observer *recordingObserver) ObserveSchedulingRedisRebuild(outcome string) {
	observer.rebuilds = append(observer.rebuilds, outcome)
}

func (provider *changingCapacity) HealthyCapacity(context.Context) (Capacity, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.capacity, provider.err
}

func (authority *fakeAuthority) LoadSchedulingSnapshot(context.Context, int) (AuthoritySnapshot, error) {
	if authority.err != nil {
		return AuthoritySnapshot{}, authority.err
	}
	return authority.snapshot, nil
}

func (authority *fakeAuthority) DispatchReady(_ context.Context, command DispatchCommand) (Task, error) {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if command.Candidate.NodeRunID == authority.failNode {
		return Task{}, errors.New("dispatch failed")
	}
	if authority.dispatch == nil {
		authority.dispatch = map[string]int{}
	}
	if authority.dispatch[command.Candidate.NodeRunID] != 0 {
		return Task{}, ErrCandidateStale
	}
	authority.dispatch[command.Candidate.NodeRunID]++
	return Task{MessageVersion: 1, AttemptID: command.AttemptID, TaskID: command.TaskID, ProjectID: command.Candidate.ProjectID}, nil
}

type memoryStore struct {
	mu           sync.Mutex
	paused       bool
	lease        BalancerLease
	candidates   map[int][]Candidate
	credits      map[int][]PlannedAdmission
	reservations map[string]Reservation
	fail         error
	failRebuild  error
	reserveCalls int
}

func (store *memoryStore) BoundWindows(_ context.Context, _ BalancerLease, desired Windows, limit float64) (Windows, error) {
	if store.fail != nil {
		return Windows{}, store.fail
	}
	return BoundWindows(Windows{}, desired, limit)
}

func (store *memoryStore) AcquireBalancerLease(context.Context, string, time.Duration) (BalancerLease, error) {
	if store.fail != nil {
		return BalancerLease{}, store.fail
	}
	store.lease = BalancerLease{Owner: "owner", Token: "token", FencingToken: store.lease.FencingToken + 1, ExpiresAt: time.Now().Add(time.Minute)}
	return store.lease, nil
}
func (store *memoryStore) PauseAdmissions(context.Context, BalancerLease, int) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fail != nil {
		return store.fail
	}
	store.paused = true
	return nil
}
func (store *memoryStore) ListReservations(context.Context, int) ([]Reservation, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fail != nil {
		return nil, store.fail
	}
	var result []Reservation
	for _, value := range store.reservations {
		result = append(result, value)
	}
	return result, nil
}
func (store *memoryStore) RebuildLane(_ context.Context, _ BalancerLease, state LaneState, _, _ time.Duration) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failRebuild != nil {
		return store.failRebuild
	}
	if store.fail != nil {
		return store.fail
	}
	if store.candidates == nil {
		store.candidates = map[int][]Candidate{}
	}
	store.candidates[state.Lane] = append([]Candidate(nil), state.Candidates...)
	return nil
}
func (store *memoryStore) Grant(_ context.Context, _ BalancerLease, lane int, values []PlannedAdmission) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fail != nil {
		return store.fail
	}
	if store.credits == nil {
		store.credits = map[int][]PlannedAdmission{}
	}
	store.credits[lane] = append(store.credits[lane], values...)
	return nil
}
func (store *memoryStore) Activate(context.Context, BalancerLease, int, uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fail != nil {
		return store.fail
	}
	store.paused = false
	return nil
}
func (store *memoryStore) ReserveNext(_ context.Context, lane int, attemptID string, _ time.Duration) (Reservation, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.reserveCalls++
	if store.fail != nil {
		return Reservation{}, false, store.fail
	}
	if store.paused {
		return Reservation{}, false, ErrAdmissionPaused
	}
	for index, credit := range store.credits[lane] {
		for candidateIndex, candidate := range store.candidates[lane] {
			if candidate.ProjectID != credit.ProjectID {
				continue
			}
			store.credits[lane] = append(store.credits[lane][:index], store.credits[lane][index+1:]...)
			store.candidates[lane] = append(store.candidates[lane][:candidateIndex], store.candidates[lane][candidateIndex+1:]...)
			reservation := Reservation{AttemptID: attemptID, ProjectID: candidate.ProjectID, Lane: lane, ResourceClass: candidate.ResourceClass, Candidate: candidate}
			if store.reservations == nil {
				store.reservations = map[string]Reservation{}
			}
			store.reservations[attemptID] = reservation
			return reservation, true, nil
		}
	}
	return Reservation{}, false, nil
}

func TestEmptyLaneStopsAfterFirstReservationMiss(t *testing.T) {
	store := &memoryStore{}
	scheduler := newFakeScheduler(t, &fakeAuthority{}, store, 8)
	tasks, err := scheduler.AdmitLane(context.Background(), 0, 8, "trace")
	if err != nil || len(tasks) != 0 || store.reserveCalls != 1 {
		t.Fatalf("tasks=%v calls=%d err=%v", tasks, store.reserveCalls, err)
	}
}
func (store *memoryStore) ConfirmReservation(_ context.Context, reservation Reservation, _ time.Duration) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	reservation.Confirmed = true
	store.reservations[reservation.AttemptID] = reservation
	return nil
}
func (store *memoryStore) AbortReservation(_ context.Context, reservation Reservation, restore bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.reservations, reservation.AttemptID)
	if restore {
		store.candidates[reservation.Lane] = append([]Candidate{reservation.Candidate}, store.candidates[reservation.Lane]...)
		store.credits[reservation.Lane] = append([]PlannedAdmission{{ProjectID: reservation.ProjectID, ResourceClass: reservation.ResourceClass}}, store.credits[reservation.Lane]...)
	}
	return nil
}

func TestSchedulerRebuildsBeforeActivationAndAdmitsWithinPlan(t *testing.T) {
	authority := &fakeAuthority{snapshot: fixtureSnapshot(2, 10, ResourceBuiltin)}
	store := &memoryStore{}
	scheduler := newFakeScheduler(t, authority, store, 8)
	plan, err := scheduler.Rebalance(context.Background())
	if err != nil || store.paused || plan.TotalAdmissions != 8 {
		t.Fatalf("plan=%+v paused=%v err=%v", plan, store.paused, err)
	}
	var tasks []Task
	for lane := 0; lane < scheduler.settings.LaneCount; lane++ {
		values, admitErr := scheduler.AdmitLane(context.Background(), lane, 8, "trace")
		if admitErr != nil {
			t.Fatal(admitErr)
		}
		tasks = append(tasks, values...)
	}
	if len(tasks) != 8 || len(store.reservations) != 8 {
		t.Fatalf("tasks=%d reservations=%d", len(tasks), len(store.reservations))
	}
}

func TestSchedulerObservesRebuildAndAuthoritativeDispatchLatency(t *testing.T) {
	authority := &fakeAuthority{snapshot: fixtureSnapshot(1, 1, ResourceBuiltin)}
	store := &memoryStore{}
	observer := &recordingObserver{}
	settings := Settings{LaneCount: 8, CreditGrantBatch: 4, CandidateBatch: 8, AdmissionConcurrency: 2, Epoch: time.Second, ActiveProjectTTL: 5 * time.Second, BalancerLease: 2 * time.Second, ReservationTTL: time.Minute, DispatchBufferFactor: 1, CapacityChangeLimit: 0.1}
	now := time.Date(2026, 8, 12, 0, 0, 1, 0, time.UTC)
	scheduler, err := New(authority, store, FixedCapacity{Pools: map[ResourceClass]int{ResourceBuiltin: 1}}, &sequenceIDs{}, clock.NewFake(now), "scheduler", settings, observer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.Rebalance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(observer.rebuilds) != 1 || observer.rebuilds[0] != "success" {
		t.Fatalf("rebuild observations=%v", observer.rebuilds)
	}
	lane, _ := LaneFor("project-000", settings.LaneCount)
	if _, err = scheduler.AdmitLane(context.Background(), lane, 1, "trace"); err != nil {
		t.Fatal(err)
	}
	if len(observer.readyLatencies) != 1 || observer.readyLatencies[0] != time.Second {
		t.Fatalf("ready latencies=%v", observer.readyLatencies)
	}
}

func TestSchedulerObservesRebuildFailure(t *testing.T) {
	store := &memoryStore{fail: errors.New("redis unavailable")}
	observer := &recordingObserver{}
	settings := Settings{LaneCount: 8, CreditGrantBatch: 4, CandidateBatch: 8, AdmissionConcurrency: 2, Epoch: time.Second, ActiveProjectTTL: 5 * time.Second, BalancerLease: 2 * time.Second, ReservationTTL: time.Minute, DispatchBufferFactor: 1, CapacityChangeLimit: 0.1}
	scheduler, err := New(&fakeAuthority{snapshot: fixtureSnapshot(1, 1, ResourceBuiltin)}, store, FixedCapacity{Pools: map[ResourceClass]int{ResourceBuiltin: 1}}, &sequenceIDs{}, clock.NewFake(time.Now()), "scheduler", settings, observer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.Rebalance(context.Background()); err == nil {
		t.Fatal("rebuild failure was hidden")
	}
	if len(observer.rebuilds) != 0 {
		t.Fatalf("failure before rebuild must not report a rebuild result: %v", observer.rebuilds)
	}

	store = &memoryStore{candidates: map[int][]Candidate{}, credits: map[int][]PlannedAdmission{}, failRebuild: errors.New("rebuild failed")}
	scheduler, err = New(&fakeAuthority{snapshot: fixtureSnapshot(1, 1, ResourceBuiltin)}, store, FixedCapacity{Pools: map[ResourceClass]int{ResourceBuiltin: 1}}, &sequenceIDs{}, clock.NewFake(time.Now()), "scheduler", settings, observer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.Rebalance(context.Background()); err == nil || len(observer.rebuilds) != 1 || observer.rebuilds[0] != "failure" {
		t.Fatalf("err=%v rebuild observations=%v", err, observer.rebuilds)
	}
}

func TestSchedulerFailsClosedWhenCoordinationStoreUnavailable(t *testing.T) {
	store := &memoryStore{fail: errors.New("redis unavailable")}
	scheduler := newFakeScheduler(t, &fakeAuthority{snapshot: fixtureSnapshot(1, 10, ResourceBuiltin)}, store, 8)
	if _, err := scheduler.Rebalance(context.Background()); err == nil {
		t.Fatal("Redis failure did not pause admission")
	}
	if _, err := scheduler.AdmitLane(context.Background(), 0, 1, "trace"); err == nil {
		t.Fatal("admission continued while coordination store failed")
	}
}

func TestConcurrentSchedulersConvergeOnOneAuthoritativeDispatch(t *testing.T) {
	candidate := fixtureCandidates("project", 1, ResourceBuiltin)[0]
	authority := &fakeAuthority{snapshot: AuthoritySnapshot{Candidates: []Candidate{candidate}}}
	stores := []*memoryStore{
		{paused: false, candidates: map[int][]Candidate{}, credits: map[int][]PlannedAdmission{}},
		{paused: false, candidates: map[int][]Candidate{}, credits: map[int][]PlannedAdmission{}},
	}
	lane, _ := LaneFor(candidate.ProjectID, 8)
	for _, store := range stores {
		store.candidates[lane] = []Candidate{candidate}
		store.credits[lane] = []PlannedAdmission{{ProjectID: candidate.ProjectID, ResourceClass: candidate.ResourceClass}}
	}
	var wait sync.WaitGroup
	for _, store := range stores {
		wait.Add(1)
		go func(store *memoryStore) {
			defer wait.Done()
			scheduler := newFakeScheduler(t, authority, store, 1)
			_, _ = scheduler.AdmitLane(context.Background(), lane, 1, "trace")
		}(store)
	}
	wait.Wait()
	if authority.dispatch[candidate.NodeRunID] != 1 {
		t.Fatalf("dispatches=%v", authority.dispatch)
	}
}

func TestMergeReservationsDeduplicatesAttemptAndExcludesReservedCandidate(t *testing.T) {
	candidate := fixtureCandidates("project", 1, ResourceBuiltin)[0]
	value := Inflight{AttemptID: "attempt", ProjectID: "project", ResourceClass: ResourceBuiltin}
	lane, _ := LaneFor("project", 8)
	merged, keep, renew := mergeReservations(AuthoritySnapshot{Candidates: []Candidate{candidate}, Inflight: []Inflight{value}}, []Reservation{
		{AttemptID: "attempt", ProjectID: "project", Lane: lane, ResourceClass: ResourceBuiltin, Candidate: candidate},
		{AttemptID: "reserved", ProjectID: "project", Lane: lane, ResourceClass: ResourceBuiltin, Candidate: candidate},
	}, 8)
	if len(merged.Inflight) != 2 || len(merged.Candidates) != 0 || len(keep[lane]) != 2 || len(renew[lane]) != 1 {
		t.Fatalf("merged=%+v keep=%v renew=%v", merged, keep, renew)
	}
}

func TestSchedulerValidatesPublicContractsAndRoutingPolicy(t *testing.T) {
	router := BuiltinV1Router()
	for coordinate, want := range map[dsl.Coordinate]ResourceClass{
		{Type: "task.python", Version: 1}: ResourceSandbox,
		{Type: "task.http", Version: 1}:   ResourceBuiltin,
		{Type: "task.rpc", Version: 1}:    ResourceBuiltin,
	} {
		if got, exists := router.Resolve(coordinate); !exists || got != want {
			t.Fatalf("route %v=%s,%v", coordinate, got, exists)
		}
	}
	if _, exists := router.Resolve(dsl.Coordinate{Type: "unknown", Version: 1}); exists {
		t.Fatal("unknown operation was routed")
	}
	if err := (Candidate{}).Validate(); err == nil {
		t.Fatal("invalid candidate accepted")
	}
	if _, err := LaneFor("", 3); err == nil {
		t.Fatal("invalid lane dimensions accepted")
	}
	if _, _, err := DispatchWindows(Capacity{Pools: map[ResourceClass]int{"unknown": 1}}, 1); err == nil {
		t.Fatal("invalid capacity accepted")
	}
	if _, _, err := DispatchWindows(Capacity{}, 0.5); err == nil {
		t.Fatal("invalid buffer factor accepted")
	}
	builtin := RequiredCapabilities(ResourceBuiltin)
	if len(builtin) != 2 || builtin[0].Type != "task.http" || builtin[1].Type != "task.rpc" {
		t.Fatalf("builtin capabilities=%v", builtin)
	}
	if CapabilityFingerprint(ResourceBuiltin) == "" || CapabilityFingerprint(ResourceBuiltin) == CapabilityFingerprint(ResourceSandbox) {
		t.Fatal("resource-class capability fingerprints are not stable and distinct")
	}
	registration := WorkerRegistration{WorkerID: "worker", ExecutorBuild: "build", ResourceClass: ResourceBuiltin, Slots: 2, Capabilities: builtin, TTL: time.Minute}
	if err := registration.Validate(); err != nil {
		t.Fatal(err)
	}
	registration.Capabilities = builtin[:1]
	if err := registration.Validate(); err == nil {
		t.Fatal("partial resource-class capability set accepted")
	}
	if _, err := CreditBatches(nil, 0); err == nil {
		t.Fatal("invalid credit batch accepted")
	}
	if _, err := BoundWindows(Windows{}, Windows{Global: -1}, 0.1); err == nil {
		t.Fatal("invalid window accepted")
	}
	if _, err := New(nil, nil, nil, nil, nil, "", Settings{}); err == nil {
		t.Fatal("missing scheduler dependencies accepted")
	}
}

func TestSchedulerRebalancePropagatesCapacityAndAuthorityFailures(t *testing.T) {
	authority := &fakeAuthority{snapshot: fixtureSnapshot(1, 2, ResourceBuiltin)}
	store := &memoryStore{}
	provider := &changingCapacity{capacity: Capacity{Pools: map[ResourceClass]int{ResourceBuiltin: 2}}, err: errors.New("capacity unavailable")}
	settings := Settings{LaneCount: 8, CreditGrantBatch: 4, CandidateBatch: 8, AdmissionConcurrency: 2, Epoch: time.Second, ActiveProjectTTL: 5 * time.Second, BalancerLease: 2 * time.Second, ReservationTTL: time.Minute, DispatchBufferFactor: 1, CapacityChangeLimit: 0.1}
	scheduler, err := New(authority, store, provider, &sequenceIDs{}, clock.NewFake(time.Now()), "scheduler", settings)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.Rebalance(context.Background()); err == nil {
		t.Fatal("capacity failure hidden")
	}
	provider.err = nil
	authority.err = errors.New("authority unavailable")
	if _, err = scheduler.Rebalance(context.Background()); err == nil {
		t.Fatal("authority failure hidden")
	}
}

func TestAdmissionRestoresReservationOnIdentityFailure(t *testing.T) {
	candidate := fixtureCandidates("project", 1, ResourceBuiltin)[0]
	lane, _ := LaneFor(candidate.ProjectID, 8)
	store := &memoryStore{candidates: map[int][]Candidate{lane: {candidate}}, credits: map[int][]PlannedAdmission{lane: {{ProjectID: candidate.ProjectID, ResourceClass: candidate.ResourceClass}}}}
	settings := Settings{LaneCount: 8, CreditGrantBatch: 4, CandidateBatch: 8, AdmissionConcurrency: 1, Epoch: time.Second, ActiveProjectTTL: 5 * time.Second, BalancerLease: 2 * time.Second, ReservationTTL: time.Minute, DispatchBufferFactor: 1, CapacityChangeLimit: 0.1}
	scheduler, err := New(&fakeAuthority{}, store, FixedCapacity{Pools: map[ResourceClass]int{ResourceBuiltin: 1}}, failingIDs{err: errors.New("identity failed")}, clock.NewFake(time.Now()), "scheduler", settings)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.AdmitLane(context.Background(), lane, 1, "trace"); err == nil {
		t.Fatal("identity failure hidden")
	}
	if len(store.candidates[lane]) != 1 || len(store.credits[lane]) != 1 || len(store.reservations) != 0 {
		t.Fatalf("reservation was not restored: candidates=%d credits=%d reservations=%d", len(store.candidates[lane]), len(store.credits[lane]), len(store.reservations))
	}
}

func TestAdmissionPreservesProjectOrderAndRestoresUndispatchedTail(t *testing.T) {
	candidates := fixtureCandidates("project", 3, ResourceBuiltin)
	lane, _ := LaneFor("project", 8)
	store := &memoryStore{
		candidates: map[int][]Candidate{lane: append([]Candidate(nil), candidates...)},
		credits: map[int][]PlannedAdmission{lane: {
			{ProjectID: "project", ResourceClass: ResourceBuiltin},
			{ProjectID: "project", ResourceClass: ResourceBuiltin},
			{ProjectID: "project", ResourceClass: ResourceBuiltin},
		}},
	}
	authority := &fakeAuthority{failNode: candidates[1].NodeRunID}
	scheduler := newFakeScheduler(t, authority, store, 3)
	tasks, err := scheduler.AdmitLane(context.Background(), lane, 3, "trace")
	if err == nil || len(tasks) != 1 {
		t.Fatalf("tasks=%v err=%v", tasks, err)
	}
	if authority.dispatch[candidates[0].NodeRunID] != 1 || authority.dispatch[candidates[2].NodeRunID] != 0 {
		t.Fatalf("out-of-order dispatch=%v", authority.dispatch)
	}
	if len(store.candidates[lane]) != 2 || store.candidates[lane][0].NodeRunID != candidates[1].NodeRunID || store.candidates[lane][1].NodeRunID != candidates[2].NodeRunID {
		t.Fatalf("restored candidates=%v", store.candidates[lane])
	}
}

func TestSchedulerServiceLifecycleAndFailClosedRetry(t *testing.T) {
	store := &memoryStore{fail: errors.New("redis unavailable")}
	scheduler := newFakeScheduler(t, &fakeAuthority{}, store, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := NewService(nil, "", nil); err == nil {
		t.Fatal("invalid scheduler service accepted")
	}
	service, err := NewService(scheduler, "trace", logger)
	if err != nil || service.Name() != "project-fair-scheduler" {
		t.Fatalf("service=%v err=%v", service, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("service stop=%v", err)
	}
	if err = service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerValidatesSettingsAndAdmissionCoordinates(t *testing.T) {
	settings := Settings{LaneCount: 3, CreditGrantBatch: 0}
	if err := settings.Validate(); err == nil {
		t.Fatal("invalid settings accepted")
	}
	scheduler := newFakeScheduler(t, &fakeAuthority{}, &memoryStore{}, 1)
	for _, call := range []struct {
		lane  int
		limit int
		trace string
	}{{-1, 1, "trace"}, {0, 0, "trace"}, {0, 9, "trace"}, {0, 1, ""}} {
		if _, err := scheduler.AdmitLane(context.Background(), call.lane, call.limit, call.trace); err == nil {
			t.Fatalf("invalid admission accepted: %+v", call)
		}
	}
	service, _ := NewService(scheduler, "trace", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestParallelLaneOperationsAreBoundedAndPropagateFailure(t *testing.T) {
	var mutex sync.Mutex
	active, maximum := 0, 0
	err := parallelLanes(context.Background(), 12, 3, func(index int) error {
		mutex.Lock()
		active++
		maximum = max(maximum, active)
		mutex.Unlock()
		time.Sleep(time.Millisecond)
		mutex.Lock()
		active--
		mutex.Unlock()
		if index == 7 {
			return errors.New("lane failed")
		}
		return nil
	})
	if err == nil || maximum > 3 {
		t.Fatalf("err=%v maximum=%d", err, maximum)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = parallelLanes(ctx, 2, 1, func(int) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lanes=%v", err)
	}
}

func newFakeScheduler(t *testing.T, authority Authority, store CoordinationStore, window int) *Scheduler {
	t.Helper()
	settings := Settings{LaneCount: 8, CreditGrantBatch: 4, CandidateBatch: 8, AdmissionConcurrency: 4, Epoch: time.Second, ActiveProjectTTL: 5 * time.Second, BalancerLease: 2 * time.Second, ReservationTTL: time.Minute, DispatchBufferFactor: 1, CapacityChangeLimit: 0.1}
	scheduler, err := New(authority, store, FixedCapacity{Pools: map[ResourceClass]int{ResourceBuiltin: window}}, &sequenceIDs{}, clock.NewFake(time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)), "scheduler", settings)
	if err != nil {
		t.Fatal(err)
	}
	return scheduler
}
