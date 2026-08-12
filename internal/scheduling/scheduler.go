package scheduling

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/identity"
)

type Scheduler struct {
	authority Authority
	store     CoordinationStore
	capacity  CapacityProvider
	ids       identity.Generator
	clock     clock.Clock
	owner     string
	settings  Settings
}

func New(authority Authority, store CoordinationStore, capacity CapacityProvider, ids identity.Generator, valueClock clock.Clock, owner string, settings Settings) (*Scheduler, error) {
	if authority == nil || store == nil || capacity == nil || ids == nil || valueClock == nil || owner == "" {
		return nil, fmt.Errorf("scheduler dependencies and owner are required")
	}
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	return &Scheduler{authority: authority, store: store, capacity: capacity, ids: ids, clock: valueClock, owner: owner, settings: settings}, nil
}

func (scheduler *Scheduler) Rebalance(ctx context.Context) (Plan, error) {
	lease, err := scheduler.store.AcquireBalancerLease(ctx, scheduler.owner, scheduler.settings.BalancerLease)
	if err != nil {
		return Plan{}, err
	}
	if err = parallelLanes(ctx, scheduler.settings.LaneCount, scheduler.settings.AdmissionConcurrency, func(lane int) error {
		return scheduler.store.PauseAdmissions(ctx, lease, lane)
	}); err != nil {
		return Plan{}, err
	}
	reservationLanes := make([][]Reservation, scheduler.settings.LaneCount)
	if err = parallelLanes(ctx, scheduler.settings.LaneCount, scheduler.settings.AdmissionConcurrency, func(lane int) error {
		values, listErr := scheduler.store.ListReservations(ctx, lane)
		if listErr != nil {
			return listErr
		}
		reservationLanes[lane] = values
		return nil
	}); err != nil {
		return Plan{}, err
	}
	reservations := make([]Reservation, 0)
	for _, values := range reservationLanes {
		reservations = append(reservations, values...)
	}
	capacity, err := scheduler.capacity.HealthyCapacity(ctx)
	if err != nil {
		return Plan{}, err
	}
	globalWindow, poolWindows, err := DispatchWindows(capacity, scheduler.settings.DispatchBufferFactor)
	if err != nil {
		return Plan{}, err
	}
	windows, err := scheduler.store.BoundWindows(ctx, lease, Windows{Global: globalWindow, Pools: poolWindows}, scheduler.settings.CapacityChangeLimit)
	if err != nil {
		return Plan{}, err
	}
	snapshot, err := scheduler.authority.LoadSchedulingSnapshot(ctx, windows.Global)
	if err != nil {
		return Plan{}, err
	}
	snapshot, keepReservations, renewReservations := mergeReservations(snapshot, reservations, scheduler.settings.LaneCount)
	plan, err := BuildPlan(snapshot, scheduler.settings.LaneCount, windows.Global, windows.Pools, lease.FencingToken)
	if err != nil {
		return Plan{}, err
	}
	if err = parallelLanes(ctx, len(plan.Lanes), scheduler.settings.AdmissionConcurrency, func(index int) error {
		lane := plan.Lanes[index]
		if rebuildErr := scheduler.store.RebuildLane(ctx, lease, LaneState{Lane: lane.Lane, Candidates: lane.Candidates, Inflight: lane.Inflight, KeepReservations: keepReservations[lane.Lane], RenewReservations: renewReservations[lane.Lane]}, scheduler.settings.ActiveProjectTTL, scheduler.settings.ReservationTTL); rebuildErr != nil {
			return rebuildErr
		}
		batches, batchErr := CreditBatches(lane.Admissions, scheduler.settings.CreditGrantBatch)
		if batchErr != nil {
			return batchErr
		}
		for _, batch := range batches {
			if grantErr := scheduler.store.Grant(ctx, lease, lane.Lane, batch); grantErr != nil {
				return grantErr
			}
		}
		return nil
	}); err != nil {
		return Plan{}, err
	}
	if err = parallelLanes(ctx, len(plan.Lanes), scheduler.settings.AdmissionConcurrency, func(index int) error {
		return scheduler.store.Activate(ctx, lease, plan.Lanes[index].Lane, plan.Epoch)
	}); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func parallelLanes(ctx context.Context, count, concurrency int, operation func(int) error) error {
	if count == 0 {
		return nil
	}
	jobs := make(chan int)
	errorsFound := make(chan error, concurrency)
	workers := min(count, concurrency)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				if err := operation(index); err != nil {
					select {
					case errorsFound <- err:
					default:
					}
				}
			}
		}()
	}
	for index := 0; index < count; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		return err
	}
	return nil
}

func (scheduler *Scheduler) AdmitLane(ctx context.Context, lane, limit int, traceID string) ([]Task, error) {
	if lane < 0 || lane >= scheduler.settings.LaneCount || limit <= 0 || limit > scheduler.settings.CandidateBatch || traceID == "" {
		return nil, fmt.Errorf("admission lane, bounded limit and trace are required")
	}
	reservations := make([]Reservation, 0, limit)
	for len(reservations) < limit {
		attemptID, err := scheduler.ids.New()
		if err != nil {
			scheduler.abortReservations(ctx, reservations, true)
			return nil, err
		}
		reservation, exists, err := scheduler.store.ReserveNext(ctx, lane, attemptID, scheduler.settings.ReservationTTL)
		if err != nil {
			scheduler.abortReservations(ctx, reservations, true)
			return nil, err
		}
		if !exists {
			break
		}
		reservations = append(reservations, reservation)
	}
	type projectBatch struct {
		projectID    string
		reservations []Reservation
	}
	type result struct {
		tasks []Task
		err   error
	}
	byProject := make(map[string][]Reservation)
	projects := make([]string, 0)
	for _, reservation := range reservations {
		if _, exists := byProject[reservation.ProjectID]; !exists {
			projects = append(projects, reservation.ProjectID)
		}
		byProject[reservation.ProjectID] = append(byProject[reservation.ProjectID], reservation)
	}
	results := make(chan result, len(projects))
	jobs := make(chan projectBatch, len(projects))
	workers := min(len(projects), scheduler.settings.AdmissionConcurrency)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for batch := range jobs {
				tasks := make([]Task, 0, len(batch.reservations))
				var batchErr error
				for index, reservation := range batch.reservations {
					task, err := scheduler.dispatchReservation(ctx, reservation, traceID)
					if err != nil {
						for restoreIndex := len(batch.reservations) - 1; restoreIndex >= index; restoreIndex-- {
							restore := restoreIndex != index || !errors.Is(err, ErrCandidateStale)
							_ = scheduler.store.AbortReservation(ctx, batch.reservations[restoreIndex], restore)
						}
						batchErr = err
						break
					}
					tasks = append(tasks, task)
				}
				results <- result{tasks: tasks, err: batchErr}
			}
		}()
	}
	for _, projectID := range projects {
		jobs <- projectBatch{projectID: projectID, reservations: byProject[projectID]}
	}
	close(jobs)
	wait.Wait()
	close(results)
	tasks := make([]Task, 0, limit)
	var firstErr error
	for value := range results {
		tasks = append(tasks, value.tasks...)
		if value.err != nil {
			if firstErr == nil {
				firstErr = value.err
			}
		}
	}
	return tasks, firstErr
}

func (scheduler *Scheduler) abortReservations(ctx context.Context, reservations []Reservation, restore bool) {
	for index := len(reservations) - 1; index >= 0; index-- {
		_ = scheduler.store.AbortReservation(ctx, reservations[index], restore)
	}
}

func (scheduler *Scheduler) dispatchReservation(ctx context.Context, reservation Reservation, traceID string) (Task, error) {
	taskID, err := scheduler.ids.New()
	if err != nil {
		return Task{}, err
	}
	task, err := scheduler.authority.DispatchReady(ctx, DispatchCommand{
		Candidate: reservation.Candidate, AttemptID: reservation.AttemptID, TaskID: taskID,
		TraceID: traceID, Now: scheduler.clock.Now().UTC(),
	})
	if err != nil {
		return Task{}, err
	}
	if err = scheduler.store.ConfirmReservation(ctx, reservation, scheduler.settings.ReservationTTL); err != nil {
		// The database already contains the authoritative queued Attempt. A
		// leaked derived reservation can only reduce capacity until TTL/rebuild;
		// it must never turn a committed Dispatch into an application failure.
		return task, nil
	}
	return task, nil
}

func mergeReservations(snapshot AuthoritySnapshot, reservations []Reservation, laneCount int) (AuthoritySnapshot, map[int]map[string]struct{}, map[int]map[string]struct{}) {
	result := AuthoritySnapshot{Candidates: make([]Candidate, 0, len(snapshot.Candidates)), Inflight: append([]Inflight(nil), snapshot.Inflight...)}
	keep := make(map[int]map[string]struct{})
	renew := make(map[int]map[string]struct{})
	reservedNodes := make(map[string]struct{}, len(reservations))
	seenAttempts := make(map[string]struct{}, len(snapshot.Inflight)+len(reservations))
	for _, value := range snapshot.Inflight {
		seenAttempts[value.AttemptID] = struct{}{}
		lane, _ := LaneFor(value.ProjectID, laneCount)
		if keep[lane] == nil {
			keep[lane] = map[string]struct{}{}
		}
		keep[lane][value.AttemptID] = struct{}{}
		if renew[lane] == nil {
			renew[lane] = map[string]struct{}{}
		}
		renew[lane][value.AttemptID] = struct{}{}
	}
	for _, reservation := range reservations {
		if reservation.Confirmed {
			if _, exists := seenAttempts[reservation.AttemptID]; !exists {
				continue
			}
		}
		if keep[reservation.Lane] == nil {
			keep[reservation.Lane] = map[string]struct{}{}
		}
		keep[reservation.Lane][reservation.AttemptID] = struct{}{}
		reservedNodes[reservation.Candidate.NodeRunID] = struct{}{}
		if _, exists := seenAttempts[reservation.AttemptID]; exists {
			continue
		}
		seenAttempts[reservation.AttemptID] = struct{}{}
		result.Inflight = append(result.Inflight, Inflight{AttemptID: reservation.AttemptID, ProjectID: reservation.ProjectID, ResourceClass: reservation.ResourceClass})
	}
	for _, candidate := range snapshot.Candidates {
		if _, reserved := reservedNodes[candidate.NodeRunID]; !reserved {
			result.Candidates = append(result.Candidates, candidate)
		}
	}
	return result, keep, renew
}

// Service runs one fail-closed rebalance before admission, then repeats it at
// every epoch. Kafka publication is deliberately outside M6; DispatchReady
// only creates the durable Task Outbox fact consumed by M7.
type Service struct {
	scheduler *Scheduler
	traceID   string
	logger    *slog.Logger
	stop      context.CancelFunc
}

func NewService(scheduler *Scheduler, traceID string, logger *slog.Logger) (*Service, error) {
	if scheduler == nil || traceID == "" || logger == nil {
		return nil, fmt.Errorf("scheduler service, trace and logger are required")
	}
	return &Service{scheduler: scheduler, traceID: traceID, logger: logger}, nil
}

func (service *Service) Name() string { return "project-fair-scheduler" }

func (service *Service) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	service.stop = cancel
	ticker := time.NewTicker(service.scheduler.settings.Epoch)
	defer ticker.Stop()
	for {
		_, rebalanceErr := service.scheduler.Rebalance(runCtx)
		if rebalanceErr == nil || errors.Is(rebalanceErr, ErrLeaseLost) {
			for lane := 0; lane < service.scheduler.settings.LaneCount; lane++ {
				if _, admitErr := service.scheduler.AdmitLane(runCtx, lane, service.scheduler.settings.CandidateBatch, service.traceID); admitErr != nil && !errors.Is(admitErr, ErrAdmissionPaused) {
					service.logger.Warn("scheduler admission paused after error", "lane", lane, "error", admitErr)
					break
				}
			}
		} else if runCtx.Err() == nil {
			// Redis or authority failure is fail-closed. Retrying on the next
			// epoch is safe because no lane is opened before a full rebuild.
			service.logger.Warn("scheduler rebalance failed closed", "error", rebalanceErr)
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
	}
}

func (service *Service) Shutdown(context.Context) error {
	if service.stop != nil {
		service.stop()
	}
	return nil
}
