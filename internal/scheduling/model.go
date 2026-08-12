package scheduling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
)

var (
	ErrAdmissionPaused = errors.New("scheduling admission is paused")
	ErrCandidateStale  = errors.New("ready candidate is stale")
	ErrLeaseLost       = errors.New("credit balancer lease is lost")
)

type ResourceClass string

const (
	ResourceBuiltin ResourceClass = "builtin"
	ResourceSandbox ResourceClass = "sandbox"
)

func (value ResourceClass) Valid() bool {
	return value == ResourceBuiltin || value == ResourceSandbox
}

type Router interface {
	Resolve(dsl.Coordinate) (ResourceClass, bool)
}

type StaticRouter map[dsl.Coordinate]ResourceClass

func (router StaticRouter) Resolve(coordinate dsl.Coordinate) (ResourceClass, bool) {
	value, exists := router[coordinate]
	return value, exists
}

func BuiltinV1Router() StaticRouter {
	return StaticRouter{
		{Type: "task.python", Version: 1}: ResourceSandbox,
		{Type: "task.http", Version: 1}:   ResourceBuiltin,
		{Type: "task.rpc", Version: 1}:    ResourceBuiltin,
	}
}

// RequiredCapabilities returns the complete executor set for a first-phase
// resource class. Pool members are deliberately homogeneous: Kafka may assign
// any partition in a class topic to any member of its consumer group.
func RequiredCapabilities(class ResourceClass) []dsl.Coordinate {
	result := make([]dsl.Coordinate, 0, 2)
	for coordinate, routedClass := range BuiltinV1Router() {
		if routedClass == class {
			result = append(result, coordinate)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Type != result[right].Type {
			return result[left].Type < result[right].Type
		}
		return result[left].Version < result[right].Version
	})
	return result
}

// CapabilityFingerprint lets a scheduler ignore stale workers from an older
// routing-policy rollout. Validation prevents partial pools within one binary;
// the fingerprint also keeps mixed-version rolling deployments fail-closed.
func CapabilityFingerprint(class ResourceClass) string {
	digest := sha256.New()
	for _, coordinate := range RequiredCapabilities(class) {
		_, _ = fmt.Fprintf(digest, "%s@%d\n", coordinate.Type, coordinate.Version)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

type Candidate struct {
	ProjectID       string        `json:"project_id"`
	RunID           string        `json:"run_id"`
	NodeRunID       string        `json:"node_run_id"`
	ExecutionNodeID string        `json:"execution_node_id"`
	StateVersion    uint64        `json:"state_version"`
	Priority        int           `json:"priority"`
	ReadyAt         time.Time     `json:"ready_at"`
	ResourceClass   ResourceClass `json:"resource_class"`
}

func (candidate Candidate) Validate() error {
	if candidate.ProjectID == "" || candidate.RunID == "" || candidate.NodeRunID == "" || candidate.ExecutionNodeID == "" || candidate.StateVersion == 0 || candidate.ReadyAt.IsZero() || !candidate.ResourceClass.Valid() {
		return fmt.Errorf("ready candidate identity, version, time and resource class are required")
	}
	return nil
}

type Inflight struct {
	AttemptID     string
	ProjectID     string
	ResourceClass ResourceClass
}

type AuthoritySnapshot struct {
	Candidates []Candidate
	Inflight   []Inflight
}

type DispatchCommand struct {
	Candidate Candidate
	AttemptID string
	TaskID    string
	TraceID   string
	Now       time.Time
}

type Task struct {
	MessageVersion  int
	TaskID          string
	ProjectID       string
	RunID           string
	NodeRunID       string
	ExecutionNodeID string
	AttemptID       string
	AttemptSequence uint32
	ResourceClass   ResourceClass
	OccurredAt      time.Time
	TraceID         string
}

type Authority interface {
	// LoadSchedulingSnapshot returns at most candidateWindow ordered Ready
	// candidates per active project. The per-project bound preserves idle
	// borrowing even when one project is the only source of remaining demand.
	LoadSchedulingSnapshot(context.Context, int) (AuthoritySnapshot, error)
	DispatchReady(context.Context, DispatchCommand) (Task, error)
}

type BalancerLease struct {
	Owner        string
	Token        string
	FencingToken uint64
	ExpiresAt    time.Time
}

type LaneState struct {
	Lane              int
	Candidates        []Candidate
	Inflight          []Inflight
	KeepReservations  map[string]struct{}
	RenewReservations map[string]struct{}
}

type PlannedAdmission struct {
	ProjectID     string
	ResourceClass ResourceClass
}

type LanePlan struct {
	Lane       int
	Candidates []Candidate
	Inflight   []Inflight
	Admissions []PlannedAdmission
}

type Plan struct {
	Epoch           uint64
	GlobalWindow    int
	PoolWindows     map[ResourceClass]int
	Lanes           []LanePlan
	TotalAdmissions int
}

type Windows struct {
	Global int
	Pools  map[ResourceClass]int
}

type Reservation struct {
	AttemptID     string        `json:"attempt_id"`
	ProjectID     string        `json:"project_id"`
	Lane          int           `json:"lane"`
	ResourceClass ResourceClass `json:"resource_class"`
	Candidate     Candidate     `json:"candidate"`
	Confirmed     bool          `json:"confirmed"`
	Epoch         string        `json:"epoch"`
}

type CoordinationStore interface {
	AcquireBalancerLease(context.Context, string, time.Duration) (BalancerLease, error)
	BoundWindows(context.Context, BalancerLease, Windows, float64) (Windows, error)
	PauseAdmissions(context.Context, BalancerLease, int) error
	ListReservations(context.Context, int) ([]Reservation, error)
	RebuildLane(context.Context, BalancerLease, LaneState, time.Duration, time.Duration) error
	Grant(context.Context, BalancerLease, int, []PlannedAdmission) error
	Activate(context.Context, BalancerLease, int, uint64) error
	ReserveNext(context.Context, int, string, time.Duration) (Reservation, bool, error)
	ConfirmReservation(context.Context, Reservation, time.Duration) error
	AbortReservation(context.Context, Reservation, bool) error
}

type Capacity struct {
	Pools map[ResourceClass]int
}

type CapacityProvider interface {
	HealthyCapacity(context.Context) (Capacity, error)
}

type WorkerRegistration struct {
	WorkerID      string
	ExecutorBuild string
	ResourceClass ResourceClass
	Slots         int
	Capabilities  []dsl.Coordinate
	TTL           time.Duration
}

func (registration WorkerRegistration) Validate() error {
	if registration.WorkerID == "" || registration.ExecutorBuild == "" || !registration.ResourceClass.Valid() || registration.Slots < 1 || registration.TTL <= 0 || len(registration.Capabilities) == 0 {
		return fmt.Errorf("worker registration identity, capabilities, slots and TTL are required")
	}
	actual := make(map[dsl.Coordinate]struct{}, len(registration.Capabilities))
	for _, coordinate := range registration.Capabilities {
		if coordinate.Type == "" || coordinate.Version == 0 {
			return fmt.Errorf("worker registration capability is invalid")
		}
		class, routable := BuiltinV1Router().Resolve(coordinate)
		if !routable || class != registration.ResourceClass {
			return fmt.Errorf("worker capability %s@%d does not belong to %s", coordinate.Type, coordinate.Version, registration.ResourceClass)
		}
		actual[coordinate] = struct{}{}
	}
	if len(actual) != len(registration.Capabilities) {
		return fmt.Errorf("worker capabilities contain duplicates")
	}
	required := RequiredCapabilities(registration.ResourceClass)
	if len(actual) != len(required) {
		return fmt.Errorf("worker must provide the complete %s capability set", registration.ResourceClass)
	}
	for _, coordinate := range required {
		if _, exists := actual[coordinate]; !exists {
			return fmt.Errorf("worker is missing required capability %s@%d", coordinate.Type, coordinate.Version)
		}
	}
	return nil
}

type CapacityRegistry interface {
	CapacityProvider
	RegisterWorker(context.Context, WorkerRegistration) error
}

type FixedCapacity Capacity

func (capacity FixedCapacity) HealthyCapacity(context.Context) (Capacity, error) {
	result := Capacity{Pools: make(map[ResourceClass]int, len(capacity.Pools))}
	for class, slots := range capacity.Pools {
		result.Pools[class] = slots
	}
	return result, nil
}

type Settings struct {
	LaneCount            int
	CreditGrantBatch     int
	CandidateBatch       int
	AdmissionConcurrency int
	Epoch                time.Duration
	ActiveProjectTTL     time.Duration
	BalancerLease        time.Duration
	ReservationTTL       time.Duration
	DispatchBufferFactor float64
	CapacityChangeLimit  float64
}

func (settings Settings) Validate() error {
	if settings.LaneCount <= 0 || settings.LaneCount&(settings.LaneCount-1) != 0 || settings.CreditGrantBatch <= 0 || settings.CandidateBatch <= 0 || settings.AdmissionConcurrency <= 0 || settings.Epoch <= 0 || settings.ActiveProjectTTL <= settings.Epoch || settings.BalancerLease <= settings.Epoch || settings.ReservationTTL <= 0 || settings.DispatchBufferFactor < 1 || settings.CapacityChangeLimit <= 0 || settings.CapacityChangeLimit > 1 {
		return fmt.Errorf("scheduler settings are invalid")
	}
	return nil
}

func BoundWindows(previous, desired Windows, limit float64) (Windows, error) {
	if desired.Global < 0 || limit <= 0 || limit > 1 {
		return Windows{}, fmt.Errorf("capacity windows and change limit are invalid")
	}
	for class, value := range desired.Pools {
		if !class.Valid() || value < 0 {
			return Windows{}, fmt.Errorf("pool dispatch window is invalid")
		}
	}
	if previous.Pools == nil {
		return Windows{Global: desired.Global, Pools: clonePoolWindows(desired.Pools)}, nil
	}
	result := Windows{Global: boundWindow(previous.Global, desired.Global, limit), Pools: make(map[ResourceClass]int, len(desired.Pools))}
	poolTotal := 0
	for class, desiredValue := range desired.Pools {
		value := boundWindow(previous.Pools[class], desiredValue, limit)
		result.Pools[class] = value
		poolTotal += value
	}
	result.Global = min(result.Global, poolTotal)
	return result, nil
}

func boundWindow(previous, desired int, limit float64) int {
	if previous <= 0 || desired <= previous {
		return desired
	}
	delta := max(1, int(math.Ceil(float64(previous)*limit)))
	return min(desired, previous+delta)
}

func LaneFor(projectID string, laneCount int) (int, error) {
	if projectID == "" || laneCount <= 0 || laneCount&(laneCount-1) != 0 {
		return 0, fmt.Errorf("project identity and power-of-two lane count are required")
	}
	digest := fnv.New64a()
	_, _ = digest.Write([]byte(projectID))
	return int(digest.Sum64() & uint64(laneCount-1)), nil
}

func DispatchWindows(capacity Capacity, factor float64) (int, map[ResourceClass]int, error) {
	if factor < 1 {
		return 0, nil, fmt.Errorf("dispatch buffer factor must be at least one")
	}
	pools := make(map[ResourceClass]int, len(capacity.Pools))
	totalSlots := 0
	for class, slots := range capacity.Pools {
		if !class.Valid() || slots < 0 {
			return 0, nil, fmt.Errorf("healthy pool capacity is invalid")
		}
		window := int(math.Ceil(float64(slots) * factor))
		pools[class] = window
		totalSlots += slots
	}
	return int(math.Ceil(float64(totalSlots) * factor)), pools, nil
}

func sortCandidates(values []Candidate) {
	sort.Slice(values, func(left, right int) bool {
		if values[left].ProjectID != values[right].ProjectID {
			return values[left].ProjectID < values[right].ProjectID
		}
		if values[left].Priority != values[right].Priority {
			return values[left].Priority > values[right].Priority
		}
		if !values[left].ReadyAt.Equal(values[right].ReadyAt) {
			return values[left].ReadyAt.Before(values[right].ReadyAt)
		}
		return values[left].NodeRunID < values[right].NodeRunID
	})
}
