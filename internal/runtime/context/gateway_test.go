package context

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestGatewayCacheAsideHitMissFallbackAndRefill(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{"run":true}`), output: json.RawMessage(`{"value":7}`)}
	cache := newFakeCache()
	gateway, err := NewGateway(repository, cache, time.Hour, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	command := loadFixture()
	first, err := gateway.Load(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if repository.snapshotReads != 1 || repository.runReads != 1 || repository.outputReads != 1 || len(first.UpstreamOutputs) != 2 {
		t.Fatalf("reads=%d/%d/%d context=%+v", repository.snapshotReads, repository.runReads, repository.outputReads, first)
	}
	if string(first.UpstreamOutputs["start"]) != `{"run":true}` || string(first.UpstreamOutputs["upstream"]) != `{"value":7}` {
		t.Fatalf("upstream outputs=%s", first.UpstreamOutputs)
	}
	second, err := gateway.Load(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if repository.snapshotReads != 1 || repository.runReads != 1 || repository.outputReads != 1 || string(second.WorkflowInput) != `{"run":true}` {
		t.Fatalf("cache hit still read PostgreSQL")
	}

	// A cache timeout is represented by a miss at this failure-isolated port.
	// The gateway remains correct and refills best effort after PostgreSQL load.
	cache.failReads = true
	if _, err = gateway.Load(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if repository.snapshotReads != 2 || repository.runReads != 2 || repository.outputReads != 2 {
		t.Fatalf("fallback reads=%d/%d/%d", repository.snapshotReads, repository.runReads, repository.outputReads)
	}
}

func TestGatewayRejectsInvalidOrInconsistentAuthority(t *testing.T) {
	if _, err := NewGateway(nil, newFakeCache(), time.Second, time.Second); err == nil {
		t.Fatal("nil repository accepted")
	}
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{}`), output: json.RawMessage(`{}`)}
	gateway, _ := NewGateway(repository, newFakeCache(), time.Second, time.Second)
	if _, err := gateway.Load(context.Background(), LoadCommand{}); err == nil {
		t.Fatal("invalid load accepted")
	}
	repository.metadata.Operation.Type = "task.rpc"
	if _, err := gateway.Load(context.Background(), loadFixture()); err == nil {
		t.Fatal("snapshot mismatch accepted")
	}
	repository.metadata.Operation.Type = "task.python"
	repository.snapshot = json.RawMessage(`{"bad":`)
	gateway, _ = NewGateway(repository, newFakeCache(), time.Second, time.Second)
	if _, err := gateway.Load(context.Background(), loadFixture()); err == nil {
		t.Fatal("bad snapshot accepted")
	}
	repository.snapshot = snapshotFixture()
	repository.err = errors.New("postgres unavailable")
	if _, err := gateway.Load(context.Background(), loadFixture()); err == nil {
		t.Fatal("repository error hidden")
	}
}

func TestGatewayTreatsCorruptDerivedCacheAsMiss(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{"run":true}`), output: json.RawMessage(`{"value":7}`)}
	cache := newFakeCache()
	cache.snapshots["snapshot"] = json.RawMessage(`{"bad":`)
	cache.runs["run"] = json.RawMessage(`{"bad":`)
	cache.outputs["runupstream"] = json.RawMessage(`{"bad":`)
	gateway, err := NewGateway(repository, cache, time.Hour, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gateway.Load(context.Background(), loadFixture()); err != nil {
		t.Fatal(err)
	}
	if repository.snapshotReads != 1 || repository.runReads != 1 || repository.outputReads != 1 {
		t.Fatalf("corrupt cache did not fall back: reads=%d/%d/%d", repository.snapshotReads, repository.runReads, repository.outputReads)
	}
}

func TestGatewayResolvesEntryNodeWorkflowInputWithoutOutputLookup(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: entryRefSnapshot(), runInput: json.RawMessage(`{"n":5}`)}
	gateway, err := NewGateway(repository, newFakeCache(), time.Hour, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	value, err := gateway.Load(context.Background(), loadFixture())
	if err != nil {
		t.Fatal(err)
	}
	if repository.outputReads != 0 {
		t.Fatalf("entry node input performed %d effective output reads", repository.outputReads)
	}
	if string(value.WorkflowInput) != `{"n":5}` || string(value.UpstreamOutputs["start"]) != `{"n":5}` || len(value.UpstreamOutputs) != 1 {
		t.Fatalf("context=%+v", value)
	}
}

func TestGatewayRecordsBoundedCacheHitAndMissMetrics(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{"run":true}`), output: json.RawMessage(`{"value":7}`)}
	metrics := &fakeCacheMetrics{accesses: map[string]int{}}
	gateway, err := NewGateway(repository, newFakeCache(), time.Hour, time.Minute, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gateway.Load(context.Background(), loadFixture()); err != nil {
		t.Fatal(err)
	}
	if _, err = gateway.Load(context.Background(), loadFixture()); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"snapshot:miss", "run_input:miss", "effective_output:miss", "snapshot:hit", "run_input:hit", "effective_output:hit"} {
		if metrics.accesses[key] != 1 {
			t.Fatalf("cache access %s=%d, want 1; all=%v", key, metrics.accesses[key], metrics.accesses)
		}
	}
}

func TestGatewayRejectsInvalidAuthoritativeContextParts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeRepository)
	}{
		{name: "snapshot read", mutate: func(value *fakeRepository) { value.snapshotErr = errors.New("snapshot unavailable") }},
		{name: "run input read", mutate: func(value *fakeRepository) { value.runErr = errors.New("run unavailable") }},
		{name: "invalid run input", mutate: func(value *fakeRepository) { value.runInput = json.RawMessage(`{"bad":`) }},
		{name: "output read", mutate: func(value *fakeRepository) { value.outputErr = errors.New("output unavailable") }},
		{name: "invalid output", mutate: func(value *fakeRepository) { value.output = json.RawMessage(`{"bad":`) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{}`), output: json.RawMessage(`{}`)}
			test.mutate(repository)
			gateway, err := NewGateway(repository, newFakeCache(), time.Second, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = gateway.Load(context.Background(), loadFixture()); err == nil {
				t.Fatal("invalid authoritative context part accepted")
			}
		})
	}
}

func TestExecutionNodeSearchesAndValidatesCoordinate(t *testing.T) {
	metadata := metadataFixture()
	document := dsl.Document{Nodes: []dsl.Node{
		{ID: "other", Operation: dsl.Operation{Type: "task.python", Version: 1}},
		{ID: "code", Operation: dsl.Operation{Type: "task.python", Version: 1}},
	}}
	raw, _ := json.Marshal(document)
	if node, err := executionNode(raw, metadata); err != nil || node.ID != "code" {
		t.Fatalf("node=%+v err=%v", node, err)
	}
	metadata.ExecutionNodeID = "missing"
	if _, err := executionNode(raw, metadata); err == nil {
		t.Fatal("missing execution node accepted")
	}
}

type fakeRepository struct {
	metadata                             Metadata
	snapshot, runInput, output           json.RawMessage
	err                                  error
	snapshotErr, runErr, outputErr       error
	snapshotReads, runReads, outputReads int
}

func (value *fakeRepository) LoadAttemptMetadata(context.Context, LoadCommand) (Metadata, error) {
	return value.metadata, value.err
}
func (value *fakeRepository) LoadSnapshotDSL(context.Context, string, string) (json.RawMessage, error) {
	value.snapshotReads++
	if value.snapshotErr != nil {
		return nil, value.snapshotErr
	}
	return value.snapshot, value.err
}
func (value *fakeRepository) LoadRunInput(context.Context, string, string) (json.RawMessage, error) {
	value.runReads++
	if value.runErr != nil {
		return nil, value.runErr
	}
	return value.runInput, value.err
}
func (value *fakeRepository) LoadEffectiveOutput(context.Context, string, string, string) (json.RawMessage, error) {
	value.outputReads++
	if value.outputErr != nil {
		return nil, value.outputErr
	}
	return value.output, value.err
}

type fakeCache struct {
	snapshots, runs map[string]json.RawMessage
	outputs         map[string]json.RawMessage
	failReads       bool
}

type fakeCacheMetrics struct{ accesses map[string]int }

func (metrics *fakeCacheMetrics) ObserveExecutionContextCache(kind, outcome string) {
	metrics.accesses[kind+":"+outcome]++
}

func newFakeCache() *fakeCache {
	return &fakeCache{snapshots: map[string]json.RawMessage{}, runs: map[string]json.RawMessage{}, outputs: map[string]json.RawMessage{}}
}
func (value *fakeCache) GetSnapshot(_ context.Context, id string) (json.RawMessage, bool) {
	result, ok := value.snapshots[id]
	return result, ok && !value.failReads
}
func (value *fakeCache) PutSnapshot(_ context.Context, id string, raw json.RawMessage, _ time.Duration) {
	value.snapshots[id] = append([]byte(nil), raw...)
}
func (value *fakeCache) GetRunInput(_ context.Context, id string) (json.RawMessage, bool) {
	result, ok := value.runs[id]
	return result, ok && !value.failReads
}
func (value *fakeCache) PutRunInput(_ context.Context, id string, raw json.RawMessage, _ time.Duration) {
	value.runs[id] = append([]byte(nil), raw...)
}
func (value *fakeCache) GetEffectiveOutput(_ context.Context, run, node string) (json.RawMessage, bool) {
	result, ok := value.outputs[run+node]
	return result, ok && !value.failReads
}
func (value *fakeCache) PutEffectiveOutput(_ context.Context, run, node string, raw json.RawMessage, _ time.Duration) {
	value.outputs[run+node] = append([]byte(nil), raw...)
}

func loadFixture() LoadCommand {
	return LoadCommand{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, LeaseToken: "lease", FencingToken: 1}
}
func metadataFixture() Metadata {
	return Metadata{ProjectID: "project", RunID: "run", NodeRunID: "node-run", ExecutionNodeID: "code", AttemptID: "attempt", AttemptSequence: 1, SnapshotID: "snapshot", Operation: dsl.Coordinate{Type: "task.python", Version: 1}, ResourceClass: scheduling.ResourceSandbox, ResolvedInputs: map[string]json.RawMessage{"result": json.RawMessage(`7`)}}
}
func snapshotFixture() json.RawMessage {
	document := dsl.Document{DSLVersion: "1", EntryNodeID: "start", ExitNodeID: "end", Nodes: []dsl.Node{
		{ID: "start", Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.start", Version: 1}, Outputs: map[dsl.PortName]dsl.DataType{"workflow_input": dsl.TypeObject}},
		{ID: "upstream", Kind: dsl.KindTask, Operation: dsl.Operation{Type: "task.python", Version: 1}, Outputs: map[dsl.PortName]dsl.DataType{"result": dsl.TypeInteger}},
		{ID: "code", Kind: dsl.KindTask, Operation: dsl.Operation{Type: "task.python", Version: 1, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{
			"source":   {Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject, Output: &dsl.OutputReference{NodeID: "start", Name: "workflow_input"}},
			"previous": {Kind: dsl.BindingNodeOutput, DataType: dsl.TypeInteger, Output: &dsl.OutputReference{NodeID: "upstream", Name: "result"}},
		}, Outputs: map[dsl.PortName]dsl.DataType{"result": dsl.TypeInteger}, ExecutionPolicy: dsl.ExecutionPolicy{AttemptTimeoutMS: 1000}},
	}}
	raw, _ := json.Marshal(document)
	return raw
}

func entryRefSnapshot() json.RawMessage {
	document := dsl.Document{DSLVersion: "1", EntryNodeID: "start", ExitNodeID: "end", Nodes: []dsl.Node{
		{ID: "start", Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.start", Version: 1}, Outputs: map[dsl.PortName]dsl.DataType{"workflow_input": dsl.TypeObject}},
		{ID: "code", Kind: dsl.KindTask, Operation: dsl.Operation{Type: "task.python", Version: 1, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{
			"source": {Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject, Output: &dsl.OutputReference{NodeID: "start", Name: "workflow_input"}},
		}, Outputs: map[dsl.PortName]dsl.DataType{"result": dsl.TypeInteger}, ExecutionPolicy: dsl.ExecutionPolicy{AttemptTimeoutMS: 1000}},
	}}
	raw, _ := json.Marshal(document)
	return raw
}
