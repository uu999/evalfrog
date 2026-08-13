package projection

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

type repositoryStub struct {
	value RunView
	calls int
}

func (stub *repositoryStub) GetDiagnosticView(_ context.Context, projectID, runID string) (DiagnosticView, error) {
	view, err := stub.GetRunView(context.Background(), projectID, runID)
	return DiagnosticView{Run: view}, err
}

func (stub *repositoryStub) GetRunView(_ context.Context, _, _ string) (RunView, error) {
	stub.calls++
	return stub.value, nil
}

type accessStub struct {
	calls int
	err   error
}

func (stub *accessStub) Require(context.Context, string, string, access.Permission) error {
	stub.calls++
	return stub.err
}

type cacheStub struct {
	value json.RawMessage
	hit   bool
	puts  int
}

func (stub *cacheStub) GetRunView(context.Context, string) (json.RawMessage, bool) {
	return stub.value, stub.hit
}
func (stub *cacheStub) PutRunView(_ context.Context, _ string, value json.RawMessage, _ time.Duration) {
	stub.puts++
	stub.value, stub.hit = append(json.RawMessage(nil), value...), true
}
func (*cacheStub) DeleteRunView(context.Context, string) {}

func TestCachedServiceRecoversFromLostProjectionByReadingAuthority(t *testing.T) {
	repository := &repositoryStub{value: RunView{RunID: "run-1", ProjectID: "project-1", State: runtime.RunRunning}}
	authorizer := &accessStub{}
	cache := &cacheStub{} // Redis loss/miss: no cached payload is available.
	service := NewCachedService(NewBuiltinService(repository, authorizer), cache, time.Minute, time.Hour)
	result, err := service.GetRun(context.Background(), "project-1", "principal-1", "run-1")
	if err != nil || result.State != runtime.RunRunning || repository.calls != 1 || cache.puts != 1 {
		t.Fatalf("result=%+v err=%v repository=%d puts=%d", result, err, repository.calls, cache.puts)
	}
	// A fresh cache entry may be used on the next read, but permission remains
	// authoritative and is evaluated again rather than cached.
	_, err = service.GetRun(context.Background(), "project-1", "principal-1", "run-1")
	if err != nil || repository.calls != 1 || authorizer.calls != 2 {
		t.Fatalf("err=%v repository=%d authorization=%d", err, repository.calls, authorizer.calls)
	}
}

func TestCachedServiceNeverLeaksCachedRunAcrossProjectOrPermission(t *testing.T) {
	cache := &cacheStub{hit: true, value: json.RawMessage(`{"run_id":"run-1","project_id":"project-a"}`)}
	repository := &repositoryStub{value: RunView{RunID: "run-1", ProjectID: "project-b"}}
	authorizer := &accessStub{err: access.ErrPermissionDenied}
	service := NewCachedService(NewBuiltinService(repository, authorizer), cache, time.Minute, time.Hour)
	if _, err := service.GetRun(context.Background(), "project-b", "principal-1", "run-1"); !errors.Is(err, access.ErrPermissionDenied) || repository.calls != 0 {
		t.Fatalf("err=%v authority reads=%d", err, repository.calls)
	}
}

func TestLocateFailureUsesImmutableSourceMapForNodeEdgeAndField(t *testing.T) {
	location := LocateFailure(sourcemap.Document{
		Nodes: map[dsl.NodeID]string{"op-1": "code_to_str"}, Edges: map[dsl.EdgeID]string{"edge-1": "start_to_code"},
		Fields: map[dsl.NodeID]map[string]string{"op-1": {"inputs.value": "/nodes/code_to_str/inputs/value"}},
	}, &runtime.Failure{ExecutionNodeID: "op-1", ExecutionEdgeID: "edge-1", DSLField: "inputs.value", Details: map[string]any{"actual": "string"}})
	if location.LogicalNodeID != "code_to_str" || location.LogicalEdgeID != "start_to_code" || location.IRField != "/nodes/code_to_str/inputs/value" || location.Details["actual"] != "string" {
		t.Fatalf("location=%+v", location)
	}
}
