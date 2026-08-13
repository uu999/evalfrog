package code

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/sandbox"
)

type fakeOrchestrator struct {
	result  sandbox.Result
	err     error
	request sandbox.Request
}

func (fake *fakeOrchestrator) Run(_ context.Context, request sandbox.Request) (sandbox.Result, error) {
	fake.request = request
	return fake.result, fake.err
}
func (*fakeOrchestrator) Cleanup(context.Context, string) error { return nil }

func TestExecutorPacksInputsAndValidatesDeclaredOutput(t *testing.T) {
	orchestrator := &fakeOrchestrator{result: sandbox.Result{Outputs: json.RawMessage(`{"total":3}`), Telemetry: sandbox.Telemetry{Duration: time.Millisecond}}}
	executor := NewExecutor(orchestrator, nil)
	result := executor.Execute(context.Background(), executionContext())
	if result.State != platformruntime.AttemptSucceeded || string(result.Outputs["total"]) != "3" {
		t.Fatalf("result = %#v", result)
	}
	if string(orchestrator.request.Inputs["items"]) != "[1,2]" || orchestrator.request.SourceCode == "" {
		t.Fatalf("request = %#v", orchestrator.request)
	}
}

func TestExecutorMapsSandboxFailuresToSourceField(t *testing.T) {
	orchestrator := &fakeOrchestrator{result: sandbox.Result{Failure: &sandbox.Failure{Code: "CODE_RUNTIME_ERROR", Message: "bad", Details: map[string]any{"source_line": float64(4), "source_column": float64(2)}}}}
	result := NewExecutor(orchestrator, nil).Execute(context.Background(), executionContext())
	if result.ErrorCode != "CODE_RUNTIME_ERROR" || result.DSLField != sandbox.SourceField || result.ErrorDetails["source_line"] != 4 {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecutorRejectsOutputContractViolations(t *testing.T) {
	tests := []struct{ name, output, code string }{
		{"non object", `[]`, "CODE_OUTPUT_NOT_OBJECT"}, {"missing", `{}`, "CODE_OUTPUT_MISSING"},
		{"invalid JSON", `not-json`, "CODE_OUTPUT_NOT_OBJECT"}, {"multiple JSON values", `{"total":3} {}`, "CODE_OUTPUT_NOT_OBJECT"},
		{"extra", `{"total":3,"extra":true}`, "CODE_OUTPUT_UNDECLARED"}, {"wrong type", `{"total":"3"}`, "OUTPUT_TYPE_MISMATCH"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := NewExecutor(&fakeOrchestrator{result: sandbox.Result{Outputs: json.RawMessage(test.output)}}, nil).Execute(context.Background(), executionContext())
			if result.ErrorCode != test.code || result.DSLField != sandbox.SourceField {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestValidateOutputsSupportsEveryJSONTopLevelType(t *testing.T) {
	tests := []struct {
		name, output string
		dataType     dsl.DataType
		ok           bool
	}{
		{"string", `{"value":"x"}`, dsl.TypeString, true}, {"boolean", `{"value":true}`, dsl.TypeBoolean, true},
		{"array", `{"value":[]}`, dsl.TypeArray, true}, {"object", `{"value":{}}`, dsl.TypeObject, true},
		{"number", `{"value":1.5}`, dsl.TypeNumber, true}, {"integer", `{"value":3}`, dsl.TypeInteger, true},
		{"integer rejects fraction", `{"value":1.5}`, dsl.TypeInteger, false}, {"unknown type", `{"value":3}`, dsl.DataType("unknown"), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outputs, failure := validateOutputs(json.RawMessage(test.output), map[dsl.PortName]dsl.DataType{"value": test.dataType})
			if test.ok && (failure != nil || len(outputs) != 1) {
				t.Fatalf("outputs=%#v failure=%#v", outputs, failure)
			}
			if !test.ok && (failure == nil || failure.ErrorCode != "OUTPUT_TYPE_MISMATCH") {
				t.Fatalf("outputs=%#v failure=%#v", outputs, failure)
			}
		})
	}
}

func TestExecutorDoesNotClaimModelArtifactsOrOrchestratorErrors(t *testing.T) {
	execContext := executionContext()
	execContext.Operation.Config["model_artifact"] = json.RawMessage(`{"id":"model","digest":"x"}`)
	result := NewExecutor(&fakeOrchestrator{}, nil).Execute(context.Background(), execContext)
	if result.ErrorCode != "MODEL_ARTIFACT_UNAVAILABLE" {
		t.Fatalf("result = %#v", result)
	}
	execContext = executionContext()
	result = NewExecutor(&fakeOrchestrator{err: errors.New("docker unavailable")}, nil).Execute(context.Background(), execContext)
	if result.ErrorCode != "SANDBOX_RUNTIME_UNAVAILABLE" {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecutorClassifiesSandboxTimeoutAndCancellation(t *testing.T) {
	for _, test := range []struct {
		code, state string
	}{
		{"SANDBOX_EXECUTION_TIMEOUT", string(platformruntime.AttemptTimedOut)},
		{"SANDBOX_CANCELED", string(platformruntime.AttemptCanceled)},
	} {
		t.Run(test.code, func(t *testing.T) {
			result := NewExecutor(&fakeOrchestrator{result: sandbox.Result{Failure: &sandbox.Failure{Code: test.code, Message: "sandbox stopped"}}}, nil).Execute(context.Background(), executionContext())
			if string(result.State) != test.state || result.ErrorCode != test.code {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

type fakeTelemetry struct{ outcome string }

func (telemetry *fakeTelemetry) Record(_ sandbox.Telemetry, outcome string) {
	telemetry.outcome = outcome
}

func TestExecutorRecordsTelemetryForSuccessAndFailure(t *testing.T) {
	telemetry := &fakeTelemetry{}
	NewExecutor(&fakeOrchestrator{result: sandbox.Result{Outputs: json.RawMessage(`{"total":3}`)}}, telemetry).Execute(context.Background(), executionContext())
	if telemetry.outcome != "succeeded" {
		t.Fatalf("outcome=%q", telemetry.outcome)
	}
	NewExecutor(&fakeOrchestrator{err: errors.New("unavailable")}, telemetry).Execute(context.Background(), executionContext())
	if telemetry.outcome != "failed" {
		t.Fatalf("outcome=%q", telemetry.outcome)
	}
}

func executionContext() runtimecontext.ExecutionContext {
	return runtimecontext.ExecutionContext{AttemptID: "attempt", Operation: dsl.Operation{Type: "task.python", Version: 1, Config: map[string]json.RawMessage{"source_code": json.RawMessage(`"def main(inputs): return {}"`)}}, Inputs: map[string]json.RawMessage{"items": json.RawMessage(`[1,2]`)}, OutputContract: map[dsl.PortName]dsl.DataType{"total": dsl.TypeInteger}}
}
