// Package code coordinates per-Attempt Python execution through a sandbox
// port. It deliberately never evaluates customer source in the Worker process.
package code

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"

	"github.com/uu999/evalfrog/internal/dsl"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/sandbox"
)

type Telemetry interface {
	Record(sandbox.Telemetry, string)
}

type Executor struct {
	Orchestrator sandbox.Orchestrator
	Telemetry    Telemetry
}

func NewExecutor(orchestrator sandbox.Orchestrator, telemetry Telemetry) Executor {
	return Executor{Orchestrator: orchestrator, Telemetry: telemetry}
}

func (executor Executor) Coordinate() dsl.Coordinate {
	return dsl.Coordinate{Type: "task.python", Version: 1}
}

func (executor Executor) Execute(ctx context.Context, value runtimecontext.ExecutionContext) platformruntime.AttemptResult {
	if executor.Orchestrator == nil {
		return failure("SANDBOX_ORCHESTRATOR_UNAVAILABLE", "sandbox orchestrator is unavailable", nil)
	}
	source, ok := stringConfig(value.Operation.Config, "source_code")
	if !ok {
		return failure("CODE_SYNTAX_ERROR", "Python source_code is unavailable", nil)
	}
	if _, hasModel := value.Operation.Config["model_artifact"]; hasModel {
		return failure("MODEL_ARTIFACT_UNAVAILABLE", "approved model artifact injection is not available", nil)
	}
	result, err := executor.Orchestrator.Run(ctx, sandbox.Request{AttemptID: value.AttemptID, SourceCode: source, Inputs: cloneInputs(value.Inputs)})
	if executor.Telemetry != nil {
		outcome := "succeeded"
		if err != nil || result.Failure != nil {
			outcome = "failed"
		}
		executor.Telemetry.Record(result.Telemetry, outcome)
	}
	if err != nil {
		return failure("SANDBOX_RUNTIME_UNAVAILABLE", "sandbox orchestrator could not execute the attempt", nil)
	}
	if result.Failure != nil {
		return failure(result.Failure.Code, result.Failure.Message, result.Failure.Details)
	}
	outputs, resultFailure := validateOutputs(result.Outputs, value.OutputContract)
	if resultFailure != nil {
		return *resultFailure
	}
	return platformruntime.AttemptResult{State: platformruntime.AttemptSucceeded, Outputs: outputs}
}

func cloneInputs(values map[string]json.RawMessage) map[string]json.RawMessage {
	cloned := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		cloned[key] = append(json.RawMessage(nil), value...)
	}
	return cloned
}

func stringConfig(values map[string]json.RawMessage, key string) (string, bool) {
	var value string
	raw, exists := values[key]
	return value, exists && json.Unmarshal(raw, &value) == nil && value != ""
}

func validateOutputs(raw json.RawMessage, contract map[dsl.PortName]dsl.DataType) (map[string]json.RawMessage, *platformruntime.AttemptResult) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		result := failure("CODE_OUTPUT_NOT_OBJECT", "sandbox output is not valid JSON", nil)
		return nil, &result
	}
	object, ok := value.(map[string]any)
	if !ok {
		result := failure("CODE_OUTPUT_NOT_OBJECT", "main(inputs) must return a JSON object", nil)
		return nil, &result
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		result := failure("CODE_OUTPUT_NOT_OBJECT", "sandbox returned more than one JSON value", nil)
		return nil, &result
	}
	outputs := make(map[string]json.RawMessage, len(object))
	for name, dataType := range contract {
		candidate, exists := object[string(name)]
		if !exists {
			result := failure("CODE_OUTPUT_MISSING", fmt.Sprintf("declared output %q is missing", name), map[string]any{"output": name})
			return nil, &result
		}
		if !matches(dataType, candidate) {
			result := failure("OUTPUT_TYPE_MISMATCH", fmt.Sprintf("declared output %q has the wrong JSON type", name), map[string]any{"output": name, "expected": dataType})
			return nil, &result
		}
		encoded, _ := json.Marshal(candidate)
		outputs[string(name)] = encoded
	}
	for name := range object {
		if _, declared := contract[dsl.PortName(name)]; !declared {
			result := failure("CODE_OUTPUT_UNDECLARED", fmt.Sprintf("sandbox returned undeclared output %q", name), map[string]any{"output": name})
			return nil, &result
		}
	}
	return outputs, nil
}

func matches(dataType dsl.DataType, value any) bool {
	switch dataType {
	case dsl.TypeString:
		_, ok := value.(string)
		return ok
	case dsl.TypeBoolean:
		_, ok := value.(bool)
		return ok
	case dsl.TypeArray:
		_, ok := value.([]any)
		return ok
	case dsl.TypeObject:
		_, ok := value.(map[string]any)
		return ok
	case dsl.TypeNumber:
		_, ok := value.(json.Number)
		return ok
	case dsl.TypeInteger:
		number, ok := value.(json.Number)
		if !ok {
			return false
		}
		rational, ok := new(big.Rat).SetString(number.String())
		return ok && rational.IsInt()
	default:
		return false
	}
}

func failure(code, message string, details map[string]any) platformruntime.AttemptResult {
	state := platformruntime.AttemptFailed
	if code == "SANDBOX_EXECUTION_TIMEOUT" {
		state = platformruntime.AttemptTimedOut
	}
	if code == "SANDBOX_CANCELED" {
		state = platformruntime.AttemptCanceled
	}
	result := platformruntime.AttemptResult{State: state, ErrorCode: code, Message: message, DSLField: sandbox.SourceField}
	line, hasLine := positiveInteger(details["source_line"])
	if hasLine {
		result.ErrorDetails = map[string]any{"source_line": line}
		if column, validColumn := positiveInteger(details["source_column"]); validColumn {
			result.ErrorDetails["source_column"] = column
		}
	}
	return result
}

func positiveInteger(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, typed > 0
	case int64:
		return int(typed), typed > 0 && int64(int(typed)) == typed
	case float64:
		return int(typed), typed > 0 && typed == float64(int(typed))
	case json.Number:
		parsed, err := typed.Int64()
		return int(parsed), err == nil && parsed > 0 && int64(int(parsed)) == parsed
	default:
		return 0, false
	}
}
