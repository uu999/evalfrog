//go:build integration

package sandbox

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

func TestDockerSandboxIntegration(t *testing.T) {
	profile := domainsandbox.DefaultProfile("evalfrog-sandbox-python:test", sandboxIntegrationRuntime(t))
	orchestrator, err := NewDockerOrchestrator("docker", profile)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-success", SourceCode: "def main(inputs):\n    return {'total': sum(inputs['items'])}", Inputs: map[string]json.RawMessage{"items": json.RawMessage(`[1,2,3]`)}})
	if err != nil || result.Failure != nil || string(result.Outputs) != `{"total":6}` {
		t.Fatalf("success result=%#v err=%v", result, err)
	}
	result, err = orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-forbidden", SourceCode: "import socket\ndef main(inputs):\n    return {}", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure == nil || result.Failure.Code != "CODE_IMPORT_FORBIDDEN" {
		t.Fatalf("forbidden result=%#v err=%v", result, err)
	}
	result, err = orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-filesystem", SourceCode: "def main(inputs):\n    return open('/tmp/x', 'w')", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure == nil || result.Failure.Code != "CODE_RUNTIME_ERROR" || result.Failure.Details["source_line"] != float64(2) {
		t.Fatalf("filesystem result=%#v err=%v", result, err)
	}
	profile.ExecutionTimeout = 50 * time.Millisecond
	orchestrator, err = NewDockerOrchestrator("docker", profile)
	if err != nil {
		t.Fatal(err)
	}
	result, err = orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-timeout", SourceCode: "def main(inputs):\n    while True:\n        pass", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure == nil || result.Failure.Code != "SANDBOX_EXECUTION_TIMEOUT" {
		t.Fatalf("timeout result=%#v err=%v", result, err)
	}
}

// sandboxIntegrationRuntime defaults to runc for local protocol coverage.
// The dedicated release gate sets it to runsc and therefore executes the
// identical contract against the hardened OCI runtime rather than treating a
// mock or an argument assertion as runsc evidence.
func sandboxIntegrationRuntime(t *testing.T) string {
	t.Helper()
	runtime := os.Getenv("EVALFROG_SANDBOX_INTEGRATION_RUNTIME")
	if runtime == "" {
		return "runc"
	}
	if runtime != "runc" && runtime != "runsc" {
		t.Fatalf("unsupported integration sandbox runtime %q", runtime)
	}
	return runtime
}
