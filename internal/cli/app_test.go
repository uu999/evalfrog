package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output}).Run(context.Background(), []string{"version"})
	if code != 0 || !strings.Contains(output.String(), "version") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestNodeTypeListUsesExternalAPISurface(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/node-types" || request.Header.Get("Authorization") != "Bearer token-value" {
			t.Fatalf("request=%s %s auth=%q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
		}
		_, _ = writer.Write([]byte(`{"node_types":[]}`))
	}))
	defer server.Close()
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output, Home: t.TempDir()}).Run(context.Background(), []string{"node-type", "list", "--server", server.URL, "--token", "token-value", "--project", "project-id"})
	if code != 0 || !strings.Contains(output.String(), "node_types") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestDraftPushReportsOptimisticConflict(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	project, workflow := "project-id", "workflow-id"
	if err := saveWorkspace(root, Workspace{ProjectID: project, WorkflowID: workflow, Revision: 1, IR: json.RawMessage(`{"ir_version":"1"}`)}); err != nil {
		t.Fatal(err)
	}
	irFile := root + "/draft.json"
	if err := os.WriteFile(irFile, []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"DRAFT_REVISION_CONFLICT","message":"changed","details":{}}}`))
	}))
	defer server.Close()
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output, Home: root}).Run(context.Background(), []string{"draft", "push", "--server", server.URL, "--token", "token-value", "--project", project, "--workflow", workflow, "--ir", irFile, "--idempotency-key", "draft-push-0001"})
	if code != 1 || !strings.Contains(output.String(), "draft revision conflict") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output}).Run(context.Background(), []string{"config", "validate", "--profile", "local", "--config-dir", directory})
	if code != 0 || !strings.Contains(output.String(), "configuration valid") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}
