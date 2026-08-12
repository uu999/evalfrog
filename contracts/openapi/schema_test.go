package openapi

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestM3ExternalContractHasNoExecutableArtifactUpload(t *testing.T) {
	raw, err := os.ReadFile("v1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	paths, ok := document["paths"].(map[string]any)
	if !ok || len(paths) != 8 {
		t.Fatalf("unexpected path contract: %#v", document["paths"])
	}
	for path := range paths {
		lower := strings.ToLower(path)
		if strings.Contains(lower, "/dsl") || strings.Contains(lower, "source-map") || strings.Contains(lower, "source_map") {
			t.Fatalf("forbidden executable artifact upload path: %s", path)
		}
	}
	for _, required := range []string{
		"/v1/projects/{project_id}/workflows",
		"/v1/projects/{project_id}/workflows/{workflow_id}/draft",
		"/v1/projects/{project_id}/workflows/{workflow_id}/publish",
	} {
		if _, exists := paths[required]; !exists {
			t.Fatalf("required path %s is missing", required)
		}
	}
}

func TestM7WorkerContractContainsOnlyInternalCoordinationSurface(t *testing.T) {
	raw, err := os.ReadFile("worker-v1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	paths, ok := document["paths"].(map[string]any)
	if !ok || len(paths) != 5 {
		t.Fatalf("unexpected worker paths: %#v", document["paths"])
	}
	for path := range paths {
		if !strings.HasPrefix(path, "/internal/v1/") {
			t.Fatalf("worker operation escaped internal boundary: %s", path)
		}
	}
}
