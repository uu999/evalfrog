package architecture

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRepositoryArchitecture(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	graph, err := LoadGraph(root)
	if err != nil {
		t.Fatal(err)
	}
	violations := append(Validate(graph), ValidateLayout(root)...)
	if len(violations) != 0 {
		messages := make([]string, len(violations))
		for index, violation := range violations {
			messages[index] = violation.Error()
		}
		t.Fatalf("architecture violations:\n%s", strings.Join(messages, "\n"))
	}
}

func TestRulesRejectForbiddenImports(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		from   string
		to     string
		reason string
	}{
		{"domain adapter", "internal/definition", "internal/adapters/postgres", "domain modules"},
		{"worker database", "internal/worker/runtime", "internal/adapters/postgres", "workers must not"},
		{"runtime authoring", "internal/runtime/engine", "internal/ir", "authoring models"},
		{"scheduler engine", "internal/scheduling", "internal/runtime/engine", "control semantics"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			graph := Graph{ModulePath + "/" + test.from: {ModulePath + "/" + test.to}}
			violations := Validate(graph)
			if len(violations) == 0 || !strings.Contains(violations[0].Reason, test.reason) {
				t.Fatalf("expected %q violation, got %+v", test.reason, violations)
			}
		})
	}
}
