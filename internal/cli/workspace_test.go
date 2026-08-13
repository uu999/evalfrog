package cli

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestWorkspaceRoundTripUsesProjectScopedOpaquePath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	value := Workspace{Server: "http://control", ProjectID: "project-a", WorkflowID: "workflow-a", Revision: 2, IR: json.RawMessage(`{"ir_version":"1"}`)}
	if err := saveWorkspace(root, value); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadWorkspace(root, value.ProjectID, value.WorkflowID)
	var got, want any
	_ = json.Unmarshal(loaded.IR, &got)
	_ = json.Unmarshal(value.IR, &want)
	if err != nil || loaded.Revision != 2 || !reflect.DeepEqual(got, want) {
		t.Fatalf("workspace=%+v err=%v", loaded, err)
	}
	if _, err = os.Stat(workspacePath(root, "project-b", value.WorkflowID)); !os.IsNotExist(err) {
		t.Fatalf("workspace path leaked across projects: %v", err)
	}
}

func TestWorkspaceRejectsInvalidAuthorityShape(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := saveWorkspace(root, Workspace{ProjectID: "project", WorkflowID: "workflow", Revision: 0, IR: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("invalid revision accepted")
	}
}
