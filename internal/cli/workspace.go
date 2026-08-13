package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Workspace is a local editing convenience, never Definition authority. It
// records the last pulled/saved immutable Draft Revision so Push can make its
// optimistic-concurrency expectation explicit.
type Workspace struct {
	Server     string          `json:"server"`
	ProjectID  string          `json:"project_id"`
	WorkflowID string          `json:"workflow_id"`
	Revision   int64           `json:"revision"`
	IR         json.RawMessage `json:"ir"`
}

func workspacePath(root, projectID, workflowID string) string {
	digest := sha256.Sum256([]byte(projectID + "\x00" + workflowID))
	return filepath.Join(root, "workspaces", hex.EncodeToString(digest[:])+".json")
}

func loadWorkspace(root, projectID, workflowID string) (Workspace, error) {
	path := workspacePath(root, projectID, workflowID)
	raw, err := os.ReadFile(path)
	if err != nil {
		return Workspace{}, err
	}
	var value Workspace
	if err = json.Unmarshal(raw, &value); err != nil {
		return Workspace{}, fmt.Errorf("decode workspace: %w", err)
	}
	if value.ProjectID != projectID || value.WorkflowID != workflowID || value.Revision < 1 || !json.Valid(value.IR) {
		return Workspace{}, fmt.Errorf("workspace identity or IR is invalid")
	}
	return value, nil
}

func saveWorkspace(root string, value Workspace) error {
	if value.ProjectID == "" || value.WorkflowID == "" || value.Revision < 1 || !json.Valid(value.IR) {
		return fmt.Errorf("workspace identity, revision and IR are required")
	}
	path := workspacePath(root, value.ProjectID, value.WorkflowID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err = os.WriteFile(temporary, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}
