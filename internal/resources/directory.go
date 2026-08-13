package resources

import (
	"context"
	"fmt"

	"github.com/uu999/evalfrog/internal/access"
)

// ConnectionSummary is safe authoring metadata. It intentionally excludes the
// endpoint and secret reference: authors select stable connection_ref values,
// while only Workers resolve the protected runtime material.
type ConnectionSummary struct {
	ID        string `json:"connection_id"`
	Reference string `json:"reference"`
	Enabled   bool   `json:"enabled"`
	Revision  uint64 `json:"resource_revision"`
}

type ConnectionDirectoryRepository interface {
	ListConnections(context.Context, string) ([]ConnectionSummary, error)
}

type ConnectionDirectory struct {
	repository ConnectionDirectoryRepository
	authorizer Authorizer
}

func NewConnectionDirectory(repository ConnectionDirectoryRepository, authorizer Authorizer) (ConnectionDirectory, error) {
	if repository == nil || authorizer == nil {
		return ConnectionDirectory{}, fmt.Errorf("connection directory dependencies are required")
	}
	return ConnectionDirectory{repository: repository, authorizer: authorizer}, nil
}

func NewBuiltinConnectionDirectory(repository ConnectionDirectoryRepository, authorizer Authorizer) ConnectionDirectory {
	value, err := NewConnectionDirectory(repository, authorizer)
	if err != nil {
		panic(err)
	}
	return value
}

func (directory ConnectionDirectory) List(ctx context.Context, projectID, principalID string) ([]ConnectionSummary, error) {
	if err := directory.authorizer.Require(ctx, projectID, principalID, access.PermissionConnectionUse); err != nil {
		return nil, err
	}
	if projectID == "" {
		return nil, ErrResourceNotFound
	}
	return directory.repository.ListConnections(ctx, projectID)
}
