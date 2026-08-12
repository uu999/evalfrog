// Package access owns external principals, project permissions, and project
// execution identities. Roles are configuration; domain code authorizes only
// stable permissions.
package access

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
)

type PrincipalKind string

const (
	PrincipalUser           PrincipalKind = "user"
	PrincipalServiceAccount PrincipalKind = "service_account"
)

type Permission string

const (
	PermissionWorkflowRead    Permission = "workflow.read"
	PermissionWorkflowWrite   Permission = "workflow.write"
	PermissionWorkflowTest    Permission = "workflow.test"
	PermissionWorkflowPublish Permission = "workflow.publish"
	PermissionRunCreate       Permission = "run.create"
	PermissionRunRead         Permission = "run.read"
	PermissionRunCancel       Permission = "run.cancel"
	PermissionConnectionUse   Permission = "connection.use"
	PermissionServiceUse      Permission = "service.use"
	PermissionProjectAdmin    Permission = "project.admin"
)

var (
	ErrUnauthenticated  = errors.New("unauthenticated")
	ErrPermissionDenied = errors.New("permission denied")
	ErrResourceNotFound = errors.New("resource not found")
)

type Principal struct {
	ID   string
	Kind PrincipalKind
	Name string
}

type ExecutionIdentity struct {
	ID        string
	ProjectID string
}

type Repository interface {
	FindPrincipalByCredentialHash(context.Context, [sha256.Size]byte) (Principal, error)
	HasPermission(context.Context, string, string, Permission) (bool, error)
	FindExecutionIdentity(context.Context, string) (ExecutionIdentity, error)
}

type Service struct {
	repository Repository
}

func NewService(repository Repository) Service {
	return Service{repository: repository}
}

func (service Service) Authenticate(ctx context.Context, bearerToken string) (Principal, error) {
	if len(bearerToken) < 16 || len(bearerToken) > 4096 || strings.TrimSpace(bearerToken) != bearerToken {
		return Principal{}, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(bearerToken))
	principal, err := service.repository.FindPrincipalByCredentialHash(ctx, digest)
	if err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return Principal{}, ErrUnauthenticated
		}
		return Principal{}, fmt.Errorf("authenticate principal: %w", err)
	}
	if principal.ID == "" || (principal.Kind != PrincipalUser && principal.Kind != PrincipalServiceAccount) {
		return Principal{}, fmt.Errorf("authenticate principal: repository returned an invalid principal")
	}
	return principal, nil
}

func (service Service) Require(ctx context.Context, projectID, principalID string, permission Permission) error {
	allowed, err := service.repository.HasPermission(ctx, projectID, principalID, permission)
	if err != nil {
		return fmt.Errorf("authorize %s: %w", permission, err)
	}
	if !allowed {
		return ErrPermissionDenied
	}
	return nil
}

func (service Service) ExecutionIdentity(ctx context.Context, projectID string) (ExecutionIdentity, error) {
	identity, err := service.repository.FindExecutionIdentity(ctx, projectID)
	if err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return ExecutionIdentity{}, ErrResourceNotFound
		}
		return ExecutionIdentity{}, fmt.Errorf("load project execution identity: %w", err)
	}
	if identity.ID == "" || identity.ProjectID != projectID {
		return ExecutionIdentity{}, fmt.Errorf("load project execution identity: repository returned an invalid identity")
	}
	return identity, nil
}
