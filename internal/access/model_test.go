package access

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

type repositoryStub struct {
	principal Principal
	allowed   map[Permission]bool
	identity  ExecutionIdentity
}

func (stub repositoryStub) FindPrincipalByCredentialHash(_ context.Context, digest [sha256.Size]byte) (Principal, error) {
	if digest != sha256.Sum256([]byte("0123456789abcdef")) {
		return Principal{}, ErrResourceNotFound
	}
	return stub.principal, nil
}

func (stub repositoryStub) HasPermission(_ context.Context, projectID, principalID string, permission Permission) (bool, error) {
	return projectID == "project-a" && principalID == stub.principal.ID && stub.allowed[permission], nil
}

func (stub repositoryStub) FindExecutionIdentity(_ context.Context, projectID string) (ExecutionIdentity, error) {
	if projectID != stub.identity.ProjectID {
		return ExecutionIdentity{}, ErrResourceNotFound
	}
	return stub.identity, nil
}

func TestUserAndServiceAccountUseSamePermissionChecks(t *testing.T) {
	for _, kind := range []PrincipalKind{PrincipalUser, PrincipalServiceAccount} {
		repository := repositoryStub{
			principal: Principal{ID: "principal-a", Kind: kind},
			allowed:   map[Permission]bool{PermissionWorkflowWrite: true},
			identity:  ExecutionIdentity{ID: "execution-a", ProjectID: "project-a"},
		}
		service := NewService(repository)
		principal, err := service.Authenticate(context.Background(), "0123456789abcdef")
		if err != nil || principal.Kind != kind {
			t.Fatalf("kind %s authentication failed: %+v %v", kind, principal, err)
		}
		if err := service.Require(context.Background(), "project-a", principal.ID, PermissionWorkflowWrite); err != nil {
			t.Fatalf("kind %s authorization failed: %v", kind, err)
		}
		if err := service.Require(context.Background(), "project-a", principal.ID, PermissionWorkflowPublish); !errors.Is(err, ErrPermissionDenied) {
			t.Fatalf("kind %s missing permission error = %v", kind, err)
		}
	}
}

func TestAuthenticationFailsClosed(t *testing.T) {
	service := NewService(repositoryStub{principal: Principal{ID: "principal-a", Kind: PrincipalUser}})
	for _, token := range []string{"short", " 0123456789abcdef", "fedcba9876543210"} {
		if _, err := service.Authenticate(context.Background(), token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("token %q error = %v", token, err)
		}
	}
}
