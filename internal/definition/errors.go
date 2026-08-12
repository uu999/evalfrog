package definition

import (
	"errors"
	"fmt"
)

const (
	CodeInvalidArgument       = "INVALID_ARGUMENT"
	CodeResourceNotFound      = "RESOURCE_NOT_FOUND"
	CodePermissionDenied      = "PERMISSION_DENIED"
	CodeDraftRevisionConflict = "DRAFT_REVISION_CONFLICT"
	CodeIdempotencyKeyReused  = "IDEMPOTENCY_KEY_REUSED"
	CodeWorkflowNotPublished  = "WORKFLOW_NOT_PUBLISHED"
	CodeCatalogUnavailable    = "CATALOG_REVISION_UNAVAILABLE"
)

var (
	ErrResourceNotFound      = errors.New("resource not found")
	ErrDraftRevisionConflict = errors.New("draft revision conflict")
	ErrIdempotencyKeyReused  = errors.New("idempotency key reused")
	ErrWorkflowNotPublished  = errors.New("workflow not published")
)

type Error struct {
	Code    string
	Message string
	Details map[string]any
	Cause   error
}

func (value *Error) Error() string {
	if value.Message != "" {
		return value.Message
	}
	return value.Code
}

func (value *Error) Unwrap() error { return value.Cause }

func wrapError(code, message string, cause error, details map[string]any) error {
	return &Error{Code: code, Message: message, Cause: cause, Details: details}
}

func repositoryError(operation string, err error) error {
	switch {
	case errors.Is(err, ErrResourceNotFound):
		return wrapError(CodeResourceNotFound, "resource was not found", err, nil)
	case errors.Is(err, ErrDraftRevisionConflict):
		return wrapError(CodeDraftRevisionConflict, "draft revision has changed", err, nil)
	case errors.Is(err, ErrIdempotencyKeyReused):
		return wrapError(CodeIdempotencyKeyReused, "idempotency key was reused with a different request", err, nil)
	case errors.Is(err, ErrWorkflowNotPublished):
		return wrapError(CodeWorkflowNotPublished, "workflow has no active published version", err, nil)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}
