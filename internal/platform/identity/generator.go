package identity

import (
	"fmt"

	"github.com/google/uuid"
)

type Generator interface {
	New() (string, error)
}

type UUIDv7Generator struct{}

func (UUIDv7Generator) New() (string, error) {
	value, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate UUIDv7: %w", err)
	}
	return value.String(), nil
}
