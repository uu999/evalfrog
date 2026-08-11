package identity

import (
	"testing"

	"github.com/google/uuid"
)

func TestUUIDv7Generator(t *testing.T) {
	t.Parallel()
	value, err := (UUIDv7Generator{}).New()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Version() != 7 {
		t.Fatalf("version=%d, want 7", parsed.Version())
	}
}
