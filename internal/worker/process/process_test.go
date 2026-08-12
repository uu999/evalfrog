package process

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkerProfilesValidateWithoutDatabaseAccess(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	for _, resourceClass := range []string{"builtin", "sandbox"} {
		resourceClass := resourceClass
		t.Run(resourceClass, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			code := RunProcess(context.Background(), []string{"--profile", "local", "--config-dir", directory, "--check-config"}, resourceClass, &output, &output)
			if code != 0 || !strings.Contains(output.String(), "configuration valid") {
				t.Fatalf("code=%d output=%q", code, output.String())
			}
		})
	}
}
