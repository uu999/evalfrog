package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output}).Run(context.Background(), []string{"version"})
	if code != 0 || !strings.Contains(output.String(), "version") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output}).Run(context.Background(), []string{"config", "validate", "--profile", "local", "--config-dir", directory})
	if code != 0 || !strings.Contains(output.String(), "configuration valid") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}
