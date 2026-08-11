package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckConfig(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := run(context.Background(), []string{"--profile", "local", "--config-dir", directory, "--check-config"}, &output, &output)
	if code != 0 || !strings.Contains(output.String(), "configuration valid") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}
