package logging

import (
	"bytes"
	"strings"
	"testing"
)

func TestJSONLoggerIncludesService(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger, err := New(&output, "evalfrog-test", "info")
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("ready")
	if value := output.String(); !strings.Contains(value, `"service":"evalfrog-test"`) || !strings.Contains(value, `"msg":"ready"`) {
		t.Fatalf("unexpected log: %s", value)
	}
}

func TestLoggerRejectsUnknownLevel(t *testing.T) {
	t.Parallel()
	if _, err := New(&bytes.Buffer{}, "test", "verbose"); err == nil {
		t.Fatal("expected invalid level error")
	}
}
