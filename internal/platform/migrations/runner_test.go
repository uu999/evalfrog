package migrations

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadPlanSortsAndChecksums(t *testing.T) {
	t.Parallel()
	plan, err := LoadPlan(fstest.MapFS{
		"000002_second.up.sql": {Data: []byte("SELECT 2;")},
		"000001_first.up.sql":  {Data: []byte("SELECT 1;")},
		"README.md":            {Data: []byte("ignored")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 2 || plan[0].Version != 1 || plan[1].Version != 2 || len(plan[0].Checksum) != 64 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}

func TestLoadPlanRejectsDuplicateVersion(t *testing.T) {
	t.Parallel()
	_, err := LoadPlan(fstest.MapFS{
		"000001_first.up.sql": {Data: []byte("SELECT 1;")},
		"000001_other.up.sql": {Data: []byte("SELECT 2;")},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}
