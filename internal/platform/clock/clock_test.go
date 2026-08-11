package clock

import (
	"testing"
	"time"
)

func TestFakeAdvance(t *testing.T) {
	t.Parallel()
	initial := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	clock := NewFake(initial)
	clock.Advance(5 * time.Second)
	want := initial.UTC().Add(5 * time.Second)
	if !clock.Now().Equal(want) {
		t.Fatalf("Now()=%s, want %s", clock.Now(), want)
	}
}
