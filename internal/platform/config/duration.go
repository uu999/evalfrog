package config

import (
	"fmt"
	"time"
)

// Duration keeps configuration human-readable while preserving time.Duration semantics.
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	value, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", text, err)
	}
	*d = Duration(value)
	return nil
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}
