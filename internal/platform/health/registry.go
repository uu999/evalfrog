package health

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Check func(context.Context) error

type Result struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

type Report struct {
	Status string   `json:"status"`
	Checks []Result `json:"checks"`
}

type Registry struct {
	mu      sync.RWMutex
	timeout time.Duration
	checks  map[string]Check
}

func New(timeout time.Duration) *Registry {
	return &Registry{timeout: timeout, checks: make(map[string]Check)}
}

func (registry *Registry) Register(name string, check Check) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if name == "" || check == nil {
		return fmt.Errorf("health check name and function are required")
	}
	if _, exists := registry.checks[name]; exists {
		return fmt.Errorf("health check %q already registered", name)
	}
	registry.checks[name] = check
	return nil
}

func (registry *Registry) Check(ctx context.Context) Report {
	registry.mu.RLock()
	names := make([]string, 0, len(registry.checks))
	checks := make(map[string]Check, len(registry.checks))
	for name, check := range registry.checks {
		names = append(names, name)
		checks[name] = check
	}
	registry.mu.RUnlock()
	sort.Strings(names)
	report := Report{Status: "ok", Checks: make([]Result, 0, len(names))}
	for _, name := range names {
		checkCtx, cancel := context.WithTimeout(ctx, registry.timeout)
		err := checks[name](checkCtx)
		cancel()
		result := Result{Name: name, Healthy: err == nil}
		if err != nil {
			result.Error = err.Error()
			report.Status = "unavailable"
		}
		report.Checks = append(report.Checks, result)
	}
	return report
}
