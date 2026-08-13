package sandbox

import (
	"context"
	"encoding/json"
	"time"
)

const (
	ProfilePythonV1 = "python-sandbox-v1"
	DefaultImage    = "evalfrog-sandbox-python:dev"
)

// Profile is platform-owned. It is never taken from IR, DSL input, or an
// Agent request. Deployment may choose the hardened OCI runtime and image, but
// cannot loosen the fixed v1 resource envelope through a workflow definition.
type Profile struct {
	Name             string
	Image, Runtime   string
	CPU              string
	MemoryBytes      int64
	ProcessLimit     int
	TemporaryBytes   int64
	OutputBytes      int64
	ExecutionTimeout time.Duration
	CleanupTimeout   time.Duration
}

func DefaultProfile(image, runtime string) Profile {
	if image == "" {
		image = DefaultImage
	}
	return Profile{
		Name: ProfilePythonV1, Image: image, Runtime: runtime, CPU: "0.50",
		MemoryBytes: 128 << 20, ProcessLimit: 32, TemporaryBytes: 16 << 20,
		OutputBytes: 1 << 20, ExecutionTimeout: 30 * time.Second, CleanupTimeout: 5 * time.Second,
	}
}

func (profile Profile) Valid() bool {
	return profile.Name == ProfilePythonV1 && profile.Image != "" && profile.Runtime != "" && profile.CPU != "" &&
		profile.MemoryBytes > 0 && profile.ProcessLimit > 0 && profile.TemporaryBytes > 0 && profile.OutputBytes > 0 && profile.ExecutionTimeout > 0 && profile.CleanupTimeout > 0
}

type Request struct {
	AttemptID  string
	SourceCode string
	Inputs     map[string]json.RawMessage
}

type Telemetry struct {
	Runtime, ContainerID string
	Duration             time.Duration
}

type Result struct {
	Outputs json.RawMessage
	Failure *Failure
	Telemetry
}

type Failure struct {
	Code, Message string
	Details       map[string]any
}

// Orchestrator is a narrow port. The Code Executor sends only customer source
// and resolved JSON inputs to it; no database, Redis, Kafka, secret, network,
// or host filesystem credential crosses this boundary.
type Orchestrator interface {
	Run(context.Context, Request) (Result, error)
	Cleanup(context.Context, string) error
}

// OrphanSweeper is optional because a non-container orchestrator can implement
// cleanup differently. The Docker adapter uses it after worker startup.
type OrphanSweeper interface {
	Sweep(context.Context) error
}
