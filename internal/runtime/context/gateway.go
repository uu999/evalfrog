// Package context owns read-only, cache-aside execution-context assembly for
// workers. PostgreSQL remains authoritative; cache failures only reduce speed.
package context

import (
	stdcontext "context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type LoadCommand struct {
	ProjectID, RunID, AttemptID, LeaseToken string
	AttemptSequence                         uint32
	FencingToken                            uint64
}

type Metadata struct {
	ProjectID, RunID, NodeRunID, ExecutionNodeID, AttemptID, SnapshotID string
	AttemptSequence                                                     uint32
	Operation                                                           dsl.Coordinate
	ResourceClass                                                       scheduling.ResourceClass
	ResolvedInputs                                                      map[string]json.RawMessage
}

type ExecutionContext struct {
	ContextVersion   int                           `json:"context_version"`
	ProjectID        string                        `json:"project_id"`
	RunID            string                        `json:"run_id"`
	NodeRunID        string                        `json:"node_run_id"`
	ExecutionNodeID  string                        `json:"execution_node_id"`
	AttemptID        string                        `json:"attempt_id"`
	AttemptSequence  uint32                        `json:"attempt_sequence"`
	LeaseToken       string                        `json:"-"`
	FencingToken     uint64                        `json:"-"`
	SnapshotID       string                        `json:"snapshot_id"`
	ResourceClass    scheduling.ResourceClass      `json:"resource_class"`
	Operation        dsl.Operation                 `json:"operation"`
	ExecutionPolicy  dsl.ExecutionPolicy           `json:"execution_policy"`
	OutputContract   map[dsl.PortName]dsl.DataType `json:"output_contract"`
	WorkflowInput    json.RawMessage               `json:"workflow_input"`
	Inputs           map[string]json.RawMessage    `json:"inputs"`
	UpstreamOutputs  map[string]json.RawMessage    `json:"upstream_outputs"`
	ResourceMaterial map[string]json.RawMessage    `json:"resource_material"`
}

type Repository interface {
	LoadAttemptMetadata(stdcontext.Context, LoadCommand) (Metadata, error)
	LoadSnapshotDSL(stdcontext.Context, string, string) (json.RawMessage, error)
	LoadRunInput(stdcontext.Context, string, string) (json.RawMessage, error)
	LoadEffectiveOutput(stdcontext.Context, string, string, string) (json.RawMessage, error)
}

type Cache interface {
	GetSnapshot(stdcontext.Context, string) (json.RawMessage, bool)
	PutSnapshot(stdcontext.Context, string, json.RawMessage, time.Duration)
	GetRunInput(stdcontext.Context, string) (json.RawMessage, bool)
	PutRunInput(stdcontext.Context, string, json.RawMessage, time.Duration)
	GetEffectiveOutput(stdcontext.Context, string, string) (json.RawMessage, bool)
	PutEffectiveOutput(stdcontext.Context, string, string, json.RawMessage, time.Duration)
}

// CacheMetrics is deliberately a small outbound port. It records only
// bounded cache-part names and hit/miss outcomes, never run or tenant IDs.
type CacheMetrics interface {
	ObserveExecutionContextCache(kind, outcome string)
}

type Gateway struct {
	repository  Repository
	cache       Cache
	snapshotTTL time.Duration
	runTTL      time.Duration
	metrics     CacheMetrics
}

func NewGateway(repository Repository, cache Cache, snapshotTTL, runTTL time.Duration, cacheMetrics ...CacheMetrics) (*Gateway, error) {
	if repository == nil || cache == nil || snapshotTTL <= 0 || runTTL <= 0 {
		return nil, fmt.Errorf("execution context repository, cache and TTLs are required")
	}
	if len(cacheMetrics) > 1 {
		return nil, fmt.Errorf("at most one execution context cache metrics observer is allowed")
	}
	var metric CacheMetrics
	if len(cacheMetrics) == 1 {
		metric = cacheMetrics[0]
	}
	return &Gateway{repository: repository, cache: cache, snapshotTTL: snapshotTTL, runTTL: runTTL, metrics: metric}, nil
}

func (gateway *Gateway) Load(ctx stdcontext.Context, command LoadCommand) (ExecutionContext, error) {
	if command.ProjectID == "" || command.RunID == "" || command.AttemptID == "" || command.AttemptSequence == 0 || command.LeaseToken == "" || command.FencingToken == 0 {
		return ExecutionContext{}, fmt.Errorf("execution context attempt coordinate and lease are required")
	}
	metadata, err := gateway.repository.LoadAttemptMetadata(ctx, command)
	if err != nil {
		return ExecutionContext{}, err
	}
	snapshotJSON, ok := gateway.cache.GetSnapshot(ctx, metadata.SnapshotID)
	node, cacheErr := executionNode(snapshotJSON, metadata)
	if !ok || cacheErr != nil {
		gateway.observeCache("snapshot", "miss")
		snapshotJSON, err = gateway.repository.LoadSnapshotDSL(ctx, metadata.ProjectID, metadata.SnapshotID)
		if err != nil {
			return ExecutionContext{}, err
		}
		node, err = executionNode(snapshotJSON, metadata)
		if err != nil {
			return ExecutionContext{}, err
		}
		gateway.cache.PutSnapshot(ctx, metadata.SnapshotID, snapshotJSON, gateway.snapshotTTL)
	} else {
		gateway.observeCache("snapshot", "hit")
	}
	workflowInput, ok := gateway.cache.GetRunInput(ctx, metadata.RunID)
	if !ok || !json.Valid(workflowInput) {
		gateway.observeCache("run_input", "miss")
		workflowInput, err = gateway.repository.LoadRunInput(ctx, metadata.ProjectID, metadata.RunID)
		if err != nil {
			return ExecutionContext{}, err
		}
		if !json.Valid(workflowInput) {
			return ExecutionContext{}, fmt.Errorf("authoritative workflow input is not valid JSON")
		}
		gateway.cache.PutRunInput(ctx, metadata.RunID, workflowInput, gateway.runTTL)
	} else {
		gateway.observeCache("run_input", "hit")
	}
	upstream := make(map[string]json.RawMessage)
	for _, binding := range node.Inputs {
		if binding.Kind != dsl.BindingNodeOutput || binding.Output == nil {
			continue
		}
		upstreamID := string(binding.Output.NodeID)
		if _, exists := upstream[upstreamID]; exists {
			continue
		}
		value, hit := gateway.cache.GetEffectiveOutput(ctx, metadata.RunID, upstreamID)
		if !hit || !json.Valid(value) {
			gateway.observeCache("effective_output", "miss")
			value, err = gateway.repository.LoadEffectiveOutput(ctx, metadata.ProjectID, metadata.RunID, upstreamID)
			if err != nil {
				return ExecutionContext{}, err
			}
			if !json.Valid(value) {
				return ExecutionContext{}, fmt.Errorf("authoritative output for node %q is not valid JSON", upstreamID)
			}
			gateway.cache.PutEffectiveOutput(ctx, metadata.RunID, upstreamID, value, gateway.runTTL)
		} else {
			gateway.observeCache("effective_output", "hit")
		}
		upstream[upstreamID] = cloneRaw(value)
	}
	return ExecutionContext{
		ContextVersion: 1, ProjectID: metadata.ProjectID, RunID: metadata.RunID,
		NodeRunID: metadata.NodeRunID, ExecutionNodeID: metadata.ExecutionNodeID,
		AttemptID: metadata.AttemptID, AttemptSequence: metadata.AttemptSequence,
		LeaseToken: command.LeaseToken, FencingToken: command.FencingToken,
		SnapshotID: metadata.SnapshotID, ResourceClass: metadata.ResourceClass,
		Operation: node.Operation, ExecutionPolicy: node.ExecutionPolicy, OutputContract: node.Outputs,
		WorkflowInput: cloneRaw(workflowInput), Inputs: cloneMap(metadata.ResolvedInputs),
		UpstreamOutputs: upstream, ResourceMaterial: map[string]json.RawMessage{},
	}, nil
}

func (gateway *Gateway) observeCache(kind, outcome string) {
	if gateway.metrics != nil {
		gateway.metrics.ObserveExecutionContextCache(kind, outcome)
	}
}

func executionNode(snapshotJSON json.RawMessage, metadata Metadata) (*dsl.Node, error) {
	var document dsl.Document
	if err := json.Unmarshal(snapshotJSON, &document); err != nil {
		return nil, fmt.Errorf("decode execution snapshot DSL: %w", err)
	}
	for index := range document.Nodes {
		node := &document.Nodes[index]
		if string(node.ID) != metadata.ExecutionNodeID {
			continue
		}
		if node.Operation.Coordinate() != metadata.Operation {
			return nil, fmt.Errorf("execution snapshot and persisted node coordinate disagree")
		}
		return node, nil
	}
	return nil, fmt.Errorf("execution snapshot and persisted node coordinate disagree")
}

func cloneRaw(value json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), value...) }

func cloneMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		result[key] = cloneRaw(value)
	}
	return result
}
