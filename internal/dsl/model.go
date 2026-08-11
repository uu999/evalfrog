// Package dsl owns EvalFrog's immutable runtime workflow representation.
//
// The package deliberately does not import the author-facing IR model. Runtime
// components can therefore consume DSL snapshots without learning about Drafts,
// layouts, logical IDs, or node descriptions.
package dsl

import "encoding/json"

const VersionV1 = "1"

type NodeID string
type EdgeID string
type PortName string
type RouteName string

type DataType string

const (
	TypeString  DataType = "string"
	TypeInteger DataType = "integer"
	TypeNumber  DataType = "number"
	TypeBoolean DataType = "boolean"
	TypeObject  DataType = "object"
	TypeArray   DataType = "array"
)

func (dataType DataType) Valid() bool {
	switch dataType {
	case TypeString, TypeInteger, TypeNumber, TypeBoolean, TypeObject, TypeArray:
		return true
	default:
		return false
	}
}

func compatible(source, target DataType) bool {
	return source == target || source == TypeInteger && target == TypeNumber
}

type Document struct {
	DSLVersion  string `json:"dsl_version"`
	EntryNodeID NodeID `json:"entry_node_id"`
	ExitNodeID  NodeID `json:"exit_node_id"`
	Nodes       []Node `json:"nodes"`
	Edges       []Edge `json:"edges"`
}

type NodeKind string

const (
	KindControl NodeKind = "control"
	KindTask    NodeKind = "task"
)

func (kind NodeKind) Valid() bool {
	return kind == KindControl || kind == KindTask
}

type Node struct {
	ID              NodeID                    `json:"id"`
	Kind            NodeKind                  `json:"kind"`
	Operation       Operation                 `json:"operation"`
	Inputs          map[PortName]InputBinding `json:"inputs"`
	Outputs         map[PortName]DataType     `json:"outputs"`
	ExecutionPolicy ExecutionPolicy           `json:"execution_policy"`
}

type Operation struct {
	Type    string                     `json:"type"`
	Version uint32                     `json:"version"`
	Config  map[string]json.RawMessage `json:"config"`
}

type InputBindingKind string

const (
	BindingLiteral    InputBindingKind = "literal"
	BindingNodeOutput InputBindingKind = "node_output"
)

type InputBinding struct {
	Kind     InputBindingKind `json:"kind"`
	DataType DataType         `json:"data_type"`
	Value    json.RawMessage  `json:"value,omitempty"`
	Output   *OutputReference `json:"output,omitempty"`
}

type OutputReference struct {
	NodeID NodeID   `json:"node_id"`
	Name   PortName `json:"name"`
}

type Edge struct {
	ID           EdgeID     `json:"id"`
	SourceNodeID NodeID     `json:"source_node_id"`
	TargetNodeID NodeID     `json:"target_node_id"`
	Activation   Activation `json:"activation"`
}

type ActivationKind string

const (
	ActivationAlways ActivationKind = "always"
	ActivationRoute  ActivationKind = "route"
)

type Activation struct {
	Kind  ActivationKind `json:"kind"`
	Route RouteName      `json:"route,omitempty"`
}

// ExecutionPolicy is the final policy resolved by the compiler. Empty policies
// are valid only for Control Nodes, which never create attempts.
type ExecutionPolicy struct {
	MaxAttempts         uint32        `json:"max_attempts,omitempty"`
	MaxRecoveries       uint32        `json:"max_recoveries,omitempty"`
	AttemptTimeoutMS    uint64        `json:"attempt_timeout_ms,omitempty"`
	RetryBackoff        *RetryBackoff `json:"retry_backoff,omitempty"`
	RetryableErrorCodes []string      `json:"retryable_error_codes,omitempty"`
}

type RetryBackoff struct {
	Kind    string `json:"kind"`
	DelayMS uint64 `json:"delay_ms"`
}

type Coordinate struct {
	Type    string
	Version uint32
}

func (operation Operation) Coordinate() Coordinate {
	return Coordinate{Type: operation.Type, Version: operation.Version}
}
