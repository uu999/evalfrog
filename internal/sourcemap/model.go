// Package sourcemap owns the immutable bridge from runtime DSL coordinates to
// author-facing IR coordinates.
package sourcemap

import "github.com/uu999/evalfrog/internal/dsl"

const VersionV1 = "1"

type Document struct {
	SourceMapVersion string                           `json:"source_map_version"`
	Nodes            map[dsl.NodeID]string            `json:"nodes"`
	Edges            map[dsl.EdgeID]string            `json:"edges"`
	Fields           map[dsl.NodeID]map[string]string `json:"fields"`
}

type Issue struct {
	Code    string
	Message string
	NodeID  dsl.NodeID
	EdgeID  dsl.EdgeID
	Field   string
}

// RequiredFields contains only fields for which Runtime may report a precise
// dsl_field. Compiler-generated policy or fixed defaults intentionally fall
// back to the node mapping instead of inventing an author IR path.
type RequiredFields map[dsl.NodeID][]string
