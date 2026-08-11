package sourcemap

import "github.com/uu999/evalfrog/internal/dsl"

type Location struct {
	LogicalNodeID string `json:"logical_node_id,omitempty"`
	LogicalEdgeID string `json:"logical_edge_id,omitempty"`
	IRField       string `json:"ir_field,omitempty"`
}

// Resolve enriches a runtime coordinate. A missing precise field mapping is
// intentionally not an error: callers retain the node mapping and highlight
// the whole IR node, which is the frozen fallback behavior.
func (document Document) Resolve(nodeID dsl.NodeID, edgeID dsl.EdgeID, dslField string) Location {
	location := Location{LogicalNodeID: document.Nodes[nodeID], LogicalEdgeID: document.Edges[edgeID]}
	if fields := document.Fields[nodeID]; fields != nil {
		location.IRField = fields[dslField]
	}
	return location
}
