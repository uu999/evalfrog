package sourcemap_test

import (
	"testing"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

func TestResolveUsesExactFieldAndFallsBackToNode(t *testing.T) {
	nodeID := dsl.NodeID("xn_000000000000000000000000")
	edgeID := dsl.EdgeID("xe_000000000000000000000000")
	document := sourcemap.Document{
		SourceMapVersion: sourcemap.VersionV1,
		Nodes:            map[dsl.NodeID]string{nodeID: "prepare_order"},
		Edges:            map[dsl.EdgeID]string{edgeID: "prepare_to_end"},
		Fields:           map[dsl.NodeID]map[string]string{nodeID: {"operation.config.source_code": "inputs.source_code"}},
	}
	precise := document.Resolve(nodeID, edgeID, "operation.config.source_code")
	if precise.LogicalNodeID != "prepare_order" || precise.LogicalEdgeID != "prepare_to_end" || precise.IRField != "inputs.source_code" {
		t.Fatalf("unexpected precise mapping: %+v", precise)
	}
	fallback := document.Resolve(nodeID, "", "execution_policy.attempt_timeout_ms")
	if fallback.LogicalNodeID != "prepare_order" || fallback.IRField != "" {
		t.Fatalf("field fallback lost node mapping: %+v", fallback)
	}
}
