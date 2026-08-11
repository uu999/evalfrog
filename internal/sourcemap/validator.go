package sourcemap

import (
	"regexp"
	"sort"

	"github.com/uu999/evalfrog/internal/dsl"
)

var logicalIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func Validate(document Document, runtime dsl.Document, required RequiredFields) []Issue {
	issues := make([]Issue, 0)
	if document.SourceMapVersion != VersionV1 {
		issues = append(issues, Issue{Code: "SOURCE_MAP_VERSION_UNSUPPORTED", Message: "source_map_version must be 1"})
	}
	nodes := make(map[dsl.NodeID]struct{}, len(runtime.Nodes))
	for _, node := range runtime.Nodes {
		nodes[node.ID] = struct{}{}
		logicalID, exists := document.Nodes[node.ID]
		if !exists {
			issues = append(issues, Issue{Code: "SOURCE_MAP_NODE_MISSING", Message: "execution node has no IR node mapping", NodeID: node.ID})
		} else if !logicalIDPattern.MatchString(logicalID) {
			issues = append(issues, Issue{Code: "SOURCE_MAP_LOGICAL_NODE_INVALID", Message: "mapped IR node id is invalid", NodeID: node.ID})
		}
		if _, exists := document.Fields[node.ID]; !exists {
			issues = append(issues, Issue{Code: "SOURCE_MAP_FIELDS_MISSING", Message: "execution node has no field mapping object", NodeID: node.ID})
		}
	}
	for nodeID := range document.Nodes {
		if _, exists := nodes[nodeID]; !exists {
			issues = append(issues, Issue{Code: "SOURCE_MAP_NODE_ORPHAN", Message: "node mapping has no DSL node", NodeID: nodeID})
		}
	}
	edges := make(map[dsl.EdgeID]struct{}, len(runtime.Edges))
	for _, edge := range runtime.Edges {
		edges[edge.ID] = struct{}{}
		logicalID, exists := document.Edges[edge.ID]
		if !exists {
			issues = append(issues, Issue{Code: "SOURCE_MAP_EDGE_MISSING", Message: "execution edge has no IR edge mapping", EdgeID: edge.ID})
		} else if !logicalIDPattern.MatchString(logicalID) {
			issues = append(issues, Issue{Code: "SOURCE_MAP_LOGICAL_EDGE_INVALID", Message: "mapped IR edge id is invalid", EdgeID: edge.ID})
		}
	}
	for edgeID := range document.Edges {
		if _, exists := edges[edgeID]; !exists {
			issues = append(issues, Issue{Code: "SOURCE_MAP_EDGE_ORPHAN", Message: "edge mapping has no DSL edge", EdgeID: edgeID})
		}
	}
	for nodeID, fields := range document.Fields {
		if _, exists := nodes[nodeID]; !exists {
			issues = append(issues, Issue{Code: "SOURCE_MAP_FIELDS_ORPHAN", Message: "field mapping has no DSL node", NodeID: nodeID})
		}
		for dslField, irField := range fields {
			if dslField == "" || irField == "" {
				issues = append(issues, Issue{Code: "SOURCE_MAP_FIELD_INVALID", Message: "field mapping paths must be non-empty", NodeID: nodeID, Field: dslField})
			}
		}
	}
	for nodeID, fields := range required {
		mapping := document.Fields[nodeID]
		for _, field := range fields {
			if _, exists := mapping[field]; !exists {
				issues = append(issues, Issue{Code: "SOURCE_MAP_FIELD_MISSING", Message: "reportable DSL field has no IR field mapping", NodeID: nodeID, Field: field})
			}
		}
	}
	sort.SliceStable(issues, func(left, right int) bool {
		if issues[left].NodeID != issues[right].NodeID {
			return issues[left].NodeID < issues[right].NodeID
		}
		if issues[left].EdgeID != issues[right].EdgeID {
			return issues[left].EdgeID < issues[right].EdgeID
		}
		if issues[left].Field != issues[right].Field {
			return issues[left].Field < issues[right].Field
		}
		return issues[left].Code < issues[right].Code
	})
	return issues
}
