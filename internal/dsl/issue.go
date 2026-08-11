package dsl

import "sort"

type Issue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	NodeID  NodeID `json:"execution_node_id,omitempty"`
	EdgeID  EdgeID `json:"execution_edge_id,omitempty"`
	Field   string `json:"dsl_field,omitempty"`
}

func sortIssues(issues []Issue) {
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
}

func HasIssues(issues []Issue) bool {
	return len(issues) != 0
}
