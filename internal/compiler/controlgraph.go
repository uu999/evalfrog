package compiler

import (
	"encoding/json"
	"sort"

	"github.com/uu999/evalfrog/internal/ir"
)

type graphAnalysis struct {
	nodes         map[ir.LogicalID]ir.Node
	nodeIndexes   map[ir.LogicalID]int
	edgeIndexes   map[ir.LogicalID]int
	incoming      map[ir.LogicalID][]ir.Edge
	outgoing      map[ir.LogicalID][]ir.Edge
	topological   []ir.LogicalID
	start         ir.LogicalID
	end           ir.LogicalID
	branchDomains map[ir.LogicalID][]ir.RouteName
	descendants   map[ir.LogicalID][]uint64
	ordinal       map[ir.LogicalID]int
}

func analyzeControlGraph(document ir.Document) (graphAnalysis, []ir.Diagnostic) {
	analysis := graphAnalysis{
		nodes: make(map[ir.LogicalID]ir.Node, len(document.Nodes)), nodeIndexes: make(map[ir.LogicalID]int, len(document.Nodes)),
		edgeIndexes: make(map[ir.LogicalID]int, len(document.Edges)), incoming: make(map[ir.LogicalID][]ir.Edge, len(document.Nodes)),
		outgoing: make(map[ir.LogicalID][]ir.Edge, len(document.Nodes)), branchDomains: make(map[ir.LogicalID][]ir.RouteName),
	}
	diagnostics := make([]ir.Diagnostic, 0)
	starts := make([]ir.LogicalID, 0, 1)
	ends := make([]ir.LogicalID, 0, 1)
	for index, node := range document.Nodes {
		analysis.nodes[node.ID] = node
		analysis.nodeIndexes[node.ID] = index
		switch node.Type {
		case "start":
			starts = append(starts, node.ID)
		case "end":
			ends = append(ends, node.ID)
		}
	}
	if len(starts) != 1 {
		diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "CONTROL_START_COUNT_INVALID", "control graph requires exactly one Start node", ir.Location{IRPath: "/nodes"}))
	} else {
		analysis.start = starts[0]
	}
	if len(ends) != 1 {
		diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "CONTROL_END_COUNT_INVALID", "control graph requires exactly one End node", ir.Location{IRPath: "/nodes"}))
	} else {
		analysis.end = ends[0]
	}
	for index, edge := range document.Edges {
		analysis.edgeIndexes[edge.ID] = index
		analysis.outgoing[edge.Source] = append(analysis.outgoing[edge.Source], edge)
		analysis.incoming[edge.Target] = append(analysis.incoming[edge.Target], edge)
	}
	for id := range analysis.nodes {
		sort.Slice(analysis.incoming[id], func(left, right int) bool { return analysis.incoming[id][left].ID < analysis.incoming[id][right].ID })
		sort.Slice(analysis.outgoing[id], func(left, right int) bool { return analysis.outgoing[id][left].ID < analysis.outgoing[id][right].ID })
	}
	for _, node := range document.Nodes {
		location := ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, analysis.nodeIndexes[node.ID])}
		incoming := len(analysis.incoming[node.ID])
		outgoing := len(analysis.outgoing[node.ID])
		switch node.Type {
		case "start":
			if incoming != 0 {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "START_INCOMING_EDGE_FORBIDDEN", "Start cannot have incoming control edges", location))
			}
			if outgoing == 0 {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "START_OUTGOING_EDGE_REQUIRED", "Start requires an outgoing control edge", location))
			}
		case "end":
			if incoming == 0 {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "END_INCOMING_EDGE_REQUIRED", "End requires an incoming control edge", location))
			}
			if outgoing != 0 {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "END_OUTGOING_EDGE_FORBIDDEN", "End cannot have outgoing control edges", location))
			}
		default:
			if incoming == 0 {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "NODE_INCOMING_EDGE_REQUIRED", "non-Start node requires an incoming control edge", location))
			}
			if outgoing == 0 {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "NODE_OUTGOING_EDGE_REQUIRED", "non-End node requires an outgoing control edge", location))
			}
		}
		if node.Type == "branch" {
			diagnostics = append(diagnostics, validateBranchRoutes(node, analysis)...)
		}
	}
	analysis.topological = topologicalOrder(analysis.nodes, analysis.incoming, analysis.outgoing)
	if len(analysis.topological) != len(analysis.nodes) {
		diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "CONTROL_GRAPH_CYCLE", "control graph must be a DAG", ir.Location{IRPath: "/edges"}))
	}
	if analysis.start != "" {
		reachable := graphReachable(analysis.start, analysis.outgoing, false)
		for _, node := range document.Nodes {
			if !reachable[node.ID] {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "NODE_NOT_REACHABLE_FROM_START", "node is not reachable from Start", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, analysis.nodeIndexes[node.ID])}))
			}
		}
	}
	if analysis.end != "" {
		reachesEnd := graphReachable(analysis.end, analysis.incoming, true)
		for _, node := range document.Nodes {
			if !reachesEnd[node.ID] {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "NODE_CANNOT_REACH_END", "node cannot reach End", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, analysis.nodeIndexes[node.ID])}))
			}
		}
	}
	if len(analysis.topological) == len(analysis.nodes) {
		analysis.buildDescendants()
	}
	ir.SortDiagnostics(diagnostics)
	return analysis, ir.LimitDiagnostics(diagnostics)
}

func validateBranchRoutes(node ir.Node, analysis graphAnalysis) []ir.Diagnostic {
	diagnostics := make([]ir.Diagnostic, 0)
	declared := make(map[ir.RouteName]struct{})
	for _, input := range node.Inputs {
		switch input.Name {
		case "default_route":
			var value string
			if json.Unmarshal(input.Value, &value) == nil {
				declared[ir.RouteName(value)] = struct{}{}
			}
		case "cases":
			var cases []struct {
				Route ir.RouteName `json:"route"`
			}
			if json.Unmarshal(input.Value, &cases) == nil {
				for _, branchCase := range cases {
					declared[branchCase.Route] = struct{}{}
				}
			}
		}
	}
	domain := make([]ir.RouteName, 0, len(declared))
	for route := range declared {
		domain = append(domain, route)
	}
	sort.Slice(domain, func(left, right int) bool { return domain[left] < domain[right] })
	analysis.branchDomains[node.ID] = domain
	used := make(map[ir.RouteName]bool)
	for _, edge := range analysis.outgoing[node.ID] {
		location := ir.Location{LogicalEdgeID: edge.ID, IRPath: ir.EdgePath(edge.ID, analysis.edgeIndexes[edge.ID]) + "/route"}
		if _, exists := declared[edge.Route]; !exists {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseGraph, "BRANCH_EDGE_ROUTE_UNDECLARED", "branch edge route is not declared by cases or default_route", location))
		} else {
			used[edge.Route] = true
		}
	}
	for _, route := range domain {
		if used[route] {
			continue
		}
		diagnostic := ir.ErrorDiagnostic(ir.PhaseGraph, "BRANCH_ROUTE_HAS_NO_EDGE", "every declared branch route requires at least one outgoing edge", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, analysis.nodeIndexes[node.ID]) + "/inputs"})
		diagnostic.Details = map[string]any{"route": route}
		diagnostics = append(diagnostics, diagnostic)
	}
	return diagnostics
}

func topologicalOrder(nodes map[ir.LogicalID]ir.Node, incoming, outgoing map[ir.LogicalID][]ir.Edge) []ir.LogicalID {
	indegree := make(map[ir.LogicalID]int, len(nodes))
	queue := make([]ir.LogicalID, 0)
	for id := range nodes {
		indegree[id] = len(incoming[id])
		if indegree[id] == 0 {
			queue = append(queue, id)
		}
	}
	sort.Slice(queue, func(left, right int) bool { return queue[left] < queue[right] })
	result := make([]ir.LogicalID, 0, len(nodes))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		result = append(result, id)
		for _, edge := range outgoing[id] {
			indegree[edge.Target]--
			if indegree[edge.Target] == 0 {
				queue = append(queue, edge.Target)
				sort.Slice(queue, func(left, right int) bool { return queue[left] < queue[right] })
			}
		}
	}
	return result
}

func graphReachable(start ir.LogicalID, edges map[ir.LogicalID][]ir.Edge, reverse bool) map[ir.LogicalID]bool {
	result := map[ir.LogicalID]bool{start: true}
	stack := []ir.LogicalID{start}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, edge := range edges[id] {
			next := edge.Target
			if reverse {
				next = edge.Source
			}
			if !result[next] {
				result[next] = true
				stack = append(stack, next)
			}
		}
	}
	return result
}

func (analysis *graphAnalysis) buildDescendants() {
	analysis.ordinal = make(map[ir.LogicalID]int, len(analysis.topological))
	words := (len(analysis.topological) + 63) / 64
	analysis.descendants = make(map[ir.LogicalID][]uint64, len(analysis.topological))
	for index, id := range analysis.topological {
		analysis.ordinal[id] = index
		analysis.descendants[id] = make([]uint64, words)
	}
	for index := len(analysis.topological) - 1; index >= 0; index-- {
		id := analysis.topological[index]
		bits := analysis.descendants[id]
		for _, edge := range analysis.outgoing[id] {
			targetOrdinal := analysis.ordinal[edge.Target]
			bits[targetOrdinal/64] |= 1 << (targetOrdinal % 64)
			for word, value := range analysis.descendants[edge.Target] {
				bits[word] |= value
			}
		}
	}
}

func (analysis graphAnalysis) isControlUpstream(source, target ir.LogicalID) bool {
	ordinal, exists := analysis.ordinal[target]
	if !exists {
		return false
	}
	bits := analysis.descendants[source]
	return len(bits) > ordinal/64 && bits[ordinal/64]&(1<<(ordinal%64)) != 0
}

func graphInternalDiagnostic(code, message string) ir.Diagnostic {
	return ir.ErrorDiagnostic(ir.PhaseGraph, code, message, ir.Location{IRPath: "/edges"})
}
