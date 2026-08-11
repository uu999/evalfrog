package compiler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/uu999/evalfrog/internal/ir"
)

const maxActivationDecisionNodes = 250_000

type decisionID uint32

const (
	decisionFalse decisionID = 0
	decisionTrue  decisionID = 1
)

type decisionNode struct {
	variable int
	children []decisionID
}

type decisionManager struct {
	domains  [][]ir.RouteName
	nodes    []decisionNode
	unique   map[string]decisionID
	apply    map[string]decisionID
	negation map[decisionID]decisionID
}

func newDecisionManager(domains [][]ir.RouteName) *decisionManager {
	return &decisionManager{
		domains: domains, nodes: []decisionNode{{variable: -1}, {variable: -1}},
		unique: make(map[string]decisionID), apply: make(map[string]decisionID), negation: make(map[decisionID]decisionID),
	}
}

func (manager *decisionManager) equal(variable int, route ir.RouteName) (decisionID, error) {
	children := make([]decisionID, len(manager.domains[variable]))
	for index, candidate := range manager.domains[variable] {
		if candidate == route {
			children[index] = decisionTrue
		}
	}
	return manager.makeNode(variable, children)
}

func (manager *decisionManager) makeNode(variable int, children []decisionID) (decisionID, error) {
	allSame := true
	for _, child := range children[1:] {
		if child != children[0] {
			allSame = false
			break
		}
	}
	if allSame {
		return children[0], nil
	}
	parts := make([]string, len(children)+1)
	parts[0] = strconv.Itoa(variable)
	for index, child := range children {
		parts[index+1] = strconv.FormatUint(uint64(child), 10)
	}
	key := strings.Join(parts, ":")
	if existing, exists := manager.unique[key]; exists {
		return existing, nil
	}
	if len(manager.nodes) >= maxActivationDecisionNodes {
		return decisionFalse, fmt.Errorf("activation decision graph exceeds %d nodes", maxActivationDecisionNodes)
	}
	id := decisionID(len(manager.nodes))
	manager.nodes = append(manager.nodes, decisionNode{variable: variable, children: append([]decisionID(nil), children...)})
	manager.unique[key] = id
	return id, nil
}

func (manager *decisionManager) and(left, right decisionID) (decisionID, error) {
	return manager.applyBinary("and", left, right)
}

func (manager *decisionManager) or(left, right decisionID) (decisionID, error) {
	return manager.applyBinary("or", left, right)
}

func (manager *decisionManager) applyBinary(operation string, left, right decisionID) (decisionID, error) {
	if left > right {
		left, right = right, left
	}
	if operation == "and" {
		if left == decisionFalse || right == decisionFalse {
			return decisionFalse, nil
		}
		if left == decisionTrue {
			return right, nil
		}
	} else {
		if right == decisionTrue || left == decisionTrue {
			return decisionTrue, nil
		}
		if left == decisionFalse {
			return right, nil
		}
	}
	if left == right {
		return left, nil
	}
	key := fmt.Sprintf("%s:%d:%d", operation, left, right)
	if existing, exists := manager.apply[key]; exists {
		return existing, nil
	}
	leftVariable := manager.variable(left)
	rightVariable := manager.variable(right)
	variable := leftVariable
	if rightVariable < variable {
		variable = rightVariable
	}
	children := make([]decisionID, len(manager.domains[variable]))
	for choice := range children {
		leftChild := manager.child(left, variable, choice)
		rightChild := manager.child(right, variable, choice)
		value, err := manager.applyBinary(operation, leftChild, rightChild)
		if err != nil {
			return decisionFalse, err
		}
		children[choice] = value
	}
	result, err := manager.makeNode(variable, children)
	if err == nil {
		manager.apply[key] = result
	}
	return result, err
}

func (manager *decisionManager) not(value decisionID) (decisionID, error) {
	if value == decisionFalse {
		return decisionTrue, nil
	}
	if value == decisionTrue {
		return decisionFalse, nil
	}
	if existing, exists := manager.negation[value]; exists {
		return existing, nil
	}
	node := manager.nodes[value]
	children := make([]decisionID, len(node.children))
	for index, child := range node.children {
		result, err := manager.not(child)
		if err != nil {
			return decisionFalse, err
		}
		children[index] = result
	}
	result, err := manager.makeNode(node.variable, children)
	if err == nil {
		manager.negation[value] = result
	}
	return result, err
}

func (manager *decisionManager) variable(id decisionID) int {
	if id <= decisionTrue {
		return len(manager.domains) + 1
	}
	return manager.nodes[id].variable
}

func (manager *decisionManager) child(id decisionID, variable, choice int) decisionID {
	if id <= decisionTrue || manager.nodes[id].variable != variable {
		return id
	}
	return manager.nodes[id].children[choice]
}

func validateDataBindings(document ir.Document, analysis graphAnalysis) []ir.Diagnostic {
	branchIDs := make([]ir.LogicalID, 0, len(analysis.branchDomains))
	for _, id := range analysis.topological {
		if len(analysis.branchDomains[id]) > 0 {
			branchIDs = append(branchIDs, id)
		}
	}
	domains := make([][]ir.RouteName, len(branchIDs))
	variables := make(map[ir.LogicalID]int, len(branchIDs))
	for index, id := range branchIDs {
		variables[id] = index
		domains[index] = append([]ir.RouteName(nil), analysis.branchDomains[id]...)
	}
	manager := newDecisionManager(domains)
	activation := make(map[ir.LogicalID]decisionID, len(analysis.nodes))
	for _, id := range analysis.topological {
		if id == analysis.start {
			activation[id] = decisionTrue
			continue
		}
		formula := decisionFalse
		for _, edge := range analysis.incoming[id] {
			edgeFormula := activation[edge.Source]
			if analysis.nodes[edge.Source].Type == "branch" {
				variable, exists := variables[edge.Source]
				if !exists {
					return []ir.Diagnostic{graphInternalDiagnostic("ACTIVATION_ANALYSIS_FAILED", "branch route domain is unavailable")}
				}
				routeFormula, err := manager.equal(variable, edge.Route)
				if err != nil {
					return []ir.Diagnostic{graphInternalDiagnostic("CONTROL_GRAPH_COMPLEXITY_EXCEEDED", err.Error())}
				}
				edgeFormula, err = manager.and(edgeFormula, routeFormula)
				if err != nil {
					return []ir.Diagnostic{graphInternalDiagnostic("CONTROL_GRAPH_COMPLEXITY_EXCEEDED", err.Error())}
				}
			}
			var err error
			formula, err = manager.or(formula, edgeFormula)
			if err != nil {
				return []ir.Diagnostic{graphInternalDiagnostic("CONTROL_GRAPH_COMPLEXITY_EXCEEDED", err.Error())}
			}
		}
		activation[id] = formula
	}
	diagnostics := make([]ir.Diagnostic, 0)
	for nodeIndex, node := range document.Nodes {
		for inputIndex, input := range node.Inputs {
			if input.Source != ir.SourceRef {
				continue
			}
			location := ir.Location{LogicalNodeID: node.ID, IRPath: ir.InputPath(node.ID, nodeIndex, input.Name, inputIndex)}
			if !analysis.isControlUpstream(input.RefNode, node.ID) {
				diagnostic := ir.ErrorDiagnostic(ir.PhaseBinding, "REF_SOURCE_NOT_CONTROL_UPSTREAM", "referenced output must come from a control-flow upstream node", location)
				diagnostic.Details = map[string]any{"source_node_id": input.RefNode, "target_node_id": node.ID}
				diagnostics = append(diagnostics, diagnostic)
				continue
			}
			notSource, err := manager.not(activation[input.RefNode])
			if err != nil {
				return []ir.Diagnostic{graphInternalDiagnostic("CONTROL_GRAPH_COMPLEXITY_EXCEEDED", err.Error())}
			}
			counterexample, err := manager.and(activation[node.ID], notSource)
			if err != nil {
				return []ir.Diagnostic{graphInternalDiagnostic("CONTROL_GRAPH_COMPLEXITY_EXCEEDED", err.Error())}
			}
			if counterexample != decisionFalse {
				diagnostic := ir.ErrorDiagnostic(ir.PhaseBinding, "UNSAFE_DATA_BINDING", "target can be active while referenced source is inactive", location)
				diagnostic.Details = map[string]any{"source_node_id": input.RefNode, "target_node_id": node.ID}
				diagnostics = append(diagnostics, diagnostic)
			}
		}
	}
	ir.SortDiagnostics(diagnostics)
	return ir.LimitDiagnostics(diagnostics)
}
