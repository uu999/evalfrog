package catalog

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/sandbox"
)

var (
	allTypes    = ir.AllDataTypes()
	bothSources = []ir.InputSource{ir.SourceLiteral, ir.SourceRef}
	literalOnly = []ir.InputSource{ir.SourceLiteral}
)

func startContract() contract {
	return contract{
		revision: 1,
		runtime:  controlRuntime("control.start"),
		description: NodeDescription{
			Type:        "start",
			Kind:        KindControl,
			Description: "Starts a workflow and exposes the JSON object run input.",
			Inputs:      []InputDescription{},
			Outputs: fixedOutputs(PortDescription{
				Name: "workflow_input", Description: "Workflow run input.", DataType: ir.TypeObject,
			}),
			Examples: []ir.Node{{
				ID: "start", Type: "start", Title: "开始", Inputs: []ir.Input{},
				Outputs: []ir.Output{{Name: "workflow_input", DataType: ir.TypeObject}},
			}},
		},
	}
}

func endContract() contract {
	return contract{
		revision: 1,
		runtime:  controlRuntime("control.end"),
		description: NodeDescription{
			Type:        "end",
			Kind:        KindControl,
			Description: "Finishes a workflow with one JSON object workflow output.",
			Inputs: []InputDescription{{
				Name: "workflow_output", Description: "Workflow run output.", Required: true,
				DataTypes: []ir.DataType{ir.TypeObject}, Sources: bothSources,
			}},
			Outputs: fixedOutputs(),
			Examples: []ir.Node{{
				ID: "end", Type: "end", Title: "结束",
				Inputs:  []ir.Input{{Name: "workflow_output", DataType: ir.TypeObject, Source: ir.SourceLiteral, Value: mustJSON(map[string]any{})}},
				Outputs: []ir.Output{},
			}},
		},
	}
}

func branchContract() contract {
	return contract{
		revision: 1,
		runtime:  controlRuntime("control.branch"),
		description: NodeDescription{
			Type:        "branch",
			Kind:        KindControl,
			Description: "Selects exactly one named route using ordered deterministic cases and a default route.",
			Inputs: []InputDescription{
				{Name: "value", Description: "Value evaluated by branch cases.", Required: true, DataTypes: allTypes, Sources: bothSources},
				{Name: "cases", Description: "Ordered route cases.", Required: true, DataTypes: []ir.DataType{ir.TypeArray}, Sources: literalOnly, Constraints: &LiteralConstraints{BranchCases: &BranchCaseConstraints{FirstMatchWins: true, PathSyntax: "a.b.c", Operators: branchOperatorMatrix}}},
				{Name: "default_route", Description: "Route selected when no case matches.", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly, Constraints: &LiteralConstraints{MinLength: 1, MaxLength: ir.MaxPortNameBytes}},
			},
			Outputs: fixedOutputs(),
			Examples: []ir.Node{{
				ID: "check_score", Type: "branch", Title: "判断分数",
				Inputs: []ir.Input{
					{Name: "value", DataType: ir.TypeInteger, Source: ir.SourceLiteral, Value: mustJSON(85)},
					{Name: "cases", DataType: ir.TypeArray, Source: ir.SourceLiteral, Value: mustJSON([]any{map[string]any{"route": "approved", "operator": "gte", "value": 80}})},
					{Name: "default_route", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("rejected")},
				},
				Outputs: []ir.Output{},
			}},
		},
		validate: validateBranch,
	}
}

func codeContract() contract {
	return contract{
		revision: 1,
		runtime:  taskRuntime("task.python"),
		description: NodeDescription{
			Type:        "code",
			Kind:        KindTask,
			Description: "Runs a fixed Python main(inputs) function in a network-denied sandbox.",
			Inputs: []InputDescription{
				{Name: "source_code", Description: "Python source defining main(inputs).", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly, Constraints: &LiteralConstraints{MinLength: 1, MaxLength: 256 << 10}},
				{Name: "model_ref", Description: "Optional approved model artifact reference.", Required: false, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly, Constraints: &LiteralConstraints{MinLength: 1, MaxLength: 256}},
			},
			AdditionalInputs: &AdditionalInputDescription{
				Description: "Named JSON data packed into the inputs object.", DataTypes: allTypes, Sources: bothSources,
				ReservedNames: []ir.PortName{"source_code", "model_ref"},
			},
			Outputs: OutputDescription{Mode: OutputAuthorDeclared, AllowedDataTypes: allTypes, MinFields: 0, MaxFields: ir.MaxPortsPerNode},
			Examples: []ir.Node{{
				ID: "normalize_order", Type: "code", Title: "规范化订单",
				Inputs: []ir.Input{
					{Name: "source_code", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("def main(inputs):\n    return {'result': inputs['order']}")},
					{Name: "order", DataType: ir.TypeObject, Source: ir.SourceLiteral, Value: mustJSON(map[string]any{"id": "O-1"})},
				},
				Outputs: []ir.Output{{Name: "result", DataType: ir.TypeObject}},
			}},
		},
		validate: validateCode,
	}
}

func validateCode(node ir.Node) []ir.Diagnostic {
	diagnostics := nonEmptyLiteral(node, "source_code")
	for _, input := range node.Inputs {
		if input.Name != "source_code" || input.Source != ir.SourceLiteral {
			continue
		}
		var source string
		if err := json.Unmarshal(input.Value, &source); err != nil || source == "" {
			return diagnostics
		}
		if sourceError := sandbox.ValidateSource(source); sourceError != nil {
			diagnostic := ir.ErrorDiagnostic(ir.PhaseCatalog, sourceError.Code, sourceError.Message, ir.Location{LogicalNodeID: node.ID, IRPath: "/nodes/" + string(node.ID) + "/inputs/source_code"})
			if sourceError.Line > 0 {
				diagnostic.Details = map[string]any{"source_line": sourceError.Line, "source_column": sourceError.Column}
			}
			diagnostics = append(diagnostics, diagnostic)
		}
	}
	return diagnostics
}

func httpContract() contract {
	minimumStatus := int64(100)
	maximumStatus := int64(599)
	return contract{
		revision: 1,
		runtime:  taskRuntime("task.http"),
		description: NodeDescription{
			Type:        "http",
			Kind:        KindTask,
			Description: "Calls an authorized project HTTP connection using a relative JSON API path.",
			Inputs: []InputDescription{
				{Name: "connection_ref", Description: "Authorized project connection reference.", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly, Constraints: &LiteralConstraints{MinLength: 1, MaxLength: 256}},
				{Name: "method", Description: "HTTP method.", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly, Constraints: &LiteralConstraints{StringEnum: []string{"GET", "POST", "PUT", "PATCH", "DELETE"}}},
				{Name: "relative_path", Description: "Path relative to the authorized connection origin.", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: bothSources, Constraints: &LiteralConstraints{StringPattern: `^/[^?#]*$`, MaxLength: 4096}},
				{Name: "query", Description: "Query parameters encoded by the executor.", Required: false, DataTypes: []ir.DataType{ir.TypeObject}, Sources: bothSources},
				{Name: "headers", Description: "Non-protected request headers.", Required: false, DataTypes: []ir.DataType{ir.TypeObject}, Sources: bothSources},
				{Name: "body", Description: "Optional JSON request body.", Required: false, DataTypes: allTypes, Sources: bothSources},
				{Name: "accepted_statuses", Description: "Accepted HTTP statuses; defaults to 2xx.", Required: false, DataTypes: []ir.DataType{ir.TypeArray}, Sources: literalOnly, Constraints: &LiteralConstraints{ArrayItems: &ArrayItemConstraint{DataType: ir.TypeInteger, Minimum: &minimumStatus, Maximum: &maximumStatus}}},
			},
			Outputs: fixedOutputs(PortDescription{Name: "response", Description: "Status, headers, and parsed JSON body.", DataType: ir.TypeObject}),
			Examples: []ir.Node{{
				ID: "create_order", Type: "http", Title: "创建订单",
				Inputs: []ir.Input{
					{Name: "connection_ref", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("customer_api")},
					{Name: "method", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("POST")},
					{Name: "relative_path", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("/v1/orders")},
					{Name: "body", DataType: ir.TypeObject, Source: ir.SourceLiteral, Value: mustJSON(map[string]any{"id": "O-1"})},
				},
				Outputs: []ir.Output{{Name: "response", DataType: ir.TypeObject}},
			}},
		},
		validate: validateHTTP,
	}
}

func rpcContract() contract {
	return contract{
		revision: 1,
		runtime:  taskRuntime("task.rpc"),
		description: NodeDescription{
			Type:        "rpc",
			Kind:        KindTask,
			Description: "Calls one authorized operation registered in the project service catalog.",
			Inputs: []InputDescription{
				{Name: "service_ref", Description: "Authorized service reference.", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly, Constraints: &LiteralConstraints{MinLength: 1, MaxLength: 256}},
				{Name: "operation", Description: "Registered service operation.", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly, Constraints: &LiteralConstraints{MinLength: 1, MaxLength: 256}},
				{Name: "request", Description: "Operation request object.", Required: true, DataTypes: []ir.DataType{ir.TypeObject}, Sources: bothSources},
			},
			Outputs: fixedOutputs(PortDescription{Name: "response", Description: "Validated operation response.", DataType: ir.TypeObject}),
			Examples: []ir.Node{{
				ID: "create_invoice", Type: "rpc", Title: "创建发票",
				Inputs: []ir.Input{
					{Name: "service_ref", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("invoice_service")},
					{Name: "operation", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("CreateInvoice")},
					{Name: "request", DataType: ir.TypeObject, Source: ir.SourceLiteral, Value: mustJSON(map[string]any{})},
				},
				Outputs: []ir.Output{{Name: "response", DataType: ir.TypeObject}},
			}},
		},
		validate: func(node ir.Node) []ir.Diagnostic {
			return append(nonEmptyLiteral(node, "service_ref"), nonEmptyLiteral(node, "operation")...)
		},
	}
}

func validateBranch(node ir.Node) []ir.Diagnostic {
	var valueType ir.DataType
	for _, input := range node.Inputs {
		if input.Name == "value" {
			valueType = input.DataType
			break
		}
	}
	for inputIndex, input := range node.Inputs {
		if input.Name == "default_route" && input.Source == ir.SourceLiteral && input.HasValue() {
			value, _, err := ir.DecodeLiteral(input.Value)
			if err == nil {
				if route, ok := value.(string); !ok || !ir.ValidRouteName(ir.RouteName(route)) {
					return []ir.Diagnostic{nodeInputDiagnostic(node, input, inputIndex, "BRANCH_ROUTE_INVALID", "default_route must be a valid route identifier")}
				}
			}
		}
		if input.Name != "cases" || input.Source != ir.SourceLiteral || !input.HasValue() {
			continue
		}
		value, _, err := ir.DecodeLiteral(input.Value)
		if err != nil {
			return nil
		}
		cases, ok := value.([]any)
		if !ok || len(cases) == 0 {
			return []ir.Diagnostic{nodeInputDiagnostic(node, input, inputIndex, "BRANCH_CASES_INVALID", "cases must contain at least one case")}
		}
		for caseIndex, rawCase := range cases {
			item, object := rawCase.(map[string]any)
			if !object {
				return []ir.Diagnostic{branchCaseDiagnostic(node, input, inputIndex, caseIndex, "case must be an object")}
			}
			route, routeOK := item["route"].(string)
			operator, operatorOK := item["operator"].(string)
			if !routeOK || !ir.ValidRouteName(ir.RouteName(route)) {
				return []ir.Diagnostic{branchCaseDiagnostic(node, input, inputIndex, caseIndex, "case route is invalid")}
			}
			if !operatorOK || !containsString(branchOperators, operator) {
				return []ir.Diagnostic{branchCaseDiagnostic(node, input, inputIndex, caseIndex, "case operator is unsupported")}
			}
			if _, exists := item["value"]; !exists {
				return []ir.Diagnostic{branchCaseDiagnostic(node, input, inputIndex, caseIndex, "case comparison value is required")}
			}
			for key := range item {
				if key != "route" && key != "path" && key != "operator" && key != "value" {
					return []ir.Diagnostic{branchCaseDiagnostic(node, input, inputIndex, caseIndex, fmt.Sprintf("case field %q is unsupported", key))}
				}
			}
			path, hasPath := item["path"]
			if hasPath {
				pathText, pathOK := path.(string)
				if !pathOK || !validBranchPath(pathText) {
					return []ir.Diagnostic{branchCaseDiagnostic(node, input, inputIndex, caseIndex, "case path is invalid")}
				}
				if valueType.Valid() && valueType != ir.TypeObject {
					return []ir.Diagnostic{branchCaseTypeDiagnostic(node, input, inputIndex, caseIndex, "case path is only valid when branch value is an object")}
				}
				if !validPathComparison(operator, item["value"]) {
					return []ir.Diagnostic{branchCaseTypeDiagnostic(node, input, inputIndex, caseIndex, "comparison value is incompatible with the path operator")}
				}
			} else if valueType.Valid() {
				if !containsString(branchOperatorMatrix[valueType], operator) {
					return []ir.Diagnostic{branchCaseTypeDiagnostic(node, input, inputIndex, caseIndex, "operator is incompatible with the branch value data_type")}
				}
				if !validBranchComparison(valueType, operator, item["value"]) {
					return []ir.Diagnostic{branchCaseTypeDiagnostic(node, input, inputIndex, caseIndex, "comparison value is incompatible with the branch value data_type and operator")}
				}
			}
		}
	}
	return nil
}

func validBranchComparison(valueType ir.DataType, operator string, comparison any) bool {
	comparisonType, valid := nestedDataType(comparison)
	switch valueType {
	case ir.TypeString:
		return valid && comparisonType == ir.TypeString
	case ir.TypeInteger, ir.TypeNumber:
		return valid && (comparisonType == ir.TypeInteger || comparisonType == ir.TypeNumber)
	case ir.TypeBoolean:
		return valid && comparisonType == ir.TypeBoolean
	case ir.TypeArray:
		if operator == "contains" || operator == "not_contains" {
			return true
		}
		return valid && comparisonType == ir.TypeArray
	case ir.TypeObject:
		if operator == "has_key" || operator == "not_has_key" {
			return valid && comparisonType == ir.TypeString
		}
		return valid && comparisonType == ir.TypeObject
	default:
		return false
	}
}

func validPathComparison(operator string, comparison any) bool {
	comparisonType, valid := nestedDataType(comparison)
	switch operator {
	case "starts_with", "ends_with", "has_key", "not_has_key":
		return valid && comparisonType == ir.TypeString
	case "gt", "gte", "lt", "lte":
		return valid && (comparisonType == ir.TypeInteger || comparisonType == ir.TypeNumber)
	case "contains", "not_contains":
		return true
	case "eq", "neq":
		return valid
	default:
		return false
	}
}

func nestedDataType(value any) (ir.DataType, bool) {
	if value == nil {
		return "", false
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	_, dataType, err := ir.DecodeLiteral(raw)
	return dataType, err == nil
}

func branchCaseTypeDiagnostic(node ir.Node, input ir.Input, inputIndex, caseIndex int, message string) ir.Diagnostic {
	diagnostic := branchCaseDiagnostic(node, input, inputIndex, caseIndex, message)
	diagnostic.Code = "BRANCH_OPERATOR_TYPE_MISMATCH"
	return diagnostic
}

var branchOperatorMatrix = map[ir.DataType][]string{
	ir.TypeString:  {"eq", "neq", "contains", "not_contains", "starts_with", "ends_with"},
	ir.TypeInteger: {"eq", "neq", "gt", "gte", "lt", "lte"},
	ir.TypeNumber:  {"eq", "neq", "gt", "gte", "lt", "lte"},
	ir.TypeBoolean: {"eq", "neq"},
	ir.TypeArray:   {"eq", "neq", "contains", "not_contains"},
	ir.TypeObject:  {"eq", "neq", "has_key", "not_has_key"},
}

var branchOperators = flattenOperators(branchOperatorMatrix)

func flattenOperators(matrix map[ir.DataType][]string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, dataType := range ir.AllDataTypes() {
		for _, operator := range matrix[dataType] {
			if _, exists := seen[operator]; exists {
				continue
			}
			seen[operator] = struct{}{}
			result = append(result, operator)
		}
	}
	return result
}

func validBranchPath(value string) bool {
	if value == "" {
		return false
	}
	for _, segment := range strings.Split(value, ".") {
		if !ir.ValidPortName(ir.PortName(segment)) {
			return false
		}
	}
	return true
}

func branchCaseDiagnostic(node ir.Node, input ir.Input, inputIndex, caseIndex int, message string) ir.Diagnostic {
	diagnostic := nodeInputDiagnostic(node, input, inputIndex, "BRANCH_CASES_INVALID", message)
	diagnostic.Locations[0].IRPath += fmt.Sprintf("/%d", caseIndex)
	return diagnostic
}

func validateHTTP(node ir.Node) []ir.Diagnostic {
	for index, input := range node.Inputs {
		if input.Name != "relative_path" || input.Source != ir.SourceLiteral || !input.HasValue() {
			continue
		}
		value, _, err := ir.DecodeLiteral(input.Value)
		if err != nil {
			return nil
		}
		path, ok := value.(string)
		parsed, parseErr := url.Parse(path)
		if !ok || parseErr != nil || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return []ir.Diagnostic{nodeInputDiagnostic(node, input, index, "HTTP_INVALID_RELATIVE_PATH", "relative_path must stay within the authorized connection origin")}
		}
	}
	return nonEmptyLiteral(node, "connection_ref")
}

func fixedOutputs(fields ...PortDescription) OutputDescription {
	if fields == nil {
		fields = []PortDescription{}
	}
	return OutputDescription{Mode: OutputFixed, Fields: fields}
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func controlRuntime(operationType string) RuntimeContract {
	return RuntimeContract{Kind: KindControl, OperationType: operationType, OperationVersion: 1}
}

func taskRuntime(operationType string) RuntimeContract {
	return RuntimeContract{
		Kind: KindTask, OperationType: operationType, OperationVersion: 1,
		DefaultExecutionPolicy: ExecutionPolicyDefaults{
			MaxAttempts: 1, MaxRecoveries: 3, AttemptTimeoutMS: 30_000, RetryBackoffMS: 1_000,
			RetryableErrorCodes: []string{},
		},
	}
}
