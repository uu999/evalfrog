package catalog

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/uu999/evalfrog/internal/ir"
)

func validateDescription(node ir.Node, description NodeDescription) []ir.Diagnostic {
	diagnostics := make([]ir.Diagnostic, 0)
	declared := make(map[ir.PortName]InputDescription, len(description.Inputs))
	actual := make(map[ir.PortName]ir.Input, len(node.Inputs))
	for _, value := range description.Inputs {
		declared[value.Name] = value
	}
	for index, input := range node.Inputs {
		actual[input.Name] = input
		rule, exists := declared[input.Name]
		if !exists {
			if description.AdditionalInputs == nil || containsPort(description.AdditionalInputs.ReservedNames, input.Name) {
				diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_UNSUPPORTED", "input is not allowed by the node description"))
				continue
			}
			if input.DataType.Valid() && !containsType(description.AdditionalInputs.DataTypes, input.DataType) {
				diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_TYPE_NOT_ALLOWED", "additional input data_type is not allowed"))
			}
			if input.Source.Valid() && !containsSource(description.AdditionalInputs.Sources, input.Source) {
				diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_SOURCE_NOT_ALLOWED", "additional input source is not allowed"))
			}
			continue
		}
		if input.DataType.Valid() && !containsType(rule.DataTypes, input.DataType) {
			diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_TYPE_NOT_ALLOWED", "input data_type is not allowed by the node description"))
		}
		if input.Source.Valid() && !containsSource(rule.Sources, input.Source) {
			diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_SOURCE_NOT_ALLOWED", "input source is not allowed by the node description"))
		}
		if input.Source == ir.SourceLiteral && input.HasValue() && rule.Constraints != nil {
			diagnostics = append(diagnostics, validateLiteralConstraints(node, input, index, *rule.Constraints)...)
		}
	}
	for _, rule := range description.Inputs {
		if _, exists := actual[rule.Name]; rule.Required && !exists {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(
				ir.PhaseCatalog,
				"NODE_INPUT_REQUIRED",
				fmt.Sprintf("required input %q is missing", rule.Name),
				ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, 0) + "/inputs/" + string(rule.Name)},
			))
		}
	}
	diagnostics = append(diagnostics, validateOutputs(node, description.Outputs)...)
	return diagnostics
}

func validateOutputs(node ir.Node, description OutputDescription) []ir.Diagnostic {
	diagnostics := make([]ir.Diagnostic, 0)
	switch description.Mode {
	case OutputFixed:
		expected := make(map[ir.PortName]ir.DataType, len(description.Fields))
		actual := make(map[ir.PortName]ir.DataType, len(node.Outputs))
		for _, field := range description.Fields {
			expected[field.Name] = field.DataType
		}
		for index, output := range node.Outputs {
			actual[output.Name] = output.DataType
			expectedType, exists := expected[output.Name]
			if !exists {
				diagnostics = append(diagnostics, nodeOutputDiagnostic(node, output, index, "NODE_OUTPUT_UNSUPPORTED", "output is not allowed by the node description"))
			} else if output.DataType.Valid() && output.DataType != expectedType {
				diagnostics = append(diagnostics, nodeOutputDiagnostic(node, output, index, "NODE_OUTPUT_TYPE_MISMATCH", "fixed output has the wrong data_type"))
			}
		}
		for name := range expected {
			if _, exists := actual[name]; !exists {
				diagnostics = append(diagnostics, ir.ErrorDiagnostic(
					ir.PhaseCatalog,
					"NODE_OUTPUT_REQUIRED",
					fmt.Sprintf("fixed output %q is missing", name),
					ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, 0) + "/outputs/" + string(name)},
				))
			}
		}
	case OutputAuthorDeclared:
		if len(node.Outputs) < description.MinFields || (description.MaxFields > 0 && len(node.Outputs) > description.MaxFields) {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCatalog, "NODE_OUTPUT_COUNT_INVALID", "author-declared output count is outside the node contract", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, 0) + "/outputs"}))
		}
		for index, output := range node.Outputs {
			if output.DataType.Valid() && !containsType(description.AllowedDataTypes, output.DataType) {
				diagnostics = append(diagnostics, nodeOutputDiagnostic(node, output, index, "NODE_OUTPUT_TYPE_NOT_ALLOWED", "author-declared output data_type is not allowed"))
			}
		}
	default:
		diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCatalog, "CATALOG_OUTPUT_MODE_INVALID", "node description has an invalid output mode", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, 0) + "/outputs"}))
	}
	return diagnostics
}

func validateLiteralConstraints(node ir.Node, input ir.Input, index int, constraints LiteralConstraints) []ir.Diagnostic {
	value, _, err := ir.DecodeLiteral(input.Value)
	if err != nil {
		return nil
	}
	diagnostics := make([]ir.Diagnostic, 0)
	if typed, ok := value.(string); ok {
		if constraints.MinLength > 0 && utf8.RuneCountInString(typed) < constraints.MinLength {
			diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_CONSTRAINT_VIOLATION", "literal string is shorter than allowed"))
		}
		if constraints.MaxLength > 0 && utf8.RuneCountInString(typed) > constraints.MaxLength {
			diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_CONSTRAINT_VIOLATION", "literal string is longer than allowed"))
		}
		if len(constraints.StringEnum) > 0 && !containsString(constraints.StringEnum, typed) {
			diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_CONSTRAINT_VIOLATION", "literal string is not an allowed value"))
		}
		if constraints.StringPattern != "" {
			pattern, compileErr := regexp.Compile(constraints.StringPattern)
			if compileErr != nil || !pattern.MatchString(typed) {
				diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_CONSTRAINT_VIOLATION", "literal string does not match the required pattern"))
			}
		}
	}
	if values, ok := value.([]any); ok && constraints.ArrayItems != nil {
		for _, item := range values {
			raw, marshalErr := json.Marshal(item)
			if marshalErr != nil {
				continue
			}
			decoded, actual, decodeErr := ir.DecodeLiteral(raw)
			if decodeErr != nil || !ir.Compatible(actual, constraints.ArrayItems.DataType) {
				diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_CONSTRAINT_VIOLATION", "array item has an invalid data_type"))
				continue
			}
			if number, ok := decoded.(json.Number); ok && actual == ir.TypeInteger {
				integer, integerErr := number.Int64()
				if integerErr != nil || (constraints.ArrayItems.Minimum != nil && integer < *constraints.ArrayItems.Minimum) || (constraints.ArrayItems.Maximum != nil && integer > *constraints.ArrayItems.Maximum) {
					diagnostics = append(diagnostics, nodeInputDiagnostic(node, input, index, "NODE_INPUT_CONSTRAINT_VIOLATION", "array integer item is outside the allowed range"))
				}
			}
		}
	}
	return diagnostics
}

func nodeInputDiagnostic(node ir.Node, input ir.Input, index int, code, message string) ir.Diagnostic {
	return ir.ErrorDiagnostic(ir.PhaseCatalog, code, message, ir.Location{LogicalNodeID: node.ID, IRPath: ir.InputPath(node.ID, 0, input.Name, index)})
}

func nodeOutputDiagnostic(node ir.Node, output ir.Output, index int, code, message string) ir.Diagnostic {
	return ir.ErrorDiagnostic(ir.PhaseCatalog, code, message, ir.Location{LogicalNodeID: node.ID, IRPath: ir.OutputPath(node.ID, 0, output.Name, index)})
}

func containsType(values []ir.DataType, target ir.DataType) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsSource(values []ir.InputSource, target ir.InputSource) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsPort(values []ir.PortName, target ir.PortName) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func nonEmptyLiteral(node ir.Node, inputName ir.PortName) []ir.Diagnostic {
	for index, input := range node.Inputs {
		if input.Name != inputName || input.Source != ir.SourceLiteral || !input.HasValue() {
			continue
		}
		value, _, err := ir.DecodeLiteral(input.Value)
		if err == nil {
			if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
				return []ir.Diagnostic{nodeInputDiagnostic(node, input, index, "NODE_INPUT_CONSTRAINT_VIOLATION", "literal string must not be blank")}
			}
		}
	}
	return nil
}
