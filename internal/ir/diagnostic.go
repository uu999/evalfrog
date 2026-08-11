package ir

import (
	"sort"
	"strings"
)

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

type Phase string

const (
	PhaseParse     Phase = "parse"
	PhaseStructure Phase = "structure"
	PhaseCatalog   Phase = "catalog"
	PhaseReference Phase = "reference"
	PhaseGraph     Phase = "control_graph"
	PhaseBinding   Phase = "binding"
	PhaseResource  Phase = "resource"
	PhaseCompile   Phase = "compile"
	PhaseDSL       Phase = "dsl"
	PhaseSourceMap Phase = "source_map"
	PhaseCanonical Phase = "canonical"
)

type Location struct {
	LogicalNodeID LogicalID `json:"logical_node_id,omitempty"`
	LogicalEdgeID LogicalID `json:"logical_edge_id,omitempty"`
	IRPath        string    `json:"ir_path,omitempty"`
}

type Diagnostic struct {
	Code      string         `json:"code"`
	Severity  Severity       `json:"severity"`
	Phase     Phase          `json:"phase"`
	Message   string         `json:"message"`
	Locations []Location     `json:"locations,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

const MaxDiagnostics = 100

func ErrorDiagnostic(phase Phase, code, message string, location Location) Diagnostic {
	diagnostic := Diagnostic{Code: code, Severity: SeverityError, Phase: phase, Message: message}
	if location != (Location{}) {
		diagnostic.Locations = []Location{location}
	}
	return diagnostic
}

func SortDiagnostics(values []Diagnostic) {
	sort.SliceStable(values, func(i, j int) bool {
		left := diagnosticKey(values[i])
		right := diagnosticKey(values[j])
		return left < right
	})
}

func diagnosticKey(value Diagnostic) string {
	severity := "1"
	if value.Severity == SeverityError {
		severity = "0"
	}
	location := Location{}
	if len(value.Locations) > 0 {
		location = value.Locations[0]
	}
	return strings.Join([]string{
		severity,
		phaseSortKey(value.Phase),
		location.IRPath,
		string(location.LogicalNodeID),
		string(location.LogicalEdgeID),
		value.Code,
	}, "\x00")
}

func phaseSortKey(value Phase) string {
	switch value {
	case PhaseParse:
		return "0"
	case PhaseStructure:
		return "1"
	case PhaseCatalog:
		return "2"
	case PhaseReference:
		return "3"
	case PhaseGraph:
		return "4"
	case PhaseBinding:
		return "5"
	case PhaseResource:
		return "6"
	case PhaseCompile:
		return "7"
	case PhaseDSL:
		return "8"
	case PhaseSourceMap:
		return "9"
	case PhaseCanonical:
		return "a"
	default:
		return "z" + string(value)
	}
}

func HasErrors(values []Diagnostic) bool {
	for _, value := range values {
		if value.Severity == SeverityError {
			return true
		}
	}
	return false
}

func LimitDiagnostics(values []Diagnostic) []Diagnostic {
	if len(values) <= MaxDiagnostics {
		return values
	}
	omitted := len(values) - (MaxDiagnostics - 1)
	limited := append([]Diagnostic(nil), values[:MaxDiagnostics-1]...)
	limited = append(limited, Diagnostic{
		Code:     "DIAGNOSTIC_LIMIT_REACHED",
		Severity: SeverityWarning,
		Phase:    PhaseStructure,
		Message:  "additional diagnostics were omitted",
		Details:  map[string]any{"omitted": omitted},
	})
	return limited
}
