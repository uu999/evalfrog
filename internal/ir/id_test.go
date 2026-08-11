package ir

import (
	"strings"
	"testing"
)

func TestLogicalIDSupportsWebAndAgentStyles(t *testing.T) {
	t.Parallel()
	valid := []LogicalID{
		"code_to_str",
		"build-order",
		"8c6c0d45-0acf-49c6-bad9-672f33b16963",
		"A1",
		LogicalID("a" + strings.Repeat("b", MaxLogicalIDBytes-1)),
	}
	for _, value := range valid {
		if !ValidLogicalID(value) {
			t.Fatalf("expected %q to be valid", value)
		}
	}

	invalid := []LogicalID{"", "has space", "has/slash", "has.dot", "_starts_with_symbol", LogicalID("a" + strings.Repeat("b", MaxLogicalIDBytes))}
	for _, value := range invalid {
		if ValidLogicalID(value) {
			t.Fatalf("expected %q to be invalid", value)
		}
	}
}

func TestPortAndNodeTypeRules(t *testing.T) {
	t.Parallel()
	if !ValidPortName("workflow_output") || ValidPortName("9output") || ValidPortName("bad-name") {
		t.Fatal("port name contract changed")
	}
	if !ValidNodeType("custom_node") || ValidNodeType("HTTP") || ValidNodeType("bad-node") {
		t.Fatal("node type contract changed")
	}
}

func FuzzLogicalID(f *testing.F) {
	for _, seed := range []string{"code_to_str", "8c6c0d45-0acf-49c6-bad9-672f33b16963", "", "a/b", strings.Repeat("x", 129)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		first := ValidLogicalID(LogicalID(value))
		second := ValidLogicalID(LogicalID(value))
		if first != second {
			t.Fatal("logical ID validation is not deterministic")
		}
	})
}
