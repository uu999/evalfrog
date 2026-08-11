package ir

import (
	"encoding/json"
	"testing"
)

func TestLiteralTopLevelTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw      string
		expected DataType
	}{
		{`"hello"`, TypeString},
		{`42`, TypeInteger},
		{`42.5`, TypeNumber},
		{`1.0`, TypeInteger},
		{`1e2`, TypeInteger},
		{`true`, TypeBoolean},
		{`{"nested":null}`, TypeObject},
		{`[null,1]`, TypeArray},
	}
	for _, test := range tests {
		_, actual, err := DecodeLiteral(json.RawMessage(test.raw))
		if err != nil {
			t.Fatalf("decode %s: %v", test.raw, err)
		}
		if actual != test.expected {
			t.Fatalf("decode %s: got %s want %s", test.raw, actual, test.expected)
		}
	}
	if _, _, err := DecodeLiteral(json.RawMessage(`null`)); err == nil {
		t.Fatal("top-level null must be rejected")
	}
}

func TestTypeCompatibilityOnlyPromotesIntegerToNumber(t *testing.T) {
	t.Parallel()
	for _, source := range AllDataTypes() {
		for _, target := range AllDataTypes() {
			expected := source == target || (source == TypeInteger && target == TypeNumber)
			if Compatible(source, target) != expected {
				t.Fatalf("unexpected compatibility %s -> %s", source, target)
			}
		}
	}
}

func TestSafeIntegerRange(t *testing.T) {
	t.Parallel()
	if !SafeInteger(json.Number("9007199254740991")) || !SafeInteger(json.Number("-9007199254740991")) {
		t.Fatal("safe bounds must be accepted")
	}
	if !SafeInteger(json.Number("1.0")) || !SafeInteger(json.Number("1e2")) {
		t.Fatal("mathematically integral JSON numbers in range must be accepted")
	}
	if SafeInteger(json.Number("9007199254740992")) || SafeInteger(json.Number("1.5")) {
		t.Fatal("unsafe or non-integral values must be rejected")
	}
}
