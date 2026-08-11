package ir

import (
	"bytes"
	"testing"
)

func TestCanonicalJSONSortsObjectsAndPreservesArrayOrder(t *testing.T) {
	t.Parallel()
	left, err := CanonicalizeJSON([]byte(`{"z":1.0,"a":{"b":2,"a":1},"items":[1,2]}`), DefaultParseLimits)
	if err != nil {
		t.Fatal(err)
	}
	right, err := CanonicalizeJSON([]byte(`{"items":[1,2],"a":{"a":1,"b":2},"z":1}`), DefaultParseLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(left, right) {
		t.Fatalf("object key order changed canonical bytes:\n%s\n%s", left, right)
	}
	reordered, err := CanonicalizeJSON([]byte(`{"z":1,"a":{"a":1,"b":2},"items":[2,1]}`), DefaultParseLimits)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(left, reordered) {
		t.Fatal("array order must remain semantically significant")
	}
}

func TestCanonicalizeAndHashRepeatOneHundredTimes(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{}}`)
	document, diagnostics := DefaultParser().ParseDraft(raw)
	if HasErrors(diagnostics) {
		t.Fatalf("parse: %+v", diagnostics)
	}
	expectedBytes, expectedHash, err := CanonicalDocumentHash(document)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 100; index++ {
		actualBytes, actualHash, hashErr := CanonicalDocumentHash(document)
		if hashErr != nil {
			t.Fatal(hashErr)
		}
		if !bytes.Equal(expectedBytes, actualBytes) || expectedHash != actualHash {
			t.Fatalf("canonical result changed at iteration %d", index)
		}
	}
}

func TestCanonicalNumberNormalization(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		`1.00`:                   `1`,
		`-0`:                     `0`,
		`0.0012300`:              `0.00123`,
		`1.2e3`:                  `1200`,
		`1e-7`:                   `1e-7`,
		`1000000000000000000000`: `1e21`,
	}
	for raw, expected := range tests {
		actual, err := CanonicalizeJSON([]byte(raw), DefaultParseLimits)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if string(actual) != expected {
			t.Fatalf("%s: got %s want %s", raw, actual, expected)
		}
	}
}

func TestCanonicalStringUsesMinimalJSONEscaping(t *testing.T) {
	t.Parallel()
	actual, err := CanonicalizeJSON([]byte(`{"value":"<frog>\u2028line\n\u0001"}`), DefaultParseLimits)
	if err != nil {
		t.Fatal(err)
	}
	expected := "{\"value\":\"<frog>\u2028line\\n\\u0001\"}"
	if string(actual) != expected {
		t.Fatalf("got %q want %q", actual, expected)
	}
}
