package ir

import (
	"bytes"
	"strings"
	"testing"
)

func TestDraftParserAllowsIncompleteDocumentButStrictValidatorRejectsIt(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{}}`)
	document, diagnostics := DefaultParser().ParseDraft(raw)
	if HasErrors(diagnostics) {
		t.Fatalf("draft parser rejected incomplete IR: %+v", diagnostics)
	}
	strict := NewStructuralValidator().Validate(document)
	assertDiagnosticCode(t, strict, "IR_NODES_REQUIRED")
}

func TestDraftParserEnforcesSafeEnvelope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  []byte
		code string
	}{
		{name: "unknown field", raw: []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{},"worker":"builtin"}`), code: "IR_SHELL_INVALID"},
		{name: "duplicate key", raw: []byte(`{"ir_version":"1","ir_version":"1","nodes":[],"edges":[],"layout":{}}`), code: "JSON_DUPLICATE_KEY"},
		{name: "invalid utf8", raw: append([]byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{},"x":"`), 0xff), code: "INVALID_UTF8"},
		{name: "unpaired surrogate", raw: []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{"bad":{"x":0,"y":0}},"x":"\ud800"}`), code: "INVALID_UNICODE"},
		{name: "missing shell", raw: []byte(`{"ir_version":"1","nodes":[]}`), code: "IR_SHELL_INVALID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, diagnostics := DefaultParser().ParseDraft(test.raw)
			assertDiagnosticCode(t, diagnostics, test.code)
		})
	}
}

func TestDraftParserAcceptsPairedUnicodeSurrogate(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"ir_version":"1","nodes":[{"id":"start","type":"start","title":"\ud83d\ude00","inputs":[],"outputs":[]}],"edges":[],"layout":{"start":{"x":0,"y":0}}}`)
	_, diagnostics := DefaultParser().ParseDraft(raw)
	if HasErrors(diagnostics) {
		t.Fatalf("valid surrogate pair was rejected: %+v", diagnostics)
	}
}

func TestDraftParserLimitsDepthAndSize(t *testing.T) {
	t.Parallel()
	deep := `{"ir_version":"1","nodes":[],"edges":[],"layout":{},"x":` + strings.Repeat(`[`, 65) + `0` + strings.Repeat(`]`, 65) + `}`
	_, diagnostics := DefaultParser().ParseDraft([]byte(deep))
	// The unknown x field is irrelevant: bounded scanning rejects depth first.
	assertDiagnosticCode(t, diagnostics, "JSON_TOO_DEEP")

	limits := DefaultParseLimits
	limits.MaxDocumentBytes = 64
	_, diagnostics = NewParser(limits).ParseDraft(bytes.Repeat([]byte("x"), 65))
	assertDiagnosticCode(t, diagnostics, "DOCUMENT_TOO_LARGE")

	limits = DefaultParseLimits
	limits.MaxStringBytes = 8
	oversizedField := []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{},"extra":"123456789"}`)
	_, diagnostics = NewParser(limits).ParseDraft(oversizedField)
	assertDiagnosticCode(t, diagnostics, "JSON_STRING_TOO_LARGE")

	unsupportedNumber := []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{"n":{"x":1e10001,"y":0}}}`)
	_, diagnostics = DefaultParser().ParseDraft(unsupportedNumber)
	assertDiagnosticCode(t, diagnostics, "JSON_NUMBER_UNSUPPORTED")
}

func FuzzParser(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{}}`),
		[]byte(strings.Repeat(`[`, 80) + strings.Repeat(`]`, 80)),
		bytes.Repeat([]byte("x"), DefaultParseLimits.MaxDocumentBytes+1),
		[]byte(`{"ir_version":"1","nodes":[{"id":"n","type":"code","title":"` + strings.Repeat("x", DefaultParseLimits.MaxStringBytes+1) + `","inputs":[],"outputs":[]}],"edges":[],"layout":{}}`),
		{0xff, 0xfe, 0xfd},
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		document, diagnostics := DefaultParser().ParseDraft(raw)
		if !HasErrors(diagnostics) {
			if _, err := CanonicalizeDocument(document); err != nil {
				t.Fatalf("parsed document did not canonicalize: %v", err)
			}
		}
	})
}

func assertDiagnosticCode(t *testing.T, diagnostics []Diagnostic, code string) {
	t.Helper()
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return
		}
	}
	t.Fatalf("diagnostic %s not found in %+v", code, diagnostics)
}
