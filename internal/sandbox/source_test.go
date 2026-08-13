package sandbox

import "testing"

func TestValidateSource(t *testing.T) {
	tests := []struct {
		name, source, code string
	}{
		{"accepts algorithm", "import math\ndef main(inputs):\n    return {'value': math.floor(inputs['value'])}", ""},
		{"rejects syntax", "def main(inputs)\n    return {}", "CODE_SYNTAX_ERROR"},
		{"rejects bad entry", "def main(value):\n    return {}", "CODE_ENTRYPOINT_INVALID"},
		{"rejects host import", "import os\ndef main(inputs):\n    return {}", "CODE_IMPORT_FORBIDDEN"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSource(test.source)
			if test.code == "" && err != nil {
				t.Fatalf("ValidateSource() error = %v", err)
			}
			if test.code != "" && (err == nil || err.Code != test.code) {
				t.Fatalf("ValidateSource() error = %#v, want %s", err, test.code)
			}
		})
	}
}
