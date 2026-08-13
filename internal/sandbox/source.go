// Package sandbox owns the portable Code sandbox contract. It has no Worker,
// database, network, or container-runtime dependency.
package sandbox

import (
	"fmt"
	"regexp"
	"strings"

	pyparser "github.com/tamnd/gopapy/parser"
)

const SourceField = "operation.config.source_code"

// SourceError is safe to return to an author. It deliberately never includes
// source text, inputs, credentials, or runtime command output.
type SourceError struct {
	Code, Message string
	Line, Column  int
}

func (err *SourceError) Error() string { return err.Message }

var importStatement = regexp.MustCompile(`(?m)^\s*(?:from\s+([A-Za-z_][A-Za-z0-9_\.]*)\s+import\b|import\s+([A-Za-z_][A-Za-z0-9_\.]*))`)

var allowedImports = map[string]struct{}{
	"collections": {}, "datetime": {}, "decimal": {}, "functools": {}, "itertools": {},
	"json": {}, "math": {}, "operator": {}, "re": {}, "statistics": {}, "string": {}, "typing": {},
}

// ValidateSource is deterministic authoring-time validation. It is not a
// security boundary: the runtime image repeats the import restriction and the
// container supplies the actual network, filesystem, process, and resource
// isolation.
func ValidateSource(source string) *SourceError {
	if strings.TrimSpace(source) == "" {
		return &SourceError{Code: "CODE_SYNTAX_ERROR", Message: "Python source_code is required"}
	}
	module, err := pyparser.ParseFile("workflow_code.py", source)
	if err != nil {
		return &SourceError{Code: "CODE_SYNTAX_ERROR", Message: "Python source has invalid syntax"}
	}
	entries := make([]*pyparser.FunctionDef, 0, 1)
	for _, statement := range module.Body {
		if function, ok := statement.(*pyparser.FunctionDef); ok && function.Name == "main" {
			entries = append(entries, function)
		}
	}
	if len(entries) != 1 || !validMainSignature(entries[0]) {
		return &SourceError{Code: "CODE_ENTRYPOINT_INVALID", Message: "Python source must define main(inputs)"}
	}
	for _, match := range importStatement.FindAllStringSubmatchIndex(source, -1) {
		module := capture(source, match, 1)
		if module == "" {
			module = capture(source, match, 2)
		}
		root := strings.Split(module, ".")[0]
		if _, allowed := allowedImports[root]; !allowed {
			line, column := lineColumn(source, match[0])
			return &SourceError{Code: "CODE_IMPORT_FORBIDDEN", Message: fmt.Sprintf("Python import %q is not in the sandbox allowlist", root), Line: line, Column: column}
		}
	}
	return nil
}

func validMainSignature(function *pyparser.FunctionDef) bool {
	args := function.Args
	return args != nil && len(args.PosOnly) == 0 && len(args.Args) == 1 && args.Args[0].Name == "inputs" &&
		args.Vararg == nil && len(args.KwOnly) == 0 && args.Kwarg == nil && len(args.Defaults) == 0
}

func capture(source string, match []int, group int) string {
	index := group * 2
	if index+1 >= len(match) || match[index] < 0 {
		return ""
	}
	return source[match[index]:match[index+1]]
}

func lineColumn(source string, offset int) (int, int) {
	line, column := 1, 1
	for index, value := range source {
		if index >= offset {
			break
		}
		if value == '\n' {
			line, column = line+1, 1
		} else {
			column++
		}
	}
	return line, column
}
