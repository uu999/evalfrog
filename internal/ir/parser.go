package ir

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"
)

type ParseLimits struct {
	MaxDocumentBytes   int
	MaxDepth           int
	MaxStringBytes     int
	MaxNumberBytes     int
	MaxCollectionItems int
}

var DefaultParseLimits = ParseLimits{
	MaxDocumentBytes:   1 << 20,
	MaxDepth:           64,
	MaxStringBytes:     256 << 10,
	MaxNumberBytes:     256,
	MaxCollectionItems: 10_000,
}

type Parser struct {
	limits ParseLimits
}

func NewParser(limits ParseLimits) Parser {
	return Parser{limits: normalizeLimits(limits)}
}

func DefaultParser() Parser {
	return NewParser(DefaultParseLimits)
}

// ParseDraft accepts incomplete authoring content while enforcing the safe IR
// envelope: valid bounded JSON, no duplicate keys, known fields, and all four
// top-level IR members. Semantic completeness belongs to Validator.
func (p Parser) ParseDraft(data []byte) (Document, []Diagnostic) {
	if issue := inspectJSON(data, p.limits); issue != nil {
		return Document{}, []Diagnostic{issue.diagnostic()}
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var document Document
	if err := decoder.Decode(&document); err != nil {
		return Document{}, []Diagnostic{ErrorDiagnostic(
			PhaseParse,
			"IR_SHELL_INVALID",
			fmt.Sprintf("IR envelope is invalid: %v", err),
			Location{},
		)}
	}
	if err := requireEOF(decoder); err != nil {
		return Document{}, []Diagnostic{ErrorDiagnostic(PhaseParse, "JSON_SYNTAX_ERROR", err.Error(), Location{})}
	}
	if document.IRVersion == "" || document.Nodes == nil || document.Edges == nil || document.Layout == nil {
		return Document{}, []Diagnostic{ErrorDiagnostic(
			PhaseParse,
			"IR_SHELL_INVALID",
			"IR must contain ir_version, nodes, edges, and layout",
			Location{},
		)}
	}
	return document, nil
}

type parseIssue struct {
	code    string
	message string
}

func (p *parseIssue) Error() string {
	return p.message
}

func (p *parseIssue) diagnostic() Diagnostic {
	return ErrorDiagnostic(PhaseParse, p.code, p.message, Location{})
}

func inspectJSON(data []byte, limits ParseLimits) *parseIssue {
	if len(data) > limits.MaxDocumentBytes {
		return &parseIssue{code: "DOCUMENT_TOO_LARGE", message: "IR exceeds the document size limit"}
	}
	if !utf8.Valid(data) {
		return &parseIssue{code: "INVALID_UTF8", message: "IR must contain valid UTF-8"}
	}
	if !validUnicodeEscapes(data) {
		return &parseIssue{code: "INVALID_UNICODE", message: "IR contains an unpaired Unicode surrogate escape"}
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, limits, 0); err != nil {
		var issue *parseIssue
		if errors.As(err, &issue) {
			return issue
		}
		return &parseIssue{code: "JSON_SYNTAX_ERROR", message: err.Error()}
	}
	if err := requireEOF(decoder); err != nil {
		return &parseIssue{code: "JSON_SYNTAX_ERROR", message: err.Error()}
	}
	return nil
}

func validUnicodeEscapes(data []byte) bool {
	inString := false
	for index := 0; index < len(data); index++ {
		switch data[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			index++
			if index >= len(data) || data[index] != 'u' {
				continue
			}
			if index+4 >= len(data) {
				return false
			}
			code, err := strconv.ParseUint(string(data[index+1:index+5]), 16, 16)
			if err != nil {
				continue
			}
			index += 4
			if code >= 0xD800 && code <= 0xDBFF {
				if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
					return false
				}
				low, lowErr := strconv.ParseUint(string(data[index+3:index+7]), 16, 16)
				if lowErr != nil || low < 0xDC00 || low > 0xDFFF {
					return false
				}
				index += 6
			} else if code >= 0xDC00 && code <= 0xDFFF {
				return false
			}
		}
	}
	return true
}

func scanJSONValue(decoder *json.Decoder, limits ParseLimits, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}

	switch value := token.(type) {
	case json.Delim:
		if depth+1 > limits.MaxDepth {
			return &parseIssue{code: "JSON_TOO_DEEP", message: "IR exceeds the JSON nesting depth limit"}
		}
		switch value {
		case '{':
			seen := make(map[string]struct{})
			members := 0
			for decoder.More() {
				keyToken, keyErr := decoder.Token()
				if keyErr != nil {
					return keyErr
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key must be a string")
				}
				if len(key) > limits.MaxStringBytes {
					return &parseIssue{code: "JSON_STRING_TOO_LARGE", message: "IR contains an oversized object key"}
				}
				if _, exists := seen[key]; exists {
					return &parseIssue{code: "JSON_DUPLICATE_KEY", message: fmt.Sprintf("duplicate JSON object key %q", key)}
				}
				seen[key] = struct{}{}
				members++
				if members > limits.MaxCollectionItems {
					return &parseIssue{code: "JSON_COLLECTION_TOO_LARGE", message: "IR contains an oversized object"}
				}
				if err := scanJSONValue(decoder, limits, depth+1); err != nil {
					return err
				}
			}
			closing, closingErr := decoder.Token()
			if closingErr != nil {
				return closingErr
			}
			if closing != json.Delim('}') {
				return errors.New("unterminated JSON object")
			}
		case '[':
			items := 0
			for decoder.More() {
				items++
				if items > limits.MaxCollectionItems {
					return &parseIssue{code: "JSON_COLLECTION_TOO_LARGE", message: "IR contains an oversized array"}
				}
				if err := scanJSONValue(decoder, limits, depth+1); err != nil {
					return err
				}
			}
			closing, closingErr := decoder.Token()
			if closingErr != nil {
				return closingErr
			}
			if closing != json.Delim(']') {
				return errors.New("unterminated JSON array")
			}
		default:
			return errors.New("invalid JSON delimiter")
		}
	case string:
		if len(value) > limits.MaxStringBytes {
			return &parseIssue{code: "JSON_STRING_TOO_LARGE", message: "IR contains an oversized string"}
		}
	case json.Number:
		if len(value.String()) > limits.MaxNumberBytes {
			return &parseIssue{code: "JSON_NUMBER_TOO_LARGE", message: "IR contains an oversized number"}
		}
		if _, err := normalizeJSONNumber(value.String()); err != nil {
			return &parseIssue{code: "JSON_NUMBER_UNSUPPORTED", message: "IR contains a number outside canonicalization limits"}
		}
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("JSON contains more than one top-level value")
	}
	return err
}

func normalizeLimits(limits ParseLimits) ParseLimits {
	defaults := DefaultParseLimits
	if limits.MaxDocumentBytes <= 0 {
		limits.MaxDocumentBytes = defaults.MaxDocumentBytes
	}
	if limits.MaxDepth <= 0 {
		limits.MaxDepth = defaults.MaxDepth
	}
	if limits.MaxStringBytes <= 0 {
		limits.MaxStringBytes = defaults.MaxStringBytes
	}
	if limits.MaxNumberBytes <= 0 {
		limits.MaxNumberBytes = defaults.MaxNumberBytes
	}
	if limits.MaxCollectionItems <= 0 {
		limits.MaxCollectionItems = defaults.MaxCollectionItems
	}
	return limits
}
