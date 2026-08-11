package ir

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const MaxDecimalExponent = 10_000

func CanonicalizeDocument(document Document) ([]byte, error) {
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	return CanonicalizeJSON(encoded, DefaultParseLimits)
}

func CanonicalizeJSON(data []byte, limits ParseLimits) ([]byte, error) {
	if issue := inspectJSON(data, normalizeLimits(limits)); issue != nil {
		return nil, issue
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := appendCanonical(&output, value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func HashCanonical(canonical []byte) string {
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}

func CanonicalDocumentHash(document Document) ([]byte, string, error) {
	canonical, err := CanonicalizeDocument(document)
	if err != nil {
		return nil, "", err
	}
	return canonical, HashCanonical(canonical), nil
}

func appendCanonical(output *bytes.Buffer, value any) error {
	switch typed := value.(type) {
	case nil:
		output.WriteString("null")
	case bool:
		if typed {
			output.WriteString("true")
		} else {
			output.WriteString("false")
		}
	case string:
		encoded, err := marshalJSONString(typed)
		if err != nil {
			return err
		}
		output.Write(encoded)
	case json.Number:
		normalized, err := normalizeJSONNumber(typed.String())
		if err != nil {
			return err
		}
		output.WriteString(normalized)
	case []any:
		output.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := appendCanonical(output, item); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			encoded, err := marshalJSONString(key)
			if err != nil {
				return err
			}
			output.Write(encoded)
			output.WriteByte(':')
			if err := appendCanonical(output, typed[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("unsupported canonical JSON value %T", value)
	}
	return nil
}

func marshalJSONString(value string) ([]byte, error) {
	var output bytes.Buffer
	output.WriteByte('"')
	for _, current := range value {
		switch current {
		case '"':
			output.WriteString(`\"`)
		case '\\':
			output.WriteString(`\\`)
		case '\b':
			output.WriteString(`\b`)
		case '\f':
			output.WriteString(`\f`)
		case '\n':
			output.WriteString(`\n`)
		case '\r':
			output.WriteString(`\r`)
		case '\t':
			output.WriteString(`\t`)
		default:
			if current < 0x20 {
				output.WriteString(fmt.Sprintf(`\u%04x`, current))
			} else {
				output.WriteRune(current)
			}
		}
	}
	output.WriteByte('"')
	return output.Bytes(), nil
}

func normalizeJSONNumber(value string) (string, error) {
	if value == "" {
		return "", errors.New("empty JSON number")
	}

	negative := false
	if value[0] == '-' {
		negative = true
		value = value[1:]
	}

	exponent := 0
	if position := strings.IndexAny(value, "eE"); position >= 0 {
		parsed, err := strconv.Atoi(value[position+1:])
		if err != nil {
			return "", fmt.Errorf("invalid JSON number exponent: %w", err)
		}
		if parsed < -MaxDecimalExponent || parsed > MaxDecimalExponent {
			return "", fmt.Errorf("JSON number exponent exceeds %d", MaxDecimalExponent)
		}
		exponent = parsed
		value = value[:position]
	}

	fractionDigits := 0
	if position := strings.IndexByte(value, '.'); position >= 0 {
		fractionDigits = len(value) - position - 1
		value = value[:position] + value[position+1:]
	}
	digits := strings.TrimLeft(value, "0")
	if digits == "" {
		return "0", nil
	}
	scale := exponent - fractionDigits
	for strings.HasSuffix(digits, "0") {
		digits = strings.TrimSuffix(digits, "0")
		scale++
	}

	adjusted := len(digits) + scale
	var normalized string
	switch {
	case scale == 0:
		normalized = digits
	case adjusted > 0 && adjusted <= 21:
		if adjusted >= len(digits) {
			normalized = digits + strings.Repeat("0", adjusted-len(digits))
		} else {
			normalized = digits[:adjusted] + "." + digits[adjusted:]
		}
	case adjusted <= 0 && adjusted >= -5:
		normalized = "0." + strings.Repeat("0", -adjusted) + digits
	default:
		normalized = digits[:1]
		if len(digits) > 1 {
			normalized += "." + digits[1:]
		}
		normalized += "e" + strconv.Itoa(adjusted-1)
	}
	if negative {
		normalized = "-" + normalized
	}
	return normalized, nil
}
