package ir

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
)

type DataType string

const (
	TypeString  DataType = "string"
	TypeInteger DataType = "integer"
	TypeNumber  DataType = "number"
	TypeBoolean DataType = "boolean"
	TypeObject  DataType = "object"
	TypeArray   DataType = "array"
)

var allDataTypes = []DataType{
	TypeString,
	TypeInteger,
	TypeNumber,
	TypeBoolean,
	TypeObject,
	TypeArray,
}

var (
	minSafeInteger = big.NewInt(-9007199254740991)
	maxSafeInteger = big.NewInt(9007199254740991)
)

func AllDataTypes() []DataType {
	return append([]DataType(nil), allDataTypes...)
}

func (t DataType) Valid() bool {
	for _, candidate := range allDataTypes {
		if t == candidate {
			return true
		}
	}
	return false
}

// Compatible reports whether a value produced as source can be consumed as target.
// The only implicit promotion in IR v1 is integer to number.
func Compatible(source, target DataType) bool {
	return source == target || (source == TypeInteger && target == TypeNumber)
}

func DecodeLiteral(raw json.RawMessage) (any, DataType, error) {
	if raw == nil {
		return nil, "", errors.New("literal value is missing")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, "", err
	}
	if err := requireEOF(decoder); err != nil {
		return nil, "", errors.New("literal contains trailing JSON")
	}

	switch typed := value.(type) {
	case nil:
		return nil, "", errors.New("top-level null is not a supported IR type")
	case string:
		return typed, TypeString, nil
	case bool:
		return typed, TypeBoolean, nil
	case []any:
		return typed, TypeArray, nil
	case map[string]any:
		return typed, TypeObject, nil
	case json.Number:
		if mathematicallyIntegral(typed.String()) {
			return typed, TypeInteger, nil
		}
		return typed, TypeNumber, nil
	default:
		return nil, "", errors.New("unsupported JSON value")
	}
}

func mathematicallyIntegral(value string) bool {
	rational, ok := new(big.Rat).SetString(value)
	return ok && rational.IsInt()
}

func SafeInteger(raw json.Number) bool {
	rational, ok := new(big.Rat).SetString(raw.String())
	if !ok || !rational.IsInt() {
		return false
	}
	value := rational.Num()
	return value.Cmp(minSafeInteger) >= 0 && value.Cmp(maxSafeInteger) <= 0
}
