// Package dslcontract embeds versioned immutable runtime DSL contracts.
package dslcontract

import _ "embed"

//go:embed v1/schema.json
var schemaV1 []byte

func SchemaV1() []byte {
	return append([]byte(nil), schemaV1...)
}
