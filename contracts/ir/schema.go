// Package ircontract embeds versioned external IR contracts.
package ircontract

import _ "embed"

//go:embed v1/schema.json
var schemaV1 []byte

func SchemaV1() []byte {
	return append([]byte(nil), schemaV1...)
}
