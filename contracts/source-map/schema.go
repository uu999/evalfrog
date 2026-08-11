// Package sourcemapcontract embeds versioned DSL-to-IR Source Map contracts.
package sourcemapcontract

import _ "embed"

//go:embed v1/schema.json
var schemaV1 []byte

func SchemaV1() []byte {
	return append([]byte(nil), schemaV1...)
}
