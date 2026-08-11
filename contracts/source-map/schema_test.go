package sourcemapcontract_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"

	sourcemapcontract "github.com/uu999/evalfrog/contracts/source-map"
	"github.com/uu999/evalfrog/internal/compiler"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

func TestCompiledGoldenMatchesSourceMapV1SchemaAndCoverage(t *testing.T) {
	base := filepath.Join("..", "..", "internal", "compiler", "testdata")
	raw := readFile(t, filepath.Join(base, "all_operations.source-map.golden.json"))
	compilerSchema := jsonschema.NewCompiler()
	compilerSchema.Draft = jsonschema.Draft2020
	if err := compilerSchema.AddResource("https://evalfrog.dev/contracts/source-map/v1/schema.json", bytes.NewReader(sourcemapcontract.SchemaV1())); err != nil {
		t.Fatal(err)
	}
	schema, err := compilerSchema.Compile("https://evalfrog.dev/contracts/source-map/v1/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var generic any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&generic); err != nil {
		t.Fatal(err)
	}
	if err = schema.Validate(generic); err != nil {
		t.Fatalf("Source Map JSON Schema rejected compiler golden: %v", err)
	}
	var sourceMap sourcemap.Document
	if err = json.Unmarshal(raw, &sourceMap); err != nil {
		t.Fatal(err)
	}
	var document dsl.Document
	if err = json.Unmarshal(readFile(t, filepath.Join(base, "all_operations.dsl.golden.json")), &document); err != nil {
		t.Fatal(err)
	}
	if diagnostics := compiler.ValidateSourceMap(document, sourceMap); ir.HasErrors(diagnostics) {
		t.Fatalf("Source Map coverage rejected compiler golden: %+v", diagnostics)
	}
}

func TestEmbeddedSourceMapSchemaIsDefensive(t *testing.T) {
	first := sourcemapcontract.SchemaV1()
	first[0] = 'x'
	if sourcemapcontract.SchemaV1()[0] != '{' {
		t.Fatal("caller mutated embedded Source Map schema")
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
