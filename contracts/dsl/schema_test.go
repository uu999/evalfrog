package dslcontract_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"

	dslcontract "github.com/uu999/evalfrog/contracts/dsl"
	"github.com/uu999/evalfrog/internal/dsl"
)

func TestCompiledGoldenMatchesDSLV1SchemaAndRuntimeContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "compiler", "testdata", "all_operations.dsl.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020
	if err = compiler.AddResource("https://evalfrog.dev/contracts/dsl/v1/schema.json", bytes.NewReader(dslcontract.SchemaV1())); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("https://evalfrog.dev/contracts/dsl/v1/schema.json")
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
		t.Fatalf("DSL JSON Schema rejected compiler golden: %v", err)
	}
	var document dsl.Document
	if err = json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if issues := dsl.BuiltinV1Contract().Validate(document); len(issues) != 0 {
		t.Fatalf("runtime DSL contract rejected compiler golden: %+v", issues)
	}
	if issues := dsl.BuiltinV1Compatibility().CheckAll(document); len(issues) != 0 {
		t.Fatalf("runtime compatibility rejected compiler golden: %+v", issues)
	}
}

func TestEmbeddedDSLSchemaIsDefensive(t *testing.T) {
	first := dslcontract.SchemaV1()
	first[0] = 'x'
	if dslcontract.SchemaV1()[0] != '{' {
		t.Fatal("caller mutated embedded DSL schema")
	}
}
