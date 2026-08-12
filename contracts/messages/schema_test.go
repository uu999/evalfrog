package messages_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestMessageDTOsMatchVersionedJSONSchemas(t *testing.T) {
	task := eventing.TaskMessage{MessageVersion: 1, TaskID: "task", ProjectID: "project", RunID: "run", NodeRunID: "node-run", ExecutionNodeID: "node", AttemptID: "attempt", AttemptSequence: 1, ResourceClass: scheduling.ResourceSandbox, OccurredAt: time.Now().UTC(), TraceID: "trace"}
	runtimeEvent := eventing.RuntimeEvent{MessageVersion: 1, EventID: "event", ProjectID: "project", RunID: "run", AggregateType: eventing.WorkflowRunAggregate, AggregateID: "run", EventType: eventing.RunCreated, OccurredAt: time.Now().UTC(), TraceID: "trace"}
	for _, value := range []struct {
		file   string
		object any
	}{{"task-v1.schema.json", task}, {"runtime-event-v1.schema.json", runtimeEvent}} {
		raw, err := os.ReadFile(value.file)
		if err != nil {
			t.Fatal(err)
		}
		compiler := jsonschema.NewCompiler()
		compiler.Draft = jsonschema.Draft2020
		if err = compiler.AddResource(value.file, bytes.NewReader(raw)); err != nil {
			t.Fatal(err)
		}
		schema, err := compiler.Compile(value.file)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(value.object)
		if err != nil {
			t.Fatal(err)
		}
		var generic any
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()
		if err = decoder.Decode(&generic); err != nil {
			t.Fatal(err)
		}
		if err = schema.Validate(generic); err != nil {
			t.Fatalf("%s rejected DTO: %v", value.file, err)
		}
	}
}
