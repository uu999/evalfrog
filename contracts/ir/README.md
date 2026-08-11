# IR Contract

`v1/schema.json` is the strict Test/Publish JSON Schema for the shared Human Web and Agent CLI authoring format. Draft Save uses the bounded Go parser and may persist structurally valid but semantically incomplete content; it does not weaken this strict contract.

`v1/fixtures/manifest.json` classifies versioned positive and negative examples used by both JSON Schema and Go contract tests. The schema, model, built-in Node Catalog descriptions, canonicalizer, and fixtures are checked together in CI.

IR deliberately contains no node contract version, operation version, Kafka topic, Worker routing, Lease, Retry, or Timeout author settings.
