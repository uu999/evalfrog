# OpenAPI contracts

`v1.yaml` is the first External HTTP/JSON contract for the M3 Definition
lifecycle. Human Web and Agent CLI call the same endpoints and bearer identity
model. Authoring requests accept IR only: there is intentionally no DSL upload
or Source Map override path.

M3 exposes compilation of an immutable Draft Test Snapshot, not a real Test
Run. The `TestDraft` Run endpoint is deliberately withheld until M5 persists
Runtime and Outbox/Inbox state.

`worker-v1.yaml` is the separate M7 internal protocol for Claim, Heartbeat,
Complete, lease-scoped Execution Context and ephemeral capacity registration.
It is not an authoring surface and does not expose PostgreSQL, Redis or Kafka.
