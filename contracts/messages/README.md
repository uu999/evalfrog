# Message contracts

M7 freezes two strict version-1 JSON envelopes:

- `task-v1.schema.json`: Scheduler Task Outbox to Worker; partition key is `attempt_id`.
- `runtime-event-v1.schema.json`: Runtime Outbox to Engine; partition key is `run_id`.

Both envelopes are limited to 64 KiB by the application. They contain only coordinates and wake-up metadata—never DSL, execution context, node output, connection credentials, or secrets. The consumer reloads authoritative facts after accepting a delivery.
