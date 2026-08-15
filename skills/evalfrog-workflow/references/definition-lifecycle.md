# Definition Lifecycle

Keep the authored full IR in a task file such as `workflow.ir.json`. The CLI
Workspace stores the latest Revision and IR for optimistic concurrency but is
not the server authority and currently has no `pull --out` flag.

Every write needs an explicit user confirmation and a unique semantic
idempotency key. Reuse the key only for a retry of the exact same request.

## Create a new Draft

```bash
evalfrog workflow create \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --name "Order normalizer" --ir ./workflow.ir.json \
  --idempotency-key "create-order-normalizer-001"
```

The command locally parses the IR before calling the API, creates a Workflow
and Draft Revision 1, then saves a local Workspace. Record the returned
Workflow UUID.

## Modify an existing Draft

```bash
evalfrog workflow pull \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --workflow "$WORKFLOW_ID"

# Read/modify the complete task IR file after Pull.
evalfrog draft push \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --workflow "$WORKFLOW_ID" --ir ./workflow.ir.json \
  --idempotency-key "update-order-normalizer-001"

evalfrog draft validate \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --workflow "$WORKFLOW_ID"
```

Push requires an existing Workspace for the Project/Workflow so always Pull
before the first update in a new Agent session. On `DRAFT_REVISION_CONFLICT`,
Pull the latest Draft, merge the minimal requested edit, and Push with a new
key. Do not overwrite the newer revision.

## Copy a Published Version

```bash
evalfrog workflow copy \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --source-workflow "$SOURCE_WORKFLOW_ID" --version 3 \
  --name "Order normalizer copy" \
  --idempotency-key "copy-order-normalizer-v3-001"
```

Copy creates a new Workflow and Draft from one immutable Published Version. It
does not mutate the source Workflow. Pull / modify / Push / Validate the copy
afterwards.

## Publish

```bash
evalfrog publish \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --workflow "$WORKFLOW_ID" --change-log "Explain the approved change" \
  --idempotency-key "publish-order-normalizer-v1-001"
```

Publish uses the Workspace Revision, performs authoritative validation and
compilation, creates an immutable Version, and automatically activates it.
Never claim a Version is published when this command fails or validation is not
valid.
