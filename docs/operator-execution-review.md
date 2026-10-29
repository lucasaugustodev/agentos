# Requesting an advisory review of an existing execution

`POST /api/objectives/:id/reviews` asks the existing ObjectiveReviewEngine to review one live Harness attempt. It does not send the submitted text to the worker as an instruction. The request becomes a durable signal with `source: "operator"`, an authenticated request event, and a bounded reference to an existing observer snapshot. The GLM reviewer decides whether to continue, guide, request input, or propose replanning at a safe boundary.

Use the existing administrative API token or authenticated operator session. Read-only credentials for `/api/operations/v1` cannot use this mutation. The operations capabilities endpoint therefore continues to advertise `request_intervention: false`.

New requests also require the actual consumer, `agentos-workers`, to have adopted the same published release as the API. Admission checks the module's current release identity, the existing rollout adoption event, matching worker readiness, and live observer/continuation ownership locks. A maintenance drain must permit that adopted worker lane. The separate backlog service is not this engine's consumer. Until those proofs match, the route returns `503 REVIEW_CONSUMER_NOT_READY` without registering a signal. Schema installation by the API never counts as consumer readiness. Replaying an existing request remains possible even when the consumer is unavailable.

Example with synthetic identities:

```json
{
  "requestKey": "consumer-review-1",
  "taskId": "22222222-2222-4222-8222-222222222222",
  "runId": "33333333-3333-4333-8333-333333333333",
  "runFence": "44444444-4444-4444-8444-444444444444",
  "contextVersion": "55555555-5555-4555-8555-555555555555",
  "expectedAttempt": 1,
  "expectedRevision": 2,
  "observationEventId": "12345",
  "reason": "The visible test reports a receipt mismatch. Review whether the write and replay paths hash the same fields, preserving the original assertion and scope."
}
```

Read the objective revision and exact execution binding before submitting. The existing authenticated task panel (`GET /api/panel/tasks/:id`) exposes persisted execution observations. `observationEventId` must identify the latest `EXECUTION_OBSERVED` event for that task and run; keep it as a decimal string, never a JavaScript floating-point number. The snapshot must be at most 180 seconds old and must not predate the bound context or execution start. A newer snapshot requires a new request with its current identity.

The body is limited to 8 KiB; `reason` is 20–1500 characters. It is an untrusted operator claim, not a grant of access or a verified diagnosis. Common secrets are rejected/redacted before text is persisted or included in the reviewer context. There is no arbitrary path fetch, URL fetch, shell command or embedded guidance field. Private diagnostic artifacts are not copied into the public repository by this feature.

The response identifies the review and its status URL. `requestOnly: true` means this POST only registered or replayed the request. It does not mean that the background reviewer has never sent guidance. Inspect `GET /api/objectives/:id/reviews` for the actual decision, action identity and receipt state.

- With review policy `off`, a new request is rejected.
- With `observe` (also the default), it is recorded without calling a model or guiding a worker.
- With `supervised`, it joins the existing review queue. The existing GLM-only policy, account/quota checks, shared bridge generation path and global request gate remain in control. This route does not create an extra model transport or increase the six-request ceiling.
- A paused/cancelled/completed goal, non-running task/run, requested cancellation, changed revision/attempt/fence/context, or an existing workforce owner rejects a new request.
- Identical concurrent/repeated `requestKey` and payload return the original receipt. Reusing the key with a different payload is a conflict. A replay after termination never requeues or targets another attempt.
- At model claim and before applying a judgment, identity, policy, provenance and evidence are checked again. New material invalidates the request; the old claim is not silently rebound to it.

Material comparison uses the set of completed tool returns and visible file/artifact metadata. Repeated identical reads, call IDs, call times, observer polling, process CPU/memory, trace modification time and model tokens do not count as new results. A new output, command, exit code, partial/truncated marker, workspace/runtime identity, file metadata or artifact evidence does. Running calls remain visible in the prompt but do not constitute completed results. Redacted actions do not disable the review of other visible evidence. The prompt states its omitted actions, redactions and per-snippet truncation; uncertainty permits `continue`, never forced stopping.

Guidance still requires a committed action intent followed by the existing fenced `worker.guide` and its receipt. An ambiguous receipt is probed rather than blindly resent. No request accepts a task, cancels an actor, widens its tools/write scope, resets its lease or bypasses existing publication/QA guards. Provider/gate failures retain the actor and retry only the judgment through the existing queue.

## Limits of this first increment

This does not automatically detect every stalled task, add an autonomous planner, change task decomposition or prove productivity. A snapshot's file metadata cannot prove unchanged file bytes. The prompt is bounded to the last four visible tool actions with explicit snippet limits; the reviewer should abstain when evidence is insufficient. Material comparison is conservative and may invalidate a request as an observation window changes.

The readiness gate does not restart an old consumer or interrupt its live execution to force adoption. If a guarded rollout is waiting for that actor, the manual review feature waits too. Development checkouts do not claim production release readiness; isolated tests inject synthetic release identity and exercise real database ownership/proof checks.

The existing one-review-owner rule includes `uncertain`, `queued_next_step` and `consumed`. A distinct request in those states is rejected with `OBJECTIVE_REVIEW_ALREADY_PENDING`; this increment does not invent a second owner or a post-consumption retry protocol. Replaying the original key remains a read of that review's identity. Automatic follow-up and safely replacing an already-consumed advisory require a separately validated transition.

## Validation

The new contract suite uses synthetic data. The HTTP/database suite exercises real PostgreSQL transactions in the repository's isolated test database, with a synthetic bridge transport. It covers authentication, stale bindings, concurrent idempotency, goal/policy changes, cancellation/new attempts, provenance tampering, evidence changes, repeated reads, provider refusal and ambiguous guidance receipts. Run it on an explicitly provisioned development server, alongside the existing objective-review and GLM-gate suites. Passing these tests is not evidence of live autonomous improvement; that still requires observing a real review, receipt and subsequent accepted delivery.
