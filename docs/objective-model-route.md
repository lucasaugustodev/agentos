# Explicit objective model-route transition

Changing the installed exclusive model policy does not rewrite existing objective requests or attempts. An operator can register a new planning request generation with the authenticated API once the objective has no live or uncertain execution.

1. `GET /api/objectives/:id/model-route` returns the exact `expectedGoalId`, `expectedRevision`, `expectedRequestDigest`, `expectedPolicyDigest` and `expectedGenerationId`, together with the destination from the installed version 2 policy. The file policy and database policy must agree, and the destination account must be connected and enabled.
2. `POST /api/objectives/:id/model-route` accepts those five expected values, an `idempotencyKey` and a `reason`. No destination account/model parameters are accepted. The operation changes only `plannerAccountId`, `plannerModel` and `allowedAccounts` in an append-only effective request generation. It does not enqueue work, resume a goal or accept a delivery.
3. Submit fresh feedback using the existing feedback API. For an AgentOS published-source replan, capture a new publication and all current accepted contributions, then register a fresh `sourceReplan`. Its receipt binds the route generation independently of the original source/request digest. Never reuse an earlier publication receipt merely because the route changed.
4. The next inserted planning run receives that generation. Claim, prompt construction, workspace preparation and plan application revalidate the same request/policy binding. New tasks inherit the effective `allowedAccounts`. Existing requests, planning runs, tasks, frozen attempts and acceptances retain their original identities and routes.

The endpoint requires the existing operator API token or operator session. Read-only operations credentials cannot invoke it; there is no agent tool or bridge RPC for this mutation. The schema is installed by the controlled migration, never by the endpoint or ordinary boot.

Paused/cancelled/completed goals, active planners, live runs or leases, pending source replans, unresolved native jobs, uncertain workspace preparations, enabled workforce coordination and draining maintenance windows prevent new transitions. Revision, original request, policy and prior generation must match the supplied preconditions. A different operation for an already-current route is rejected.

A retry with the same key and identical payload returns the immutable receipt with `activation: "none"` and `authority: "historical_receipt"`. `current: false` means the original preconditions no longer hold; the response cannot renew authority. A reused key with different payload is rejected. A planning run's generation cannot be changed, including from a legacy null value; an old retry never silently adopts the new account.

Workforce objectives require a separate explicit coordinator-binding transition. This operation intentionally refuses them and does not alter historical worker routes. It also does not confer publication, GPU submission or production access.
