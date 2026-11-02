# Physical termination evidence for native cycles

The Harness may confirm a sandbox stop and then throw its terminal error. Native
cycles previously retained the error but lost the successful stop receipt because
`worker.execute` had no result. This leaves `receipt` empty even when the remote
state already contains a physical stop proof.

The cycle engine now runs a separate, bounded evidence sweep for the latest failed
agent attempt in an attention step and occurrence. It requires the frozen launch
identity and the authenticated runner's `HARNESS_STARTED` event, then calls the
controller release's existing read-only Harness termination inspector over SSH.
No workspace report or model-supplied JSON can supply this proof.

The initial scope is a typed `HARNESS_SANDBOX_OOM`, or a `RUNTIME_EXITED` backed by
the same exact unit's typed systemd OOM record. Missing journal evidence remains
unconfirmed. The inspector verifies immutable state identity twice and fresh
process/cgroup state twice. `checkedAt` denotes the inspection time; it is not an
invented timestamp for the historical OOM.

Before committing, the controller compares the exact attempt, fence, launch
manifest, definitions, account and failure with the original claim. It appends one
receipt and one evidence event. The failure code, finished time, artifacts,
failed/attention states and existing memory allocation remain unchanged. A newer
attempt, pause, changed fence or existing receipt prevents adoption.

At most two inspections run concurrently outside worker dispatch. Failed
inspections back off persistently for one minute, then five, then thirty minutes.
Recovery of SSH or the inspector therefore remains discoverable without a busy
loop. Successful evidence is idempotent. Inspection attempts do not refresh
worker heartbeats.

This does not authorize an OOM retry, increase memory, accept QA, or reconstruct
missing artifacts. Native QA's binding-only retry contract remains intact.
Never-started, RPC and unconfirmed-stop failures keep their existing paths.
