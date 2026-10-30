# Reusing a verified repair baseline

An accepted patch can remain available as an artifact while a later planning
revision starts from older source bytes. Asking another model to reconstruct
those changes wastes work and can reintroduce defects. The planner can instead
copy an existing accepted candidate when its complete lineage is provable.

The first version supports a single physical producer chain with unchanged
witnesses. It deliberately falls back for parallel changed branches, incomplete
catalogs, missing historical generation links, active owners and ambiguous
source ancestry. A later integration task alone does not prove that every
branch was incorporated. Fallback retains the existing recovery artifacts and
planning behavior. Explicit source overrides also keep their precedence.

## Controller flow

1. Capture the entire bounded accepted cohort for the previous revision. Read
   canonical artifact bytes and protected source/run inspections outside the
   admission transaction. Match the historical run policy, immutable context,
   acceptance, artifact catalog, generation and current candidate.
2. If the verifier selects a source, recheck current database authority and the
   planning fence/output/feedback, then reserve an intent in `settings` under
   `planning-source-preparation:<planning-run-id>:<base-index>`. The intent binds
   a fresh preparation UUID, exact digests and a unique destination. It is never
   created from a model-supplied proof or workspace preparation request.
3. Inspect that exact preparation before taking action. Confirmed absence can
   lead to preparation with the same UUID. A completed copy requires a matching
   protected receipt, marker, baseline and current inventory. An uncertain RPC
   does not permit a different source, a new UUID or legacy fallback.
4. Refresh producer stop/generation evidence and inspect the destination after
   copying. Before applying the plan, recheck the complete database projection,
   planning owner, new feedback and the oldest observation timestamp. No remote
   reads or source hashing occur while holding the admission lock.
5. Create tasks and mark the intent applied in one transaction. Root consumers
   point to the unique prepared baseline. Their future immutable instruction
   snapshots include its exact digest/generation and a separate preparation UUID
   for their own workspace. Descendants still obtain their actual producer's
   source through the existing dependency flow.

The new context lists the artifact IDs and digests already incorporated in the
baseline. Recovery reports remain available. Those references do not become new
dependency patches, grant permissions or advance requirement acceptance. Fresh
integration work and independent final QA remain mandatory.

## Interrupted operations

The controller reads an existing intent before considering any fallback. A
retry observes and reconciles the same operation; it does not assume that a lost
response means the copy failed. Transport outages and busy source locks can
retry observation. Changed authority, conflicting receipts and modified initial
destinations remain explicit failures for supervised resolution.

Preparation checks use the real oldest observation time. Copy duration and
heartbeats do not renew a physical-stop proof. The private workspace manager
checks and reserves both preparation UUID and destination under its locks.

This is source continuity, not acceptance continuity. No reuse operation changes
`verifiedBatch`, `incrementalAdvance`, final criteria or publication authority.
Measure benefit from a subsequent accepted delivery on the reused source, not
from an emitted reuse event or a count of active sessions.

## Validation boundaries

The pure verifier tests source chains and negative proofs. PostgreSQL tests use
real instruction/metadata triggers and exercise reservation, stale plans,
feedback changes, changed acceptance and lost RPC responses with strict mocked
runtime responses. Python/Node fixtures exercise the protected filesystem and
physical-stop contracts independently. A deployed worker trial is still needed
to establish the complete cross-process path and any latency reduction.
