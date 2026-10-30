# Authenticated source replanning

`POST /api/objectives/:id/feedback` accepts an optional strict `sourceReplan`
object from an API token or operator session. Ordinary textual feedback keeps
its existing behavior. Read-only operations credentials cannot use this route.

The request contains `idempotencyKey`, `description`, and:

```typescript
sourceReplan: {
  version: 1,
  expectedRevision: number,
  baseWorkspace: string, // original request's single registered framework root
  published: {
    kind: 'current_published_runtime',
    workspace: string,
    releaseDigest: string,
    registeredReceiptDigest: string
  },
  contributions: Array<{
    taskId: string; runId: string; acceptanceId: string;
    candidateArtifactId: string; candidateArtifactDigest: string;
    patchArtifactId: string; patchArtifactDigest: string;
  }>
}
```

The controller captures every accepted work/integration producer in exactly the
current revision. The caller's contribution list must match that complete set;
historical revisions cannot be added or substituted. Current accepted attempt,
fence, context, frozen execution, dependency references, acceptance identity,
artifact catalog and original request digest are pinned in the receipt.
Canonical candidate and patch bytes are checked against their stored hashes.
Source-parent relationships must agree with dependency and frozen input refs.
These artifacts remain inputs: their existence does not certify incorporation
onto the new baseline or retroactively establish a physical source generation.

The protected publication broker verifies the registered release and its
publication, validation and registration receipts. A separate protected source
inspection pins the actual source digest and generation. Release digest,
candidate digest and generation digest are distinct identities.

Registration rejects pause, cancellation, closed objectives, workforce ownership,
other active planning runs, source preparations, live/uncertain actors, literal
workspace leases, validation ownership and pending validation handoffs. A failed
objective may register the request; the existing planner alone transitions it to
replanning. No prior task, attempt, instruction snapshot or context is rewritten.

One feedback row owns the request. Concurrent identical requests return the same
receipt; a different payload under the same key conflicts. Replaying an old
receipt returns that receipt even after the revision or publication changes,
without enqueuing anything. Another key cannot create a second pending source
request. A consumed receipt cannot authorize another planning run.

The planner copies the receipt reference, canonical input manifest and exact
`sourceOverrides[originalBase]` into one new planning run. Admission and source
preparation recapture database authority. Before preparation, publication and
artifact bytes are checked again; a changed source or acceptance blocks the run
instead of silently adopting a different publication. Existing C3
`captureExplicitSourceAuthority` must classify the origin as `published_root`.
Normal durable preparation allocates new UUID destinations and new tasks. Its
receipt is required when applying the plan. Cross-baseline conflicts remain
explicit repair work, followed by independent verification of the original
requirements; no accepted-chain or final-acceptance claim is created here.

The compatible, idempotent migration adds nullable
`objective_feedback.source_replan`. Existing rows stay null. New source receipts
contain a typed payload, canonical hashes and an accompanying event. A trigger
prevents changing or deleting a source receipt and prevents attaching one to an
old text-only row. This is metadata on the existing feedback queue, not another
worker, queue or scheduler. Apply the migration only through the qualified normal
deployment; never patch production rows to manufacture authority.
