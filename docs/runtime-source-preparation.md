# Bound runtime source preparation

New planning may freeze `execution.sourcePreparation` together with
`execution.sourceReuse`. The runtime accepts this opt-in only when the current
run, fence, context, task revision, complete lease set and immutable admission
agree with the applied controller planning intent. That intent binds the exact
proof, baseline receipt, source generation and incorporated artifact identities.
Worker-supplied JSON or a receipt alone grants no preparation authority.

`prepareBoundRuntimeWorkspace(input)` returns false only for a legacy admission
without the opt-in. Legacy admissions do not add database queries or inspection
RPCs. A bound unknown, partial, tampered or superseded preparation raises a typed
failure; it never enters legacy preparation or allocates another identity.

First dispatch requires an unchanged clone. Confirmed absence permits preparing
the same frozen UUID, followed by protected inspection of the resulting marker,
receipt and baseline. A retry requires the same frozen binding on every earlier
attempt, terminal history, current ownership and fresh physical stop evidence
for the immediate predecessor. Its modified candidate is retained; `prepare`
is never called on an existing retry clone. If the clone is absent, creation is
permitted only when every earlier attempt is recorded as not started without a
contradictory runtime/model/tool event, and the predecessor's physical proof also
confirms non-start. Missing history or any prior execution requires reconciliation.

`assertBoundRuntimePreparationCurrent(input)` runs at the end of preflight,
immediately before `worker.execute`. The in-process receipt belongs to the exact
runtime input object. Slow input materialization may expire its observations;
these are refreshed by read-only RPCs outside the admission transaction. The
final transaction checks projected identity metadata, lease ownership and proof
freshness only. It never reads source/artifact bodies or performs remote I/O.

The runtime preserves the current stopped-result and final-acceptance contracts.
This is not a cross-system transaction: the scheduler lease and protected remote
workspace manager remain the execution boundaries. Privileged controller/database
administrators are trusted, and historical admissions are not backfilled. The
bounded history check currently supports at most 64 prior attempts; larger or
incomplete histories require explicit reconciliation rather than implicit reset.
