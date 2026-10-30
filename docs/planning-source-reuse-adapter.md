# Accepted source authority adapter

`planning-source-reuse-adapter.ts` supplies the DB and runtime authority required
by the pure accepted-source-chain verifier. It does not prepare workspaces,
change tasks, accept requirements or publish releases.

## Controller-only interface

1. `captureSourceReuseAuthority(db, {goalId, revision, baseWorkspace})` obtains a
   complete bounded cohort in one SQL statement and therefore one MVCC snapshot.
   Call it before remote inspection, outside the admission lock. `baseWorkspace`
   is the logical plan scope; `rootWorkspace` is the unique actual prepared root
   derived from the source edges of the complete cohort. Missing execution
   metadata is a fallback, not a filter that silently drops a contributor.
2. Read the base using the protected `worker.inspectGeneration` operation and
   each contributor using `worker.inspectSourceBinding`. Fetch the two canonical
   artifact bodies by their captured IDs from the controller artifact store.
   These responses must originate in trusted controller transports. JSON shape
   validation and hashes do not authenticate model-supplied attestations.
3. `completeSourceReuseCapture` binds protected admission policy to the captured
   goal/task/run/fence/context, checks the current marker and candidate inventory,
   matches runtime-generated artifact hashes against the DB catalog, and calls
   the pure verifier. Physical stop must be fresh. Missing historical parent
   generation remains a fallback; no metadata is backfilled.
4. Immediately before preparing/applying, `recaptureSourceReuseAuthority` performs
   SQL-only revalidation under the admission lock. It compares the complete cohort,
   acceptances, dependencies, artifacts, frozen input identities, current execution,
   ownership and objective authority. It does not hash source trees or fetch remote
   artifacts under that lock. The existing immutable admission trigger protects
   the initially hashed instruction snapshot for an unchanged run; its enabled
   presence is required. The compact current manifest is checked on every capture.
5. The caller must separately retain the planner's planning-run/fence and new
   feedback guards. This adapter is scoped to source authority, not authorization
   to apply an obsolete planning decision. Generation-bound workspace preparation
   and post-copy inspection close the filesystem race; this adapter cannot do so
   by itself.

Catalog limits are 64 accepted contributors, 64 artifacts per contributor and
128 dependencies/frozen artifact references per contributor. Exceeding a bound
or encountering unknown workspace ownership returns a conservative reason.

## Deliberate first-version boundary

Only a single physical producer chain with generation-bound unchanged witnesses
qualifies. Parallel changed branches still return `SOURCE_BRANCHES`, even if a
later integration task exists. The existing runtime composition receipt contains
patch/report membership and before/after digests, but does not supply each producer's
generation, fence and context; it is not promoted into a merge certificate here.
An eligible reuse is a repair baseline and explicitly requires final verification.
Existing accepted artifacts remain available on fallback.

The PostgreSQL tests exercise real admission snapshot/metadata triggers and
transactional capture, then complete it with strict protected-inspector fixtures.
They do not pretend a fixture is a production runtime receipt or prove the bridge
deployment. Run them only against the isolated `agentos_test` database.
