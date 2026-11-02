# Official observation identity diagnostics

An official status reply with the wrong or missing job ID or board still raises
the exact `ValueError("OFFICIAL_IDENTITY")`. The controller remains blocked for
that observation. This change adds a bounded `officialObservationFailure` field
to the existing blocked status so an intermittent failure can be diagnosed from
the failing tick itself, rather than inferred from a later successful query.

The diagnostic distinguishes CLI responses, cached responses and injected client
responses. It records hashes and sizes, field presence and types, and only permits
16-character hexadecimal job IDs, the three known boards and known status values
as text. Unrecognized strings become hashes. Response bodies, arbitrary JSON keys,
stderr text, URLs, commands, credentials and environment variables are excluded.
Metadata is limited to 2 KiB. The CLI does not provide an HTTP status here, so that
field remains null. Cache hashes describe canonical JSON, not original CLI bytes.

The diagnostic also identifies the watcher source, canonical in-memory config and,
when readable through the existing protected-file guard, the policy at failure
time. That last pin is an observation at the failure boundary, not a claim about
an earlier request's authority. Failure to construct metadata never replaces the
original rejection. A successful response, transport error, parse error or
non-object response keeps its previous behavior. There is no new acceptance,
retry, cache update, official request or ledger transition.

## Qualification and activation boundary

Tests mock the CLI and use isolated controller fixtures. They cover hostile
strings and keys, large and nontext fields, cache versus CLI provenance, unchanged
error types, missing identities, unchanged successful replies, failed diagnostic
construction, blocked status publication, and preservation of cache and ledger
bytes. Run the complete Python fixture collection on an isolated development
server with no production environment or network.

Activation is a watcher-only deployment; it does not require a core release or
draining workers. The reviewed package must preserve the active config, control,
adapter, core, reservations, authority expiry and all effect records. Updating the
watcher code pin nevertheless changes the policy hash. Therefore the package must
qualify a new explicit jobs-observation namespace anchored to the current immutable
report. It must never reset or reuse the previous mutable index under a new policy.

The four compare-and-swap targets are the policy, watcher service and two CHECK
services. Pause only the watcher and CHECK timers, let their existing oneshots
finish, acquire the existing locks, verify preimages, install reviewed bytes and
reload units. Resume CHECK first and require a fresh status for the new policy
before resuming the watcher. No execution history, index or ledger is restored on
failure; ambiguous partial installation holds the timers for review. Preparing a
package is not authorization to activate it.
