# Individual official observation failures

An official CLI invocation can exit successfully with JSON that does not identify
the requested job and board. The identity guard still rejects that response.
The controller contains only its own authenticated `OFFICIAL_IDENTITY`
diagnostic from that individual CLI call, with an exact expected job and board.
Transport, parsing and other validation errors remain errors. A corrupt local
terminal cache still fails closed.

The registered identity and authenticated dispatch receipt continue to reserve
the candidate's pipeline slot. The observation becomes
`status: observation_unavailable`, with no result, case count, result digest or
result file. It is not a replacement job, terminal failure, acceptance or reason
to repeat a CHECK. Other official jobs are observed and persisted normally.
The next scheduled observation may recover the same job through the existing
read path. Existing terminal results continue to use the authenticated cache.

Only bounded diagnostic metadata is published in `officialObservationFailures`.
Raw response bodies and stderr remain excluded. Volatile diagnostic timestamps
and response hashes do not enter the plan's semantic evidence. Repeated failed
observations therefore do not invalidate an otherwise identical read set;
recovering a related job's real result does. Capacity, ownership, official
quotas, dispatch intents and independent QA gates are unchanged.

Qualification is server-only with network disabled. Dedicated cases cover an
invalid first job followed by a valid terminal result, all jobs unavailable,
later recovery, exact CLI boundaries, local-cache corruption, stable source
proofs, preserved terminal timestamps and unknown intents, bounded metadata,
and no replacement POST.
