# Exclusive API model policy

The legacy version 1 `glm-only` policy is unchanged. Version 2 accepts only
`mode: "exclusive-model"` with `model: "deepseek-v4-pro-0813"`, an operator-owned
matching API account/bridge UUID pair, `requestLimit: 6`, and `sessionLimit: 12`.
The policy file is external configuration, never model or RPC input. Additional
fields, other models, mismatched accounts, or larger limits are rejected.

Install version 2 only after physical drain and reconciliation of existing
actors. This change does not implement an online transition or a grandfather
allowlist. Existing task sources, acceptance criteria, QA and attempts are not
rewritten. A preserved legacy task cannot silently acquire a DeepSeek route.
Create a new authorized occurrence for a new experiment. The historical five-game
GLM admission command explicitly refuses a policy mismatch before any effect.

The selected DeepSeek route uses the inspected OpenAI-compatible transport:
thinking enabled, high reasoning, 32768 worker and planner output tokens. The
planner allowance is scoped to the selected version 2 route: legacy incremental
and complete planners retain 8192 and 12000. Planner deadlines retain their
existing non-GLM values. DeepSeek
does not receive Z.ai's `clear_thinking` field. The selected aggregator route
advertises high/max effort; low/minimal requests map to high rather than assuming
the native API's low capability. Full `reasoning_content` survives
assistant tool and ordinary turns; telemetry reports actual serialized options
without prompt, credential or reasoning bytes. Protocol reference:
https://api-docs.deepseek.com/guides/thinking_mode/

The selected account shares the existing six-permit socket. Cancellation retains
its permit until transport/body teardown completes. The twelve-session ceiling
and workforce capacity count the selected account independently of model naming.
Manager authority still requires the configured coordinator account/model and
frozen management contract. A model name alone grants neither authority nor a
transport profile.

Fixtures exercise wire serialization, reasoning roundtrips, permit ownership,
SQL concurrency and preservation. They do not certify the upstream route,
credentials, tool support, throughput, or drain. Those require a sanitized canary
through the real authorized route and a separate rollout review. This source
change does not install a policy, restart a service or activate a provider.

An isolated full-context planner comparison exhausted 8192 high-effort tokens
without final JSON. A 32768 allowance produced schema-valid JSON, as did a faster
disabled-thinking probe, but their proposed changes differed. The default keeps
high reasoning; speed alone does not establish equivalent plans, task completion
or QA acceptance. No probe output was applied by this change.

## Installing the operator file into SQL authority

`scripts/install-model-policy.ts` is a root-only command for the existing global
worker-rollout window. The wrapper keeps `/var/lib/agentos/releases/publication.lock`
exclusively locked, installs `/etc/agentos/model-policy.json` while services are
stopped, and passes that open lock descriptor using `pass_fds`. Invoke the command
from the exact managed candidate with `--publication-lock-fd <fd>` and one JSON
object on stdin:

```json
{
  "version": 1,
  "token": "11111111-2222-4333-8444-555555555555",
  "candidate": "/opt/agentos/releases/managed-11111111-2222-4333-8444-555555555555",
  "digest": "<exact workerSourceDigest>",
  "expectedPolicyFileDigest": "<SHA256 of exact protected file bytes>",
  "expectedPreviousPolicyDigest": "<modelPolicyDigest of current SQL value, or null if absent>"
}
```

The descriptor must name the canonical lock and show a kernel `FLOCK WRITE`
reservation. The policy must be a bounded regular root-owned single-link file,
mode 0600 or 0640. The selected account must be connected and enabled, have the
exact bridge ID and model in its catalog, and have twelve configured sessions.
The CLI requires both source-bound schema receipts, the same global/restarting
drain token and release identity, all controllers physically stopped, and no
uncertain preparations, jobs, chats, leases or active queue jobs. It first requires
an empty six-slot gate, then owns all six administrative permits until the SQL
transaction finishes; this barrier makes no provider request.

Policy, `model-policy-install:<token>` receipt and `MODEL_POLICY_INSTALLED` event
commit together after recapture. Repeating the exact operation returns the same
receipt; mismatched previous policy, file, source, owner or completed receipt
fails closed. A lost reply requires reconciliation under that same window, never
a blind reset. Failure leaves maintenance active. The CLI never writes the
policy file, performs DDL, stops/starts services, or releases the drain.

This does not migrate existing managers or their workforce configuration. Legacy
tasks/runs and their QA remain bound; new occurrences may use v2. Any explicit
operator retry must use the existing reviewed retry path. Updating the configured
coordinator account/model for an old workforce needs a separate canonical
operation; this installer does not create one or claim autonomous legacy migration.
