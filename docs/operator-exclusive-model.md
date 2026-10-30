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
thinking enabled, high reasoning, 32768 worker output tokens and 8192 planner
output tokens. Planner deadlines retain their existing non-GLM values. DeepSeek
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
