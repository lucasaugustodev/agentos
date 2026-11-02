# DeepSeek response ceiling

The exclusive DeepSeek V4 Pro 0813 route uses a maximum of 393,216 generated
tokens per provider response. Worker and OpenCode requests derive this value
from the same operator-owned profile. The legacy GLM route keeps 32,768; an
unverified account/model does not gain this profile. The global six-request
gate, twelve-session bound, cancellation and partial-response checks remain.

The previous profile forced 32,768. In a live failure the request reached that
exact output count and returned `finish_reason: length`; the truncated tool call
was correctly rejected. Raising the ceiling must never make an incomplete tool
call executable, nor turn the failed attempt into success.

[DeepSeek documents 393,216 as the maximum output parameter](https://api-docs.deepseek.com/api/create-chat-completion/).
[Verboo describes output limits as model-specific](https://code.verboo.ai/en/docs/api/chat-completions).
Its authenticated model catalogue confirmed this route and a 1,048,576-token
context, but did not advertise an output maximum. A short, tool-free probe on
2026-11-02 requested 393,216 through the existing shared gate and returned HTTP
200, `finish_reason: stop`, and 13 generated tokens. This proves parameter
acceptance only. It does not prove that the router will actually generate beyond
32,768 or eliminate every output-limit failure. Observe real task responses and
completion rates after adoption; do not generate a huge answer merely to fill
the allowance.

This is a response protocol ceiling, not a total task token budget. A larger
ceiling can increase response duration. Productive task decomposition, compact
reports and preserving artifacts still matter, and provider context limits
still apply.
