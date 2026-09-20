# Native protocol execution

Tasks are acquired through outbound polling. A native task carries a versioned
operation, original JSON and bounded protocol headers. Operations map only to
fixed paths on the configured local Ollama endpoint: `/v1/chat/completions`,
`/v1/messages`, or `/v1/responses`. Callers cannot supply an arbitrary target URL.

Messages/Responses discovery requires verified Ollama metadata at version 0.14.0
or later. Tools, vision and thinking are intersected with the model's metadata.
Discovery performs no inference. Capability changes require the server to accept
a replacement session; old sessions may finish their already-issued leases.

JSON fields, nulls, tool history and extensions survive task and result transport.
Safe protocol headers are forwarded, but platform keys/cookies are never passed
to Ollama. Bodies are bounded and redacted in diagnostic formatting. Retrying
result delivery never repeats inference.

Phase 3 supports non-streaming Chat, Messages and stateless Responses. Native
streaming and platform Responses state are later stages, not silently emulated
here. Known unsupported Ollama features are rejected rather than removed.
Cross-repository fixtures under `tests/fixtures` verify compatibility with
KeyCompute, including operation-specific usage and error response validation.
