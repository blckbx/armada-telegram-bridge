# ADR 0003: Direct Telegram Bot API port

Status: accepted

Date: 2026-10-22

## Context

Bridge orchestration must not depend on Telegram SDK response types. It needs
exact numeric route matching, stable sender identities, durable update-offset
handoff, explicit retry classification, and secret-safe diagnostics. Telegram
tokens appear in Bot API request paths, so propagating raw HTTP errors or URLs
would risk credential disclosure.

## Decision

- Define a narrow asynchronous `TelegramPort` for bot verification, read-only
  route verification, one bounded long poll, and plaintext sends.
- Use a small direct `reqwest 0.12.28` client with redirects disabled. Keep Bot
  API JSON types private to the adapter and convert inbound messages to the
  transport-neutral `BridgeEvent` model.
- Pass a relevant normalized event and its next update offset together to a
  `TelegramUpdateSink`. The adapter advances its working offset only after the
  sink confirms durable acceptance.
- Require strictly increasing update IDs in each response, preventing malformed
  batches from moving a cursor past unaccepted work.
- Match routes only by numeric chat ID and, when configured, exact topic ID.
  Ignore the bridge bot and other bots by default. Preserve numeric user IDs or
  sender-chat IDs independently from mutable names.
- Treat group-to-supergroup migration as a safety event: pause every affected
  in-process route, surface the old and proposed new chat IDs, and never rewrite
  configuration automatically.
- Classify `429` with `retry_after`, server errors, terminal client errors,
  timeouts, transport failures, and malformed responses separately.
- Enforce Telegram's 4,096-character text and 1,024-character caption limits.
  Include configured `protect_content` and topic ID on outbound messages.
- Discard remote error descriptions and underlying HTTP errors at the adapter
  boundary. Operator-visible errors and scripted request records contain no
  bot token or token-bearing URL.

## Consequences

The direct client exposes only the Bot API surface the bridge uses and avoids
SDK types leaking into later routing parts. New methods, attachments, and rich
entities must be added deliberately behind this port in P08 and P09.

The working poll offset is not an independent persistence mechanism. P06 will
connect the sink's event-plus-offset acceptance to the P03 transaction and seed
the adapter from the committed cursor after restart.

Migration pause state must likewise be connected to P03 route state when the
service orchestration lands. Until an operator changes the configured numeric
chat ID, the adapter neither receives from nor sends to a migrated route.

## Test evidence

The reusable port contract passes against both the in-memory fake and the real
HTTP adapter backed by a loopback scripted server. Focused tests cover bot-ID
verification, exact scope and loop filtering, user/anonymous/sender-chat
identity, durable offset rejection, malformed ordering, error classification,
migration pausing, outbound limits, `protect_content`, and secret-free request
records.

On 2026-10-22, the ignored read-only live smoke test passed against the
operator-created bot and every configured private Telegram group. `getMe`
matched the configured bot ID and `getChat` returned each exact configured chat
ID. The test sent no message and changed no Telegram state.
