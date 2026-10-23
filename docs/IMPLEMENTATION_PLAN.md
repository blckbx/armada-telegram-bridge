# Armada Telegram Bridge — Implementation Plan

Status: proposed

Last researched: 2026-10-22

## 1. Outcome

Build a self-hosted bridge that joins one Armada channel as a normal Concord
identity and one Telegram group as a Bot API bot. It relays new text and files
in both directions, identifies the original sender, preserves formatting where
the two systems overlap, and survives restarts without intentionally replaying
or losing acknowledged messages.

Account creation and introduction are deliberately outside the service. An
operator creates the Armada Nostr keypair, manually accepts the Armada
invitation, creates and adds the Telegram bot, and manually supplies both
secrets and route IDs. Normal bridge startup only validates and uses that
already-provisioned state.

The first release should support one statically configured route:

```text
(Armada community ID, Armada channel ID) <-> (Telegram chat ID, optional topic ID)
```

The data model and configuration should allow multiple independent routes
later. The implementation must never infer a route from a display name.

## 2. Research conclusions

### 2.1 Armada is a Concord client, not a conventional bot API

Armada's default private-community architecture is
[Concord](https://concordprotocol.org/spec/cord-01/), implemented over Nostr
relays. A bot is an ordinary Nostr identity that is invited to a community and
given the keys needed to read and write its channels. It must therefore:

- own a persistent Nostr keypair;
- load the membership and channel keys produced by the operator-accepted
  Concord invitation;
- maintain encrypted community state and key rotations;
- unwrap events and verify the signed inner author before displaying a sender;
- publish valid Concord messages as the configured bridge identity; and
- upload and download encrypted attachments through Blossom servers.

The public Nostr author of a Concord stream event is a derived stream author,
not the human sender. Attribution must come from the authenticated inner event.
Using the outer event author would misidentify every message and create a
spoofing risk.

Concord public channels derive from the community root key. Private channels
use independent channel keys. A direct invite can include only selected private
channel keys, but possession of the community root also grants access to all
public channels. Application-level exact channel filtering is required even
when the bot is invited only for one intended route.

### 2.2 Use the supported Rust SDK

Use the MIT-licensed [`vector_sdk`](https://docs.rs/vector_sdk/latest/vector_sdk/)
as the Concord implementation instead of recreating CORD event formats and key
rotation logic. It exposes bot event handlers, channel send methods, history
cursors, profiles, invitation handling, attachment transfer, edits, deletions,
reactions, and typing operations. Its current bounded attachment API can enforce
the actual decrypted byte count rather than trusting remote metadata.

The bridge will intentionally not expose the SDK's identity generation or
automatic invitation-acceptance capabilities during normal operation.

Pin the exact SDK version and commit `Cargo.lock`. At implementation start,
complete an interoperability spike against the current Armada release before
settling the version. The Concord site currently points bot authors at the SDK,
while the published crate may move faster than the website.

Do not copy Armada client code into this repository: Armada is AGPL-licensed,
while this repository is MIT-licensed. Protocol behavior can be tested against
Armada; implementation should use the SDK and independently written bridge
code.

### 2.3 What to reuse from `blckbx/armada-bot`

The existing [Armada OpenClaw bot](https://github.com/blckbx/armada-bot) is a
useful operational reference, but not a transport base. It handles NIP-17
one-to-one direct messages; this bridge needs Concord community streams.

Reuse its design patterns:

- ignored dotenv support and redacted configuration output;
- verification of the authenticated inner sender;
- replay/startup gates;
- explicit relay readiness and reconnect handling;
- bounded media, rate limits, and queue limits;
- SSRF-resistant remote media access; and
- sanitized structured logs.

Do not reuse its NIP-17 message flow, kind-10050 relay discovery, single-owner
authorization model, or OpenClaw plugin lifecycle.

### 2.4 Telegram constraints shape the bridge

Telegram's [Bot API](https://core.telegram.org/bots/api) is an HTTPS API with
long polling or webhooks. Start with long polling because it needs no public
endpoint. Telegram retains updates for no longer than 24 hours, so local durable
state is still necessary.

The bot must either have privacy mode disabled through BotFather or be an admin
with the permissions needed to receive ordinary group messages. Being invited
to a group alone is insufficient. The bridge must ignore messages authored by
itself and, by default, all other bots.

Relevant hosted Bot API limits include:

- downloaded files are limited to 20 MB;
- new documents are generally limited to 50 MB;
- text messages are limited to 4,096 characters after entity parsing;
- captions are limited to 1,024 characters after entity parsing; and
- rate limiting is dynamic; `429 retry_after` must always be honored. Telegram's
  [bot FAQ](https://core.telegram.org/bots/faq) advises staying near one message
  per second in a single chat and no more than 20 messages per minute in a group.

Set the MVP's shared file limit to 20,000,000 bytes so a file accepted in either
direction can traverse the hosted Telegram API. A local Bot API server can be a
later deployment option for larger files.

Use `teloxide` for update dispatch and common API calls, behind a small
`TelegramPort` trait. Keep a direct Bot API escape hatch because the library's
supported Bot API version can lag Telegram's current version. Use explicit
message entities for outbound formatting rather than raw MarkdownV2.

### 2.5 Compatibility summary

| Capability | Armada / Concord | Telegram | Bridge consequence |
| --- | --- | --- | --- |
| Bot identity | Operator-provided Nostr keypair with an accepted invitation | Operator-created BotFather bot and token | Validate two supplied identities; never create or equate them |
| Room scope | Community stream/channel keys | Numeric chat and optional forum topic | Route only exact immutable IDs |
| Sender identity | Verified author inside encrypted event | Numeric user/sender-chat ID | Render names, authorize by stable IDs |
| Text format | Discord-flavored Markdown in message content | Text plus UTF-16 message entities | Convert through a neutral rich-text model |
| Attachments | Client-side encryption plus Blossom | Bot API file IDs/download and multipart upload | Stream through bounded temporary storage |
| History | Relay queries and SDK cursors | Updates retained for at most 24 hours | Maintain local durable cursors and outbox |
| Edits/deletes | Concord event operations | Bot can mutate messages it sent | Use persisted cross-system message links |
| Key/access change | Channel/community rotations and refounding | Token revocation and chat permissions | Treat either loss of access as route-not-ready |
| Delivery guarantee | Relay/event replication | Request/response without a general idempotency key | Offer at-least-once delivery and disclose rare duplicates |

## 3. Privacy and trust model

This bridge is a trusted endpoint in both rooms, not end-to-end encryption
between them:

```text
Armada members
    | Concord encrypted events
    v
Bridge: decrypts, queues, reformats, and re-encrypts
    | Telegram Bot API over TLS; Telegram can read the copy
    v
Telegram group
```

Operators must state this clearly before inviting the bridge. Members of both
rooms should consent to cross-posting. Telegram's `protect_content` can hinder
casual forwarding and saving, but it is not encryption and must not be described
as such.

The Concord secret key, Telegram token, SDK state, attachment plaintext, and
durable outbox are all high-impact secrets. Compromise of the bridge can expose
both current traffic and any history accessible to its Armada account. Incident
response requires revoking the Telegram token, removing the Armada identity,
and rotating/refounding Concord keys where appropriate.

## 4. Proposed architecture

Use one Rust/Tokio process with clear adapters and a durable relay core:

```text
 Concord relays / Blossom                 Telegram Bot API
            |                                     |
     Concord adapter                       Telegram adapter
            |                                     |
            +------ normalized inbound events ---+
                                  |
                          route + policy engine
                                  |
                   formatter / attachment service
                                  |
                      SQLite inbox + outbox
                                  |
                   per-route ordered dispatchers
```

Recommended components:

- `ConcordAdapter`: owns `vector_sdk`, accepted membership state, history
  cursors, profile lookup, message publication, and encrypted attachment
  operations.
- `TelegramAdapter`: owns long polling, upload/download, chat migration handling,
  entity conversion, and rate-limit responses.
- `Router`: performs exact scope checks, sender labeling, loop prevention,
  policy checks, and creation of destination jobs.
- `Store`: SQLite in WAL mode for inbox deduplication, outbox jobs, mappings,
  cursors, cached profiles, and route state.
- `Dispatcher`: one ordered worker per route and direction, with bounded queues,
  exponential backoff plus jitter, and dead-letter state.
- `AttachmentService`: streams files through private temporary storage, verifies
  size/type, and always removes plaintext after processing.
- `Diagnostics`: readiness, health, metrics, redacted logs, and a `doctor`
  command.

Only one process may own a data directory. Take an advisory instance lock at
startup so two replicas cannot poll Telegram or race Concord state.

## 5. Manual onboarding and configuration

Keep routing in a versioned TOML configuration and the two credentials in an
ignored, mode-`0600` `.env` beside it. Never require raw secrets on a process
command line.

The bridge has no account-creation wizard and no automatic introduction mode.
It must not generate a Nostr keypair, create a Telegram bot, accept a Concord
invitation, add itself to a Telegram group, or select a room. Those actions are
performed and reviewed by the operator before the bridge starts.

Example:

```toml
version = 1
data_dir = "/var/lib/armada-telegram-bridge"

[nostr]
expected_npub = "npub1..."

[telegram]
expected_bot_id = 123456789
mode = "long_polling"
protect_content = true
relay_other_bots = false

[[routes]]
name = "private-community"
community_id = "<canonical Concord community ID>"
channel_id = "<canonical Concord channel ID>"
telegram_chat_id = -1001234567890
# telegram_thread_id = 42

[limits]
max_text_bytes = 60000
max_file_bytes = 20000000
max_pending_jobs = 500
max_catchup_age_hours = 24
max_catchup_events = 1000
```

### 5.1 Armada setup — performed manually

1. Generate a dedicated Nostr keypair outside the bridge. Record its `npub` and
   keep its `nsec` in the operator's secret-management workflow.
2. Use Armada or another compatible Concord client with that identity and
   manually accept the invitation to the intended community and private
   channel. Review the community and channel before accepting.
3. Record the canonical community ID, channel ID, and required relay settings.
   Do not use community or channel display names as route identifiers.
4. Put the `nsec` in `ARMADA_NSEC` in the ignored, mode-`0600` `.env` and
   configure `expected_npub` in TOML. Never commit, print, pass the secret on
   the command line, or serialize it into SQLite.
5. Start the bridge only after invitation acceptance has completed. The bridge
   reconstructs the accepted community state from the identity's relay-backed
   state and verifies that it can read and write the configured channel.

The interoperability spike must prove step 5 with the current Armada client and
`vector_sdk`. If accepted membership or channel keys cannot be recovered from
relay-backed account state, define a narrow encrypted state export/import
handoff. Do not solve that gap by adding automatic invite acceptance.

### 5.2 Telegram setup — performed manually

1. Create the bot manually with BotFather and retain the issued authentication
   token in the operator's secret-management workflow.
2. Disable group privacy or make the bot an administrator with the minimum
   permissions needed to receive ordinary messages and post files.
3. Add the bot manually to the intended private group.
4. Determine and review the immutable numeric chat ID and, when applicable, the
   forum topic ID. Enter them explicitly in the route configuration; the bridge
   does not auto-discover or auto-select a chat.
5. Put the token in `TELEGRAM_BOT_TOKEN` in `.env` and configure
   `expected_bot_id` in TOML. Never commit, log, or put the token on the command
   line.

### 5.3 Startup validation

Run `bridge doctor`, then start normal relay mode. `doctor` is read-only and
validates:

- `.env` presence and permissions without printing either secret;
- the derived Nostr `npub` against `expected_npub`;
- relay connectivity and recovery of the already-accepted Concord membership;
- exact community/channel existence plus read and write access;
- Telegram `getMe` identity against `expected_bot_id`;
- membership and permissions in the configured numeric chat/topic;
- SQLite/data-directory safety and configured resource limits.

Any mismatch is a hard failure. The bridge must not create or replace external
identity credentials, accept pending invitations, change BotFather settings,
add itself to rooms, or rewrite route IDs as a recovery action.

Telegram group-to-supergroup migrations emit a new numeric chat ID. Pause the
route, record the migration atomically, and require an operator confirmation (or
an explicit opt-in policy) before resuming. A title or username is not a safe
replacement for a numeric chat ID.

## 6. Canonical message and identity model

Normalize both transports into a small internal event model:

```rust
struct BridgeEvent {
    source: Source,             // Concord or Telegram
    source_event_id: String,    // immutable, namespaced ID
    route_id: RouteId,
    sender: SenderIdentity,
    timestamp: DateTime<Utc>,
    body: RichText,
    attachments: Vec<AttachmentRef>,
    reply_to: Option<SourceMessageRef>,
    mutation: Mutation,         // create, edit, delete, expire
}
```

`SenderIdentity` must separate stable identity from mutable presentation:

- Armada stable identity: verified inner Nostr public key.
- Telegram stable identity: numeric user ID, with numeric sender-chat ID for
  anonymous administrators or channel-originated posts.
- Display names and usernames are untrusted decoration and never authorization
  inputs.

Destination messages should visibly identify their origin:

```text
⛵ Alice (npub1abc…xyz)
Hello from Armada

✈️ @bob (tg:123456789)
Hello from Telegram
```

Use the best available name, but always include a short stable identifier.
Normalize newlines and strip or visibly escape bidi overrides, zero-width
controls, and other characters that could forge the attribution header. Do not
allow sender-controlled text to become a clickable fake header.

## 7. Routing, loops, durability, and ordering

Every inbound event passes all of these checks before enqueueing:

1. It comes from the exact configured community/channel or Telegram chat/topic.
2. Its authenticated author is not the bridge's Armada key or Telegram bot ID.
3. Its immutable, transport-namespaced source ID is not in `inbox_events`.
4. It is a supported event type and within configured limits.
5. It is not older than the bounded catch-up policy.

Persist the inbox decision and destination job in one SQLite transaction. A job
is successful only after the destination API returns a canonical message/event
ID, which is stored in `message_links`. On restart, retry pending jobs.

This yields at-least-once delivery. Telegram does not provide a general client
idempotency key, so a process crash after Telegram accepts a send but before the
transaction commits can produce a rare duplicate. Document that limitation and
include a short source marker in stored metadata for diagnosis; never expose a
secret marker to users.

Process each route and direction in source order, while allowing independent
routes/directions to proceed concurrently. Bound every queue. When it fills,
stop acknowledging/cursor advancement where the source permits it and make
readiness fail; do not silently drop old messages.

Startup behavior:

- First-ever startup records a baseline and relays only new messages. It must
  not dump existing Armada or Telegram history into the other room.
- Normal restart resumes from committed cursors and catches up within both a
  count and age bound.
- If a cursor is irrecoverably stale, pause the route and require a documented
  operator resync choice.

The outbox may temporarily contain message plaintext. Encrypt sensitive payload
columns with XChaCha20-Poly1305 using a separate local spool key, restrict both
database and key to the service user, and erase completed payloads after the
retention window. Database encryption does not replace full-disk encryption.

## 8. Formatting conversion

Parse inbound text into a small rich-text AST/spans, then render for the other
system. Never transform by a chain of regular-expression replacements.

| Meaning | Armada input | Telegram output |
| --- | --- | --- |
| Bold | `**text**` | bold entity |
| Italic | `*text*` | italic entity |
| Underline | `__text__` | underline entity |
| Strike | `~~text~~` | strikethrough entity |
| Spoiler | `||text||` | spoiler entity |
| Inline code | backticks | code entity |
| Code block | fenced code | pre entity, language when valid |
| Link | Markdown link/autolink | text-link entity |
| Quote | blockquote | blockquote entity or prefixed text |
| Headings/lists/rules | Discord-style Markdown | readable plaintext approximation |

Telegram inbound entities should be the primary source of meaning; render them
as escaped Armada Markdown. For Armada outbound, parse Armada's Discord-flavored
Markdown and send Telegram text with explicit entity offsets. Telegram offsets
are UTF-16 code units, so calculate offsets after final attribution and escaping
and test with emoji, combining marks, RTL text, and astral Unicode.

Unsupported or ambiguous formatting becomes plain text. Links must retain a
visible, safe scheme; reject control characters and unsafe schemes. Split long
messages on paragraph/grapheme boundaries while maintaining valid entities and
add `part 1/N` markers. Formatting fidelity must never cause message loss.

## 9. Attachment flow

### 9.1 Telegram to Armada

1. Accept supported photo, document, audio, video, voice, animation, and sticker
   metadata only from the configured chat/topic.
2. Reject declared sizes above the route limit before download.
3. Stream `getFile` content into a mode-`0600` temporary file while enforcing
   the actual-byte limit; never buffer the whole file in memory.
4. Sanitize the filename, sniff MIME from bytes, and keep claimed MIME only as a
   hint. Do not execute, unpack, or render server-side active content.
5. Ask `vector_sdk` to encrypt and upload the attachment to configured/selected
   Blossom servers and publish its Concord attachment metadata.
6. Include the sender attribution and Telegram caption. The initial SDK spike
   must prove that file messages can carry a caption; if the public API cannot,
   add an upstreamable SDK method or send a linked text message plus file.
7. Remove plaintext on success and on every failure path.

### 9.2 Armada to Telegram

1. Obtain attachment metadata only from a verified, decrypted Concord message.
2. Use the SDK's bounded download/decrypt/integrity path with the configured
   actual-byte maximum. Preserve its SSRF and Blossom fallback protections.
3. Stream to a private temporary file and classify it from bytes.
4. Select `sendPhoto`, `sendVideo`, `sendAudio`, `sendVoice`, or `sendDocument`
   only when compatible; fall back to `sendDocument` rather than failing.
5. Put attribution and the original caption in the Telegram caption when it
   fits; otherwise send an ordered companion text message.
6. Set `protect_content = true` by default and delete the plaintext temporary
   file on all paths.

Use randomized internal filenames and a separately sanitized user-facing name.
Defend every outbound URL against localhost, private/link-local ranges, DNS
rebinding, redirects to forbidden addresses, non-HTTP schemes, excessive
redirects, timeouts, decompression bombs, and declared/actual size mismatch.
Where the SDK owns fetching, keep its protections enabled and add regression
tests rather than duplicating the fetcher.

Telegram albums arrive as separate updates sharing a `media_group_id`. For the
MVP, persist and relay each item in update order with a common `album 1/N` label
when the group can be assembled within a short bounded window. If the group is
incomplete, release the available items rather than blocking the route. A later
version may map albums to a native multi-attachment Armada message if the SDK
and current client interoperate on that representation.

## 10. Replies, edits, deletion, reactions, and expiration

Store mappings in both directions:

```text
(route, concord_event_id) <-> (telegram_chat_id, telegram_message_id)
```

- Replies: map only when the referenced destination message exists. Otherwise
  include a compact plaintext quote. Confirm Armada's expected reply event
  shape during the interoperability spike: the SDK's generic threaded reply
  operation may not be identical to Armada's inline quote behavior.
- Edits: an original source edit may update the bridge-owned destination mirror.
  Never attempt to edit messages owned by a human or another bot.
- Deletion: source-author deletion may delete the bridge-owned mirror when the
  source semantics are authenticated and unambiguous. Moderator deletion policy
  is a product decision before enabling it.
- Expiration: if Concord exposes an expiry time, schedule deletion of the
  Telegram mirror on a best-effort basis and disclose that Telegram or users may
  retain copies.
- Reactions: defer them. Mirroring through one bot falsely implies that the bot
  reacted, and reaction storms can overwhelm group rate limits.
- Typing/presence/read receipts: do not bridge them.

MVP may launch with new text and file messages only, but the schema must retain
message links so replies and mutations can be added without migration pain.

## 11. Safety and abuse controls

- Exact allowlists for community ID, channel ID, chat ID, and optional topic ID.
- Ignore the bridge identities and other bots by default.
- Per-user, per-route, and global token-bucket limits; destination-specific
  Telegram limits are stricter than ingress limits.
- Bounded text, attachment count, file bytes, download duration, total temporary
  disk, queue depth, retry count, and retention.
- Exponential backoff with jitter for relays, Blossom, and Telegram; honor
  Telegram `retry_after` exactly.
- Circuit breakers that pause one unhealthy route without blocking another.
- Safe rendering of Unicode, filenames, URLs, logs, and metrics labels.
- Never log `nsec`, bot token, Concord keys, decrypted attachment URLs/keys,
  complete private message bodies, or raw Telegram update payloads.
- Log stable IDs only in shortened or keyed-hash form unless debug logging is
  explicitly enabled for a short window.
- Optional malware scanning should be an isolated hook with timeout and size
  limits, not an implicit promise that all files are safe.

## 12. Operations and observability

Provide:

- `/live`: process/event-loop health only;
- `/ready`: false when secrets, database, Concord state, configured route, or
  Telegram connectivity is unusable, or when queues are saturated;
- Prometheus counters/gauges for received, relayed, retried, rejected, duplicate,
  queue depth, oldest-job age, reconnects, bytes, and route readiness;
- structured JSON logs with route and correlation IDs but no content;
- `bridge doctor`, `bridge status`, `bridge retry <job>`, and
  `bridge quarantine <route>` operator commands; and
- graceful shutdown that stops intake, checkpoints cursors, drains for a bounded
  period, then leaves remaining jobs durable.

Ship a rootless container and a hardened systemd example. Run as a dedicated
unprivileged user with a read-only root filesystem, private temporary directory,
minimal writable data directory, dropped capabilities, and no Docker socket.
Back up encrypted state only with a documented restore test; old backups retain
old secrets and need a destruction policy.

## 13. Repository layout

```text
Cargo.toml
src/
  main.rs
  config.rs
  domain.rs
  router.rs
  concord.rs
  telegram.rs
  formatting.rs
  attachments.rs
  store.rs
  dispatch.rs
  diagnostics.rs
migrations/
tests/
  fixtures/
  integration/
deploy/
  Dockerfile
  compose.yaml
  armada-telegram-bridge.service
docs/
  IMPLEMENTATION_PLAN.md
  OPERATIONS.md
  PRIVACY.md
```

Keep transport clients behind traits so unit tests use deterministic fakes and
so Bot API/SDK upgrades do not leak throughout the domain model.

## 14. Test-first delivery backlog

The executable roadmap is maintained in
[`IMPLEMENTATION_PARTS.md`](IMPLEMENTATION_PARTS.md). It divides the work into
stable parts P00–P12. Each part defines:

- its dependencies and externally visible outcome;
- the failing tests to write before production code;
- the minimum implementation scope;
- deterministic versus credential-gated live tests; and
- an objective completion gate.

The order retires the highest protocol risk early: P01 proves that
`vector_sdk` can recover an Armada membership accepted manually by the operator.
The first end-to-end plaintext bridge lands in P06, files complete the core MVP
in P09, and P12 is the release gate.

## 15. Verification strategy

Unit and property tests:

- exact route matching and every loop-prevention rule;
- stable sender labels, spoofing controls, and hostile Unicode;
- Markdown/entity round trips and UTF-16 offsets;
- size accounting using declared and actual bytes;
- retry state transitions, cursor commits, and queue bounds;
- filename/MIME/URL safety and cleanup on every error path.

Integration tests with fakes:

- startup refuses missing, mismatched, unaccepted, or unauthorized manually
  supplied identities without mutating either account;
- duplicate delivery, out-of-order events, restart at each transaction boundary;
- Telegram `429`, timeout-after-accept, migration, missing username, anonymous
  admin, albums, forwarded messages, and bot-authored messages;
- relay disconnect/reconnect, rotated keys, stale cursors, removed member,
  unavailable Blossom server, and invalid attachment integrity.

End-to-end tests in disposable real rooms:

- current Armada client plus real Concord relay/Blossom stack;
- private Telegram supergroup with privacy-mode permutations;
- text, captions, replies, Unicode, multiple file types, process restart, network
  partition, key rotation, and bot removal.

Security/release gates:

- no secrets or plaintext content in default logs, crash output, metrics, or
  database after successful delivery/retention cleanup;
- dependency vulnerabilities and licenses reviewed;
- container runs without root/capabilities and with bounded writable storage;
- privacy and incident-response documentation reviewed by both community owners.

## 16. MVP acceptance criteria

The MVP is complete when:

- the service uses an operator-created Nostr keypair with a manually accepted
  Armada invitation and an operator-created Telegram bot with a manually
  supplied token; it creates or accepts none of these itself;
- one explicitly configured readable Armada channel and one private Telegram
  group exchange new text and files in both directions;
- both sides show a sanitized display name and stable source identifier;
- common formatting is preserved and unsupported formatting remains readable;
- files up to 20 MB are streamed, bounded, encrypted for Concord/Blossom, and
  cleaned from bridge temporary storage;
- messages outside the exact route, old first-start history, the bridge's own
  messages, and other bots are not relayed;
- restart, transient outage, rate-limit, and duplicate inputs are handled by the
  durable queue without silent loss;
- key rotation and current Armada compatibility pass end-to-end tests;
- operators can detect a stalled route without reading private content; and
- the privacy boundary and residual at-least-once duplicate risk are documented.

Deferred by default: reactions, typing/presence, read receipts, large files via a
local Bot API server, auto-discovered routes, and active-active replicas.
Configured Armada routes may use either community-visible or separately private
channels; the bridge does not infer or discover additional channels. Automated
key generation, Telegram bot creation,
credential acquisition, and Armada invitation acceptance are deliberate
non-goals rather than deferred features.

## 17. Decisions needed before implementation

1. Repository/product name: the request says `armada-nostr-bridge`, while the
   actual repository is `armada-telegram-bridge`. The latter is more precise for
   this product; confirm before publishing packages and deployment names.
2. Privacy policy: confirm member-consent wording, retention period, and whether
   Telegram `protect_content` remains enabled by default.
3. History policy: confirm first startup is new-messages-only and set restart
   catch-up age/count limits.
4. Moderation: decide whether moderator deletions propagate or only deletions by
   the original author.
5. Telegram forums: decide whether MVP binds the whole group or one topic.
6. Identity policy: decide whether messages from Telegram anonymous admins,
   channel senders, forwarded messages, and other bots are rejected or relayed
   with explicit origin labels. The safe defaults are reject other bots, label
   anonymous/channel senders, and label forwarding without claiming the forward
   origin as an authenticated participant.

The interoperability spike can begin before decisions 2–6 are finalized; the
configuration and route model should keep them explicit rather than hard-coded.

## 18. Primary references

- [Armada source and project documentation](https://github.com/soapbox-pub/armada)
- [Concord CORD-01: Private Streams](https://concordprotocol.org/spec/cord-01/)
- [Concord CORD-02: Communities](https://concordprotocol.org/spec/cord-02/)
- [Concord CORD-03: Channels](https://concordprotocol.org/spec/cord-03/)
- [Concord CORD-04: Roles](https://concordprotocol.org/spec/cord-04/)
- [Concord CORD-05: Invites](https://concordprotocol.org/spec/cord-05/)
- [Concord CORD-06: Rekeys and Refoundings](https://concordprotocol.org/spec/cord-06/)
- [Concord CORD-08: Disappearing Messages](https://concordprotocol.org/spec/cord-08/)
- [Concord event examples](https://concordprotocol.org/spec/examples/)
- [Concord bot guide](https://concordprotocol.org/build/bots/)
- [Concord SDK guide](https://concordprotocol.org/build/sdks/)
- [Concord implementer checklist](https://concordprotocol.org/build/checklist/)
- [`vector_sdk` API documentation](https://docs.rs/vector_sdk/latest/vector_sdk/)
- [Telegram Bot API](https://core.telegram.org/bots/api)
- [Telegram Bot FAQ](https://core.telegram.org/bots/faq)
- [`teloxide` API documentation](https://docs.rs/teloxide/latest/teloxide/)
- [`blckbx/armada-bot`](https://github.com/blckbx/armada-bot)
