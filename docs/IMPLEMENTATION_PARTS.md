# TDD Implementation Parts

Status: proposed execution backlog

This document turns the architecture in
[`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) into independently reviewable
parts. Part IDs are stable so branches, pull requests, and issues can refer to
them directly.

## Working agreement

Every behavior change follows the same loop:

1. Add the smallest test that describes the externally visible behavior and
   confirm that it fails for the expected reason.
2. Add only enough production code to make the test pass.
3. Refactor while the suite remains green.
4. Run the part's completion gate before merging.

A PR may contain the red, green, and refactor commits together; the main branch
must remain green. Tests should assert behavior at module boundaries rather
than implementation details.

Default tests must be deterministic and require no network, real credentials,
or wall-clock sleeps. Put live Armada and Telegram checks behind explicit,
ignored test targets. Never place an `nsec`, bot token, community key, private
message, or live room identifier in a fixture or CI log.

Use these test seams from the start:

- `Clock` for current time and retry scheduling;
- `RandomSource` for jitter and temporary names;
- `TelegramPort` and `ConcordPort` for external transports;
- real temporary SQLite databases for store tests, not a mocked database;
- temporary directories for filesystem tests; and
- deterministic in-memory fakes that record calls and can inject failures.

The common completion gate is:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Dependency and container checks are added when their corresponding parts land.
Coverage is diagnostic, not a substitute for meaningful assertions; no numeric
coverage target is required for the first iteration.

## Dependency order

```text
P00 -> P01
P00 -> P02 -> P03
       P02 -> P04
P01 + P02 -> P05
P03 + P04 + P05 -> P06 -> P07 -> P08 -> P09
P09 -> P10 (optional conversation fidelity)
P09 -> P11 -> P12
```

P01 is deliberately early because recovery of a manually accepted Armada
membership is the largest unresolved integration risk. Its result may adjust
P05, but it should not weaken the manual-onboarding requirement.

| Part | Depends on | Status | Mergeable result |
| --- | --- | --- | --- |
| P00 | — | Implemented; local gates green, CI pending | Green Rust/CI test foundation |
| P01 | P00 | Live recovery passed; receive/send markers pending | Proven manual Armada membership recovery |
| P02 | P00 | Implemented; local gates green, CI pending | Validated offline configuration and domain types |
| P03 | P02 | Implemented; local gates green, CI pending | Transactional durable delivery state |
| P04 | P02 | Implemented; local and live smoke green, CI pending | Telegram adapter satisfying its port contract |
| P05 | P01, P02 | Implemented; local gates and live recovery/history green, CI pending | Concord adapter satisfying its port contract |
| P06 | P03, P04, P05 | Complete | Bidirectional attributed plaintext bridge |
| P07 | P06 | Complete | Retry, recovery, ordering, and backpressure |
| P08 | P07 | Complete | Safe rich-text conversion and splitting |
| P09 | P08 | Implemented; local gates green, live uploads pending | Bidirectional files; core MVP feature-complete |
| P10 | P09 | Implemented; local gates green, Armada live shapes pending | Optional replies, edits, deletion, and expiry |
| P11 | P09 | Implemented; local gates and store restore drill green, container/host acceptance pending | Operable and hardened deployment |
| P12 | P09, P11 | Release gates implemented; offline checks green, live/deployment/provenance/operator acceptance pending | Releasable MVP with recorded evidence |

The core MVP path is P00–P09, P11, and P12. P10 is conversation-fidelity work
that may land before or after the first release without blocking the user's
minimum text-and-file bridge.

## P00 — Rust and test foundation

Outcome: the repository has a minimal executable, a library boundary, and a
fast test command suitable for all later red-green-refactor cycles.

Write first:

- a CLI smoke test proving `--help` exits successfully and contains no secrets;
- a configuration-free startup test proving the binary fails with a concise
  missing-config error rather than panicking; and
- a unit test for a fixed `Clock` test implementation.

Implement:

- a Rust workspace with `src/lib.rs` and a thin `src/main.rs`;
- error/reporting conventions with secret-safe `Display` implementations;
- unit, integration, and ignored live-test directories;
- formatting, Clippy, and test commands in CI; and
- a test-support module containing the fixed clock; deterministic transport
  fakes are added alongside their port traits in P04 and P05.

Do not add Telegram, Concord, SQLite, or web-server behavior in this part.

Done when the common completion gate passes locally and in CI, and a test can
prove that an error containing a secret wrapper is redacted.

## P01 — Manual Armada onboarding interoperability spike

Outcome: prove that the chosen `vector_sdk` version can start from the manually
provided Nostr identity and recover an invitation already accepted in Armada.

This is an ignored live test, not part of the credential-free default suite.
The operator manually creates the disposable Nostr keypair, accepts the Armada
invitation, and supplies temporary secret/config files.

Write first:

- `manual_membership_is_recovered`: starts a fresh process with the supplied
  `nsec` and asserts the expected `npub`, community, and channel are visible;
- `configured_channel_can_receive`: observes a marker sent manually from Armada
  and verifies the authenticated inner author and exact channel ID;
- `configured_channel_can_send`: publishes a unique marker that is visible in
  Armada; and
- `other_channel_is_not_selected`: proves an event from another accessible
  channel is rejected by the bridge's scope predicate.

Then extend the spike with ignored tests for a small encrypted file, reconnect,
history cursor, key rotation, member removal, and refounding. Record the exact
Armada revision, SDK version, relay setup, and results in an ADR.

If a fresh process cannot recover the accepted membership from relay-backed
state, stop this part and define the smallest encrypted state export/import
contract. Automatic invitation acceptance is not an acceptable fallback.

Done when the recovery, receive, send, and scope tests pass against a disposable
real community and `vector_sdk` is pinned exactly in `Cargo.lock`.

## P02 — Domain model, configuration, and secret loading

Outcome: the bridge can parse and validate a complete static route without
contacting either network.

Write first:

- valid configuration parses into typed IDs and limits;
- a missing or group/world-accessible `.env`, malformed `nsec`, malformed
  token, zero/positive Telegram group IDs, and duplicate route IDs are rejected;
- the `npub` derived from `ARMADA_NSEC` must equal `expected_npub`;
- all errors and debug output redact the `nsec` and Telegram token;
- channel/chat names cannot be used in place of canonical IDs; and
- defaults are safe: long polling, `protect_content = true`, other bots ignored,
  20 MB files, bounded queues, and new-messages-only first startup.

Implement:

- typed configuration and route identifiers;
- a restricted dotenv reader using redacting/zeroizing wrappers where supported;
- the initial `BridgeEvent`, `SenderIdentity`, `Route`, and policy types;
- validated limit types rather than unchecked integers; and
- configuration fingerprinting that excludes secret values.

Done when table-driven tests cover every validation branch and snapshot tests
prove serialized errors/configuration cannot reveal either secret.

## P03 — Durable inbox, outbox, cursors, and message links

Outcome: one SQLite transaction can deduplicate an inbound event and create its
destination work item, and work survives process restart.

Write first:

- applying the same namespaced source event twice creates one inbox row and one
  job;
- inbox insertion and outbox creation commit or roll back together;
- jobs retain FIFO order per route/direction;
- a destination receipt atomically marks a job delivered and records both
  message IDs;
- pending jobs and cursors survive closing and reopening the database;
- cursor advancement cannot overtake an uncommitted inbound event;
- completed plaintext payloads are erased after the configured retention; and
- migration from an empty database is repeatable.

Implement the minimum schema for routes, inbox events, outbox jobs, cursors,
message links, cached profiles, and route state. Enable WAL mode and foreign-key
checks. Encrypt sensitive outbox payloads with a separate random spool key that
is created atomically with mode `0600` in the data directory on first startup
and reused thereafter; never reuse the Nostr key or Telegram token. Test that a
missing key is created, an existing key is preserved, and unsafe permissions or
malformed key material fail closed.

Test against a real SQLite file in a temporary directory. Add crash-boundary
tests by reopening the database between state transitions.

Done when all store invariants are covered by integration tests and no delivered
message body remains in normal query results after cleanup.

## P04 — Telegram port contract

Outcome: Telegram updates and sends are represented by a narrow adapter without
letting Bot API types leak into routing code.

Write first against a scripted HTTP server:

- `getMe` must match `expected_bot_id`;
- only the configured numeric chat and optional topic are normalized;
- the bot's own messages and all other bots are ignored;
- user, anonymous-admin, and sender-chat identities retain stable numeric IDs;
- update offsets advance only after the handler durably accepts an update;
- `429 retry_after`, retryable 5xx, terminal 4xx, timeout, and malformed JSON are
  classified distinctly;
- group-to-supergroup migration pauses instead of silently rewriting the route;
- text/caption limits and `protect_content` are present on outbound calls; and
- tokens are absent from errors, logs, and recorded HTTP fixtures.

Implement `TelegramPort`, long polling, update normalization, identity checks,
and send receipts. Use `teloxide` where it gives adequate control, with a small
direct Bot API client only for uncovered behavior.

Done when the same reusable port contract passes for the in-memory fake and the
HTTP-backed adapter. Add one ignored live smoke test using an operator-created
test bot and manually configured private group.

## P05 — Concord port contract

Outcome: Concord events are normalized through `vector_sdk` while routing code
remains independent of SDK types.

Write first:

- startup fails if the manually accepted community/channel cannot be recovered;
- the configured community and channel are matched by canonical IDs;
- other channels, DMs, and the bridge's own events are rejected;
- sender identity comes from the verified inner author, never the stream author;
- reconnect/catch-up yields stable namespaced event IDs without duplicate
  normalization;
- send returns the canonical destination event ID; and
- removal, missing keys, incomplete rekey chunks, and stale state mark the route
  not ready instead of silently continuing.

Implement `ConcordPort`, `vector_sdk` state loading, live-event/history handling,
profile lookup, publication, and readiness classification. Do not implement key
generation or invitation acceptance.

Done when the fake port contract passes by default and the P01 live fixtures
exercise the real adapter. Version-specific SDK assumptions belong only in this
adapter and its ADR.

## P06 — Bidirectional plaintext bridge

Outcome: new text messages cross the exact configured route in both directions
with trustworthy attribution and persistent deduplication.

Write first as service-level tests with both fake ports and real SQLite:

- an Armada message creates one correctly attributed Telegram send;
- a Telegram message creates one correctly attributed Armada send;
- duplicate source events, bridge-authored events, other bots, wrong channels,
  wrong chats, and wrong topics produce no destination call;
- display-name control characters cannot forge the attribution header;
- missing usernames fall back to a display name plus stable short ID;
- first startup establishes baselines and sends no existing history;
- successful delivery stores the cross-system message link; and
- a restart does not relay an already committed input again.

Implement the router, attribution renderer, inbox/outbox transaction, and one
ordered dispatcher per route/direction. Keep the body plaintext in this part;
rich formatting follows in P08.

Done when a deterministic integration test drives both directions through the
same service orchestration used by the binary, including stop and restart.

## P07 — Retry, ordering, backpressure, and recovery

Outcome: transient failures do not cause silent loss or unbounded resource use.

Write first using a paused Tokio clock and failure-injecting fake ports:

- retryable failures use capped exponential backoff with deterministic jitter;
- Telegram `retry_after` overrides the local backoff;
- terminal failures enter inspectable dead-letter state;
- a failure blocks later jobs only in the same route/direction;
- another route/direction continues independently;
- a saturated queue stops intake/cursor advancement and makes readiness false;
- shutdown stops intake, drains for its deadline, and leaves remaining jobs
  durable;
- bounded catch-up enforces both age and count; and
- an ambiguous timeout may retry and is recorded as a possible duplicate.

Implement retry classification, scheduler, route quarantine, bounded queues,
graceful shutdown, and operator retry/resume operations.

Done when tests contain no real sleeps and fault injection at every store/send
boundary demonstrates at-least-once behavior without silent loss.

## P08 — Rich-text conversion and safe splitting

Outcome: supported formatting survives both directions and unsupported syntax
remains readable.

Write first:

- golden cases for bold, italic, underline, strike, spoiler, code, fenced code,
  links, quotes, headings, lists, and unsupported constructs;
- Telegram UTF-16 offsets remain correct with emoji, combining marks, and RTL;
- nested/overlapping entities resolve deterministically;
- unsafe link schemes and bidi/control characters cannot spoof attribution;
- long messages split on grapheme/paragraph boundaries within Telegram limits;
- every output chunk contains valid entity ranges; and
- plain text round-trips without changes other than documented normalization.

Implement a small neutral rich-text model, Telegram entity parser/renderer,
Armada Markdown parser/renderer, and grapheme-safe splitter. Avoid chained regex
rewrites.

Add property tests asserting that renderers never panic, entity ranges always
fall within their text, and arbitrary valid Unicode remains valid UTF-8.

Done when the golden corpus and property tests pass and P06 uses rich text
without changing its delivery semantics.

## P09 — Bidirectional attachments

Outcome: supported files up to the common 20 MB limit cross both ways without
unbounded memory use or leftover plaintext.

Write first:

- declared-oversize files are rejected before download;
- actual bytes exceeding the limit abort even when metadata lies;
- downloads stream to mode-`0600` randomized temporary files;
- cleanup occurs after success, timeout, cancellation, malformed input, failed
  upload, and process-level task error;
- MIME is sniffed from bytes and hostile filenames are sanitized;
- localhost, private/link-local addresses, forbidden schemes, and unsafe
  redirects are rejected where the bridge controls fetching;
- captions keep attribution or create an ordered companion message when too
  long;
- media groups release available items after their bounded assembly window; and
- destination media-method failure falls back to a document when safe.

Implement Telegram download/upload streaming, SDK bounded attachment handling,
temporary-file lifecycle, file-type selection, captions, and per-user/route disk
quotas. Preserve the SDK's encryption, integrity, Blossom fallback, and SSRF
protections rather than replacing them.

Done when integration tests transfer boundary-sized fixtures in both directions
with bounded memory, and an ignored live test proves Armada/Blossom and Telegram
interoperability.

## P10 — Optional conversation fidelity

Outcome: linked conversations can preserve replies, edits, author deletions,
and expiry without acting on messages the bridge does not own.

Write first:

- a reply uses the mapped destination message when present and a safe plaintext
  quote when absent;
- inline Armada quotes are not confused with Concord threaded replies;
- an edit changes only the bridge-owned destination mirror;
- an authenticated source-author deletion removes only its linked mirror;
- unauthenticated, cross-route, or moderator deletion is ignored by the default
  policy;
- expiry schedules best-effort Telegram deletion and survives restart; and
- missing/already-deleted destination messages resolve idempotently.

Implement message-link lookup, reply rendering, edits, deletes, and expiry jobs.
Keep reactions, typing, presence, and read receipts out of scope.

Done when fake-port contract tests prove ownership and route checks precede every
mutation, and ignored live tests confirm the exact Armada event shapes found in
P01.

## P11 — Diagnostics, privacy, and deployment

Outcome: an operator can validate, run, observe, stop, and recover the bridge
without inspecting private content.

Write first:

- `doctor` performs read-only validation and never creates credentials, accepts
  invitations, changes chat settings, or rewrites route IDs;
- `/live` remains healthy while `/ready` reflects transport/store/queue state;
- metrics have bounded labels and contain no user names, message bodies, tokens,
  URLs with attachment keys, or raw public keys;
- default structured logs pass secret/content redaction tests;
- a second process cannot acquire the same data-directory lock; and
- SIGTERM follows the bounded shutdown behavior established in P07.

Implement `doctor`, health endpoints, Prometheus metrics, structured logging,
instance locking, rootless container files, hardened systemd unit, operations
runbook, privacy notice, and incident-response guide.

Done when deployment smoke tests run as a non-root user with a read-only root
filesystem and the documented backup/restore drill succeeds on test data.

## P12 — MVP release gate

Outcome: produce the first releasable text-and-file bridge with evidence for all
MVP acceptance criteria.

Write or complete the release tests first:

- an end-to-end matrix for both text directions, supported files, Unicode,
  formatting, restart, network partition, Telegram rate limiting, Concord key
  rotation, bot removal, and exact-scope rejection;
- an upgrade test from every released database schema (initially the empty
  database) and a rollback/recovery rehearsal;
- parser fuzz targets and security regression fixtures;
- dependency, license, and secret scans; and
- a compatibility record for the pinned Armada, Concord/vector SDK, Telegram
  Bot API, and `teloxide` versions.

Complete the privacy review, SBOM, container provenance, release notes, and
known-limitations document. State the at-least-once duplicate window and the
Telegram privacy boundary prominently.

Done when the default suite, ignored live suite, security checks, clean-machine
deployment test, and manual operator acceptance checklist all pass with their
results attached to the release.

## P13 — Attributed reaction summaries

Outcome: reactions to mapped messages are visible across both platforms without
impersonating users or losing removal state.

Write first:

- an Armada reaction snapshot creates one attributed Telegram reply, later
  snapshots edit it, and an empty snapshot deletes it;
- Telegram per-user reaction selection creates the equivalent Armada reply;
- reaction updates are explicitly requested from Telegram and normalized with
  stable actor identity;
- reaction state and the destination summary mapping survive restart; and
- bounded Armada history reconciles reactions added while the bridge was down.

Implement durable per-message reaction state and one bridge-owned summary reply
per source direction. Preserve Unicode emoji and available display names, retain
anonymous Telegram aggregate counts, ignore Telegram custom emoji, and render
Armada custom emoji as text. Require a durable message link before mutating a
destination, filter the bridge's own reactions, and treat repeat snapshots as
idempotent.

Done when create/change/removal, Telegram normalization, live Armada update
intake, restart reconciliation, strict formatting, Clippy, and the full offline
suite pass. Live acceptance still requires manually reacting in both clients;
the Telegram bot must first be promoted to group administrator.

## Per-part PR template

Use this checklist in every implementation PR:

```text
Part: Pxx
Depends on:

Red:
- [ ] Tests added and observed failing for the intended reason

Green:
- [ ] Minimum implementation makes new tests pass

Refactor:
- [ ] Names/boundaries simplified with all tests green

Verification:
- [ ] cargo fmt --check
- [ ] cargo clippy --all-targets --all-features -- -D warnings
- [ ] cargo test --all-targets --all-features
- [ ] Part-specific contract/live/security checks completed

Safety:
- [ ] No secrets or private message content in fixtures, logs, snapshots, or diff
- [ ] Manual onboarding boundary remains intact

Documentation:
- [ ] Behavior, operational impact, and known limitations updated
```
