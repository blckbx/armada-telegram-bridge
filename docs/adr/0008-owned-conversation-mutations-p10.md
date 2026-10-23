# ADR 0008: Ownership-checked conversation mutations

Status: accepted

Date: 2026-10-22

## Context

Replies, edits, deletes, and expiring messages refer to earlier messages rather
than standing alone. Relaying such an event without binding it to the exact
route, direction, source message, authenticated author, and bridge-created
destination could mutate unrelated content. The bridge also has to recover
expiry work after restart and treat an already-missing destination as an
idempotent outcome.

Telegram supplies native reply metadata and edited-message updates, but the Bot
API does not send bots a general update when users delete group messages.
Concord supplies explicit reply IDs, authenticated author-scoped edits, and
NIP-40 expiry. In `vector_sdk 0.11`, the Community removal callback combines an
author delete with a moderator hide and omits the acting identity.

## Decision

Every successfully created mirror records a SHA-256 pseudonymous digest of the
stable authenticated source author alongside its route, direction, source ID,
and destination ID. Mutation
intake must find that exact link and match the current authenticated author
before it creates a durable job. Missing links, foreign routes or directions,
unauthenticated actors, and owner mismatches are ignored while their source
cursor still advances safely. Mutation jobs never create a second message
link.

A mapped reply uses the destination's native reply operation. An unmapped reply
gets a bounded, control-stripped plaintext excerpt; inline Markdown quotes do
not become reply metadata. Edits replace only the mapped bridge-created
message. Deletes remove only mapped bridge-created IDs. Only Telegram's explicit
already-missing and unchanged-message errors are treated as idempotent success.
Other HTTP 400 errors remain failures, not successful delivery receipts.

An Armada create carrying NIP-40 expiry atomically schedules a Telegram mirror
deletion when its delivery receipt is committed. Scheduled deletions are
durable SQLite rows processed on every scheduler pass and survive restart.
Transient deletion failures leave the task pending for a later pass.

The Concord adapter consumes authenticated update views for edits. It
deliberately ignores the current ambiguous Community removal callback: without
the actor or an explicit author-delete/moderator-hide discriminator, propagating
it would violate the ownership rule. The domain and service support an
authenticated author-delete event so a future SDK callback can enable that path
without changing persistence or destination behavior. Telegram source deletes
remain unavailable until the Bot API exposes a trustworthy update.

## Consequences

Replies and edits work in both directions, safe quote fallback works when the
referenced source predates bridge history, and Armada expiry removes Telegram
mirrors after restart. Authenticated delete behavior is fully contract-tested,
but live source-delete observation is intentionally narrower than the domain
capability because both current transports omit required evidence in at least
one direction. Moderator hides never cascade across the bridge by default.

Edits are limited to single-message text mirrors whose original retained payload
confirms that no attachments were sent. Split mirrors, attachment mirrors,
purged original payloads, and edits requiring multiple Telegram chunks are
ignored rather than risking loss of content. Replies resolve links available at
intake; pending or reverse-direction references use quote fallback. Mutations
received before their original delivery has a mapping are ignored. Recovery of
Armada edits is bounded by the configured history window.
Delivery remains at least once around ambiguous network failures.

## Test evidence

Store tests cover durable author ownership and restart-safe expiry rows. Service
tests exercise mapped and unmapped replies, inline quote separation, author and
foreign-actor edits/deletes, both relay directions, already-missing idempotency,
and expiry after reopening the database. Telegram adapter fixtures verify real
`edited_message`, `reply_to_message`, `reply_parameters`, `editMessageText`, and
`deleteMessage` shapes. Concord normalization tests verify `replied_to`, folded
edit history, and NIP-40 expiration. Ignored live tests let an operator confirm
the exact reply/edit/expiry shapes against the configured Armada channel.
