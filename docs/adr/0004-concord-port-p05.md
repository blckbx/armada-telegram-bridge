# ADR 0004: Concord SDK adapter boundary

Status: accepted

Date: 2026-10-22

## Context

The bridge needs history, live messages, profiles, and publication from
Concord without exposing `vector_sdk` types to routing. It must reject DMs,
other channels, self-authored echoes, unverified authors, missing membership or
keys, and stale control state. Invitation acceptance and identity creation stay
manual operator actions.

## Decision

- Keep `vector_sdk 0.11.0` behind an asynchronous `ConcordPort`. Routing sees
  only bridge-domain events, stable cursors, profiles, send receipts, and
  fail-closed readiness states.
- Build the production adapter only with the supplied `nsec`, expected `npub`,
  a dedicated data directory, and `InvitePolicy::Manual`. Never call identity
  generation or invitation-acceptance APIs.
- Refresh relay-backed communities before declaring startup ready. Match both
  canonical community and channel IDs, require a readable channel, and use the
  SDK's explicit `is_readable()` value as the key-availability signal.
- Do not infer missing keys from `CommunityChannel::epoch()`. Live Armada state
  proved that a readable private channel can report epoch `0` through this SDK
  version.
- Normalize only new community messages from an exact configured channel.
  Reject DMs, other channels, own echoes, and messages without the SDK-verified
  inner author. Use the inner `Message.id` for `armada:<id>` deduplication and
  never the outer wrapper event ID.
- Use the SDK's wire-stable `(timestamp, inner message ID)` cursor. Catch-up
  filters strictly after a committed cursor, producing the same namespaced ID
  if relay reconnect or history overlap presents an event again.
- Feed ordered SDK community callbacks into the adapter's live intake. Removal
  and dissolution immediately make affected routes not ready; key and control
  refresh events re-evaluate readiness.
- Accept sends only for ready routes and validate that the SDK returns a
  canonical 64-character inner event ID.
- Strip underlying SDK error details at the public boundary so an `nsec`, relay
  credential, or decrypted content cannot enter operator diagnostics.

## Readiness interpretation

The port distinguishes unchecked, ready, missing community, missing channel,
missing key, incomplete rekey, removed, dissolved, and
stale-state conditions. All except `Ready` block history and sends.

`vector_sdk 0.11.0` does not expose a dedicated incomplete-rekey callback. An
unreadable channel is classified as `MissingKey`; a failed control/rekey refresh
is classified as `StaleState`. Both fail closed. `IncompleteRekey` remains an
explicit port state for deterministic orchestration tests and for a future SDK
surface that can identify it precisely.

## Consequences

P06 can consume Telegram and Concord through symmetric, transport-neutral
ports. Live delivery does not need to trust an outer Nostr stream author. If a
process stops after the SDK persists an event but before the bridge accepts it,
the next bounded catch-up recreates the identical namespaced event and cursor.

The SDK uses process-global identity state, so the bridge supports one Concord
identity per process. SDK upgrades must repeat the P01/P05 live recovery,
history, receive, and send matrix.

## Test evidence

Default tests cover the reusable fake-port contract, cursor resume, canonical
send receipts, every fail-closed readiness state, exact channel and DM scope,
self-message rejection, missing authors, authenticated inner author/ID use, and
stable replay normalization.

On 2026-10-22, the production adapter recovered the manually accepted private
channel from a fresh SDK data directory and successfully performed a bounded
history sync against the configured real relays. Marker-specific receive and
send fixtures remain operator-triggered P01 checks.
