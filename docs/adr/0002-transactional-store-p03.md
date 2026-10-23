# ADR 0002: Transactional delivery store and encrypted spool

Status: accepted

Date: 2026-10-22

## Context

The bridge must not lose an accepted source message, send it twice after a
normal restart, or advance a transport cursor beyond work that was not durably
queued. Pending private message bodies also need protection independent of the
Armada identity and Telegram bot credentials.

## Decision

- Use one local SQLite database with WAL journaling, full synchronous writes,
  and foreign-key enforcement.
- Commit the inbox event, destination outbox job, and source cursor in one
  transaction. A globally namespaced source event ID is the deduplication key.
- Order pending work by its monotonically increasing outbox row ID within each
  route and direction; source timestamps do not determine delivery order.
- Commit the destination receipt, delivered state, and source/destination
  message link in one transaction.
- Encrypt each retained outbox payload using XChaCha20-Poly1305 with a fresh
  random nonce. Bind its route, direction, and source event ID as authenticated
  associated data.
- Generate a separate random 32-byte spool key on first open. Publish it
  atomically, create it as mode `0600`, preserve it across restarts, and reject
  symlinks, malformed contents, and modes other than `0600` or `0400`.
- Erase the encrypted payload column after the delivered-message retention
  cutoff while retaining delivery metadata and message links.

The Nostr private key and Telegram token are never used for spool encryption.

## Consequences

Delivery is at-least-once across failures: a crash before a receipt commits
leaves the job pending. Destination adapters must therefore use the durable
message link where possible and tolerate a retry after an ambiguous remote
send. FIFO is guaranteed independently per configured route and direction,
not globally across the bridge.

Deleting or rotating `spool.key` while retained payloads exist makes those
payloads unreadable. Operational key backup and rotation procedures belong to
the deployment-hardening part.

## Test evidence

The P03 integration suite uses real temporary SQLite files and covers
repeatable migration, WAL and foreign-key guards, duplicate acceptance,
transaction rollback, per-stream FIFO, atomic receipts and links, restart
recovery, cursor durability, encrypted-at-rest payloads, retention cleanup,
and strict spool-key creation and validation.
