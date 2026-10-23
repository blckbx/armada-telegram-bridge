# Privacy notice for bridge participants

Messages and files in this Armada channel are copied to the configured Telegram
group, and vice versa. Sender display names/usernames are included for attribution.
Reaction emoji, reactor display names/usernames, and stable reactor identifiers
needed for reconciliation are also processed and stored; anonymous aggregate
Telegram reactions retain only a count and emoji.
The bridge account can read channel traffic; its operator controls the host,
credentials, storage, and routing. Invite only participants who understand this
cross-platform copying, and announce the bridge in both destinations.

**Telegram bot/group chats are not end-to-end encrypted.** Copying a private
Armada message to Telegram changes its privacy boundary. Telegram, group members,
their clients, and recipients of further copies may retain it. Telegram content
protection reduces some forwarding actions but is not a confidentiality or
anti-screenshot guarantee. Membership in the two groups is managed separately.

The bridge stores encrypted message payloads in SQLite and keeps an independent
spool key beside the database with private permissions. Anyone with both can
decrypt queued data. Routing metadata, message IDs, cursors, timestamps, and
stable author digests remain stored. Author digests are pseudonymous, **not
anonymous**. SDK caches and state can also contain private material.

Plaintext files are temporarily staged in private files for transfer and removed
on normal completion/cancellation. Abrupt process/host failure may leave staging
files behind. Disk deletion is not secure erasure. Armada attachments are
encrypted for the SDK's Blossom upload path; relays and Telegram have independent
retention policies that this bridge cannot enforce.

Current retention is not automatic: the store has a payload-cleanup primitive,
but the runtime does not schedule it yet. Delivered/pending/dead-letter payloads,
mapping metadata, SDK caches, and backups may remain until operator-managed
removal. Do not promise a fixed retention interval. Stop the bridge and follow a
reviewed data-removal procedure; removing the entire state loses deduplication,
recovery, keys, and history. Do not delete arbitrary database rows while running.

Deleting an original message does **not** currently delete its mirror. Scheduled
Armada expiry attempts to delete the Telegram mirror, but outages, API limits,
backups, screenshots, and prior copies prevent guaranteed removal. Edits to
attachment/split-message mirrors are not relayed. Announce corrections explicitly
when an edit cannot be mirrored.

Reactions appear as ordinary bridge-owned summary replies. They can therefore
be copied, quoted, retained, or screenshotted like other messages even after the
source reaction is removed. The bridge edits or deletes its live summary when it
observes a change, but cannot erase earlier copies or backups.

Health/metrics expose aggregate operational state, not traffic content. Default
runtime logs contain only fixed events and counts. The bridge forces the current
Concord SDK's built-in logger off because its download warnings can include full
attachment URLs; transport failures are exposed only as bounded bridge state.
Protect monitoring access, backups, crash artifacts, and any independently
enabled diagnostics.
