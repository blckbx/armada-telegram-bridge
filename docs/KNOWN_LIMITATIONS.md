# Known limitations — 0.1.0 candidate

- Private Armada traffic becomes readable Telegram group/bot traffic; the bridge
  host/operator has access to plaintext. This is not end-to-end encryption across
  the two platforms. Membership and onward copies are controlled separately.
- At-least-once delivery: a successful send without a committed receipt may be
  duplicated after timeout/restart. Multi-chunk/file sends can be partially
  delivered and then repeated. Old backups can replay already-delivered work.
- Only explicitly configured routes/topics are bridged. Invitations, identity
  provisioning, migrations, rekey recovery and quarantine release are manual.
- Source deletion forwarding is disabled. Expiry performs best-effort scheduled
  Telegram deletion, not guaranteed erasure of downloaded or backed-up copies.
- Edits require ownership, a retained original payload and a single text mirror.
  Attachment/caption edits and split-message edits are skipped. Replies without
  an available same-direction mapping use quote fallback. Mutations arriving
  before original delivery has a link are ignored.
- Catch-up is bounded by configured history limits; an extended outage or old
  edit can fall outside that window. Backlog admission is bounded; unreadable
  keys, removed membership and unsafe routes fail closed.
- Reactions are represented by one attributed bridge-owned summary reply, not
  native destination reactions. Telegram reaction intake requires the bot to be
  a group administrator. Telegram custom-emoji reactions are ignored; Armada
  custom emoji use their textual shortcode. Anonymous Telegram reaction-count
  updates retain counts but cannot name users. A reaction whose target has no
  durable message mapping is ignored. Armada restart reconciliation is bounded
  to recent synchronized history, and Telegram recovery remains subject to
  Telegram's pending-update retention.
- Supported files are size-limited (maximum 20,000,000 bytes). Media albums are
  not recreated as identical destination albums. Animated/special media may fall
  back to documents. Filename/content-type metadata is untrusted. An Armada file
  that fails decryption/hash verification is omitted and represented by a plain
  integrity-warning marker; temporarily unreachable files retry and can still
  require an operator retry or acknowledged skip after dead-lettering.
- Formatting is a safe common subset, not full Markdown/Telegram fidelity.
- `doctor` is offline metadata validation, not a live connection/database-integrity
  test. Readiness is a recent scheduler observation, not continuous relay probing.
- No automatic retention cleanup is scheduled. Queued and delivered payloads,
  metadata, SDK caches and backups can remain indefinitely. Encryption at rest
  does not protect against access to both the spool key and database.
- Locks protect one local directory, not duplicated identities across hosts.
  Network filesystems and active/active deployment are unsupported.
- Source security scans and fuzz smoke do not constitute a security audit.
  Image/native dependency review, container provenance, clean-host deployment,
  exact-client live validation and operator sign-off remain release gates.
