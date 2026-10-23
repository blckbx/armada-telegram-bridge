# 0.1.0 candidate — unreleased

This candidate bridges one or more explicitly configured private Armada channels
and Telegram groups/topics with attributed text, common formatting and bounded
file transfer. Onboarding stays manual: operators provision keys/token and accept
invitations outside the bridge. SQLite delivery state is encrypted, restartable,
and protected by a single-instance lock. Operators get offline doctor, health,
aggregate metrics, retries/dead letters, an explicitly acknowledged dead-letter
skip, bounded shutdown, and deployment guides. Corrupt Armada attachments are
now omitted with a visible integrity marker instead of blocking later messages.

P12 adds release-gate tests for rekey/removal quarantine and future-schema
rollback, a parser fuzz target/corpus, dependency/license/secret scan policy,
source SBOM/notices generation, and non-publishing CI evidence workflows.
Private runtime state and generated artifacts are now excluded from Git.
The candidate now requires Rust 1.99, matching the verified toolchain rather
than the previous, incompatible 1.85 claim.

P13 adds reaction bridging as one bridge-owned summary reply beneath the mapped
message. Unicode reactions and available actor names are attributed; subsequent
changes edit the summary and removing all reactions removes it. Telegram
per-user updates and anonymous aggregate counts are supported. Armada snapshots
are reconciled from bounded history after restart. This deliberately does not
impersonate users with native destination reactions.

**Important:** bridging changes the privacy boundary; Telegram receives plaintext.
Delivery is at least once and can duplicate messages/files after ambiguous sends
or restoring old backups. User deletions, attachment edits and split-message
edits are not relayed. No automatic retention period is enforced.

No production release or tag has been created. See RELEASE_GATE.md for incomplete
live, image-provenance, deployment and operator acceptance requirements, and
KNOWN_LIMITATIONS.md and PRIVACY.md before any deployment.
