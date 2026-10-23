# Incident response

## Contain

Stop the bridge on suspected credential exposure, wrong routing, unexpected
membership, or cross-group disclosure. Do not repeatedly resume/retry a
quarantined route. Prevent the supervisor from restarting it while investigating.
Stop other copies using the same identity; the instance lock covers only one
local state directory. Record times and bounded error classes, not message text,
tokens, raw keys, or attachment URLs in tickets.

## Preserve and assess

With the process stopped, preserve a permission-restricted encrypted copy of the
complete state/configuration for authorized investigation. Determine which routes,
time window, and recipients were affected. Do not upload the database, `.env`,
SDK cache, or full logs to a public issue. A spool key and database together
expose queued/retained payloads; the Nostr key additionally exposes identity and
accessible Armada state. Assume exposure if an attacker had host-account access.

## Revoke and correct manually

Revoke/replace an exposed Telegram bot token through BotFather and update the
protected `.env`. For exposed Nostr keys, remove the old identity from affected
communities/channels and manually provision/invite a replacement; changing a
local nsec alone does not revoke the old identity's historical access. Rotate
channel access material using Armada's supported operator workflow where needed.
Do not erase forensic state to make startup pass. Review intended channel/group
IDs and membership before restoring service.

Notify affected participants about the disclosure boundary and copies that may
remain on Telegram, relays, participant devices, or backups. Do not promise that
deleting the source retracts delivered mirrors or prior copies.

## Recover

Use the stopped backup/restore procedure in OPERATIONS.md. Run offline doctor,
then verify live membership and harmless bidirectional delivery with authorized
operators. Inspect dead-letter error classes and duplicate flags before selective
retry. Resume only reviewed routes, one bridge instance at a time. Monitor
readiness and pending/dead-letter counts. Document cause, scope, corrective
actions, and any remaining uncertainty without including private payloads.
