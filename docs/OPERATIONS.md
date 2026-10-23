# Operating the bridge

P11 adds local diagnostics, a single-instance lock, health/metrics, bounded
SIGTERM shutdown, and hardened deployment templates. No command creates a Nostr
identity or Telegram bot, accepts invitations, or changes route IDs.

## Validate, start, observe, stop

```sh
cargo run --locked -- doctor --config bridge.toml
cargo run --locked -- --config bridge.toml
curl --fail http://127.0.0.1:9090/live
curl --fail http://127.0.0.1:9090/ready
curl --fail http://127.0.0.1:9090/metrics
```

`doctor` is **offline and read-only**: validates the existing TOML and adjacent
`.env`, identity consistency, and permissions/type/completeness of local state.
It does not open SQLite or the SDK, create a directory/key, probe write access,
or verify database integrity, network access, invitations, or live membership.
Missing state is valid before first startup. A pass is not a live readiness
certificate. Live access is verified at normal startup; the separate opt-in
tests in CONFIGURATION.md provide transport checks.

State must be a local, non-symlink directory with mode `0700`. `.env` must be
`0600` or `0400`. Run as its owner. A new state directory is created privately.
An OS advisory lock prevents cooperating bridge/operator processes from sharing
the same data directory. Keep `instance.lock` in place: a leftover lock file is
normal, and OS locks release automatically on process exit. Do not unlink the
lock to bypass a running instance. Network filesystems and two separate state
directories using the same bot identity are unsupported.

The default `health_listen = "127.0.0.1:9090"` needs no configuration change.
Only exact GET paths are supported. Requests have a 2 KiB header limit, a
two-second deadline, and at most 16 concurrent connections.

| Endpoint | Meaning |
| --- | --- |
| `/live` | HTTP server/task is responsive, including during degraded readiness |
| `/ready` | Last completed scheduler observation passed queue, SQLite, and route-safety checks and is less than 90 seconds old |
| `/metrics` | `bridge_ready`, `bridge_pending_jobs`, `bridge_dead_letters`; aggregate gauges without labels |

Readiness starts false, becomes false for queue saturation, dead letters,
quarantined/paused routes, pending retry waits, and permanently on shutdown.
Transport initialization must succeed before observations are published. Fatal
transport/store failures terminate the process; stalled work ages readiness out.
Readiness is **not** an active probe of every relay: silent Concord disconnection
may only become visible through SDK callbacks or a later transport operation.
Pending includes queued, retrying, and dead-lettered jobs. A long healthy batch
may also age out readiness; investigate rather than restarting solely on `/ready`.

Endpoints have no authentication. Keep them on loopback or a restricted monitoring
network. Never expose them directly to the Internet. Default runtime logs are
JSON with fixed event names and numeric counts only; no message bodies, names,
raw keys, bot tokens, attachment URLs, or SDK errors are logged. Offline CLI
validation still prints concise configuration errors. No verbose SDK subscriber
is enabled. Do not add raw error/debug logging to diagnose private traffic.

Startup failures use bounded stage events: `lock_unavailable` (another process
or unsafe state permissions), `health_bind_failed` (address unavailable),
`store_failed`, `armada_connect_failed`, and `startup_failed` (transport checks
or recovery). `runtime_failed` marks unsuccessful process exit without dumping
the underlying SDK/database error.

SIGINT and SIGTERM stop intake and allow up to ten seconds for immediately
eligible work/retries to drain. Remaining jobs stay durable. The supervisor
should allow at least twenty seconds. A forced kill or ambiguous network result
can cause a duplicate after restart: delivery is at least once, not exactly once.

## Recovery commands

Stop the bridge first: operator commands acquire the same exclusive lock.

```sh
armada-telegram-bridge dead-letters --config /etc/armada-telegram-bridge/bridge.toml
armada-telegram-bridge retry --config /etc/armada-telegram-bridge/bridge.toml --job 7
armada-telegram-bridge skip --config /etc/armada-telegram-bridge/bridge.toml --job 7 --acknowledge-loss
armada-telegram-bridge resume --config /etc/armada-telegram-bridge/bridge.toml --route private-community
```

Inspect the bounded error class and fix credentials, access, or destination
limits before retrying. Review `possible_duplicate` before retrying an ambiguous
send. Do not resume a migrated Telegram group or removed Armada membership
without manually checking the configured IDs and intended privacy boundary.

`skip` is the last resort for a permanently undeliverable item: that one mirror
will never reach its destination. It is accepted only for a dead-letter job,
erases the queued encrypted payload, retains deduplication metadata, and records
the prior bounded error class in `skipped_jobs`. The explicit acknowledgement is
intentional. After restart, later jobs on the same FIFO route can proceed.

An Armada attachment that decrypts incorrectly or fails its claimed hash does
not require a manual skip on current versions. The bridge omits those unsafe
bytes, adds a neutral media-aware notice such as
`[GIF could not be transferred]`, and continues the queue. A temporarily
unreachable source is retried; if every
attempt is exhausted, inspect and either retry or explicitly skip its job.

## Reaction summaries

The bridge renders reactions as one reply beneath the mapped message, for
example `Reactions from Armada: 👍 Alice`. It edits that reply as the source
reaction set changes and deletes it when the set becomes empty. The reverse
direction uses `Reactions from Telegram: …`. These are bridge-authored text
summaries, not native destination reactions.

Promote the Telegram bot to group administrator before relying on reactions;
Telegram only delivers per-message reaction updates to administrator bots. The
adapter explicitly subscribes to both per-user and aggregate reaction update
types. Existing text/file bridging does not require this promotion. After a
restart, Armada reaction state is reconciled from bounded recent history after
missed messages are delivered. Telegram recovery depends on the updates still
being present in Telegram's bounded pending-update window.

## Container deployment

Use a rootless Docker/Podman daemon where available. The image also defaults to
UID/GID 10001; the Compose template allows overriding this to match the owner of
bind-mounted files. Building/installing these templates is an operator action;
P11 does not start a second bridge automatically.

1. Prepare a dedicated configuration directory containing `bridge.toml` and
   its mode-0600 `.env`, and a separate mode-0700 state directory owned by the
   selected non-root UID. Do not mount the repository or unrelated credentials.
2. In the container's TOML set `data_dir = "/state"` and
   `health_listen = "0.0.0.0:9090"`. Only Compose's host-loopback mapping exposes
   port 9090. Never use that listen address on an unprotected host deployment.
3. Export `BRIDGE_CONFIG_DIR` and `BRIDGE_STATE_DIR` as absolute paths, plus
   `BRIDGE_UID` and `BRIDGE_GID` if not 10001. Account for rootless UID mapping.
4. Run `docker compose -f deploy/compose.yaml up --build -d` after stopping any
   other instance. Stop with `docker compose -f deploy/compose.yaml stop`.

The template has a read-only root filesystem, only state and tmpfs writable,
all capabilities dropped, no-new-privileges, resource/log limits, and a 20-second
stop grace period. `.dockerignore` is an allowlist: real configuration, `.env`,
state, tests, and Git history never enter the build context. Base-image digest
pinning and provenance are P12 release work.

Offline deployment smoke (synthetic credentials only, network disabled):

```sh
docker build -t armada-telegram-bridge:p11 .
cargo test --locked --test config container_doctor_smoke -- --ignored --exact
```

This checks non-root execution, read-only root, a read-only configuration mount,
and offline doctor success. It does not prove live transport delivery. As of
this P11 implementation the local Docker daemon was unavailable, so this test
is provided but **not yet executed**. Complete it before deployment acceptance.

## systemd deployment

Use `deploy/armada-telegram-bridge.service` with a dedicated non-login system
user/group named `armada-bridge`. Install the release binary at
`/usr/local/bin/armada-telegram-bridge` and configuration in
`/etc/armada-telegram-bridge`, readable only by that account. Set TOML `data_dir`
to `/var/lib/armada-telegram-bridge` and keep health bound to loopback.

The unit creates private persistent state, uses a read-only system filesystem,
drops capabilities, restricts address families, disables core dumps, and limits
resources. Run `systemd-analyze verify deploy/armada-telegram-bridge.service` on
the target host before installing/enabling it. Adjust documented paths only;
do not weaken filesystem or privilege restrictions to work around ownership.

## Backup/restore drill

Backups contain private data and decryption material. Encrypt them separately,
limit access, and retain the matching binary/configuration version.

1. Stop the service and verify its process exited. Do not copy live SQLite/WAL
   files independently. Keep the bridge stopped throughout the copy.
2. Copy **the whole data directory**, including `bridge.sqlite3`, any `-wal`
   and `-shm` files, `spool.key`, and `concord/` SDK state, preserving permissions.
   Include configuration and `.env` in the protected backup set. Never separate
   the spool key from its database or regenerate it during restore.
3. Restore into a **new, empty, private directory** owned by the service user.
   Keep the existing installation untouched as rollback material. Point a copy
   of the configuration at the restored directory and run offline `doctor`.
4. With the original instance stopped, start exactly one restored instance.
   Check readiness, pending/dead-letter gauges, membership, and a harmless
   operator-approved message in each direction. A stale backup can replay work
   already delivered since that backup: review the duplicate window first.
5. For rollback, stop the restored instance before selecting the preserved
   original state and matching binary. Never run both copies concurrently.

Automated synthetic rehearsal:

```sh
cargo test --locked --test store stopped_backup_restore_preserves_encrypted_work_links_and_expiry
cargo test --locked --test store missing_backup_key_is_not_silently_replaced
```

These tests verify decrypted pending work, cursors, ownership links, and expiry
after a stopped store copy, and rejection of an incomplete key backup. They do
not certify restoration of live Concord memberships; retain its complete SDK
state and confirm membership on the target host before enabling traffic.
