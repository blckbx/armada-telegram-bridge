# Armada Telegram Bridge

A self-hosted, bidirectional bridge between explicitly configured
[Armada](https://github.com/soapbox-pub/armada) channels and Telegram groups.
Messages written on either platform are copied to the other while preserving
sender attribution and as much common formatting as possible.

The bridge uses a manually provisioned Nostr identity for Armada and a manually
created Telegram bot. It never creates accounts, accepts Armada invitations, or
discovers destinations automatically.

> [!IMPORTANT]
> Bridging changes the privacy boundary. The bridge host reads message content,
> and Telegram receives readable copies of Armada traffic. Obtain the consent of
> both communities and protect the host, credentials, state, and backups.

## Features

- Bidirectional text messages with Armada display names and Telegram usernames.
- Exact routing between configured Armada community/channel IDs and Telegram
  group or forum-topic IDs.
- Community-visible and separately private Armada channels, provided the bridge
  identity can read the selected channel.
- A safe common formatting subset for bold, italic, underline, strike-through,
  spoilers, code, quotes, and links.
- Files, photos, audio, and documents in both directions. Telegram uploads are
  encrypted and uploaded through Armada's Blossom flow.
- Reply and supported edit propagation using durable message mappings.
- Best-effort propagation of Armada expiry to Telegram mirrors.
- Reaction bridging through editable attributed summary replies.
- Durable SQLite-backed queues, cursors, deduplication, bounded retries, and
  restart catch-up.
- Health, readiness, and aggregate metrics endpoints.
- Hardened systemd and container deployment templates.

Delivery is at least once. A timeout or crash after the destination accepts a
message but before the receipt is saved can produce a duplicate.

## Requirements

- Rust 1.99 or later for a source build, or Docker/Podman.
- A manually created Armada/Nostr keypair (`nsec` and matching `npub`).
- The Armada identity must already have accepted its invitation and be able to
  read the configured channel.
- A Telegram bot token created through BotFather.
- The Telegram bot must be a member of the target group.

Make the Telegram bot a group administrator for complete operation. Telegram
requires administrator status to deliver reaction updates. For ordinary group
messages, the bot must either be an administrator or have Group Privacy Mode
disabled through BotFather; after changing privacy mode, remove and re-add the
bot to the group.

Only one bridge instance may poll a Telegram bot token at a time. Stop an old
instance before starting a replacement.

## Build

```sh
git clone https://github.com/blckbx/armada-telegram-bridge.git
cd armada-telegram-bridge
cargo build --locked --release
```

The binary is created at `target/release/armada-telegram-bridge`.

## Configuration

Configuration is split deliberately:

- `bridge.toml` contains non-secret routing and operational settings.
- `.env`, in the same directory, contains the Armada `nsec` and Telegram token.

Create the files:

```sh
cp bridge.toml.sample bridge.toml
touch .env
chmod 600 .env
```

Put only the runtime credentials in `.env`:

```dotenv
ARMADA_NSEC=nsec1...
TELEGRAM_BOT_TOKEN=123456789:...
```

The bridge rejects a symlinked, oversized, group-readable, or world-readable
`.env`. Never commit this file.

Configure `bridge.toml`:

```toml
version = 1
data_dir = "./data"
health_listen = "127.0.0.1:9090"

[nostr]
expected_npub = "npub1..."

[telegram]
expected_bot_id = 123456789
mode = "long_polling"
protect_content = true
relay_other_bots = false

[[routes]]
name = "community-main"
community_id = "<64-character-lowercase-hex-community-id>"
channel_id = "<64-character-lowercase-hex-channel-id>"
telegram_chat_id = -1001234567890
# telegram_thread_id = 42

[limits]
max_text_bytes = 60000
max_file_bytes = 20000000
max_pending_jobs = 500
max_catchup_age_hours = 24
max_catchup_events = 1000

[startup]
first_run = "new_messages_only"
```

Important fields:

- `expected_npub` must be the public key derived from `ARMADA_NSEC`.
- `expected_bot_id` is the numeric prefix of the Telegram bot token.
- `community_id` and `channel_id` are canonical Armada IDs, not display names.
- `telegram_chat_id` is the negative Telegram group or supergroup ID.
- `telegram_thread_id` optionally restricts a route to one forum topic.
- `name` is the stable route ID. Keep it unchanged when migrating the same
  route, because durable cursors and message mappings are stored against it.
- `protect_content` asks Telegram to restrict forwarding and saving of messages
  sent by the bot. It is not screenshot or confidentiality protection.
- `relay_other_bots` controls whether messages authored by other Telegram bots
  may enter the bridge. The bridge always ignores its own messages.

Additional `[[routes]]` blocks can map more channels. A channel or route ID may
appear only once. Relative paths are resolved relative to `bridge.toml`, not the
current working directory.

## Validate and run

Run the offline configuration and local-state check first:

```sh
./target/release/armada-telegram-bridge doctor --config bridge.toml
```

Start the bridge:

```sh
./target/release/armada-telegram-bridge --config bridge.toml
```

A successful startup logs fixed, content-free JSON events:

```json
{"count":0,"event":"starting"}
{"count":0,"event":"ready"}
```

The process handles `SIGINT` and `SIGTERM`, stops intake, and performs a bounded
graceful drain. Give it at least 20 seconds before forcing termination.

### Health and metrics

The default listener is available only on loopback:

```sh
curl --fail http://127.0.0.1:9090/live
curl --fail http://127.0.0.1:9090/ready
curl --fail http://127.0.0.1:9090/metrics
```

- `/live` confirms that the HTTP task is responsive.
- `/ready` confirms a recent healthy scheduler observation.
- `/metrics` exposes aggregate readiness, pending-job, and dead-letter gauges.

These endpoints have no authentication. Do not expose them directly to the
Internet.

## Startup and downtime behavior

With `first_run = "new_messages_only"`, the first successful startup records a
baseline and does not replay pre-existing conversations. On later starts, the
bridge resumes from durable cursors and catches up missed Armada events within
`max_catchup_age_hours` and `max_catchup_events`. Telegram recovery is also
limited by Telegram's pending-update retention.

Pending destination deliveries remain in the encrypted local spool and resume
after restart. Long outages can exceed upstream or configured history bounds,
so this is not unlimited archival synchronization.

Do not delete the data directory to resolve an operational problem. It contains
delivery cursors, deduplication records, message mappings, pending jobs, the
spool encryption key, and Concord SDK state.

## Attachments and formatting

The default and maximum supported attachment size is 20,000,000 bytes. Plaintext
files are staged temporarily with private permissions and removed after normal
success, failure, timeout, or cancellation. A host crash may leave temporary
files behind; disk deletion is not secure erasure.

Telegram uploads are sent through Armada's encrypted Blossom attachment flow.
Armada attachments are downloaded, integrity-checked, decrypted, and uploaded
to Telegram. An attachment that fails its cryptographic integrity check is
omitted instead of forwarding unsafe bytes.

Formatting uses a conservative common subset. Unsupported styles remain
readable as unformatted text. Plain Telegram URLs remain plain URLs in Armada;
explicitly labelled links remain labelled links.

## Reactions and mutations

Reactions are represented by a bridge-owned summary reply rather than native
reactions on the destination. The summary is edited as reactions change and
removed when the set becomes empty. Telegram custom emoji and some anonymous
reaction attribution cannot be reproduced exactly.

Source deletion forwarding is disabled. Armada expiry triggers a best-effort
deletion of the corresponding Telegram mirror. Attachment edits and edits to
messages that were split into multiple Telegram messages are skipped. Replies
fall back to a quote when the original destination mapping is unavailable.

## Operator commands

Stop the running bridge before using these commands; they acquire the same
exclusive state lock.

```sh
# List dead-lettered jobs.
armada-telegram-bridge dead-letters --config bridge.toml

# Retry a reviewed dead letter.
armada-telegram-bridge retry --config bridge.toml --job 17

# Permanently skip an undeliverable job and unblock its route.
armada-telegram-bridge skip --config bridge.toml --job 17 --acknowledge-loss

# Resume a route after reviewing and correcting a safety failure.
armada-telegram-bridge resume --config bridge.toml --route community-main
```

Skipping loses that destination delivery permanently. A retry marked as a
possible duplicate may repeat a message that the destination already accepted.

## systemd deployment

The supplied unit runs the bridge as the dedicated `armada-bridge` user with a
read-only system filesystem, inaccessible home directories, no capabilities,
and bounded resources.

Install the binary and prepare configuration:

```sh
sudo install -m 0755 target/release/armada-telegram-bridge /usr/local/bin/armada-telegram-bridge
sudo groupadd --system armada-bridge
sudo useradd --system --gid armada-bridge --home /nonexistent --shell /usr/sbin/nologin armada-bridge
sudo install -d -m 0750 -o root -g armada-bridge /etc/armada-telegram-bridge
sudo install -m 0640 -o root -g armada-bridge bridge.toml /etc/armada-telegram-bridge/bridge.toml
sudo install -m 0600 -o armada-bridge -g armada-bridge .env /etc/armada-telegram-bridge/.env
sudo install -m 0644 deploy/armada-telegram-bridge.service /etc/systemd/system/armada-telegram-bridge.service
```

If the service account or group already exists, skip the corresponding creation
command. For this deployment, set:

```toml
data_dir = "/var/lib/armada-telegram-bridge"
```

Then enable the service:

```sh
sudo systemd-analyze verify /etc/systemd/system/armada-telegram-bridge.service
sudo systemctl daemon-reload
sudo systemctl enable --now armada-telegram-bridge
sudo journalctl -u armada-telegram-bridge -f
```

Do not place configuration or state under `/home` while `ProtectHome=true` is
enabled. `User=` selects the process identity but does not override the systemd
filesystem sandbox.

## Container deployment

Prepare an absolute configuration directory containing `bridge.toml` and its
mode-`0600` `.env`, plus a separate mode-`0700` state directory owned by the
container UID/GID. In the container configuration use:

```toml
data_dir = "/state"
health_listen = "0.0.0.0:9090"
```

Start with the supplied Compose file:

```sh
export BRIDGE_CONFIG_DIR=/absolute/path/to/config
export BRIDGE_STATE_DIR=/absolute/path/to/state
export BRIDGE_UID=10001
export BRIDGE_GID=10001
docker compose -f deploy/compose.yaml up --build -d
```

The Compose template publishes health only on host loopback, uses a read-only
root filesystem, drops all capabilities, and gives shutdown a 20-second grace
period.

## Backup and migration

Stop the bridge before copying state. Back up the complete data directory,
including `bridge.sqlite3`, any SQLite `-wal`/`-shm` files, `spool.key`, and the
entire `concord/` directory. Preserve ownership and permissions, and protect the
backup as private message data.

Restore the entire directory into an empty mode-`0700` destination owned by the
bridge user. Keep the same route names. Run `doctor`, then start exactly one
instance. An old backup can replay work delivered after the backup was created.

## Security and limitations

- Telegram group chats are not end-to-end encrypted.
- The bridge operator and anyone with both the database and spool key can access
  retained payloads.
- Delivered data may remain on relays, Telegram, participant devices, and
  backups after the source is edited, expired, or removed.
- Active/active operation, network filesystems, and two hosts using the same bot
  token are unsupported.
- Runtime logs deliberately contain fixed event names and counts rather than raw
  SDK errors, message bodies, credentials, usernames, or attachment URLs.
- No automatic retention cleanup is currently scheduled.

For more detail, see:

- [Configuration](docs/CONFIGURATION.md)
- [Operations and recovery](docs/OPERATIONS.md)
- [Privacy and retention](docs/PRIVACY.md)
- [Known limitations](docs/KNOWN_LIMITATIONS.md)
- [Incident response](docs/INCIDENT_RESPONSE.md)

## License

MIT. See [LICENSE](LICENSE).
