# Configuration

P02 introduces an offline-validated TOML configuration. It performs no Armada
or Telegram network requests.

Start from the committed template:

```sh
cp bridge.toml.sample bridge.toml
cp .env.sample .env
chmod 600 .env
```

Set the two runtime credentials in `.env`:

```dotenv
ARMADA_NSEC=nsec1...
TELEGRAM_BOT_TOKEN=123456789:...
```

Set the derived `npub`, Telegram bot ID, canonical 64-character hexadecimal
Armada community/channel IDs, and negative numeric Telegram group ID in
`bridge.toml`. Relative paths are resolved relative to `bridge.toml`.

The same `.env` also supports P01's credential-gated interoperability tests;
the remaining `ARMADA_BRIDGE_LIVE_*` values are test-only. Runtime routing and
policy always come from `bridge.toml`. No credential is placed in TOML or on the
command line.

The loader rejects:

- a missing, symlinked, oversized, group-accessible, or world-accessible `.env`;
- malformed credentials or an `nsec`/`npub` or token/bot-ID mismatch;
- display names in place of canonical Armada identifiers;
- zero or positive Telegram group IDs, duplicate route IDs, and invalid topic
  IDs; and
- zero or excessive operational limits.

Both secrets serialize and format as `[REDACTED]`. The configuration
fingerprint includes effective routing and policy values but never credential
contents.

For reaction bridging, make the Telegram bot an administrator of each configured
group. No additional token or configuration key is needed. Without administrator
status, ordinary messages continue to bridge but Telegram does not deliver the
per-message reaction updates required for attributed summaries.

P04 adds a read-only live smoke test for the manually created Telegram bot and
private group. It calls `getMe` and `getChat`; it does not send messages or
modify Telegram state:

```sh
cargo test --locked --test live_telegram configured_bot_and_private_group_are_accessible -- --ignored --nocapture
```

## Attachment policy and live checks

P09 uses `limits.max_file_bytes` as both the per-file limit and the aggregate
limit for attachments in one source message. The default and supported maximum
is 20,000,000 bytes. Downloads are staged beneath `data_dir/tmp/attachments`
in randomized mode-`0600` files and removed after success, failure, timeout, or
cancellation. Filenames and declared media types are untrusted; the bridge
sanitizes names and detects supported media types from file bytes.

The two attachment live tests are intentionally separate from the read-only
smoke checks because each one uploads the disposable file configured by
`ARMADA_BRIDGE_LIVE_FILE` to the first configured route. Run only the side you
want to verify:

```sh
cargo test --locked --test live_telegram configured_private_group_can_receive_attachment -- --ignored --nocapture
cargo test --locked --test live_concord configured_channel_can_send_attachment_through_blossom -- --ignored --nocapture
```

Telegram receives an ordinary Bot API media/document upload. Armada receives
an encrypted Concord attachment whose ciphertext is uploaded through the
SDK's Blossom path. The bridge never persists the disposable plaintext beyond
its temporary staging lifetime.

P10 also provides opt-in Armada event-shape checks. These tests wait up to three
minutes while an operator performs the described action in the configured
channel; they never create, edit, or expire the source messages themselves:

```sh
cargo test --locked --test live_concord configured_channel_reports_reply_and_edit_shapes -- --ignored --nocapture
cargo test --locked --test live_concord configured_channel_reports_expiry_shape -- --ignored --nocapture
```

Set the corresponding `ARMADA_BRIDGE_LIVE_P10_*` markers in `.env` first. The
reply/edit test expects the reply marker as a real threaded reply and then an
edit of that same message to the edit marker. The expiry test expects its marker
to be sent with Armada's self-destruct timer enabled.

## Recovery operations

P07 persists retry timing, terminal failures, and route quarantine state in the
encrypted-spool SQLite database. These commands inspect or change only local
durable state; they do not contact Armada or Telegram:

```sh
cargo run -- dead-letters --config bridge.toml
cargo run -- retry --config bridge.toml --job 17
cargo run -- skip --config bridge.toml --job 17 --acknowledge-loss
cargo run -- resume --config bridge.toml --route private-community
```

`dead-letters` prints job IDs and secret-free failure classes, never message
plaintext. Review the cause before retrying a job or resuming a route. A job
marked `possible_duplicate=true` had an ambiguous destination outcome and may
produce a duplicate when retried.

Use `skip` only when the destination copy can be permanently omitted. It clears
the encrypted queued payload, records a local audit row, and unblocks later FIFO
work. The acknowledgement flag is mandatory. Stop the bridge before running any
operator command because they acquire the same exclusive state lock.
