# Credential-gated live tests

Live Armada, Concord, Blossom, and Telegram tests belong here. They must be
marked `#[ignore]`, require explicit operator-supplied configuration, and never
embed or print credentials, private messages, or real room identifiers.

The default `cargo test --all-targets --all-features` command may compile these
tests but must not contact a network.

## P01 Concord setup

Use a disposable identity and an exact test channel. The channel may be
community-visible or separately private, but the bot must be able to read it.
Perform every introduction step manually before running a test:

1. Create the Nostr keypair outside this repository.
2. Sign into Armada with that identity and manually accept the intended invite.
3. Record the `nsec` as `ARMADA_NSEC`, plus the expected `npub`, canonical
   community ID, and channel ID.

Copy the committed sample to the ignored local dotenv file and lock down its
permissions:

```sh
cp .env.sample .env
chmod 600 .env
```

Replace every required placeholder in `.env`. The live tests load it
automatically. Put real values only in `.env`; `.env.sample` is committed and
must always contain placeholders.

Run each test in its own process because `vector_sdk` supports one identity per
process. The test creates a fresh temporary SDK data directory, so membership
must be recovered from relay-backed account state rather than a previous local
database:

```text
cargo test --test live_concord manual_membership_is_recovered -- --ignored --exact
```

For receive verification, manually send a unique harmless marker from a second
Armada identity, then set:

```text
ARMADA_BRIDGE_LIVE_RECEIVE_MARKER=<unique marker>
ARMADA_BRIDGE_LIVE_EXPECTED_AUTHOR_NPUB=npub1...
```

Run:

```text
cargo test --test live_concord configured_channel_can_receive -- --ignored --exact
```

For send verification, set a new harmless marker:

```text
ARMADA_BRIDGE_LIVE_SEND_MARKER=<unique marker>
```

Run the test and confirm the marker appears in the configured Armada channel:

```text
cargo test --test live_concord configured_channel_can_send -- --ignored --exact
```

Securely remove `.env` when finished. Never paste live values into
`.env.sample`, an issue, pull request, fixture, screenshot, or test log.
