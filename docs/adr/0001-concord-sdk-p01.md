# ADR 0001: Concord SDK boundary and manual-membership probe

Status: provisionally accepted; live recovery confirmed, message probes pending

Date: 2026-10-22

## Context

The bridge identity, Armada invitation acceptance, and secret provisioning are
operator-controlled. The first protocol risk is whether a fresh bridge process
can recover an invitation already accepted in Armada using only the supplied
identity and relay-backed account state. Automatic invitation acceptance is not
permitted as a fallback.

Concord protocol behavior is too broad and security-sensitive to reimplement in
the bridge. The public Rust SDK provides identity login, community-list sync,
channel key recovery, authenticated inner authors, history, and sends.

## Decision

- Pin `vector_sdk` exactly to `0.11.0` with default features disabled.
- Accept its locked `vector-core 0.10.0` and `nostr-sdk 0.45.1` protocol stack.
- Build the SDK only with an explicit operator-supplied `nsec` and data
  directory.
- Set `InvitePolicy::Manual` explicitly. The probe never calls identity
  generation, `accept_invite`, `public`, or `whitelist` APIs.
- Call `sync_communities`, then select the community and channel by exact
  canonical IDs and require the private channel to be readable.
- Query only the configured channel and surface the SDK-provided inner author.
- Keep SDK types inside `concord_probe` now and the `ConcordPort` adapter in P05.

The P01 live suite creates a new temporary SDK data directory for every test.
This prevents an old local database from making membership recovery appear to
work. Because the SDK owns process-global identity state, live tests are run one
at a time in separate test processes.

## Test evidence

Credential-free evidence:

- The exact-scope test rejects a sibling channel and a different community.
- The full default suite compiles the real SDK adapter without contacting the
  network.
- Three credential-gated tests are present and ignored by default:
  `manual_membership_is_recovered`, `configured_channel_can_receive`, and
  `configured_channel_can_send`.

Live evidence recorded on 2026-10-22:

- `manual_membership_is_recovered` passed against the operator-provisioned
  private community/channel with real relay access;
- the test used a new temporary SDK data directory, proving recovery from
  relay-backed state rather than reuse of a local database; and
- exact canonical community/channel selection returned a private, readable
  channel for the configured identity.

P05 reran membership recovery and a bounded history sync through the production
`ConcordAdapter` on 2026-10-22. Both passed with a fresh SDK data directory.
The live state also established that `is_readable()` is the authoritative key
signal: this Armada-compatible private channel is readable while the SDK
reports epoch `0`, so consumers must not infer missing key material from the
epoch value alone.

The receive/send marker values remain intentionally unconfigured. P01 is not
complete until those two ignored tests pass and the outbound marker is visibly
confirmed in Armada.

## Compatibility record

- `vector_sdk 0.11.0`, published from Vector commit
  `b4c5b6eff89a6efbfeef726aae4e176451b8e6d6`
- `vector-core 0.10.0`
- `nostr-sdk 0.45.1`
- researched Armada revision
  `eb69625977b887f988ce6bab2bd70d2688d88feb`
- researched Concord specification revision
  `f84d55523593688c70c1b1a1b765992103157f1b`

These versions now have live proof for membership recovery. Message receive and
send compatibility remain pending their explicit marker runs.

## Follow-up live matrix

After the core recovery/receive/send tests pass, extend the same disposable
environment with test-first cases for:

- encrypted file upload and bounded download;
- disconnect, reconnect, and cursor-based history recovery;
- private-channel key rotation;
- member removal and loss of readability;
- community refounding and recovery of the new root; and
- Armada-compatible reply, edit, and delete event shapes.

Failures in this matrix may change the P05 adapter design. They must not broaden
the bridge into accepting invites or generating identities.

## Consequences

The production dependency graph becomes substantially larger, but the bridge
does not own cryptographic derivations or Concord state transitions. The live
tests require manual setup and cannot run in ordinary CI. SDK upgrades must
repeat this ADR's live matrix before merge.
