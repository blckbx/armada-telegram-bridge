# Armada Telegram Bridge

A self-hosted, bidirectional bridge between a private
[Armada](https://github.com/soapbox-pub/armada) channel and a Telegram group.

Identity onboarding is deliberately manual: an operator creates and provisions
the Armada keypair, accepts the Armada invitation, creates the Telegram bot,
and supplies its token before starting the bridge.

The project is in implementation. P00-P11 provide the Rust/test foundation, a
credential-gated Armada interoperability harness, and offline-validated static
configuration/domain types, plus encrypted transactional SQLite delivery
state, narrow Telegram and Concord transport adapters, the first durable
bidirectional attributed relay, bounded retry/recovery with graceful shutdown,
safe rich-text conversion with Telegram-aware splitting, bounded bidirectional
attachment transfer with private temporary-file cleanup, and ownership-checked
reply, edit, delete, and expiry fidelity. P13 adds attributed, editable reaction
summary replies in both directions, including removal and restart recovery.
P11 adds offline `doctor`, health and
aggregate metrics, private runtime logging, instance locking, SIGTERM handling,
and deployment templates (container acceptance remains pending).
Configuration setup is documented in
[docs/CONFIGURATION.md](docs/CONFIGURATION.md). The researched architecture,
security model, delivery semantics, and staged build plan are documented in
[docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md).
The PR-sized, test-first execution backlog is in
[docs/IMPLEMENTATION_PARTS.md](docs/IMPLEMENTATION_PARTS.md).

See [operations and backup/restore](docs/OPERATIONS.md),
[privacy and retention](docs/PRIVACY.md), and
[incident response](docs/INCIDENT_RESPONSE.md) before deployment.

P12 release tooling and offline regression gates are in place. This is still an
**unreleased candidate**, not a production-accepted release. See the
[release checklist](docs/RELEASE_GATE.md), [compatibility record](docs/COMPATIBILITY.md),
and [known limitations](docs/KNOWN_LIMITATIONS.md) for pending live/deployment gates.

> [!IMPORTANT]
> Bridging an Armada channel to Telegram changes the privacy boundary: the
> bridge reads messages and Telegram receives readable copies. Channel
> visibility remains governed by the configured Armada community and channel.
> Only deploy it with the informed consent of both communities.
