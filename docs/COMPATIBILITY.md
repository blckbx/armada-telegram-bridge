# Compatibility record — candidate 0.1.0

| Component | Evidence / boundary |
| --- | --- |
| Bridge | Cargo version 0.1.0; uncommitted candidate, not released |
| `vector_sdk` | Exactly 0.11.0 in Cargo.lock; published SDK revision `b4c5b6eff89a6efbfeef726aae4e176451b8e6d6` recorded in ADR 0001 |
| `vector-core` | 0.10.0 in Cargo.lock |
| `nostr-sdk` | 0.45.1 in Cargo.lock |
| Armada | Researched revision `eb69625977b887f988ce6bab2bd70d2688d88feb`; actual deployed client build must be recorded during acceptance |
| Concord specification | Researched revision `f84d55523593688c70c1b1a1b765992103157f1b`; SDK behavior, not the document alone, governs compatibility |
| Telegram transport | Direct Bot API JSON/HTTP using reqwest 0.12.28; **teloxide is not used** |
| Telegram Bot API | Official documentation retrieved 2026-10-23 lists 10.3 (2026-08-24). The hosted endpoint is unversioned; this is not a negotiated or fully tested API version |
| Database | Schema 5; new/empty database migration tested; future schema rejected; restore backup rather than downgrade in place |
| Rust requirement | Candidate requires tested rustc 1.99.0; fuzz toolchain 1.101.0-nightly (2026-10-06). The previous 1.85 declaration was removed: an explicit 1.89 build fails in vector-core on `floor_char_boundary`. This is a tested baseline, not a claim to have found the oldest compatible compiler |
| Deployment | Unix locks/signals tested locally; rootless read-only image and systemd target-host verification pending |

The Telegram adapter uses the established `getMe`, `getChat`, `getUpdates`,
`sendMessage`, entity/reply parameters, media/file methods, `editMessageText`,
`deleteMessage`, and reaction-update subset. Reaction intake explicitly requests
`message_reaction` and `message_reaction_count`; the bot must be a group
administrator for Telegram to deliver those updates. New rich-message/ephemeral API features are not
implemented merely because the current documentation describes them.
See the [official Bot API](https://core.telegram.org/bots/api) for method contracts.

The exact private community/channel and numeric Telegram group/topic are static.
SDK updates must rerun normalization, ownership, attachment, recovery, and live
event-shape tests before changing these pins. Do not infer moderation authors
from the SDK's ambiguous removal callback. Telegram group deletions do not
produce the general source event this bridge would need to relay them safely.

Record the actual Armada client build, relay/Blossom implementations and the
candidate commit/image digest in the private operator acceptance record. Do not
put private relay URLs with attachment keys, credentials, room IDs, or raw public
keys in public release artifacts.
