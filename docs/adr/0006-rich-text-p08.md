# ADR 0006: Neutral rich text and explicit Telegram entities

Status: accepted

## Context

Armada carries Discord-flavored Markdown in message content. Telegram carries
plain text plus entities whose ranges use UTF-16 code units. Forwarding either
wire format directly would corrupt offsets around emoji, make nested syntax
fragile, and allow source markup to interfere with the bridge attribution.

## Decision

Formatting crosses the relay through a small transport-neutral model: visible
UTF-8 text plus semantic byte-aligned spans. The Telegram adapter converts
inbound UTF-16 ranges to checked UTF-8 boundaries and sends outbound formatting
as explicit Bot API entities without a Telegram parse mode. The Armada side
uses a hand-written parser and renderer for the supported Discord-flavored
Markdown subset rather than a chain of regular-expression substitutions.

The bridge preserves bold, italic, underline, strike, spoiler, inline code,
fenced code, safe links, quotes, and heading emphasis when the destination has
an equivalent. Unsupported syntax remains readable. Only `http`, `https`, and
`mailto` links become active. Directional controls and other spoofing-relevant
invisible characters are removed before rendering, while emoji joiners remain
intact.

Armada messages that exceed Telegram's 4,096-character limit are split first
at paragraphs, then line or whitespace boundaries, and finally at Unicode
grapheme boundaries. Spans are clipped and rebased per chunk before UTF-16
entities are generated. Each chunk repeats the trusted attribution and carries
a part number. The durable outbox stores a versioned serialized chunk set;
legacy P06/P07 plaintext payloads remain readable after upgrade.

## Consequences

Formatting conversion is deterministic and never depends on Telegram
MarkdownV2 escaping. Unsupported or unsafe formatting degrades to visible
text. A multi-chunk message remains one durable job, so a failure after an
earlier chunk was accepted can duplicate that earlier chunk on retry; this is
consistent with the bridge's documented at-least-once delivery guarantee.

## Test evidence

Golden tests cover the supported syntax, unsafe links and directional
controls, nested and crossing ranges, astral emoji, combining marks, RTL text,
and broad Unicode samples. Adapter tests exercise inbound and outbound Bot API
entities. Service tests prove both relay directions and validate grapheme-safe
splitting, per-chunk limits, and entity rebasing.
