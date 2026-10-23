# ADR 0007: Bounded private attachment relay

Status: accepted

Date: 2026-10-22

## Context

An attachment crosses two different trust and transport boundaries. Telegram
exposes file metadata followed by a Bot API download, while Concord supplies
encrypted attachment metadata, integrity data, and Blossom fallback URLs.
Source sizes, names, media types, URLs, and network responses cannot be treated
as trustworthy. Plaintext also must not remain on disk after a delivery attempt
ends or its task is cancelled.

## Decision

The bridge validates every declared size before download and caps both each
file and the aggregate attachments of a source message at the configured
`max_file_bytes`. At most ten attachments are accepted. It stages every source
file before sending any destination text or media, using randomized files in a
mode-`0700` directory with mode-`0600` file permissions. Drop guards remove
staged files and SDK-created preview copies on success, failure, timeout,
cancellation, and unwind. Destination names are basename-only, stripped of
controls, and bounded; media selection uses magic-byte detection instead of a
source MIME declaration.

Telegram downloads are streamed with declared, response, and actual byte
checks. Redirects are disabled, and the returned Bot API file path is validated
before it is joined to the operator-configured API origin. Uploads use explicit
multipart Bot API requests. A specialized photo, animation, video, audio, or
voice method may fall back to `sendDocument` only after an unambiguous Bot API
400 rejection; an uncertain network result is never retried as a different
method because that could duplicate delivery.

Concord downloads remain inside `vector_sdk`'s bounded attachment API so its
authenticated decryption, integrity verification, SSRF checks, and Blossom
fallback behavior are preserved. The SDK currently returns a bounded byte
buffer rather than a stream, so bridge memory is bounded by the same 20 MB
policy. Concord's current file-send API has no caption parameter. The bridge
therefore sends the attributed text as an ordered companion message before the
file. Telegram uses a caption only when the complete attributed text fits its
caption limit; otherwise it also receives ordered companion text.

Telegram media-group items found in one bounded `getUpdates` response are
annotated with stable positions and released immediately. The bridge does not
wait indefinitely for missing album members. Durable attachment locators are
stored only inside the already encrypted outbox payload, and debug output
redacts their encryption keys, nonces, and URLs.

## Consequences

No plaintext attachment is intentionally durable. Staging all files before
destination sends prevents a late malformed or oversized source file from
creating a partial destination message, at the cost of temporary disk usage up
to the per-message bound. Delivery remains at least once: a crash after a
destination accepts one item but before the job is committed can produce a
duplicate on retry. A multi-file Armada message is currently emitted as a
companion followed by individual files because the SDK has no atomic album
send primitive.

## Test evidence

Unit and adapter tests cover size lies, aggregate limits, private randomized
temporary files, cleanup, hostile filenames, MIME sniffing, unsafe URL classes,
Telegram streamed downloads, multipart captions, safe media-method fallback,
media-group ordering, and redacted Concord locators. Service tests transfer
boundary-sized fixtures in both directions and cancel a blocked upload while
checking that plaintext is gone and the durable job remains retryable. The
ignored live tests perform explicit operator-triggered Telegram and
Concord/Blossom uploads.
