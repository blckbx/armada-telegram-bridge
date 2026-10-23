# ADR 0005: Durable per-stream recovery

Status: accepted

## Context

P06 stopped at the first destination failure. That preserved pending work but
did not isolate routes, bound intake, remember retry timing, or provide a safe
shutdown and operator recovery path.

## Decision

Each outbox job has durable scheduler metadata: state, failed-attempt count,
next-attempt timestamp, secret-free failure class, and a possible-duplicate
flag. A scheduler processes only the oldest pending job in each
route/direction stream. A blocked head therefore preserves FIFO ordering while
other streams continue.

Retryable failures use capped exponential backoff with stable per-job jitter.
Telegram `retry_after` replaces the calculated delay. Ambiguous Telegram
timeouts and Concord SDK publish failures retain `possible_duplicate=true`.
Terminal failures and exhausted retries become dead letters until an operator
explicitly retries them.

Admission checks the configured global pending-job bound before inbox, outbox,
and cursor commit. Saturation rejects intake without acknowledging it and makes
service readiness false. Restart catch-up is bounded by event count and age.
Route quarantine is persisted and requires explicit resume.

On SIGINT the executable stops intake and drains immediately eligible jobs for
ten seconds. A send still running at the deadline is cancelled; its job remains
pending for restart.

## Consequences

Delivery remains at least once. A destination may accept a send immediately
before a timeout or process crash, so retry can create a duplicate. The flag is
diagnostic and cannot make Telegram sends exactly once.

Dead letters and quarantined routes reduce readiness until reviewed. Operator
commands expose only IDs and failure classes, never encrypted message bodies or
credentials.

## Test evidence

Deterministic service tests use failure-injecting ports, an adjustable wall
clock, and paused Tokio time. They cover capped retry delays, Telegram rate
limits, ambiguous timeout recording, FIFO isolation, dead-letter retry, queue
saturation without cursor advancement, bounded catch-up, persisted recovery
state, quarantine/resume, and deadline cancellation of a hung send.
