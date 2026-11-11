# Temporary rusqlite compatibility wrapper

`vector-core 0.10.0` requires `rusqlite ^0.38`. That release bundles a SQLite
version affected by RUSTSEC-2026-0459, and no patched 0.38 release exists.
Cargo permits only one `libsqlite3-sys` version because it links `sqlite3`, so
upgrading just the bridge's direct dependency is insufficient.

This local patch satisfies the SDK's old version requirement and re-exports
`rusqlite 0.41.1` without copying or changing its implementation. Both the bridge
and the SDK therefore use the same upstream types and patched SQLite library.
Only the features required by this dependency graph are forwarded. This wrapper
is for this repository and is not a general replacement for rusqlite 0.38.

Remove the `[patch.crates-io]` entry and this directory once a published SDK
supports a patched rusqlite release. The advisory policy remains unchanged.

Validation: compile the full SDK dependency graph; run all bridge tests,
including migrations, backup/restore, and SDK adapter tests; run cargo-deny
against the complete locked dependency graph. Private live SDK tests still
require the operator's configured test community.
