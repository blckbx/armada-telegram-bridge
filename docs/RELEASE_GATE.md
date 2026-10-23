# P12 release gate — 0.1.0 candidate

Status: **NOT RELEASE READY**. This is an evidence checklist, not a published
release. Passing offline contracts does not certify live protocol behavior.
No tag, registry publication, bot membership change, or real message send is
performed by the release tooling.

**Privacy boundary:** Telegram receives readable copies of private Armada
traffic. **Delivery is at least once:** a destination may accept a send before
the bridge commits its receipt; a retry/restart can duplicate that message or
some chunks/files. Restoring an older backup can replay previously delivered work.

## Automated acceptance matrix

The suite drives the real service/store/rich-text code through fake transport
ports; HTTP fixtures separately exercise Telegram wire behavior. It is not a
live end-to-end matrix. Run `cargo test --locked --all-targets --all-features`.

| Requirement | Executable evidence | Live acceptance |
| --- | --- | --- |
| Attributed text in both directions, restart deduplication | `plaintext_bridge_baselines_relays_links_and_deduplicates_across_restart` | Previously confirmed by operator; repeat on exact candidate |
| Files both directions, boundaries and cleanup | `boundary_sized_attachments_cross_both_directions_and_plaintext_is_cleaned`, attachment and Telegram HTTP suites | Pending uploads/downloads with checksums |
| Unicode, styles, long messages | `rich_text_crosses_both_transports_and_long_armada_text_splits_safely`, rich-text suite | Pending client rendering checks |
| Network failure and ambiguous sends | `retries_use_deterministic_backoff_rate_limit_override_and_ambiguity_tracking`, `concord_partition_retry_survives_restart_without_losing_work` | Pending controlled partition/reconnect |
| Telegram rate limits | Same service test plus `bot_api_failures_have_distinct_retry_classifications` | Deterministic synthetic 429; do not flood Telegram to force a limit |
| Missing keys, incomplete rekey, removal | `rekey_and_removed_membership_quarantine_until_explicit_recovery` | Pending disposable-channel rotation/removal; fake states do not prove cryptography |
| Telegram removal/403 | `telegram_removal_forbidden_is_terminal_not_a_retry_storm` | Pending disposable-group removal/re-invite |
| Exact route/topic/identity scope | `router_rejects_wrong_source_route_mutation_and_bridge_identities`, Telegram filtering suite, Concord normalization tests | Pending negative route checks |
| Empty-database upgrade to schema 5 | `migrations_are_repeatable_and_enable_required_sqlite_guards` | No previously released database schemas exist |
| Rollback and recovery | `newer_schema_is_rejected_without_downgrading_and_backup_still_opens`, stopped backup/restore drill | Pending full live SDK-state restoration |
| Lock, health, SIGTERM | operations suite, shutdown/cancellation tests | Pending target-host supervisor smoke |
| Optional reply/edit/expiry | P10 service/adapter tests and opt-in live shapes | Pending; live deletion forwarding is not supported |
| Attributed reaction summaries and removals | reaction service/store and Telegram normalization tests | Pending live reactions in both clients; Telegram bot must be an administrator |

Do not run the whole ignored suite indiscriminately: some tests send/upload to
real chats. CONFIGURATION.md lists individually scoped commands and prerequisites.

## Release-blocking checklist

- [ ] Rerun the P13-expanded full offline Rust suite in an environment that
  permits localhost fixtures and Unix signal tests. Formatting, strict Clippy,
  and all non-socket reaction/service/store/Concord tests pass; the current
  sandbox could compile but not execute the Telegram HTTP and operations suites.
  Nine opt-in tests remain skipped (eight live, one container).
- [x] Source SBOM and dependency notice bundle generated from locked metadata.
- [x] Local security scans: RustSec advisories, configured license/source policy,
  redacted source/history Gitleaks. Repeat on the final committed candidate.
- [x] Final local bounded parser fuzz run: 164,717 executions in 31 seconds, no failure.
  Full local log is in `release-artifacts/fuzz-smoke.log`; seed SHA-256 is
  `801980f3b5a06d960d3daa506333233b23c6ce723f098c5d1813a7fdf9c91013`.
  A smoke run is not exhaustive fuzzing; rerun against the final candidate.
- [ ] Complete live matrix above on disposable rooms with informed participants.
- [ ] Clean-machine non-root/read-only container smoke and systemd-host smoke.
- [ ] Actual container provenance and image SBOM generated, inspected, and
  attached to the exact image digest. Source SBOM is not an image SBOM.
- [x] Rust requirement corrected to tested 1.99 after the older compiler check failed.
- [ ] Reviewed immutable base-image/action pins.
- [ ] Privacy/retention limitations explicitly accepted by the operator.
- [ ] Final operator acceptance: exact routing, manual onboarding, files/styles,
  recovery and rollback reviewed; record date, candidate commit, image digest,
  and result without usernames, chat IDs, private keys, or message bodies.

## Reproduce evidence

Tools used locally: cargo-deny 0.20.2, cargo-fuzz 0.13.2, Gitleaks source tag
v8.24.3 (Go builds may not embed the version string). Tools are not runtime
dependencies. Install into a dedicated tooling environment; never run scans
against the live `.env`, state, or plaintext attachment cache.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
python3 -m unittest discover -s scripts -p 'test_*.py'
cargo deny --locked check
cargo fetch --locked
python3 scripts/release_artifacts.py
cargo +nightly fuzz run rich_text -- -max_total_time=30 -max_len=8192 -print_funcs=0
```

Run Gitleaks with `.gitleaks.toml` and `--redact=100` on a clean checkout or a
temporary export of `git ls-files --cached --others --exclude-standard`; scan
Git history separately. A directory scan of a live checkout would include local
credentials and is not the intended source gate. Only one exact public Nostr
test vector is allowlisted; tests/directories are not blanket-exempted.

Artifacts are written under ignored `release-artifacts/`. The source SBOM is
CycloneDX 1.5 and includes 313 Cargo packages from the resolved graph, including
build/dev/other-platform dependencies. It records crate checksums and graph
edges, not local paths or environment variables. Notice collection includes
top-level LICENSE/COPYING/NOTICE files; distribution review must also consider
vendored/native and operating-system components in the final image.

Local evidence files: `offline-tests.log`, `fuzz-smoke.log`,
`dependency-check.log`, `source-secrets.json`, `history-secrets.json`,
`cargo-source.cdx.json`, and `THIRD_PARTY_NOTICES.txt`. The failed older-toolchain
probe is retained as `rust-1.89-check.log`. Cargo.lock SHA-256 for this run:
`981cdb8b9effa0e348209187d61c8d38aa044133c62a63599593566fa8ef9d02`.

`deny.toml` allows the certificate bundle's CDLA-Permissive-2.0 license and
the generated notice bundle includes its text. The sharing condition is in the
[upstream license](https://cdla.dev/permissive-2-0/). A scanner pass is a configured
policy check, not a claim that all redistribution obligations are automatically met.

CI includes offline tests, source security checks, a short fuzz run, and container
smoke. The manual `release-evidence.yml` workflow exports an OCI image archive
with SBOM/provenance requested and its checksum, without publishing to a registry.
The archive must be inspected after a successful build; a workflow file alone is
not provenance evidence. Docker's attestation behavior is described in its
[official documentation](https://docs.docker.com/build/ci/github-actions/attestations/).

No live or container gate was automatically marked passed in this workspace.
The Docker daemon was unavailable during P11. No CI run or signed image evidence
has been observed. Current work is an uncommitted candidate, not an immutable
release revision; rerun and attach sanitized results after operator review.
