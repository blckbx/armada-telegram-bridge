# Candidate privacy review

Scope: source behavior and offline tests, not an independent security audit or
approval of the operator's live membership/configuration.

| Risk | Current control | Remaining acceptance |
| --- | --- | --- |
| Cross-platform disclosure | Static exact routing, authenticated author attribution, manual onboarding | Obtain informed consent in both rooms; verify actual membership |
| Credentials/state in source artifacts | `.env`, `bridge.toml`, `data/` excluded; Docker context allowlist; redacted secret scans | Scan final commit/history and protect exported artifacts |
| Operational content leaks | Fixed runtime log vocabulary, unlabeled aggregate metrics, loopback defaults | Keep monitoring private; do not enable raw SDK logging |
| Spool theft | AEAD payloads; private independent key; incomplete restore rejected | Protect both state and encrypted backups; host compromise defeats this boundary |
| File remnants/hostile inputs | Bounded staging, safe filenames/types, cancellation cleanup and parser tests | Abrupt-kill leftovers, client content risks and OS caches remain |
| False delete/expiry expectations | Ownership checks; ambiguous source deletion ignored | Communicate lack of guaranteed remote erasure |
| Excess retention | Cleanup primitive exists | No scheduled cleanup: operator must accept/document retention before release |
| Untrusted supply chain | Locked dependencies, source scans, license/SBOM tooling | Image/native review, immutable pins and actual provenance still pending |

No usernames, private room IDs, payloads, bot tokens, nsec values, or attachment
decryption URLs should appear in release evidence. The exact Nostr test vector
allowlisted in Gitleaks is public synthetic test material, not an operator key.
See PRIVACY.md and INCIDENT_RESPONSE.md for participant/operator obligations.
