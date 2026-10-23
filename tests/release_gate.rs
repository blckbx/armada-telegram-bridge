use std::process::Command;

#[test]
fn live_state_and_generated_artifacts_are_excluded_from_git() {
    for path in [
        "data/spool.key",
        "data/bridge.sqlite3",
        "data/concord/cache",
        "release-artifacts/bom.json",
        "fuzz/artifacts/crash",
    ] {
        let status = Command::new("git")
            .args(["check-ignore", "--quiet", path])
            .status()
            .expect("git");
        assert!(
            status.success(),
            "sensitive/generated path must be ignored: {path}"
        );
    }
}

#[test]
fn release_documents_do_not_claim_unverified_acceptance() {
    let checklist = std::fs::read_to_string("docs/RELEASE_GATE.md").expect("release gate");
    assert!(checklist.contains("NOT RELEASE READY"));
    for criterion in [
        "live matrix",
        "security scans",
        "container provenance",
        "operator acceptance",
    ] {
        assert!(
            checklist.contains(criterion),
            "missing release gate: {criterion}"
        );
    }
}
