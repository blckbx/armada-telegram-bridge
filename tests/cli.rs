use std::process::Command;

const SYNTHETIC_NSEC: &str = "nsec1synthetic-test-secret";
const SYNTHETIC_TOKEN: &str = "123456:synthetic-test-token";

fn bridge_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_armada-telegram-bridge"));
    command
        .env("ARMADA_NSEC", SYNTHETIC_NSEC)
        .env("TELEGRAM_BOT_TOKEN", SYNTHETIC_TOKEN);
    command
}

#[test]
fn help_succeeds_without_disclosing_environment_secrets() {
    let output = bridge_command()
        .arg("--help")
        .output()
        .expect("bridge binary should start");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
    let combined = format!("{stdout}{stderr}");

    assert!(combined.contains("Usage:"));
    assert!(combined.contains("--config"));
    assert!(!combined.contains(SYNTHETIC_NSEC));
    assert!(!combined.contains(SYNTHETIC_TOKEN));
}

#[test]
fn starting_without_configuration_fails_concisely_without_panicking() {
    let output = bridge_command()
        .output()
        .expect("bridge binary should start");

    assert_eq!(output.status.code(), Some(2));

    let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
    assert_eq!(stderr, "error: missing required --config <PATH>\n");
    assert!(!stderr.to_lowercase().contains("panicked"));
    assert!(!stderr.contains(SYNTHETIC_NSEC));
    assert!(!stderr.contains(SYNTHETIC_TOKEN));
}

#[test]
fn configured_startup_invokes_the_offline_loader() {
    let missing = tempfile::tempdir()
        .expect("temporary directory should be created")
        .path()
        .join("missing.toml");
    let output = bridge_command()
        .arg("--config")
        .arg(&missing)
        .output()
        .expect("bridge binary should start");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
    assert!(stderr.contains("cannot read configuration"));
    assert!(stderr.contains("missing.toml"));
    assert!(!stderr.contains(SYNTHETIC_NSEC));
    assert!(!stderr.contains(SYNTHETIC_TOKEN));
}
