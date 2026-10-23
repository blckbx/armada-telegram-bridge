#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use armada_telegram_bridge::config::{BridgeConfig, ConfigError, FirstStartupPolicy, TelegramMode};
use tempfile::TempDir;
use vector_sdk::nostr::{Keys, ToBech32};

const NSEC: &str = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";
const TOKEN_SECRET_A: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TOKEN_SECRET_B: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";

struct Fixture {
    _directory: TempDir,
    config_path: PathBuf,
    dotenv_path: PathBuf,
    expected_npub: String,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let expected_npub = Keys::parse(NSEC)
            .expect("fixture nsec should parse")
            .public_key()
            .to_bech32()
            .expect("fixture npub should encode");
        let dotenv_path = directory.path().join(".env");
        write_dotenv(&dotenv_path, NSEC, TOKEN_SECRET_A);
        let config_path = directory.path().join("bridge.toml");

        let fixture = Self {
            _directory: directory,
            config_path,
            dotenv_path,
            expected_npub,
        };
        fixture.write_config(&fixture.valid_toml());
        fixture
    }

    fn valid_toml(&self) -> String {
        format!(
            r#"version = 1
data_dir = "data"

[nostr]
expected_npub = "{}"

[telegram]
expected_bot_id = 123456

[[routes]]
name = "private-community"
community_id = "{}"
channel_id = "{}"
telegram_chat_id = -1001234567890
"#,
            self.expected_npub,
            "11".repeat(32),
            "22".repeat(32),
        )
    }

    fn write_config(&self, content: &str) {
        fs::write(&self.config_path, content).expect("configuration should be written");
    }

    fn load(&self) -> Result<BridgeConfig, ConfigError> {
        BridgeConfig::load(&self.config_path)
    }
}

fn write_dotenv(path: &Path, nsec: &str, token_secret: &str) {
    fs::write(
        path,
        format!("ARMADA_NSEC={nsec}\nTELEGRAM_BOT_TOKEN=123456:{token_secret}\n"),
    )
    .expect("dotenv should be written");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .expect("dotenv permissions should be restricted");
}

#[test]
fn doctor_validates_offline_without_creating_state_or_changing_configuration() {
    let fixture = Fixture::new();
    let original = fs::read(&fixture.config_path).expect("config bytes");
    let secrets = fs::read(&fixture.dotenv_path).expect("secret bytes");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_armada-telegram-bridge"))
        .arg("doctor")
        .arg("--config")
        .arg(&fixture.config_path)
        .output()
        .expect("doctor starts");
    assert!(output.status.success());
    assert!(
        !fixture
            .config_path
            .parent()
            .expect("parent")
            .join("data")
            .exists()
    );
    assert_eq!(fs::read(&fixture.config_path).expect("config"), original);
    assert_eq!(fs::read(&fixture.dotenv_path).expect("dotenv"), secrets);
    let output = String::from_utf8(output.stdout).expect("UTF-8");
    assert!(output.contains("doctor_passed"));
    assert!(!output.contains(NSEC));
    assert!(!output.contains(&fixture.expected_npub));
}

#[test]
#[ignore = "requires locally built Docker image; runs offline with synthetic credentials"]
fn container_doctor_smoke() {
    let fixture = Fixture::new();
    let uid = std::process::Command::new("id")
        .arg("-u")
        .output()
        .expect("UID");
    let uid = String::from_utf8(uid.stdout).expect("UID string");
    assert_ne!(uid.trim(), "0", "run smoke as a non-root operator");
    let mount = format!(
        "type=bind,src={},dst=/config,readonly",
        fixture.config_path.parent().expect("parent").display()
    );
    let output = std::process::Command::new("docker").args([
        "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--user", uid.trim(), "--mount", &mount,
        "--entrypoint", "/bin/sh", "armada-telegram-bridge:p11", "-ec",
        r#"test $(id -u) -ne 0; root_ro=no; while read -r device mount kind options rest; do if [ "$mount" = / ]; then case ",$options," in *,ro,*) root_ro=yes;; esac; fi; done < /proc/mounts; test "$root_ro" = yes; exec /usr/local/bin/armada-telegram-bridge doctor --config /config/bridge.toml"#,
    ]).output().expect("docker starts");
    assert!(
        output.status.success(),
        "container smoke failed (inspect Docker locally)"
    );
    assert!(
        String::from_utf8(output.stdout)
            .expect("UTF-8")
            .contains("doctor_passed")
    );
}

#[test]
fn valid_configuration_has_typed_values_and_safe_defaults() {
    let fixture = Fixture::new();
    let config = fixture.load().expect("valid configuration should load");

    assert_eq!(config.version(), 1);
    assert_eq!(
        config.data_dir(),
        fixture
            .config_path
            .parent()
            .expect("config should have a parent")
            .join("data")
    );
    assert_eq!(config.routes()[0].id().as_str(), "private-community");
    assert_eq!(config.routes()[0].community_id().as_str(), "11".repeat(32));
    assert_eq!(config.routes()[0].channel_id().as_str(), "22".repeat(32));
    assert_eq!(
        config.routes()[0].telegram_chat_id().get(),
        -1_001_234_567_890
    );
    assert_eq!(config.telegram().mode(), TelegramMode::LongPolling);
    assert!(config.telegram().protect_content());
    assert!(!config.telegram().relay_other_bots());
    assert_eq!(config.limits().max_file_bytes().get(), 20_000_000);
    assert!(config.limits().max_pending_jobs().get() > 0);
    assert_eq!(config.first_startup(), FirstStartupPolicy::NewMessagesOnly);
    assert_eq!(config.nostr().nsec().expose(), NSEC);
}

#[test]
fn missing_and_unsafe_dotenv_files_are_rejected() {
    let fixture = Fixture::new();
    fs::remove_file(&fixture.dotenv_path).expect("dotenv should be removed");
    assert!(matches!(fixture.load(), Err(ConfigError::DotenvIo { .. })));

    write_dotenv(&fixture.dotenv_path, NSEC, TOKEN_SECRET_A);
    fs::set_permissions(&fixture.dotenv_path, fs::Permissions::from_mode(0o620))
        .expect("permissions should change");
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::UnsafeDotenvPermissions { .. })
    ));
}

#[test]
fn dotenv_requires_each_credential_exactly_once() {
    let fixture = Fixture::new();

    for (content, missing) in [
        (
            format!("TELEGRAM_BOT_TOKEN=123456:{TOKEN_SECRET_A}\n"),
            "ARMADA_NSEC",
        ),
        (format!("ARMADA_NSEC={NSEC}\n"), "TELEGRAM_BOT_TOKEN"),
    ] {
        fs::write(&fixture.dotenv_path, content).expect("dotenv should be written");
        let error = fixture.load().expect_err("missing credential must fail");
        assert!(matches!(
            error,
            ConfigError::MissingDotenvKey { name } if name == missing
        ));
    }

    fs::write(
        &fixture.dotenv_path,
        format!(
            "ARMADA_NSEC={NSEC}\nARMADA_NSEC={NSEC}\nTELEGRAM_BOT_TOKEN=123456:{TOKEN_SECRET_A}\n"
        ),
    )
    .expect("dotenv should be written");
    assert!(matches!(fixture.load(), Err(ConfigError::InvalidDotenv)));
}

#[test]
fn malformed_credentials_and_identity_mismatches_are_rejected_without_leaking() {
    let fixture = Fixture::new();

    for bad_value in ["not-an-nsec", "nsec1bad"] {
        write_dotenv(&fixture.dotenv_path, bad_value, TOKEN_SECRET_A);
        let error = fixture.load().expect_err("malformed credential must fail");
        let rendered = format!("{error:?} {error}");
        assert!(rendered.contains("valid nsec"));
        assert!(!rendered.contains(bad_value));
    }

    for bad_value in [
        "not-a-token",
        "abc:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "0:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "123456:short",
        "123456:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!",
    ] {
        fs::write(
            &fixture.dotenv_path,
            format!("ARMADA_NSEC={NSEC}\nTELEGRAM_BOT_TOKEN={bad_value}\n"),
        )
        .expect("dotenv should be written");
        fs::set_permissions(&fixture.dotenv_path, fs::Permissions::from_mode(0o600))
            .expect("dotenv permissions should be restricted");
        let error = fixture.load().expect_err("malformed credential must fail");
        let rendered = format!("{error:?} {error}");
        assert!(rendered.contains("valid Telegram bot token"));
        assert!(!rendered.contains(bad_value));
    }

    write_dotenv(&fixture.dotenv_path, NSEC, TOKEN_SECRET_A);

    let mismatched = fixture.valid_toml().replace(
        &fixture.expected_npub,
        "npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqzqujme",
    );
    fixture.write_config(&mismatched);
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::IdentityMismatch { .. })
    ));
}

#[test]
fn nonnegative_telegram_groups_duplicate_routes_and_names_as_ids_are_rejected() {
    for chat_id in [0, 1] {
        let fixture = Fixture::new();
        fixture.write_config(
            &fixture
                .valid_toml()
                .replace("-1001234567890", &chat_id.to_string()),
        );
        assert!(matches!(
            fixture.load(),
            Err(ConfigError::InvalidField {
                field: "routes.telegram_chat_id",
                ..
            })
        ));
    }

    let fixture = Fixture::new();
    let duplicate = format!(
        "{}\n{}",
        fixture.valid_toml(),
        route_toml("private-community", "33", "44")
    );
    fixture.write_config(&duplicate);
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::DuplicateRouteId { .. })
    ));

    for field in ["community_id", "channel_id"] {
        let fixture = Fixture::new();
        let canonical = if field == "community_id" {
            "11".repeat(32)
        } else {
            "22".repeat(32)
        };
        fixture.write_config(&fixture.valid_toml().replace(&canonical, "general"));
        assert!(matches!(
            fixture.load(),
            Err(ConfigError::InvalidField { .. })
        ));
    }
}

#[test]
fn zero_and_excessive_limits_are_rejected() {
    for (field, value) in [
        ("max_text_bytes", 1_000_001),
        ("max_file_bytes", 20_000_001),
        ("max_pending_jobs", 100_001),
        ("max_catchup_age_hours", 721),
        ("max_catchup_events", 100_001),
    ] {
        let fixture = Fixture::new();
        fixture.write_config(&format!(
            "{}\n[limits]\n{field} = {value}\n",
            fixture.valid_toml()
        ));
        assert!(matches!(
            fixture.load(),
            Err(ConfigError::InvalidLimit { .. })
        ));
    }

    let fixture = Fixture::new();
    fixture.write_config(&format!(
        "{}\n[limits]\nmax_pending_jobs = 0\n",
        fixture.valid_toml()
    ));
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::InvalidLimit { .. })
    ));
}

#[test]
fn configuration_shape_and_additional_identity_fields_are_validated() {
    let fixture = Fixture::new();
    fixture.write_config(&fixture.valid_toml().replace("version = 1", "version = 2"));
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::UnsupportedVersion(2))
    ));

    let without_route = fixture
        .valid_toml()
        .split("[[routes]]")
        .next()
        .expect("fixture should have routes")
        .replace("data_dir = \"data\"", "data_dir = \"data\"\nroutes = []");
    fixture.write_config(&without_route);
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::InvalidField {
            field: "routes",
            ..
        })
    ));

    for (from, to, expected) in [
        ("private-community", "Private Community", "routes.name"),
        (
            "expected_bot_id = 123456",
            "expected_bot_id = 0",
            "telegram.expected_bot_id",
        ),
    ] {
        fixture.write_config(&fixture.valid_toml().replace(from, to));
        assert!(matches!(
            fixture.load(),
            Err(ConfigError::InvalidField { field, .. }) if field == expected
        ));
    }

    fixture.write_config(&fixture.valid_toml().replace(
        "telegram_chat_id = -1001234567890",
        "telegram_chat_id = -1001234567890\ntelegram_thread_id = 0",
    ));
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::InvalidField {
            field: "routes.telegram_thread_id",
            ..
        })
    ));

    fixture.write_config(
        &fixture
            .valid_toml()
            .replace("expected_bot_id = 123456", "expected_bot_id = 654321"),
    );
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::TelegramBotIdMismatch { .. })
    ));

    fixture.write_config(
        &fixture
            .valid_toml()
            .replace(&fixture.expected_npub, "not-an-npub"),
    );
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::InvalidExpectedNpub)
    ));
}

#[test]
fn malformed_toml_diagnostics_cannot_echo_an_accidentally_inline_secret() {
    let fixture = Fixture::new();
    let leaked = "nsec1must-never-appear-in-an-error";
    fixture.write_config(&format!(
        "{}\ninline_secret = \"{leaked}\"\n",
        fixture.valid_toml()
    ));

    let error = fixture.load().expect_err("unknown fields must fail");
    let rendered = format!("{error:?} {error}");
    assert!(matches!(error, ConfigError::Toml));
    assert!(!rendered.contains(leaked));
}

#[test]
fn dotenv_must_be_a_regular_small_file() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let target = fixture
        .config_path
        .parent()
        .expect("config should have a parent")
        .join("alternate.env");
    write_dotenv(&target, NSEC, TOKEN_SECRET_A);
    fs::remove_file(&fixture.dotenv_path).expect("dotenv should be removed");
    symlink(&target, &fixture.dotenv_path).expect("dotenv symlink should be created");
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::UnsafeDotenvFile { .. })
    ));

    fs::remove_file(&fixture.dotenv_path).expect("symlink should be removed");
    fs::write(&fixture.dotenv_path, "A".repeat(65_537)).expect("large dotenv should be written");
    fs::set_permissions(&fixture.dotenv_path, fs::Permissions::from_mode(0o600))
        .expect("dotenv permissions should be restricted");
    assert!(matches!(
        fixture.load(),
        Err(ConfigError::DotenvTooLarge { .. })
    ));
}

#[test]
fn diagnostics_serialization_and_fingerprint_exclude_secret_values() {
    let fixture = Fixture::new();
    let first = fixture.load().expect("first configuration should load");
    let first_fingerprint = first.fingerprint().to_string();
    let first_serialized = toml::to_string(&first).expect("configuration should serialize safely");

    write_dotenv(&fixture.dotenv_path, NSEC, TOKEN_SECRET_B);
    let second = fixture.load().expect("second configuration should load");
    let diagnostic = format!("{second:?}");
    let serialized = toml::to_string(&second).expect("configuration should serialize safely");

    for output in [
        diagnostic.as_str(),
        serialized.as_str(),
        first_serialized.as_str(),
    ] {
        assert!(output.contains("[REDACTED]"));
        assert!(!output.contains(NSEC));
        assert!(!output.contains(TOKEN_SECRET_A));
        assert!(!output.contains(TOKEN_SECRET_B));
    }
    assert_eq!(first_fingerprint, second.fingerprint().to_string());
}

fn route_toml(name: &str, community_byte: &str, channel_byte: &str) -> String {
    format!(
        r#"[[routes]]
name = "{name}"
community_id = "{}"
channel_id = "{}"
telegram_chat_id = -1009876543210
"#,
        community_byte.repeat(32),
        channel_byte.repeat(32),
    )
}
