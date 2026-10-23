const DOTENV_SAMPLE: &str = include_str!("../.env.sample");
const GITIGNORE: &str = include_str!("../.gitignore");

#[test]
fn dotenv_sample_declares_runtime_and_p01_values_as_placeholders() {
    let expected_keys = [
        "ARMADA_NSEC",
        "TELEGRAM_BOT_TOKEN",
        "ARMADA_BRIDGE_LIVE_EXPECTED_NPUB",
        "ARMADA_BRIDGE_LIVE_COMMUNITY_ID",
        "ARMADA_BRIDGE_LIVE_CHANNEL_ID",
        "ARMADA_BRIDGE_LIVE_RECEIVE_MARKER",
        "ARMADA_BRIDGE_LIVE_EXPECTED_AUTHOR_NPUB",
        "ARMADA_BRIDGE_LIVE_SEND_MARKER",
    ];

    for key in expected_keys {
        let prefix = format!("{key}=");
        let value = DOTENV_SAMPLE
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("{key} is missing from .env.sample"));

        assert!(
            value.starts_with('<') && value.ends_with('>'),
            "{key} must remain a placeholder in .env.sample"
        );
    }
}

#[test]
fn real_dotenv_is_ignored_but_sample_is_not() {
    let rules: Vec<_> = GITIGNORE.lines().collect();

    assert!(rules.contains(&".env"), ".env must be ignored");
    assert!(
        rules.contains(&"!.env.sample"),
        ".env.sample must be explicitly unignored"
    );
    assert!(
        !rules.contains(&".env.sample"),
        ".env.sample must remain trackable"
    );
}
