#[test]
fn deployment_templates_enforce_minimum_privilege_and_exclude_secrets() {
    let docker = include_str!("../Dockerfile");
    assert!(docker.contains("USER 10001:10001"));
    assert!(docker.contains("cargo build --locked --release"));
    let ignore = include_str!("../.dockerignore");
    assert_eq!(ignore.lines().next(), Some("**"));
    assert!(!ignore.contains("!.env"));
    assert!(!ignore.contains("!data"));
    let compose = include_str!("../deploy/compose.yaml");
    for setting in [
        "read_only: true",
        "cap_drop: [ALL]",
        "no-new-privileges:true",
        "127.0.0.1:9090:9090",
        "stop_grace_period: 20s",
        "create_host_path: false",
    ] {
        assert!(compose.contains(setting), "missing {setting}");
    }
    let unit = include_str!("../deploy/armada-telegram-bridge.service");
    for setting in [
        "User=armada-bridge",
        "ProtectSystem=strict",
        "NoNewPrivileges=true",
        "UMask=0077",
        "TimeoutStopSec=20",
        "KillSignal=SIGTERM",
        "LimitCORE=0",
    ] {
        assert!(unit.contains(setting), "missing {setting}");
    }
}
