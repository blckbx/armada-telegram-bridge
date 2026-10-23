use std::time::Duration;

use armada_telegram_bridge::operations::{Health, InstanceLock, LogEvent, structured_log};

#[test]
fn instance_lock_excludes_another_process_and_releases_on_exit() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let state = directory.path().join("state");
    let lock = InstanceLock::acquire(&state).expect("first lock");
    let child = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "lock_child"])
        .env("BRIDGE_TEST_LOCK_DIRECTORY", &state)
        .status()
        .expect("child starts");
    assert!(child.success());
    drop(lock);
    InstanceLock::acquire(&state).expect("lock released");
}

#[test]
fn lock_child() {
    let Some(directory) = std::env::var_os("BRIDGE_TEST_LOCK_DIRECTORY") else {
        return;
    };
    assert!(InstanceLock::acquire(std::path::Path::new(&directory)).is_err());
}

#[cfg(unix)]
#[test]
fn sigterm_is_handled_instead_of_abruptly_terminating() {
    let directory = tempfile::tempdir().expect("directory");
    let marker = directory.path().join("listening");
    let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "signal_child"])
        .env("BRIDGE_TEST_SIGNAL_MARKER", &marker)
        .spawn()
        .expect("child");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !marker.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(marker.exists(), "child registered signal handler");
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .expect("send SIGTERM")
            .success()
    );
    assert!(child.wait().expect("child exits").success());
}

#[cfg(unix)]
#[tokio::test]
async fn signal_child() {
    let Some(marker) = std::env::var_os("BRIDGE_TEST_SIGNAL_MARKER") else {
        return;
    };
    let signal = tokio::spawn(armada_telegram_bridge::operations::shutdown_signal());
    tokio::task::yield_now().await;
    std::fs::write(marker, "ready").expect("ready marker");
    tokio::time::timeout(Duration::from_secs(8), signal)
        .await
        .expect("signal received")
        .expect("task")
        .expect("signal handler");
}

#[test]
fn health_fails_closed_and_metrics_have_no_dynamic_labels() {
    let health = Health::default();
    assert_eq!(health.response("/live").0, 200);
    assert_eq!(health.response("/ready").0, 503);
    health.update(true, 7, 2);
    assert_eq!(health.response("/ready").0, 200);
    let metrics = health.response("/metrics").1;
    assert!(metrics.contains("bridge_pending_jobs 7\n"));
    assert!(metrics.contains("bridge_dead_letters 2\n"));
    assert!(!metrics.contains('{'));
    health.update(false, 7, 2);
    assert_eq!(health.response("/ready").0, 503);
    assert_eq!(health.response("/live").0, 200);
    assert_eq!(health.response("/unknown").0, 404);
}

#[test]
fn default_logs_only_accept_fixed_events_and_numeric_counts() {
    let log = structured_log(LogEvent::Shutdown, 12);
    let json: serde_json::Value = serde_json::from_str(&log).expect("structured JSON");
    assert_eq!(json["event"], "shutdown");
    assert_eq!(json["count"], 12);
    assert_eq!(json.as_object().expect("object").len(), 2);
}

#[tokio::test(start_paused = true)]
async fn stale_health_and_shutdown_never_report_ready() {
    let health = Health::default();
    health.update(true, 0, 0);
    tokio::time::advance(Duration::from_secs(91)).await;
    assert_eq!(health.response("/ready").0, 503);
    assert_eq!(health.response("/live").0, 200);
    health.update(true, 0, 0);
    assert_eq!(health.response("/ready").0, 200);
    health.stop();
    health.update(true, 0, 0);
    assert_eq!(health.response("/ready").0, 503);
}

#[cfg(unix)]
#[test]
fn lock_rejects_symlinks_and_world_readable_state() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let directory = tempfile::tempdir().expect("directory");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private directory");
    let target = directory.path().join("target");
    std::fs::write(&target, "unchanged").expect("target");
    symlink(&target, directory.path().join("instance.lock")).expect("symlink");
    assert!(InstanceLock::acquire(directory.path()).is_err());
    assert_eq!(
        std::fs::read_to_string(&target).expect("target"),
        "unchanged"
    );
    let other = tempfile::tempdir().expect("other directory");
    std::fs::set_permissions(other.path(), std::fs::Permissions::from_mode(0o755))
        .expect("permissions");
    assert!(InstanceLock::acquire(other.path()).is_err());
}

#[tokio::test]
async fn health_endpoint_serves_without_transport_credentials() {
    let health = Health::default();
    let server = health
        .serve("127.0.0.1:0".parse().expect("address"))
        .await
        .expect("bind");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    for (path, expected) in [("live", 200), ("ready", 503), ("metrics", 200)] {
        let response = client
            .get(format!("http://{}/{path}", server.address()))
            .send()
            .await
            .expect("response");
        assert_eq!(response.status().as_u16(), expected);
    }
}
