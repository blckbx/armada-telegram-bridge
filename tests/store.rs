#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;

use armada_telegram_bridge::domain::RouteId;
use armada_telegram_bridge::store::{
    AcceptOutcome, DeliveryDirection, DeliveryState, JobStatus, NewDelivery, RouteState, Store,
    StoreError,
};

fn route(name: &str) -> RouteId {
    RouteId::try_from(name).expect("test route should be valid")
}

fn delivery(
    route_id: &RouteId,
    direction: DeliveryDirection,
    source_id: &str,
    body: &str,
    cursor: &str,
    at_ms: i64,
) -> NewDelivery {
    NewDelivery::new(
        route_id.clone(),
        direction,
        source_id,
        body.as_bytes().to_vec(),
        at_ms,
    )
    .with_cursor(cursor)
}

#[test]
fn stopped_backup_restore_preserves_encrypted_work_links_and_expiry() {
    let source = tempfile::tempdir().expect("source");
    let backup = tempfile::tempdir().expect("backup");
    let route_id = route("backup-drill");
    let pending_id;
    {
        let mut store = Store::open(source.path()).expect("store");
        store.register_route(&route_id).expect("route");
        let first = store
            .accept_inbound(
                &delivery(
                    &route_id,
                    DeliveryDirection::ArmadaToTelegram,
                    "armada:first",
                    "private test payload",
                    "1",
                    1,
                )
                .with_source_author("author-digest"),
            )
            .expect("accept");
        let AcceptOutcome::Enqueued(first) = first else {
            panic!("new job expected")
        };
        store
            .mark_delivered_with_expiry(first, "telegram:-123:1", 2, Some(100))
            .expect("receipt");
        let pending = store
            .accept_inbound(&delivery(
                &route_id,
                DeliveryDirection::ArmadaToTelegram,
                "armada:pending",
                "pending private payload",
                "2",
                3,
            ))
            .expect("accept");
        let AcceptOutcome::Enqueued(id) = pending else {
            panic!("new job expected")
        };
        pending_id = id;
    }
    // The runbook requires the process to stop before copying ALL state files.
    for entry in fs::read_dir(source.path()).expect("source files") {
        let entry = entry.expect("entry");
        fs::copy(entry.path(), backup.path().join(entry.file_name())).expect("backup copy");
    }
    let restored = Store::open(backup.path()).expect("restored store");
    assert_eq!(restored.pending_job_count().expect("pending"), 1);
    assert_eq!(
        restored
            .job(pending_id)
            .expect("job")
            .expect("exists")
            .payload
            .expect("payload"),
        b"pending private payload"
    );
    assert_eq!(
        restored
            .cursor(&route_id, DeliveryDirection::ArmadaToTelegram)
            .expect("cursor")
            .as_deref(),
        Some("2")
    );
    assert_eq!(restored.due_deletions(101).expect("expiry").len(), 1);
    assert_eq!(
        restored
            .message_link(
                &route_id,
                DeliveryDirection::ArmadaToTelegram,
                "armada:first"
            )
            .expect("link")
            .expect("exists")
            .source_author_id,
        "author-digest"
    );
}

#[test]
fn missing_backup_key_is_not_silently_replaced() {
    let directory = tempfile::tempdir().expect("directory");
    drop(Store::open(directory.path()).expect("store"));
    fs::remove_file(directory.path().join("spool.key")).expect("simulate incomplete restore");
    assert!(Store::open(directory.path()).is_err());
    assert!(!directory.path().join("spool.key").exists());
}

#[test]
fn newer_schema_is_rejected_without_downgrading_and_backup_still_opens() {
    let directory = tempfile::tempdir().expect("directory");
    drop(Store::open(directory.path()).expect("initial upgrade from empty"));
    let path = directory.path().join("bridge.sqlite3");
    let backup = directory.path().join("before-upgrade.sqlite3");
    fs::copy(&path, &backup).expect("stopped backup");
    {
        let connection = rusqlite::Connection::open(&path).expect("fixture connection");
        connection
            .pragma_update(None, "user_version", 999)
            .expect("simulate future schema");
    }
    assert!(Store::open(directory.path()).is_err());
    {
        let connection = rusqlite::Connection::open(&path).expect("inspect");
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
                .expect("version"),
            999
        );
    }
    fs::copy(backup, path).expect("restore stopped original; do not downgrade in place");
    assert_eq!(
        Store::open(directory.path())
            .expect("rollback opens")
            .schema_version()
            .expect("version"),
        5
    );
}

#[test]
fn migrations_are_repeatable_and_enable_required_sqlite_guards() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let expected_tables = [
        "cached_profiles",
        "cursors",
        "inbox_events",
        "job_delivery_state",
        "message_links",
        "outbox_jobs",
        "reaction_entries",
        "reaction_summaries",
        "route_state",
        "routes",
        "scheduled_deletions",
        "skipped_jobs",
    ];

    for _ in 0..2 {
        let store = Store::open(directory.path()).expect("store should open repeatedly");
        assert_eq!(store.schema_version().expect("version should load"), 5);
        assert_eq!(
            store.journal_mode().expect("journal mode should load"),
            "wal"
        );
        assert!(
            store
                .foreign_keys_enabled()
                .expect("foreign-key state should load")
        );
        let tables = store.table_names().expect("schema should be inspectable");
        for table in expected_tables {
            assert!(
                tables.iter().any(|actual| actual == table),
                "missing {table}"
            );
        }
    }
}

#[test]
fn retry_dead_letter_and_route_recovery_state_survive_restart() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let route_id = route("alpha");
    let job_id;
    {
        let mut store = Store::open(directory.path()).expect("store should open");
        store.register_route(&route_id).expect("route registers");
        job_id = store
            .accept_inbound(&delivery(
                &route_id,
                DeliveryDirection::ArmadaToTelegram,
                "armada:retry",
                "retry me",
                "cursor:1",
                1_000,
            ))
            .expect("job enqueues")
            .job_id();
        store
            .schedule_retry(job_id, 2_000, "telegram_timeout", true, 1_000)
            .expect("retry state persists");
        store
            .mark_dead_letter(job_id, "telegram_terminal", true, 2_000)
            .expect("dead letter persists");
        store
            .set_route_state(
                &route_id,
                RouteState::Quarantined,
                Some("operator_review"),
                2_000,
            )
            .expect("route quarantines");
    }

    let mut store = Store::open(directory.path()).expect("store should reopen");
    let state = store
        .delivery_attempt_state(job_id)
        .expect("state query works")
        .expect("state exists");
    assert_eq!(state.state, DeliveryState::DeadLetter);
    assert_eq!(state.attempt_count, 2);
    assert!(state.possible_duplicate);
    assert_eq!(state.last_error_class.as_deref(), Some("telegram_terminal"));
    assert_eq!(store.dead_letters().expect("dead letters load").len(), 1);
    assert_eq!(
        store.route_state(&route_id).expect("route state loads"),
        RouteState::Quarantined
    );

    store
        .retry_dead_letter(job_id, 3_000)
        .expect("operator retry succeeds");
    store
        .set_route_state(&route_id, RouteState::Active, None, 3_000)
        .expect("operator resume succeeds");
    assert_eq!(
        store
            .delivery_attempt_state(job_id)
            .expect("state query works")
            .expect("state exists")
            .state,
        DeliveryState::Ready
    );
    assert_eq!(
        store.route_state(&route_id).expect("route state loads"),
        RouteState::Active
    );

    store
        .mark_dead_letter(job_id, "attachment_integrity", false, 4_000)
        .expect("job can fail again");
    store
        .skip_dead_letter(job_id, 5_000)
        .expect("operator explicitly skips failed mirror");
    assert_eq!(store.dead_letters().expect("dead letters"), Vec::new());
    assert_eq!(store.skipped_job_count().expect("skip audit"), 1);
    let skipped = store.job(job_id).expect("job query").expect("job exists");
    assert_eq!(skipped.status, JobStatus::Delivered);
    assert_eq!(skipped.payload, None);
    assert!(store.skip_dead_letter(job_id, 6_000).is_err());
}

#[test]
fn spool_key_is_created_once_and_unsafe_or_malformed_keys_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let key_path = directory.path().join("spool.key");

    {
        Store::open(directory.path()).expect("first open should create a key");
    }
    let first = fs::read(&key_path).expect("spool key should be readable by its owner");
    assert_eq!(first.len(), 32);
    assert_eq!(
        fs::metadata(&key_path)
            .expect("key metadata should load")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    {
        Store::open(directory.path()).expect("second open should reuse the key");
    }
    assert_eq!(
        fs::read(&key_path).expect("key should remain readable"),
        first
    );

    fs::set_permissions(&key_path, fs::Permissions::from_mode(0o640))
        .expect("permissions should change");
    assert!(matches!(
        Store::open(directory.path()),
        Err(StoreError::UnsafeSpoolKeyPermissions { .. })
    ));

    fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600))
        .expect("permissions should be restored");
    fs::write(&key_path, [7_u8; 31]).expect("malformed key should be written");
    assert!(matches!(
        Store::open(directory.path()),
        Err(StoreError::MalformedSpoolKey)
    ));

    fs::remove_file(&key_path).expect("malformed key should be removable");
    let symlink_target = directory.path().join("other.key");
    fs::write(&symlink_target, [9_u8; 32]).expect("symlink target should be written");
    fs::set_permissions(&symlink_target, fs::Permissions::from_mode(0o600))
        .expect("symlink target permissions should be restricted");
    symlink(&symlink_target, &key_path).expect("spool key symlink should be created");
    assert!(matches!(
        Store::open(directory.path()),
        Err(StoreError::UnsafeSpoolKeyFile { .. })
    ));
}

#[test]
fn duplicate_inbound_event_creates_exactly_one_inbox_row_and_job() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let mut store = Store::open(directory.path()).expect("store should open");
    let route_id = route("alpha");
    store
        .register_route(&route_id)
        .expect("route should register");
    let inbound = delivery(
        &route_id,
        DeliveryDirection::TelegramToArmada,
        "telegram:100:7",
        "hello once",
        "update:7",
        1_000,
    );

    let first = store
        .accept_inbound(&inbound)
        .expect("first event should enqueue");
    let second = store
        .accept_inbound(&inbound)
        .expect("duplicate should be harmless");

    let first_id = match first {
        AcceptOutcome::Enqueued(id) => id,
        AcceptOutcome::Duplicate(_) => panic!("first event must enqueue"),
    };
    assert_eq!(second, AcceptOutcome::Duplicate(first_id));
    assert_eq!(store.counts().expect("counts should load").inbox_events, 1);
    assert_eq!(store.counts().expect("counts should load").outbox_jobs, 1);
}

#[test]
fn inbox_job_and_cursor_commit_or_roll_back_together() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let mut store = Store::open(directory.path()).expect("store should open");
    let registered = route("registered");
    store
        .register_route(&registered)
        .expect("route should register");
    store
        .accept_inbound(&delivery(
            &registered,
            DeliveryDirection::ArmadaToTelegram,
            "armada:event:1",
            "committed",
            "relay:10",
            10,
        ))
        .expect("initial event should commit");

    let missing = route("missing");
    assert!(
        store
            .accept_inbound(&delivery(
                &missing,
                DeliveryDirection::ArmadaToTelegram,
                "armada:event:2",
                "must roll back",
                "relay:11",
                11,
            ))
            .is_err()
    );

    let counts = store.counts().expect("counts should load");
    assert_eq!(counts.inbox_events, 1);
    assert_eq!(counts.outbox_jobs, 1);
    assert_eq!(
        store
            .cursor(&missing, DeliveryDirection::ArmadaToTelegram)
            .expect("cursor query should work"),
        None
    );
    assert_eq!(
        store
            .cursor(&registered, DeliveryDirection::ArmadaToTelegram)
            .expect("cursor query should work")
            .as_deref(),
        Some("relay:10")
    );
}

#[test]
fn pending_jobs_are_fifo_per_route_and_direction() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let mut store = Store::open(directory.path()).expect("store should open");
    let alpha = route("alpha");
    let beta = route("beta");
    store.register_route(&alpha).expect("route should register");
    store.register_route(&beta).expect("route should register");

    for (route_id, direction, source, body, cursor, at_ms) in [
        (
            &alpha,
            DeliveryDirection::ArmadaToTelegram,
            "a:1",
            "first",
            "1",
            30,
        ),
        (
            &beta,
            DeliveryDirection::ArmadaToTelegram,
            "b:1",
            "other route",
            "1",
            10,
        ),
        (
            &alpha,
            DeliveryDirection::TelegramToArmada,
            "a:reverse",
            "reverse",
            "2",
            20,
        ),
        (
            &alpha,
            DeliveryDirection::ArmadaToTelegram,
            "a:2",
            "second",
            "3",
            10,
        ),
        (
            &alpha,
            DeliveryDirection::ArmadaToTelegram,
            "a:3",
            "third",
            "4",
            20,
        ),
    ] {
        store
            .accept_inbound(&delivery(route_id, direction, source, body, cursor, at_ms))
            .expect("event should enqueue");
    }

    let jobs = store
        .pending_jobs(&alpha, DeliveryDirection::ArmadaToTelegram, 10)
        .expect("pending jobs should load");
    let payloads: Vec<_> = jobs
        .iter()
        .map(|job| String::from_utf8(job.payload.clone()).expect("payload should be UTF-8"))
        .collect();
    assert_eq!(payloads, ["first", "second", "third"]);
    assert!(jobs.windows(2).all(|pair| pair[0].id < pair[1].id));
}

#[test]
fn delivery_receipt_atomically_marks_job_and_records_message_link() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let mut store = Store::open(directory.path()).expect("store should open");
    let route_id = route("alpha");
    store
        .register_route(&route_id)
        .expect("route should register");

    let first_id = store
        .accept_inbound(
            &delivery(
                &route_id,
                DeliveryDirection::ArmadaToTelegram,
                "armada:one",
                "one",
                "1",
                1,
            )
            .with_source_author("npub1alice"),
        )
        .expect("event should enqueue")
        .job_id();
    let second_id = store
        .accept_inbound(&delivery(
            &route_id,
            DeliveryDirection::ArmadaToTelegram,
            "armada:two",
            "two",
            "2",
            2,
        ))
        .expect("event should enqueue")
        .job_id();

    store
        .mark_delivered_with_expiry(first_id, "telegram:900", 100, Some(500))
        .expect("receipt should commit");
    let first = store
        .job(first_id)
        .expect("job query should work")
        .expect("job should exist");
    assert_eq!(first.status, JobStatus::Delivered);
    let link = store
        .message_link(&route_id, DeliveryDirection::ArmadaToTelegram, "armada:one")
        .expect("link query should work")
        .expect("link should exist");
    assert_eq!(link.source_message_id, "armada:one");
    assert_eq!(link.destination_message_id, "telegram:900");
    assert_eq!(link.source_author_id, "npub1alice");
    assert_eq!(store.due_deletions(499).expect("expiry query"), Vec::new());
    let due = store.due_deletions(500).expect("expiry is durable");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].destination_message_id, "telegram:900");
    store
        .complete_deletion(due[0].id, 501)
        .expect("expiry completes");
    assert_eq!(
        store.due_deletions(1_000).expect("expiry query"),
        Vec::new()
    );

    assert!(
        store
            .mark_delivered(second_id, "telegram:900", 101)
            .is_err()
    );
    let second = store
        .job(second_id)
        .expect("job query should work")
        .expect("job should exist");
    assert_eq!(second.status, JobStatus::Pending);
    assert!(
        store
            .message_link(&route_id, DeliveryDirection::ArmadaToTelegram, "armada:two")
            .expect("link query should work")
            .is_none()
    );
}

#[test]
fn pending_jobs_and_cursors_survive_reopen() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let route_id = route("alpha");

    {
        let mut store = Store::open(directory.path()).expect("store should open");
        store
            .register_route(&route_id)
            .expect("route should register");
        store
            .accept_inbound(&delivery(
                &route_id,
                DeliveryDirection::TelegramToArmada,
                "telegram:1",
                "survives restart",
                "offset:44",
                44,
            ))
            .expect("event should enqueue");
    }

    let store = Store::open(directory.path()).expect("store should reopen");
    let jobs = store
        .pending_jobs(&route_id, DeliveryDirection::TelegramToArmada, 10)
        .expect("pending job should reload");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].payload, b"survives restart");
    assert_eq!(
        store
            .cursor(&route_id, DeliveryDirection::TelegramToArmada)
            .expect("cursor query should work")
            .as_deref(),
        Some("offset:44")
    );
}

#[test]
fn payloads_are_encrypted_at_rest_and_erased_after_delivery_retention() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let route_id = route("alpha");
    let body = "private-message-body-unique-7f18";
    let mut store = Store::open(directory.path()).expect("store should open");
    store
        .register_route(&route_id)
        .expect("route should register");
    let job_id = store
        .accept_inbound(&delivery(
            &route_id,
            DeliveryDirection::ArmadaToTelegram,
            "armada:private",
            body,
            "private:1",
            1,
        ))
        .expect("event should enqueue")
        .job_id();

    let database = fs::read(directory.path().join("bridge.sqlite3"))
        .expect("database bytes should be readable");
    let wal = fs::read(directory.path().join("bridge.sqlite3-wal")).unwrap_or_default();
    assert!(
        !database
            .windows(body.len())
            .any(|window| window == body.as_bytes())
    );
    assert!(
        !wal.windows(body.len())
            .any(|window| window == body.as_bytes())
    );
    assert_eq!(
        store
            .job(job_id)
            .expect("job query should work")
            .expect("job should exist")
            .payload
            .as_deref(),
        Some(body.as_bytes())
    );

    store
        .mark_delivered(job_id, "telegram:private", 1_000)
        .expect("receipt should commit");
    assert_eq!(
        store
            .cleanup_delivered_payloads(999)
            .expect("early cleanup should work"),
        0
    );
    assert_eq!(
        store
            .cleanup_delivered_payloads(1_000)
            .expect("retention cleanup should work"),
        1
    );
    let cleaned = store
        .job(job_id)
        .expect("job query should work")
        .expect("job should exist");
    assert_eq!(cleaned.status, JobStatus::Delivered);
    assert!(cleaned.payload.is_none());
}
