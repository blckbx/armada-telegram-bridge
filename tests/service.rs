use std::time::{Duration, UNIX_EPOCH};

use armada_telegram_bridge::concord::{ConcordInboundMessage, ConcordReadiness, ConcordRoute};
use armada_telegram_bridge::domain::{
    AttachmentLocator, AttachmentRef, BridgeEvent, BridgeReactionUpdate, ChannelId, CommunityId,
    Mutation, ReactionChange, ReactionEntry, RouteId, SenderIdentity, Source, SourceMessageRef,
    TelegramChatId,
};
use armada_telegram_bridge::rich_text::{TelegramEntity, TelegramEntityKind, from_telegram};
use armada_telegram_bridge::service::{
    BridgeService, BridgeServiceSettings, IntakeOutcome, RetryPolicy, ServiceError,
    render_attributed, retry_delay_ms,
};
use armada_telegram_bridge::store::{DeliveryDirection, DeliveryState, RouteState, Store};
use armada_telegram_bridge::telegram::{
    InboundTelegramMessage, TelegramBotIdentity, TelegramError, TelegramPort, TelegramRoute,
};
use armada_telegram_bridge::test_support::{
    AdjustableClock, FakeConcordPort, FakeTelegramPort, FixedClock,
};

const COMMUNITY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHANNEL: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BOT_ID: u64 = 123_456;
const CHAT_ID: i64 = -1_001_234;
const BOT_NPUB: &str = "npub1bridgeidentity";
const ALICE_NPUB: &str = "npub1alice0000000000000000000000000000000000000000000000000000000";

fn route_id() -> RouteId {
    RouteId::try_from("alpha").expect("valid route")
}

fn concord_route() -> ConcordRoute {
    ConcordRoute::new(
        route_id(),
        CommunityId::try_from(COMMUNITY).expect("valid community"),
        ChannelId::try_from(CHANNEL).expect("valid channel"),
    )
}

fn telegram_route() -> TelegramRoute {
    TelegramRoute::new(
        route_id(),
        TelegramChatId::try_from(CHAT_ID).expect("valid chat"),
        None,
    )
}

fn settings() -> BridgeServiceSettings {
    BridgeServiceSettings::new(vec![route_id()], BOT_NPUB, BOT_ID, 100, 64 * 1024)
}

fn recovery_settings(
    max_pending_jobs: usize,
    max_catchup_events: usize,
    max_catchup_age: Duration,
    retry_policy: RetryPolicy,
) -> BridgeServiceSettings {
    BridgeServiceSettings::new(
        vec![route_id()],
        BOT_NPUB,
        BOT_ID,
        max_catchup_events,
        64 * 1024,
    )
    .with_delivery_policy(max_pending_jobs, max_catchup_age, retry_policy)
}

fn retry_policy(jitter_percent: u8) -> RetryPolicy {
    RetryPolicy {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(4),
        max_attempts: 4,
        jitter_percent,
    }
}

fn clock() -> FixedClock {
    FixedClock::new(UNIX_EPOCH + Duration::from_secs(10_000))
}

fn concord_fake() -> FakeConcordPort {
    let mut fake = FakeConcordPort::new(vec![concord_route()]);
    fake.set_readiness(route_id(), ConcordReadiness::Ready);
    fake
}

fn telegram_fake() -> FakeTelegramPort {
    FakeTelegramPort::new(
        TelegramBotIdentity {
            id: BOT_ID,
            username: Some("bridge_bot".to_owned()),
        },
        vec![telegram_route()],
    )
}

#[tokio::test]
async fn rekey_and_removed_membership_quarantine_until_explicit_recovery() {
    for readiness in [
        ConcordReadiness::MissingKey,
        ConcordReadiness::IncompleteRekey,
        ConcordReadiness::Removed,
    ] {
        let data = tempfile::tempdir().expect("directory");
        let mut service = BridgeService::new(
            Store::open(data.path()).expect("store"),
            telegram_fake(),
            concord_fake(),
            clock(),
            settings(),
        );
        service.initialize().await.expect("initialize");
        service.telegram_mut().push_inbound(telegram_message(
            1,
            2,
            Some(44),
            None,
            None,
            "held during rekey",
        ));
        service.poll_telegram_once().await.expect("accept");
        service.concord_mut().set_readiness(route_id(), readiness);
        assert_eq!(
            service
                .dispatch_ready()
                .await
                .expect("dispatch")
                .dead_lettered,
            1
        );
        assert_eq!(
            service.store().route_state(&route_id()).expect("route"),
            RouteState::Quarantined
        );
        assert_eq!(service.concord().sent(), []);
        let job = service.dead_letters().expect("dead letter")[0].job_id;
        drop(service);
        let mut restored = BridgeService::new(
            Store::open(data.path()).expect("reopen"),
            telegram_fake(),
            concord_fake(),
            clock(),
            settings(),
        );
        assert!(
            restored.initialize().await.is_err(),
            "readable keys alone must not clear quarantine"
        );
        restored
            .resume_route(&route_id())
            .expect("operator reviewed membership");
        restored.retry_dead_letter(job).expect("operator retry");
        restored.initialize().await.expect("recovery");
        assert_eq!(restored.dispatch_ready().await.expect("send").delivered, 1);
    }
}

#[tokio::test]
async fn concord_partition_retry_survives_restart_without_losing_work() {
    let data = tempfile::tempdir().expect("directory");
    let time = AdjustableClock::new(10_000_000);
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store"),
        telegram_fake(),
        concord_fake(),
        time.clone(),
        recovery_settings(10, 100, Duration::from_secs(60), retry_policy(0)),
    );
    service.initialize().await.expect("initialize");
    service.telegram_mut().push_inbound(telegram_message(
        5,
        6,
        Some(44),
        None,
        None,
        "durable through partition",
    ));
    service.poll_telegram_once().await.expect("accept");
    service.concord_mut().push_send_result(Err(
        armada_telegram_bridge::concord::ConcordError::Sdk { operation: "send" },
    ));
    assert_eq!(
        service
            .dispatch_ready()
            .await
            .expect("retry scheduled")
            .retried,
        1
    );
    assert_eq!(service.concord().sent(), []);
    assert!(!service.is_ready().expect("readiness"));
    drop(service);
    let mut restored = BridgeService::new(
        Store::open(data.path()).expect("reopen"),
        telegram_fake(),
        concord_fake(),
        time.clone(),
        recovery_settings(10, 100, Duration::from_secs(60), retry_policy(0)),
    );
    restored
        .initialize()
        .await
        .expect("initialize after restart");
    assert_eq!(
        restored.dispatch_ready().await.expect("not due").delivered,
        0
    );
    time.advance(Duration::from_secs(1));
    assert_eq!(
        restored
            .dispatch_ready()
            .await
            .expect("retry delivered")
            .delivered,
        1
    );
    assert_eq!(restored.concord().sent().len(), 1);
    assert!(restored.is_ready().expect("readiness restored"));
}

#[tokio::test]
async fn telegram_removal_forbidden_is_terminal_not_a_retry_storm() {
    let data = tempfile::tempdir().expect("directory");
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store"),
        telegram_fake(),
        concord_fake(),
        clock(),
        settings(),
    );
    service.initialize().await.expect("initialize");
    service
        .accept_armada(armada_message(
            "removed",
            "removed:1",
            ALICE_NPUB,
            None,
            "held",
        ))
        .expect("accept");
    service
        .telegram_mut()
        .push_send_result(Err(TelegramError::Terminal { status: 403 }));
    assert_eq!(
        service
            .dispatch_ready()
            .await
            .expect("dispatch")
            .dead_lettered,
        1
    );
    assert!(!service.is_ready().expect("readiness"));
    assert_eq!(
        service.dispatch_ready().await.expect("blocked").delivered,
        0
    );
    assert_eq!(service.telegram().send_attempts().len(), 1);
}

#[tokio::test]
async fn operational_readiness_tracks_store_queue_and_route_safety() {
    let directory = tempfile::tempdir().expect("directory");
    let health = armada_telegram_bridge::operations::Health::default();
    let mut service = BridgeService::new(
        Store::open(directory.path()).expect("store"),
        telegram_fake(),
        concord_fake(),
        clock(),
        settings(),
    );
    service.set_health(health.clone());
    assert_eq!(health.response("/ready").0, 503);
    service.initialize().await.expect("initialize");
    service.dispatch_ready().await.expect("dispatch");
    assert_eq!(health.response("/ready").0, 200);
    service
        .quarantine_route(&route_id(), "test_safety")
        .expect("quarantine");
    service.dispatch_ready().await.expect("dispatch");
    assert_eq!(health.response("/ready").0, 503);
    assert_eq!(health.response("/live").0, 200);
    let metrics = health.response("/metrics").1;
    for forbidden in [BOT_NPUB, ALICE_NPUB, COMMUNITY, CHANNEL, "test_safety"] {
        assert!(!metrics.contains(forbidden));
    }
}

fn armada_message(
    id: &str,
    cursor: &str,
    sender_npub: &str,
    display_name: Option<&str>,
    body: &str,
) -> ConcordInboundMessage {
    ConcordInboundMessage {
        event: BridgeEvent {
            source: Source::Armada,
            source_event_id: format!("armada:{id}"),
            route_id: route_id(),
            sender: SenderIdentity::Armada {
                npub: sender_npub.to_owned(),
                display_name: display_name.map(str::to_owned),
            },
            timestamp_ms: 1_000,
            body: body.to_owned(),
            rich_text: None,
            attachments: Vec::new(),
            reply_to: None,
            reply_quote: None,
            mutation_target: None,
            mutation: Mutation::Create,
            expires_at_ms: None,
        },
        next_cursor: cursor.to_owned(),
    }
}

fn telegram_message(
    message_id: u64,
    next_offset: u64,
    user_id: Option<u64>,
    username: Option<&str>,
    display_name: Option<&str>,
    body: &str,
) -> InboundTelegramMessage {
    InboundTelegramMessage {
        event: BridgeEvent {
            source: Source::Telegram,
            source_event_id: format!("telegram:{CHAT_ID}:{message_id}"),
            route_id: route_id(),
            sender: SenderIdentity::Telegram {
                user_id,
                sender_chat_id: None,
                username: username.map(str::to_owned),
                display_name: display_name.map(str::to_owned),
            },
            timestamp_ms: 1_000,
            body: body.to_owned(),
            rich_text: None,
            attachments: Vec::new(),
            reply_to: None,
            reply_quote: None,
            mutation_target: None,
            mutation: Mutation::Create,
            expires_at_ms: None,
        },
        next_offset,
    }
}

#[test]
fn attribution_uses_usernames_and_neutralizes_header_markup_and_controls() {
    let mut message = telegram_message(7, 8, Some(77), Some("bob_bot"), None, "hello");
    assert_eq!(
        render_attributed(&message.event).expect("Telegram attribution renders"),
        "✈️ @bob\\_bot\nhello"
    );

    message.event.sender = SenderIdentity::Telegram {
        user_id: Some(77),
        sender_chat_id: None,
        username: None,
        display_name: Some("[Admin](https://invalid)\n\u{202e}forged".to_owned()),
    };
    let rendered = render_attributed(&message.event).expect("fallback attribution renders");
    assert_eq!(rendered.lines().count(), 2);
    assert_eq!(
        rendered,
        "✈️ \\[Admin\\]\\(https://invalid\\) forged\nhello"
    );
}

#[tokio::test]
async fn rich_text_crosses_both_transports_and_long_armada_text_splits_safely() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let store = Store::open(data.path()).expect("store opens");
    let mut service =
        BridgeService::new(store, telegram_fake(), concord_fake(), clock(), settings());
    service.initialize().await.expect("startup succeeds");

    let armada = armada_message(
        "rich",
        "cursor-rich",
        ALICE_NPUB,
        Some("Alice"),
        "**bold 😀** and [safe](https://example.com) [unsafe](javascript:alert)",
    );
    assert_eq!(
        service
            .accept_armada(armada)
            .expect("Armada intake succeeds"),
        IntakeOutcome::Enqueued
    );
    service
        .dispatch_pending()
        .await
        .expect("rich Armada message dispatches");
    assert_eq!(
        service.telegram().sent()[0].1,
        "⛵ Alice (npub1alice00…0000)\nbold 😀 and safe unsafe"
    );
    let sent_entities = &service.telegram().sent_entities()[0];
    assert!(
        sent_entities
            .iter()
            .any(|entity| entity.kind == TelegramEntityKind::Bold)
    );
    assert!(sent_entities.iter().any(|entity| {
        entity.kind == TelegramEntityKind::TextLink
            && entity.url.as_deref() == Some("https://example.com")
    }));
    assert!(!sent_entities.iter().any(|entity| {
        entity
            .url
            .as_deref()
            .is_some_and(|url| url.starts_with("javascript:"))
    }));

    let telegram_entities = [TelegramEntity {
        kind: TelegramEntityKind::Bold,
        offset: 0,
        length: 2,
        url: None,
        language: None,
    }];
    let mut telegram = telegram_message(91, 92, Some(41), Some("bob"), None, "😀 hello");
    telegram.event.rich_text = Some(
        from_telegram(&telegram.event.body, &telegram_entities)
            .expect("Telegram UTF-16 entity is valid"),
    );
    service.telegram_mut().push_inbound(telegram);
    service
        .poll_telegram_once()
        .await
        .expect("Telegram message is accepted");
    service
        .dispatch_pending()
        .await
        .expect("Telegram rich text dispatches");
    assert_eq!(service.concord().sent()[0].1, "✈️ @bob\n**😀** hello");

    let long_body = format!("**{}**", "word 👨‍👩‍👧‍👦 ".repeat(500));
    let long = armada_message("long", "cursor-long", ALICE_NPUB, Some("Alice"), &long_body);
    service.accept_armada(long).expect("long message queues");
    service
        .dispatch_pending()
        .await
        .expect("long message splits and dispatches");
    assert!(service.telegram().sent().len() >= 3);
    for (_, text) in &service.telegram().sent()[1..] {
        assert!(text.chars().count() <= 4_096);
        assert!(!text.ends_with('\u{200d}'));
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn boundary_sized_attachments_cross_both_directions_and_plaintext_is_cleaned() {
    const LIMIT: usize = 1_024;
    let data = tempfile::tempdir().expect("temporary data directory");
    let staging = data.path().join("attachment-staging");
    let png = [b"\x89PNG\r\n\x1a\n".as_slice(), &vec![0_u8; LIMIT - 8]].concat();
    let pdf = [b"%PDF-1.7\n".as_slice(), &vec![b'x'; LIMIT - 9]].concat();

    let mut concord = concord_fake();
    concord.set_attachment("armada-file", png.clone());
    let store = Store::open(data.path()).expect("store opens");
    let mut service = BridgeService::new(
        store,
        telegram_fake(),
        concord,
        clock(),
        settings().with_attachment_policy(LIMIT as u64, staging.clone()),
    );
    service.initialize().await.expect("startup succeeds");

    let mut armada = armada_message(
        "file-from-armada",
        "cursor-file-from-armada",
        ALICE_NPUB,
        Some("Alice"),
        "project image",
    );
    armada.event.attachments.push(AttachmentRef {
        source_id: "armada-file".to_owned(),
        file_name: Some("../../project.png".to_owned()),
        media_type: Some("text/html".to_owned()),
        size_bytes: LIMIT as u64,
        locator: AttachmentLocator::Armada {
            id: "armada-file".to_owned(),
            key: "secret-key".to_owned(),
            nonce: "secret-nonce".to_owned(),
            extension: "png".to_owned(),
            url: "https://blossom.example/armada-file".to_owned(),
            fallback_urls: Vec::new(),
            original_hash: None,
            author_npub: ALICE_NPUB.to_owned(),
        },
        media_group_id: None,
        media_group_index: None,
        media_group_total: None,
    });
    service.accept_armada(armada).expect("attachment queues");
    service
        .dispatch_pending()
        .await
        .expect("Armada attachment dispatches");

    let sent = &service.telegram().sent_attachments()[0];
    assert_eq!(sent.bytes, png);
    assert_eq!(sent.metadata.file_name, "project.png");
    assert_eq!(sent.metadata.media_type, "image/png");
    assert!(
        sent.caption
            .as_ref()
            .is_some_and(|caption| caption.text.ends_with("project image"))
    );

    service
        .concord_mut()
        .set_attachment("long-caption-file", b"%PDF-1.7".to_vec());
    let long_caption = "x".repeat(1_100);
    let mut long = armada_message(
        "long-caption-file-message",
        "cursor-long-caption-file",
        ALICE_NPUB,
        Some("Alice"),
        &long_caption,
    );
    long.event.attachments.push(AttachmentRef {
        source_id: "long-caption-file".to_owned(),
        file_name: Some("long.pdf".to_owned()),
        media_type: Some("application/pdf".to_owned()),
        size_bytes: 8,
        locator: AttachmentLocator::Armada {
            id: "long-caption-file".to_owned(),
            key: "key".to_owned(),
            nonce: "nonce".to_owned(),
            extension: "pdf".to_owned(),
            url: "https://blossom.example/long-caption-file".to_owned(),
            fallback_urls: Vec::new(),
            original_hash: None,
            author_npub: ALICE_NPUB.to_owned(),
        },
        media_group_id: None,
        media_group_index: None,
        media_group_total: None,
    });
    service.accept_armada(long).expect("long caption queues");
    service
        .dispatch_pending()
        .await
        .expect("long caption uses companion text");
    assert_eq!(service.telegram().sent().len(), 1);
    assert!(service.telegram().sent()[0].1.ends_with(&long_caption));
    assert_eq!(service.telegram().sent_attachments()[1].caption, None);

    service
        .telegram_mut()
        .set_attachment("telegram-file", pdf.clone());
    let mut telegram = telegram_message(201, 202, Some(41), Some("bob"), None, "quarterly report");
    telegram.event.attachments.push(AttachmentRef {
        source_id: "telegram-file".to_owned(),
        file_name: Some("..\\report.pdf".to_owned()),
        media_type: Some("image/jpeg".to_owned()),
        size_bytes: LIMIT as u64,
        locator: AttachmentLocator::Telegram {
            file_id: "telegram-download-id".to_owned(),
        },
        media_group_id: None,
        media_group_index: None,
        media_group_total: None,
    });
    service.telegram_mut().push_inbound(telegram);
    service
        .poll_telegram_once()
        .await
        .expect("Telegram attachment queues");
    service
        .dispatch_pending()
        .await
        .expect("Telegram attachment dispatches");

    assert_eq!(service.concord().sent()[0].1, "✈️ @bob\nquarterly report");
    let sent = &service.concord().sent_attachments()[0];
    assert_eq!(sent.bytes, pdf);
    assert_eq!(sent.metadata.file_name, "report.pdf");
    assert_eq!(sent.metadata.media_type, "application/pdf");
    assert_eq!(
        std::fs::read_dir(&staging)
            .expect("staging directory exists")
            .count(),
        0
    );
}

#[tokio::test]
async fn corrupt_armada_attachment_degrades_safely_and_does_not_block_following_text() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut concord = concord_fake();
    concord.push_download_result(Err(
        armada_telegram_bridge::concord::ConcordError::AttachmentIntegrity,
    ));
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord,
        clock(),
        settings(),
    );
    service.initialize().await.expect("service initializes");

    let mut gif = armada_message("bad-gif", "bad-gif:1", ALICE_NPUB, Some("Alice"), "GIF");
    gif.event.attachments.push(AttachmentRef {
        source_id: "claimed-hash".to_owned(),
        file_name: Some("animation.gif".to_owned()),
        media_type: Some("image/gif".to_owned()),
        size_bytes: 123,
        media_group_id: None,
        media_group_index: None,
        media_group_total: None,
        locator: AttachmentLocator::Armada {
            id: "claimed-hash".to_owned(),
            key: "redacted-test-key".to_owned(),
            nonce: "redacted-test-nonce".to_owned(),
            extension: "gif".to_owned(),
            url: "https://example.invalid/original.gif".to_owned(),
            fallback_urls: Vec::new(),
            original_hash: Some("different-hash".to_owned()),
            author_npub: ALICE_NPUB.to_owned(),
        },
    });
    service.accept_armada(gif).expect("GIF enqueues");
    service
        .accept_armada(armada_message(
            "after-gif",
            "bad-gif:2",
            ALICE_NPUB,
            Some("Alice"),
            "message after GIF",
        ))
        .expect("following text enqueues");

    let report = service.dispatch_ready().await.expect("queue drains");
    assert_eq!(report.delivered, 2);
    assert_eq!(report.dead_lettered, 0);
    assert_eq!(service.store().pending_job_count().expect("count"), 0);
    assert_eq!(service.telegram().sent_attachments(), []);
    assert!(
        service.telegram().sent()[0]
            .1
            .contains("[GIF could not be transferred]")
    );
    assert!(!service.telegram().sent()[0].1.contains("integrity"));
    assert!(service.telegram().sent()[1].1.contains("message after GIF"));
}

#[tokio::test]
async fn declared_and_actual_oversize_attachments_never_reach_a_destination() {
    const LIMIT: u64 = 32;
    let data = tempfile::tempdir().expect("temporary data directory");
    let staging = data.path().join("attachment-staging");
    let store = Store::open(data.path()).expect("store opens");
    let mut service = BridgeService::new(
        store,
        telegram_fake(),
        concord_fake(),
        clock(),
        settings().with_attachment_policy(LIMIT, staging.clone()),
    );
    service.initialize().await.expect("startup succeeds");

    for (message_id, declared_size, actual_size) in [(301, LIMIT + 1, 1), (302, 1, 33)] {
        let source_id = format!("telegram-file-{message_id}");
        service
            .telegram_mut()
            .set_attachment(&source_id, vec![0_u8; actual_size]);
        let mut message = telegram_message(
            message_id,
            message_id + 1,
            Some(41),
            Some("bob"),
            None,
            "oversize",
        );
        message.event.attachments.push(AttachmentRef {
            source_id,
            file_name: Some("large.bin".to_owned()),
            media_type: None,
            size_bytes: declared_size,
            locator: AttachmentLocator::Telegram {
                file_id: "download-id".to_owned(),
            },
            media_group_id: None,
            media_group_index: None,
            media_group_total: None,
        });
        service.telegram_mut().push_inbound(message);
        service
            .poll_telegram_once()
            .await
            .expect("intake safely handles oversize attachment");
        service
            .dispatch_pending()
            .await
            .expect("oversize handling remains durable");
    }

    assert_eq!(service.concord().sent_attachments().len(), 0);
    assert_eq!(
        service.store().dead_letters().expect("dead letters").len(),
        1
    );
    assert_eq!(
        std::fs::read_dir(&staging)
            .expect("staging directory exists")
            .count(),
        0
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn plaintext_bridge_baselines_relays_links_and_deduplicates_across_restart() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let old_armada = armada_message("old", "cursor-old", ALICE_NPUB, Some("Old Alice"), "old");
    let new_armada = armada_message(
        "new",
        "cursor-new",
        ALICE_NPUB,
        Some("Alice\n✈️ forged\u{202e}"),
        "hello from Armada",
    );
    let old_telegram = telegram_message(1, 8, Some(40), Some("oldbob"), None, "old");
    let new_telegram = telegram_message(
        2,
        9,
        Some(41),
        None,
        Some("Bob Builder"),
        "hello from Telegram",
    );

    let mut telegram = telegram_fake();
    telegram.push_inbound(old_telegram);
    let mut concord = concord_fake();
    concord.set_history(vec![old_armada.clone()]);
    let store = Store::open(data.path()).expect("store opens");
    let mut service = BridgeService::new(store, telegram, concord, clock(), settings());

    let startup = service.initialize().await.expect("first startup succeeds");
    assert!(startup.telegram_baselined);
    assert_eq!(startup.armada_enqueued, 0);
    assert_eq!(
        service.dispatch_pending().await.expect("nothing to send"),
        0
    );
    assert_eq!(service.store().counts().expect("counts").inbox_events, 0);

    let mut concord_after_baseline = new_armada.clone();
    concord_after_baseline.event.timestamp_ms = 10_001_000;
    service.concord_mut().push_live(concord_after_baseline);
    service.telegram_mut().push_inbound(new_telegram.clone());
    assert_eq!(
        service
            .receive_armada_once()
            .await
            .expect("Armada intake succeeds"),
        Some(IntakeOutcome::Enqueued)
    );
    service
        .poll_telegram_once()
        .await
        .expect("Telegram update accepted");
    assert_eq!(service.dispatch_pending().await.expect("jobs dispatch"), 2);

    assert_eq!(service.telegram().sent().len(), 1);
    let telegram_text = &service.telegram().sent()[0].1;
    assert_eq!(telegram_text.lines().count(), 2);
    assert!(telegram_text.starts_with("⛵ Alice ✈️ forged (npub1alice00…0000)\n"));
    assert!(telegram_text.ends_with("hello from Armada"));

    assert_eq!(service.concord().sent().len(), 1);
    assert_eq!(
        service.concord().sent()[0].1,
        "✈️ Bob Builder\nhello from Telegram"
    );

    let armada_link = service
        .store()
        .message_link(
            &route_id(),
            DeliveryDirection::ArmadaToTelegram,
            "armada:new",
        )
        .expect("link query")
        .expect("Armada link exists");
    assert_eq!(
        armada_link.destination_message_id,
        format!("telegram:{CHAT_ID}:1")
    );
    let telegram_link = service
        .store()
        .message_link(
            &route_id(),
            DeliveryDirection::TelegramToArmada,
            &format!("telegram:{CHAT_ID}:2"),
        )
        .expect("link query")
        .expect("Telegram link exists");
    assert_eq!(telegram_link.destination_message_id, format!("{:064x}", 1));

    drop(service);

    let mut telegram = telegram_fake();
    telegram.push_inbound(new_telegram);
    let mut concord = concord_fake();
    concord.set_history(vec![old_armada, new_armada]);
    let store = Store::open(data.path()).expect("store reopens again");
    let mut restarted = BridgeService::new(store, telegram, concord, clock(), settings());
    restarted
        .initialize()
        .await
        .expect("second restart recovers");
    let poll = restarted
        .poll_telegram_once()
        .await
        .expect("duplicate update safely skipped");
    assert_eq!(poll.accepted, 0);
    assert_eq!(restarted.dispatch_pending().await.expect("no jobs"), 0);
    assert_eq!(restarted.telegram().sent(), []);
    assert_eq!(restarted.concord().sent(), []);
    assert_eq!(restarted.store().counts().expect("counts").inbox_events, 2);
}

#[tokio::test]
async fn router_rejects_wrong_source_route_mutation_and_bridge_identities() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let store = Store::open(data.path()).expect("store opens");
    let mut service =
        BridgeService::new(store, telegram_fake(), concord_fake(), clock(), settings());
    service.initialize().await.expect("service initializes");

    let bot_message = telegram_message(1, 2, Some(BOT_ID), Some("bridge_bot"), None, "loop");
    let mut wrong_route_telegram =
        telegram_message(2, 3, Some(44), Some("bob"), None, "wrong route");
    wrong_route_telegram.event.route_id = RouteId::try_from("other").expect("valid route");
    service.telegram_mut().push_inbound(bot_message);
    service.telegram_mut().push_inbound(wrong_route_telegram);
    service
        .poll_telegram_once()
        .await
        .expect("ignored Telegram updates advance safely");

    let own = armada_message("own", "c1", BOT_NPUB, Some("Bridge"), "loop");
    assert_eq!(
        service.accept_armada(own).expect("own event handled"),
        IntakeOutcome::Ignored
    );

    let mut wrong_route = armada_message("wrong", "c2", ALICE_NPUB, None, "wrong route");
    wrong_route.event.route_id = RouteId::try_from("other").expect("valid route");
    assert_eq!(
        service
            .accept_armada(wrong_route)
            .expect("wrong route handled"),
        IntakeOutcome::Ignored
    );

    let mut edit = armada_message("edit", "c3", ALICE_NPUB, None, "edited");
    edit.event.mutation = Mutation::Edit;
    assert_eq!(
        service.accept_armada(edit).expect("edit handled"),
        IntakeOutcome::Ignored
    );

    let accepted = armada_message("duplicate", "c4", ALICE_NPUB, None, "once");
    assert_eq!(
        service
            .accept_armada(accepted.clone())
            .expect("first accepted"),
        IntakeOutcome::Enqueued
    );
    assert_eq!(
        service.accept_armada(accepted).expect("duplicate handled"),
        IntakeOutcome::Duplicate
    );
    assert_eq!(service.dispatch_pending().await.expect("one send"), 1);
    assert_eq!(service.telegram().sent().len(), 1);
    assert_eq!(service.store().counts().expect("counts").inbox_events, 1);
}

#[tokio::test]
async fn retries_use_deterministic_backoff_rate_limit_override_and_ambiguity_tracking() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let clock = AdjustableClock::new(10_000_000);
    let policy = retry_policy(0);
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock.clone(),
        recovery_settings(10, 100, Duration::from_secs(60), policy),
    );
    service.initialize().await.expect("service initializes");
    service
        .accept_armada(armada_message(
            "timeout",
            "retry:1",
            ALICE_NPUB,
            Some("Alice"),
            "first",
        ))
        .expect("message enqueues");
    service
        .telegram_mut()
        .push_send_result(Err(TelegramError::Timeout));

    let first = service
        .store()
        .pending_jobs(&route_id(), DeliveryDirection::ArmadaToTelegram, 1)
        .expect("job loads")[0]
        .clone();
    let report = service
        .dispatch_ready()
        .await
        .expect("failure is scheduled");
    assert_eq!(report.retried, 1);
    let state = service
        .store()
        .delivery_attempt_state(first.id)
        .expect("state loads")
        .expect("state exists");
    assert_eq!(state.state, DeliveryState::RetryWait);
    assert_eq!(state.attempt_count, 1);
    assert_eq!(state.next_attempt_at_ms, 10_001_000);
    assert_eq!(state.last_error_class.as_deref(), Some("telegram_timeout"));
    assert!(state.possible_duplicate);

    assert_eq!(
        service.dispatch_ready().await.expect("not due").delivered,
        0
    );
    assert_eq!(service.telegram().send_attempts().len(), 1);
    clock.advance(Duration::from_millis(999));
    assert_eq!(
        service
            .dispatch_ready()
            .await
            .expect("still not due")
            .delivered,
        0
    );
    clock.advance(Duration::from_millis(1));
    assert_eq!(
        service
            .dispatch_ready()
            .await
            .expect("retry succeeds")
            .delivered,
        1
    );

    service
        .accept_armada(armada_message(
            "rate",
            "retry:2",
            ALICE_NPUB,
            Some("Alice"),
            "second",
        ))
        .expect("rate-limited message enqueues");
    service
        .telegram_mut()
        .push_send_result(Err(TelegramError::RateLimited {
            retry_after_seconds: 7,
        }));
    let rate_job = service
        .store()
        .pending_jobs(&route_id(), DeliveryDirection::ArmadaToTelegram, 1)
        .expect("rate job loads")[0]
        .clone();
    service
        .dispatch_ready()
        .await
        .expect("rate limit schedules");
    let rate_state = service
        .store()
        .delivery_attempt_state(rate_job.id)
        .expect("state loads")
        .expect("state exists");
    assert_eq!(rate_state.next_attempt_at_ms, 10_008_000);
    assert_eq!(
        retry_delay_ms(first.id, 8, retry_policy(20)),
        retry_delay_ms(first.id, 8, retry_policy(20))
    );
    assert!((3_200..=4_000).contains(&retry_delay_ms(first.id, 8, retry_policy(20))));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn dead_letter_blocks_only_its_stream_and_operator_retry_resumes_fifo() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let clock = AdjustableClock::new(20_000_000);
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock,
        recovery_settings(10, 100, Duration::from_secs(60), retry_policy(0)),
    );
    service.initialize().await.expect("service initializes");
    for (id, cursor, body) in [("terminal", "d:1", "first"), ("later", "d:2", "second")] {
        service
            .accept_armada(armada_message(id, cursor, ALICE_NPUB, Some("Alice"), body))
            .expect("Armada message enqueues");
    }
    service.telegram_mut().push_inbound(telegram_message(
        9,
        10,
        Some(99),
        Some("bob"),
        None,
        "reverse",
    ));
    service
        .poll_telegram_once()
        .await
        .expect("reverse direction enqueues");
    service
        .telegram_mut()
        .push_send_result(Err(TelegramError::Terminal { status: 400 }));

    let report = service
        .dispatch_ready()
        .await
        .expect("streams dispatch independently");
    assert_eq!(report.dead_lettered, 1);
    assert_eq!(report.delivered, 1);
    assert_eq!(service.telegram().send_attempts().len(), 1);
    assert_eq!(service.telegram().sent(), []);
    assert_eq!(service.concord().sent().len(), 1);
    assert!(!service.is_ready().expect("readiness loads"));

    let dead = service.dead_letters().expect("dead letters load");
    assert_eq!(dead.len(), 1);
    assert_eq!(
        dead[0].last_error_class.as_deref(),
        Some("telegram_terminal")
    );
    service
        .retry_dead_letter(dead[0].job_id)
        .expect("operator retry succeeds");
    let resumed = service.dispatch_ready().await.expect("FIFO resumes");
    assert_eq!(resumed.delivered, 2);
    assert_eq!(service.telegram().sent().len(), 2);
    assert_eq!(service.telegram().sent()[0].1.lines().last(), Some("first"));
    assert_eq!(
        service.telegram().sent()[1].1.lines().last(),
        Some("second")
    );
    assert_eq!(service.dead_letters().expect("dead letters load").len(), 0);
    assert!(service.is_ready().expect("readiness loads"));

    service
        .accept_armada(armada_message(
            "safety",
            "d:3",
            ALICE_NPUB,
            Some("Alice"),
            "safety failure",
        ))
        .expect("safety message enqueues");
    service
        .telegram_mut()
        .push_send_result(Err(TelegramError::RoutePaused {
            route_id: route_id(),
        }));
    assert_eq!(
        service
            .dispatch_ready()
            .await
            .expect("safety failure persists")
            .dead_lettered,
        1
    );
    assert_eq!(
        service
            .store()
            .route_state(&route_id())
            .expect("route state loads"),
        RouteState::Quarantined
    );
    assert!(!service.is_ready().expect("readiness loads"));
    let safety_job = service.dead_letters().expect("dead letters load")[0].job_id;
    service
        .retry_dead_letter(safety_job)
        .expect("safety job retries");
    service.resume_route(&route_id()).expect("route resumes");
    assert_eq!(
        service
            .dispatch_ready()
            .await
            .expect("resumed route dispatches")
            .delivered,
        1
    );
    assert!(service.is_ready().expect("readiness loads"));
}

#[tokio::test]
async fn saturation_rejects_intake_without_advancing_telegram_offset() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let clock = AdjustableClock::new(30_000_000);
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock,
        recovery_settings(1, 100, Duration::from_secs(60), retry_policy(0)),
    );
    service.initialize().await.expect("service initializes");
    service
        .accept_armada(armada_message(
            "fills",
            "full:1",
            ALICE_NPUB,
            Some("Alice"),
            "fills queue",
        ))
        .expect("first job enqueues");
    service.telegram_mut().push_inbound(telegram_message(
        10,
        11,
        Some(50),
        Some("bob"),
        None,
        "must wait",
    ));

    assert!(matches!(
        service.poll_telegram_once().await,
        Err(ServiceError::QueueSaturated)
    ));
    assert_eq!(service.telegram().next_offset(), None);
    assert_eq!(
        service
            .store()
            .cursor(&route_id(), DeliveryDirection::TelegramToArmada)
            .expect("cursor loads"),
        None
    );
    assert!(!service.is_ready().expect("readiness loads"));

    assert_eq!(
        service
            .dispatch_ready()
            .await
            .expect("queue drains")
            .delivered,
        1
    );
    assert!(service.is_ready().expect("capacity recovered"));
    service
        .poll_telegram_once()
        .await
        .expect("same Telegram update is accepted after recovery");
    assert_eq!(service.telegram().next_offset(), Some(11));
}

#[tokio::test]
async fn restart_catchup_enforces_age_and_count_bounds() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut store = Store::open(data.path()).expect("store opens");
    store.register_route(&route_id()).expect("route registers");
    store
        .checkpoint_cursor(
            &route_id(),
            DeliveryDirection::ArmadaToTelegram,
            "baseline",
            1,
        )
        .expect("baseline persists");
    let mut concord = concord_fake();
    let mut baseline = armada_message("baseline", "baseline", ALICE_NPUB, None, "baseline");
    baseline.event.timestamp_ms = 1;
    let mut old = armada_message("old", "old", ALICE_NPUB, None, "too old");
    old.event.timestamp_ms = 8_000;
    let mut recent = armada_message("recent", "recent", ALICE_NPUB, None, "recent");
    recent.event.timestamp_ms = 9_500;
    let mut beyond_count = armada_message("later", "later", ALICE_NPUB, None, "later");
    beyond_count.event.timestamp_ms = 9_600;
    concord.set_history(vec![baseline, old, recent, beyond_count]);
    let mut service = BridgeService::new(
        store,
        telegram_fake(),
        concord,
        AdjustableClock::new(10_000),
        recovery_settings(10, 2, Duration::from_secs(1), retry_policy(0)),
    );

    let startup = service
        .initialize()
        .await
        .expect("bounded recovery succeeds");
    assert_eq!(startup.armada_enqueued, 1);
    assert_eq!(service.store().pending_job_count().expect("count loads"), 1);
    assert_eq!(
        service
            .store()
            .cursor(&route_id(), DeliveryDirection::ArmadaToTelegram)
            .expect("cursor loads")
            .as_deref(),
        Some("recent")
    );
}

#[tokio::test(start_paused = true)]
async fn graceful_shutdown_cancels_hung_send_and_leaves_job_durable() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        AdjustableClock::new(40_000_000),
        recovery_settings(10, 100, Duration::from_secs(60), retry_policy(0)),
    );
    service.initialize().await.expect("service initializes");
    service
        .accept_armada(armada_message(
            "shutdown",
            "shutdown:1",
            ALICE_NPUB,
            None,
            "survives",
        ))
        .expect("job enqueues");
    service.telegram_mut().set_send_hangs(true);

    let report = service
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown returns at paused deadline");
    assert!(report.timed_out);
    assert_eq!(report.remaining, 1);
    assert_eq!(service.store().pending_job_count().expect("count loads"), 1);
    assert_eq!(service.telegram().send_attempts().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_hung_attachment_upload_removes_plaintext_and_keeps_the_job() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let staging = data.path().join("attachment-staging");
    let mut concord = concord_fake();
    concord.set_attachment("cancel-file", b"%PDF-1.7".to_vec());
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord,
        AdjustableClock::new(40_000_000),
        recovery_settings(10, 100, Duration::from_secs(60), retry_policy(0))
            .with_attachment_policy(1_024, staging.clone()),
    );
    service.initialize().await.expect("service initializes");
    let mut message = armada_message(
        "attachment-shutdown",
        "attachment-shutdown:1",
        ALICE_NPUB,
        None,
        "survives",
    );
    message.event.attachments.push(AttachmentRef {
        source_id: "cancel-file".to_owned(),
        file_name: Some("cancel.pdf".to_owned()),
        media_type: Some("application/pdf".to_owned()),
        size_bytes: 8,
        locator: AttachmentLocator::Armada {
            id: "cancel-file".to_owned(),
            key: "key".to_owned(),
            nonce: "nonce".to_owned(),
            extension: "pdf".to_owned(),
            url: "https://blossom.example/cancel-file".to_owned(),
            fallback_urls: Vec::new(),
            original_hash: None,
            author_npub: ALICE_NPUB.to_owned(),
        },
        media_group_id: None,
        media_group_index: None,
        media_group_total: None,
    });
    service.accept_armada(message).expect("job enqueues");
    service.telegram_mut().set_send_hangs(true);

    let report = service
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown returns at paused deadline");
    assert!(report.timed_out);
    assert_eq!(report.remaining, 1);
    assert_eq!(
        std::fs::read_dir(&staging)
            .expect("staging directory exists")
            .count(),
        0
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn replies_edits_and_author_deletes_use_only_owned_armada_links() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock(),
        settings(),
    );
    service.initialize().await.expect("service initializes");

    let original = armada_message("conversation", "conversation:1", ALICE_NPUB, None, "hello");
    service.accept_armada(original).expect("original enqueues");
    service
        .dispatch_pending()
        .await
        .expect("original dispatches");
    let target = format!("telegram:{CHAT_ID}:1");

    let mut reply = armada_message(
        "conversation-reply",
        "conversation:2",
        "npub1bob",
        Some("Bob"),
        "answer",
    );
    reply.event.reply_to = Some(SourceMessageRef {
        source: Source::Armada,
        source_event_id: "armada:conversation".to_owned(),
    });
    reply.event.reply_quote = Some("hello".to_owned());
    assert_eq!(
        service.accept_armada(reply).expect("reply accepted"),
        IntakeOutcome::Enqueued
    );
    service.dispatch_pending().await.expect("reply dispatches");
    assert_eq!(service.telegram().replies()[0].1, target);

    let mut missing_reply = armada_message(
        "missing-reply",
        "conversation:3",
        "npub1bob",
        None,
        "fallback",
    );
    missing_reply.event.reply_to = Some(SourceMessageRef {
        source: Source::Armada,
        source_event_id: "armada:not-mirrored".to_owned(),
    });
    missing_reply.event.reply_quote = Some("unsafe\u{202e} quote\nline".to_owned());
    service
        .accept_armada(missing_reply)
        .expect("fallback reply accepted");
    service
        .dispatch_pending()
        .await
        .expect("fallback dispatches");
    assert_eq!(service.telegram().replies().len(), 1);
    assert!(
        service
            .telegram()
            .sent()
            .last()
            .expect("fallback was sent")
            .1
            .contains("↪ “unsafe quote line”")
    );

    let inline = armada_message(
        "inline-quote",
        "conversation:4",
        "npub1bob",
        None,
        "> ordinary inline quote",
    );
    service
        .accept_armada(inline)
        .expect("inline quote accepted");
    service.dispatch_pending().await.expect("inline dispatches");
    assert_eq!(service.telegram().replies().len(), 1);

    let target_ref = SourceMessageRef {
        source: Source::Armada,
        source_event_id: "armada:conversation".to_owned(),
    };
    let mut edit = armada_message(
        "edit:conversation:2",
        "conversation:5",
        ALICE_NPUB,
        None,
        "fixed",
    );
    edit.event.mutation = Mutation::Edit;
    edit.event.mutation_target = Some(target_ref.clone());
    assert_eq!(
        service.accept_armada(edit).expect("author edit accepted"),
        IntakeOutcome::Enqueued
    );
    service.dispatch_pending().await.expect("edit dispatches");
    assert_eq!(service.telegram().edits()[0].1, target);

    let mut moderator_delete = armada_message(
        "moderator-delete",
        "conversation:6",
        "npub1moderator",
        None,
        "",
    );
    moderator_delete.event.mutation = Mutation::Delete;
    moderator_delete.event.mutation_target = Some(target_ref.clone());
    assert_eq!(
        service
            .accept_armada(moderator_delete)
            .expect("moderator delete handled"),
        IntakeOutcome::Ignored
    );

    let mut author_delete = armada_message("author-delete", "conversation:7", ALICE_NPUB, None, "");
    author_delete.event.mutation = Mutation::Delete;
    author_delete.event.mutation_target = Some(target_ref);
    assert_eq!(
        service
            .accept_armada(author_delete)
            .expect("author delete accepted"),
        IntakeOutcome::Enqueued
    );
    service.dispatch_pending().await.expect("delete dispatches");
    assert_eq!(service.telegram().deletions()[0].1, target);
}

#[tokio::test]
async fn telegram_conversation_mutations_use_mapped_armada_messages() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock(),
        settings(),
    );
    service.initialize().await.expect("service initializes");

    service.telegram_mut().push_inbound(telegram_message(
        70,
        71,
        Some(41),
        Some("alice"),
        None,
        "hello",
    ));
    service.poll_telegram_once().await.expect("create accepted");
    service.dispatch_pending().await.expect("create dispatches");
    let armada_target = format!("{:064x}", 1);

    let source_target = SourceMessageRef {
        source: Source::Telegram,
        source_event_id: format!("telegram:{CHAT_ID}:70"),
    };
    let mut reply = telegram_message(71, 72, Some(42), Some("bob"), None, "answer");
    reply.event.reply_to = Some(source_target.clone());
    reply.event.reply_quote = Some("hello".to_owned());
    service.telegram_mut().push_inbound(reply);
    service.poll_telegram_once().await.expect("reply accepted");
    service.dispatch_pending().await.expect("reply dispatches");
    assert_eq!(service.concord().replies()[0].1, armada_target);

    let mut edit = telegram_message(72, 73, Some(41), Some("alice"), None, "fixed");
    edit.event.source_event_id = format!("telegram:edit:{CHAT_ID}:70:72");
    edit.event.mutation = Mutation::Edit;
    edit.event.mutation_target = Some(source_target.clone());
    service.telegram_mut().push_inbound(edit);
    service.poll_telegram_once().await.expect("edit accepted");
    service.dispatch_pending().await.expect("edit dispatches");
    assert_eq!(service.concord().edits()[0].1, armada_target);

    let mut wrong_author = telegram_message(73, 74, Some(99), None, None, "");
    wrong_author.event.source_event_id = format!("telegram:delete:{CHAT_ID}:70:73");
    wrong_author.event.mutation = Mutation::Delete;
    wrong_author.event.mutation_target = Some(source_target.clone());
    service.telegram_mut().push_inbound(wrong_author);
    service
        .poll_telegram_once()
        .await
        .expect("foreign delete ignored");
    assert_eq!(service.concord().deletions().len(), 0);

    let mut deletion = telegram_message(74, 75, Some(41), None, None, "");
    deletion.event.source_event_id = format!("telegram:delete:{CHAT_ID}:70:74");
    deletion.event.mutation = Mutation::Delete;
    deletion.event.mutation_target = Some(source_target);
    service.telegram_mut().push_inbound(deletion);
    service.poll_telegram_once().await.expect("delete accepted");
    service.dispatch_pending().await.expect("delete dispatches");
    assert_eq!(service.concord().deletions()[0].1, armada_target);
}

#[tokio::test]
async fn reaction_snapshots_create_edit_and_remove_one_attributed_summary_reply() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock(),
        settings(),
    );
    service.initialize().await.expect("service initializes");

    service
        .accept_armada(armada_message(
            "reaction-target",
            "reaction:1",
            ALICE_NPUB,
            Some("Alice"),
            "react here",
        ))
        .expect("target enqueues");
    service.dispatch_pending().await.expect("target dispatches");

    let snapshot = |source_event_id: &str, entries| BridgeReactionUpdate {
        source: Source::Armada,
        source_event_id: source_event_id.to_owned(),
        route_id: route_id(),
        target_source_event_id: "armada:reaction-target".to_owned(),
        change: ReactionChange::Snapshot { entries },
    };
    service.concord_mut().push_reaction(snapshot(
        "armada:reaction-snapshot:1",
        vec![
            ReactionEntry {
                reactor_id: ALICE_NPUB.to_owned(),
                display_name: Some("Alice".to_owned()),
                emoji: "👍".to_owned(),
                count: 1,
            },
            ReactionEntry {
                reactor_id: "npub1bob".to_owned(),
                display_name: Some("Bob".to_owned()),
                emoji: "❤️".to_owned(),
                count: 1,
            },
        ],
    ));
    assert_eq!(
        service
            .receive_armada_once()
            .await
            .expect("live reaction accepted"),
        Some(IntakeOutcome::Enqueued)
    );
    service
        .dispatch_pending()
        .await
        .expect("summary is created");
    assert_eq!(
        service.telegram().replies()[0],
        (
            route_id(),
            format!("telegram:{CHAT_ID}:1"),
            "Reactions from Armada: ❤️ Bob · 👍 Alice".to_owned(),
        )
    );

    service
        .accept_reaction(snapshot(
            "armada:reaction-snapshot:2",
            vec![ReactionEntry {
                reactor_id: ALICE_NPUB.to_owned(),
                display_name: Some("Alice".to_owned()),
                emoji: "🔥".to_owned(),
                count: 1,
            }],
        ))
        .expect("changed snapshot enqueues");
    service.dispatch_pending().await.expect("summary is edited");
    assert_eq!(
        service.telegram().edits()[0],
        (
            route_id(),
            format!("telegram:{CHAT_ID}:2"),
            "Reactions from Armada: 🔥 Alice".to_owned(),
        )
    );

    service
        .accept_reaction(snapshot("armada:reaction-snapshot:3", Vec::new()))
        .expect("empty snapshot enqueues");
    service
        .dispatch_pending()
        .await
        .expect("summary is removed");
    assert_eq!(
        service.telegram().deletions()[0].1,
        format!("telegram:{CHAT_ID}:2")
    );
}

#[tokio::test]
async fn telegram_reaction_updates_are_durable_and_summarized_in_armada() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock(),
        settings(),
    );
    service.initialize().await.expect("service initializes");

    service.telegram_mut().push_inbound(telegram_message(
        70,
        71,
        Some(41),
        Some("alice"),
        None,
        "react here",
    ));
    service.poll_telegram_once().await.expect("target accepted");
    service.dispatch_pending().await.expect("target dispatched");

    service.telegram_mut().push_reaction(
        BridgeReactionUpdate {
            source: Source::Telegram,
            source_event_id: "telegram:reaction:71".to_owned(),
            route_id: route_id(),
            target_source_event_id: format!("telegram:{CHAT_ID}:70"),
            change: ReactionChange::ActorSelection {
                reactor_id: "telegram-user:42".to_owned(),
                display_name: Some("@bob".to_owned()),
                emojis: vec!["👍".to_owned()],
            },
        },
        72,
    );
    service
        .poll_telegram_once()
        .await
        .expect("reaction accepted durably");
    service
        .dispatch_pending()
        .await
        .expect("reaction summary dispatched");

    assert_eq!(
        service.concord().replies()[0],
        (
            route_id(),
            format!("{:064x}", 1),
            "Reactions: 👍 @bob".to_owned(),
        )
    );
    assert_eq!(service.telegram().next_offset(), Some(72));

    service.telegram_mut().push_reaction(
        BridgeReactionUpdate {
            source: Source::Telegram,
            source_event_id: "telegram:reaction-count:72".to_owned(),
            route_id: route_id(),
            target_source_event_id: format!("telegram:{CHAT_ID}:70"),
            change: ReactionChange::Snapshot {
                entries: vec![ReactionEntry {
                    reactor_id: "telegram-anonymous:❤️".to_owned(),
                    display_name: None,
                    emoji: "❤️".to_owned(),
                    count: 3,
                }],
            },
        },
        73,
    );
    service
        .poll_telegram_once()
        .await
        .expect("anonymous reaction count accepted");
    service
        .dispatch_pending()
        .await
        .expect("combined summary dispatched");
    assert_eq!(service.concord().edits()[0].2, "Reactions: ❤️ 3 anonymous");
}

#[tokio::test]
async fn armada_reactions_are_reconciled_after_downtime_once_message_links_exist() {
    let data = tempfile::tempdir().expect("temporary data directory");
    {
        let mut service = BridgeService::new(
            Store::open(data.path()).expect("store opens"),
            telegram_fake(),
            concord_fake(),
            clock(),
            settings(),
        );
        service.initialize().await.expect("service initializes");
        service
            .accept_armada(armada_message(
                "offline-reaction-target",
                "reaction:offline",
                ALICE_NPUB,
                Some("Alice"),
                "react while offline",
            ))
            .expect("target enqueues");
        service.dispatch_pending().await.expect("target dispatches");
    }

    let mut concord = concord_fake();
    concord.set_reaction_history(vec![BridgeReactionUpdate {
        source: Source::Armada,
        source_event_id: "armada:reaction:offline:1".to_owned(),
        route_id: route_id(),
        target_source_event_id: "armada:offline-reaction-target".to_owned(),
        change: ReactionChange::Snapshot {
            entries: vec![ReactionEntry {
                reactor_id: "npub1bob".to_owned(),
                display_name: Some("Bob".to_owned()),
                emoji: "👍".to_owned(),
                count: 1,
            }],
        },
    }]);
    let mut restarted = BridgeService::new(
        Store::open(data.path()).expect("store reopens"),
        telegram_fake(),
        concord,
        clock(),
        settings(),
    );
    restarted.initialize().await.expect("restart initializes");
    restarted
        .dispatch_pending()
        .await
        .expect("messages recover");
    assert_eq!(
        restarted
            .recover_armada_reactions()
            .await
            .expect("reaction history reconciles"),
        1
    );
    restarted
        .dispatch_pending()
        .await
        .expect("summary dispatches");
    assert_eq!(
        restarted.telegram().replies()[0].2,
        "Reactions from Armada: 👍 Bob"
    );
}

#[tokio::test]
async fn edits_of_split_mirrors_are_ignored_without_removing_content() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let mut service = BridgeService::new(
        Store::open(data.path()).expect("store opens"),
        telegram_fake(),
        concord_fake(),
        clock(),
        settings(),
    );
    service.initialize().await.expect("service initializes");
    let original = armada_message(
        "split-original",
        "split:1",
        ALICE_NPUB,
        None,
        &"a".repeat(5000),
    );
    assert_eq!(
        service.accept_armada(original).expect("original accepted"),
        IntakeOutcome::Enqueued
    );
    service
        .dispatch_pending()
        .await
        .expect("original dispatches");
    let mut edit = armada_message("split-edit", "split:2", ALICE_NPUB, None, "shorter");
    edit.event.mutation = Mutation::Edit;
    edit.event.mutation_target = Some(SourceMessageRef {
        source: Source::Armada,
        source_event_id: "armada:split-original".to_owned(),
    });
    assert_eq!(
        service.accept_armada(edit).expect("edit handled"),
        IntakeOutcome::Ignored
    );
    service.dispatch_pending().await.expect("dispatch finishes");
    assert_eq!(service.telegram().edits(), []);
    assert_eq!(service.telegram().deletions(), []);
}

#[tokio::test]
async fn expiring_armada_mirror_is_deleted_after_restart() {
    let data = tempfile::tempdir().expect("temporary data directory");
    let clock = AdjustableClock::new(50_000_000);
    {
        let mut service = BridgeService::new(
            Store::open(data.path()).expect("store opens"),
            telegram_fake(),
            concord_fake(),
            clock.clone(),
            settings(),
        );
        service.initialize().await.expect("service initializes");
        let mut expiring = armada_message("expiring", "expiring:1", ALICE_NPUB, None, "temporary");
        expiring.event.expires_at_ms = Some(50_001_000);
        service.accept_armada(expiring).expect("message accepted");
        service.dispatch_pending().await.expect("mirror is created");
        assert_eq!(service.telegram().deletions(), []);
    }

    clock.advance(Duration::from_secs(2));
    let mut restarted = BridgeService::new(
        Store::open(data.path()).expect("store reopens"),
        telegram_fake(),
        concord_fake(),
        clock,
        settings(),
    );
    restarted
        .dispatch_ready()
        .await
        .expect("due deletion dispatches after restart");
    assert_eq!(
        restarted.telegram().deletions()[0].1,
        format!("telegram:{CHAT_ID}:1")
    );
    restarted
        .dispatch_ready()
        .await
        .expect("completed deletion is idempotent");
    assert_eq!(restarted.telegram().deletions().len(), 1);
}
