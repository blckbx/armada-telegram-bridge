use armada_telegram_bridge::attachments::PrivateTempFile;
use armada_telegram_bridge::concord::{ConcordAdapter, ConcordPort, ConcordRoute};
use armada_telegram_bridge::domain::Mutation;
use armada_telegram_bridge::domain::{ChannelId, CommunityId, RouteId};
use tempfile::tempdir;

#[path = "live/support.rs"]
mod support;

use support::LiveConcordSetup;

fn route(setup: &LiveConcordSetup) -> ConcordRoute {
    ConcordRoute::new(
        RouteId::try_from("live").expect("live route ID should be valid"),
        CommunityId::try_from(setup.community_id.as_str())
            .expect("live community ID should be canonical"),
        ChannelId::try_from(setup.channel_id.as_str())
            .expect("live channel ID should be canonical"),
    )
}

async fn adapter(setup: &LiveConcordSetup) -> (ConcordAdapter, tempfile::TempDir) {
    let data_dir = tempdir().expect("a fresh SDK data directory should be available");
    let adapter = ConcordAdapter::connect(
        &setup.nsec,
        &setup.expected_npub,
        vec![route(setup)],
        data_dir.path(),
    )
    .await
    .expect("the manually accepted identity should connect");
    (adapter, data_dir)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a manually provisioned disposable Armada identity"]
async fn manual_membership_is_recovered() {
    let setup = LiveConcordSetup::from_environment();
    let (mut adapter, _data_dir) = adapter(&setup).await;

    adapter
        .verify_ready()
        .await
        .expect("private membership and channel key should recover from relays");
    adapter
        .catch_up(route(&setup).route_id(), None, 1)
        .await
        .expect("configured channel history should be readable");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires an operator to send the configured marker from Armada"]
async fn configured_channel_can_receive() {
    let setup = LiveConcordSetup::from_environment();
    let marker = setup.required_receive_marker();
    let expected_author = setup.required_author_npub();
    let (mut adapter, _data_dir) = adapter(&setup).await;
    adapter
        .verify_ready()
        .await
        .expect("membership should recover before receiving");
    let observed = adapter
        .catch_up(route(&setup).route_id(), None, 500)
        .await
        .expect("channel history sync should succeed")
        .into_iter()
        .find(|message| message.event.body == marker)
        .expect("the operator-sent marker should be present");

    assert!(matches!(
        observed.event.sender,
        armada_telegram_bridge::domain::SenderIdentity::Armada { npub, .. }
            if npub == expected_author
    ));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "sends the configured marker to a real private Armada channel"]
async fn configured_channel_can_send() {
    let setup = LiveConcordSetup::from_environment();
    let marker = setup.required_send_marker();
    let (mut adapter, _data_dir) = adapter(&setup).await;
    adapter
        .verify_ready()
        .await
        .expect("membership should recover before sending");
    let receipt = adapter
        .send_text(route(&setup).route_id(), marker)
        .await
        .expect("the marker should publish to the configured channel");

    assert_eq!(receipt.destination_event_id.len(), 64);
    assert!(
        receipt
            .destination_event_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "uploads the configured disposable file through Blossom into the real Armada channel"]
async fn configured_channel_can_send_attachment_through_blossom() {
    let setup = LiveConcordSetup::from_environment();
    let source = std::env::var("ARMADA_BRIDGE_LIVE_FILE")
        .expect("ARMADA_BRIDGE_LIVE_FILE must point to a disposable file");
    let source = std::path::Path::new(&source);
    let (mut adapter, data_dir) = adapter(&setup).await;
    adapter
        .verify_ready()
        .await
        .expect("membership should recover before sending");
    let staged = PrivateTempFile::new(
        data_dir.path(),
        source.file_name().and_then(|name| name.to_str()),
    )
    .expect("private staging file should be available");
    tokio::fs::copy(source, staged.path())
        .await
        .expect("live fixture should copy");
    let metadata = staged
        .inspect(20_000_000)
        .expect("live fixture must be at most 20 MB");

    let receipt = adapter
        .send_attachment(route(&setup).route_id(), staged.path(), &metadata)
        .await
        .expect("encrypted Blossom upload and Concord publish should succeed");
    assert_eq!(receipt.destination_event_id.len(), 64);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "waits for an operator to send a threaded Armada reply and then edit it"]
async fn configured_channel_reports_reply_and_edit_shapes() {
    let setup = LiveConcordSetup::from_environment();
    let reply_marker = std::env::var("ARMADA_BRIDGE_LIVE_P10_REPLY_MARKER")
        .expect("ARMADA_BRIDGE_LIVE_P10_REPLY_MARKER is required");
    let edit_marker = std::env::var("ARMADA_BRIDGE_LIVE_P10_EDIT_MARKER")
        .expect("ARMADA_BRIDGE_LIVE_P10_EDIT_MARKER is required");
    let (mut adapter, _data_dir) = adapter(&setup).await;
    adapter
        .verify_ready()
        .await
        .expect("membership should recover");

    let created = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        loop {
            if let Some(message) = adapter.next_live().await.expect("live event should load")
                && message.event.body == reply_marker
            {
                break message;
            }
        }
    })
    .await
    .expect("send the configured marker as a threaded reply within three minutes");
    let reply_target = created
        .event
        .reply_to
        .expect("the marker must be an actual threaded reply, not inline quoted text");
    assert_eq!(
        reply_target.source,
        armada_telegram_bridge::domain::Source::Armada
    );
    let created_id = created.event.source_event_id;

    let edited = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        loop {
            if let Some(message) = adapter.next_live().await.expect("live event should load")
                && message.event.body == edit_marker
            {
                break message;
            }
        }
    })
    .await
    .expect("edit the same reply to the configured edit marker within three minutes");
    assert_eq!(edited.event.mutation, Mutation::Edit);
    assert_eq!(
        edited
            .event
            .mutation_target
            .expect("edit has an authenticated original target")
            .source_event_id,
        created_id
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "waits for an operator to send a self-destructing Armada message"]
async fn configured_channel_reports_expiry_shape() {
    let setup = LiveConcordSetup::from_environment();
    let marker = std::env::var("ARMADA_BRIDGE_LIVE_P10_EXPIRY_MARKER")
        .expect("ARMADA_BRIDGE_LIVE_P10_EXPIRY_MARKER is required");
    let (mut adapter, _data_dir) = adapter(&setup).await;
    adapter
        .verify_ready()
        .await
        .expect("membership should recover");

    let message = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        loop {
            if let Some(message) = adapter.next_live().await.expect("live event should load")
                && message.event.body == marker
            {
                break message;
            }
        }
    })
    .await
    .expect("send the configured marker with a self-destruct timer within three minutes");
    assert!(
        message
            .event
            .expires_at_ms
            .is_some_and(|expiry| expiry > message.event.timestamp_ms)
    );
}

#[test]
fn other_channel_is_not_selected() {
    let scope =
        armada_telegram_bridge::concord_probe::ConcordScope::new("community-1", "channel-allowed");

    assert!(scope.contains("community-1", "channel-allowed"));
    assert!(!scope.contains("community-1", "channel-other"));
    assert!(!scope.contains("community-other", "channel-allowed"));
}
