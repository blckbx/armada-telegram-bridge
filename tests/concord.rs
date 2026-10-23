use armada_telegram_bridge::concord::{
    ConcordError, ConcordInboundMessage, ConcordPort, ConcordProfile, ConcordReadiness,
    ConcordRoute, ConcordSendReceipt,
};
use armada_telegram_bridge::domain::{
    BridgeEvent, ChannelId, CommunityId, Mutation, RouteId, SenderIdentity, Source,
};
use armada_telegram_bridge::test_support::FakeConcordPort;

const COMMUNITY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHANNEL: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const EVENT_ONE: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const EVENT_TWO: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const AUTHOR: &str = "npub1verifiedauthor";

fn route() -> ConcordRoute {
    ConcordRoute::new(
        RouteId::try_from("alpha").expect("valid route ID"),
        CommunityId::try_from(COMMUNITY).expect("valid community ID"),
        ChannelId::try_from(CHANNEL).expect("valid channel ID"),
    )
}

fn inbound(id: &str, body: &str, at_ms: i64) -> ConcordInboundMessage {
    ConcordInboundMessage {
        event: BridgeEvent {
            source: Source::Armada,
            source_event_id: format!("armada:{id}"),
            route_id: RouteId::try_from("alpha").expect("valid route ID"),
            sender: SenderIdentity::Armada {
                npub: AUTHOR.to_owned(),
                display_name: Some("Verified Alice".to_owned()),
            },
            timestamp_ms: at_ms,
            body: body.to_owned(),
            rich_text: None,
            attachments: Vec::new(),
            reply_to: None,
            reply_quote: None,
            mutation_target: None,
            mutation: Mutation::Create,
            expires_at_ms: None,
        },
        next_cursor: format!("{at_ms}:{id}"),
    }
}

async fn assert_port_contract(port: &mut dyn ConcordPort) {
    let route_id = RouteId::try_from("alpha").expect("valid route ID");
    port.verify_ready()
        .await
        .expect("configured route should be ready");
    assert_eq!(port.readiness(&route_id), ConcordReadiness::Ready);

    let history = port
        .catch_up(&route_id, None, 100)
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[0].event.source_event_id,
        format!("armada:{EVENT_ONE}")
    );
    assert_eq!(
        history[1].event.source_event_id,
        format!("armada:{EVENT_TWO}")
    );
    let resumed = port
        .catch_up(&route_id, Some(&history[0].next_cursor), 100)
        .await
        .expect("cursor catch-up should load");
    assert_eq!(resumed, vec![history[1].clone()]);

    let live = port
        .next_live()
        .await
        .expect("live intake should work")
        .expect("one live event should exist");
    assert_eq!(live.event.body, "live three");

    let profile = port
        .profile(AUTHOR)
        .await
        .expect("profile lookup should work")
        .expect("profile should exist");
    assert_eq!(profile.display_name.as_deref(), Some("Verified Alice"));

    let receipt = port
        .send_text(&route_id, "outbound")
        .await
        .expect("send should work");
    assert_eq!(receipt.destination_event_id.len(), 64);
}

#[tokio::test]
async fn reusable_port_contract_passes_for_the_in_memory_fake() {
    let mut fake = FakeConcordPort::new(vec![route()]);
    fake.set_readiness(
        RouteId::try_from("alpha").expect("valid route"),
        ConcordReadiness::Ready,
    );
    fake.set_history(vec![
        inbound(EVENT_ONE, "history one", 1_000),
        inbound(EVENT_TWO, "history two", 2_000),
    ]);
    fake.push_live(inbound(
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        "live three",
        3_000,
    ));
    fake.set_profile(ConcordProfile {
        npub: AUTHOR.to_owned(),
        display_name: Some("Verified Alice".to_owned()),
    });

    assert_port_contract(&mut fake).await;
    assert_eq!(
        fake.sent(),
        &[(
            RouteId::try_from("alpha").expect("valid route"),
            "outbound".to_owned()
        )]
    );
}

#[tokio::test]
async fn every_unsafe_membership_state_fails_closed() {
    for readiness in [
        ConcordReadiness::MissingCommunity,
        ConcordReadiness::MissingChannel,
        ConcordReadiness::MissingKey,
        ConcordReadiness::IncompleteRekey,
        ConcordReadiness::Removed,
        ConcordReadiness::Dissolved,
        ConcordReadiness::StaleState,
    ] {
        let route_id = RouteId::try_from("alpha").expect("valid route");
        let mut fake = FakeConcordPort::new(vec![route()]);
        fake.set_readiness(route_id.clone(), readiness);

        assert!(matches!(
            fake.verify_ready().await,
            Err(ConcordError::RouteNotReady {
                route_id: failed,
                reason
            }) if failed == route_id && reason == readiness
        ));
        assert!(matches!(
            fake.send_text(&route_id, "must not send").await,
            Err(ConcordError::RouteNotReady { .. })
        ));
    }
}

#[test]
fn send_receipts_require_canonical_inner_event_ids() {
    assert!(ConcordSendReceipt::new(EVENT_ONE).is_ok());
    assert!(ConcordSendReceipt::new("outer-wrapper-or-pending-id").is_err());
}
