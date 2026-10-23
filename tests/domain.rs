use std::convert::TryFrom;

use armada_telegram_bridge::domain::{
    ChannelId, CommunityId, RouteId, TelegramChatId, TelegramThreadId,
};

#[test]
fn canonical_route_identifiers_are_typed_and_normalized() {
    let community = CommunityId::try_from("AB".repeat(32)).expect("hex ID should parse");
    let channel = ChannelId::try_from("cd".repeat(32)).expect("hex ID should parse");
    let route = RouteId::try_from("private-community").expect("route ID should parse");
    let chat = TelegramChatId::try_from(-1_001_234_567_890).expect("negative chat ID should parse");
    let topic = TelegramThreadId::try_from(42).expect("positive topic ID should parse");

    assert_eq!(community.as_str(), "ab".repeat(32));
    assert_eq!(channel.as_str(), "cd".repeat(32));
    assert_eq!(route.as_str(), "private-community");
    assert_eq!(chat.get(), -1_001_234_567_890);
    assert_eq!(topic.get(), 42);
}

#[test]
fn display_names_and_invalid_numbers_cannot_become_route_identifiers() {
    assert!(CommunityId::try_from("My Armada group").is_err());
    assert!(ChannelId::try_from("general").is_err());
    assert!(RouteId::try_from("Private Community").is_err());
    assert!(TelegramChatId::try_from(0).is_err());
    assert!(TelegramChatId::try_from(123).is_err());
    assert!(TelegramThreadId::try_from(0).is_err());
}
