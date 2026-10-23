use armada_telegram_bridge::attachments::PrivateTempFile;
use armada_telegram_bridge::config::BridgeConfig;
use armada_telegram_bridge::telegram::{TelegramAdapter, TelegramPort, TelegramRoute};

#[tokio::test]
#[ignore = "requires an operator-created Telegram bot and configured private group"]
async fn configured_bot_and_private_group_are_accessible() {
    let config_path =
        std::env::var("ARMADA_BRIDGE_LIVE_CONFIG").unwrap_or_else(|_| "bridge.toml".to_owned());
    let config = BridgeConfig::load(config_path).expect("live configuration should load");
    let telegram = config.telegram();
    let routes: Vec<_> = config.routes().iter().map(TelegramRoute::from).collect();
    let mut adapter = TelegramAdapter::new(
        telegram.token().expose(),
        telegram.expected_bot_id(),
        telegram.protect_content(),
        telegram.relay_other_bots(),
        routes,
    )
    .expect("Telegram adapter should build");

    let identity = adapter
        .verify_identity()
        .await
        .expect("configured Telegram bot should match getMe");
    assert_eq!(identity.id, telegram.expected_bot_id());
    for route in config.routes() {
        let access = adapter
            .verify_route_access(route.id())
            .await
            .expect("bot should resolve the configured private group");
        assert_eq!(access.chat_id, route.telegram_chat_id().get());
    }
}

#[tokio::test]
#[ignore = "uploads the configured disposable file to the real private Telegram group"]
async fn configured_private_group_can_receive_attachment() {
    dotenvy::dotenv().ok();
    let config_path =
        std::env::var("ARMADA_BRIDGE_LIVE_CONFIG").unwrap_or_else(|_| "bridge.toml".to_owned());
    let config = BridgeConfig::load(config_path).expect("live configuration should load");
    let source = std::env::var("ARMADA_BRIDGE_LIVE_FILE")
        .expect("ARMADA_BRIDGE_LIVE_FILE must point to a disposable file");
    let source = std::path::Path::new(&source);
    let telegram = config.telegram();
    let routes: Vec<_> = config.routes().iter().map(TelegramRoute::from).collect();
    let mut adapter = TelegramAdapter::new(
        telegram.token().expose(),
        telegram.expected_bot_id(),
        telegram.protect_content(),
        telegram.relay_other_bots(),
        routes,
    )
    .expect("Telegram adapter should build");
    let staged = PrivateTempFile::new(
        &config.data_dir().join("tmp/live"),
        source.file_name().and_then(|name| name.to_str()),
    )
    .expect("private staging file should be available");
    tokio::fs::copy(source, staged.path())
        .await
        .expect("live fixture should copy");
    let metadata = staged
        .inspect(config.limits().max_file_bytes().get())
        .expect("live fixture must fit the configured limit");
    let route = config.routes().first().expect("one route is configured");

    let receipt = adapter
        .send_attachment(route.id(), staged.path(), &metadata, None)
        .await
        .expect("Telegram multipart upload should succeed");
    assert!(receipt.message_id > 0);
}
