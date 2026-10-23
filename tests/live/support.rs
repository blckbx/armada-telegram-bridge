use std::env;

use armada_telegram_bridge::error::Sensitive;

pub struct LiveConcordSetup {
    pub nsec: Sensitive,
    pub expected_npub: String,
    pub community_id: String,
    pub channel_id: String,
    receive_marker: Option<String>,
    author_npub: Option<String>,
    send_marker: Option<String>,
}

impl LiveConcordSetup {
    pub fn from_environment() -> Self {
        dotenvy::dotenv().ok();
        let nsec = required("ARMADA_NSEC");
        assert!(!nsec.trim().is_empty(), "ARMADA_NSEC must not be empty");

        Self {
            nsec: Sensitive::new(nsec),
            expected_npub: required("ARMADA_BRIDGE_LIVE_EXPECTED_NPUB"),
            community_id: required("ARMADA_BRIDGE_LIVE_COMMUNITY_ID"),
            channel_id: required("ARMADA_BRIDGE_LIVE_CHANNEL_ID"),
            receive_marker: env::var("ARMADA_BRIDGE_LIVE_RECEIVE_MARKER").ok(),
            author_npub: env::var("ARMADA_BRIDGE_LIVE_EXPECTED_AUTHOR_NPUB").ok(),
            send_marker: env::var("ARMADA_BRIDGE_LIVE_SEND_MARKER").ok(),
        }
    }

    pub fn required_receive_marker(&self) -> &str {
        self.receive_marker
            .as_deref()
            .expect("ARMADA_BRIDGE_LIVE_RECEIVE_MARKER must be set")
    }

    pub fn required_author_npub(&self) -> &str {
        self.author_npub
            .as_deref()
            .expect("ARMADA_BRIDGE_LIVE_EXPECTED_AUTHOR_NPUB must be set")
    }

    pub fn required_send_marker(&self) -> &str {
        self.send_marker
            .as_deref()
            .expect("ARMADA_BRIDGE_LIVE_SEND_MARKER must be set")
    }
}

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this ignored live test"))
}
