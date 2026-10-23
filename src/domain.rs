//! Transport-neutral identifiers and events.

use std::fmt::{self, Display, Formatter};
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::rich_text::RichText;

/// A validation failure for a domain value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidDomainValue {
    reason: &'static str,
}

impl InvalidDomainValue {
    const fn new(reason: &'static str) -> Self {
        Self { reason }
    }
}

impl Display for InvalidDomainValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.reason)
    }
}

impl std::error::Error for InvalidDomainValue {}

macro_rules! hex_identifier {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Returns the canonical lowercase hexadecimal representation.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidDomainValue;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(InvalidDomainValue::new(
                        "expected an exact 32-byte hexadecimal identifier",
                    ));
                }
                Ok(Self(value.to_ascii_lowercase()))
            }
        }

        impl TryFrom<&str> for $name {
            type Error = InvalidDomainValue;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::try_from(value.to_owned())
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

hex_identifier!(CommunityId, "A canonical Concord community ID.");
hex_identifier!(ChannelId, "A canonical Concord channel ID.");

/// Stable operator-defined route identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RouteId(String);

impl RouteId {
    /// Returns the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RouteId {
    type Error = InvalidDomainValue;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut bytes = value.bytes();
        let starts_validly = bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
        let remainder_is_valid = bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        });
        if value.len() > 64 || !starts_validly || !remainder_is_valid {
            return Err(InvalidDomainValue::new(
                "expected 1-64 lowercase ASCII letters, digits, hyphens, or underscores",
            ));
        }
        Ok(Self(value))
    }
}

impl TryFrom<&str> for RouteId {
    type Error = InvalidDomainValue;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_owned())
    }
}

impl Display for RouteId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Immutable numeric Telegram group or supergroup ID.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TelegramChatId(i64);

impl TelegramChatId {
    /// Returns the Telegram chat ID.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for TelegramChatId {
    type Error = InvalidDomainValue;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value >= 0 {
            return Err(InvalidDomainValue::new(
                "Telegram group IDs must be negative integers",
            ));
        }
        Ok(Self(value))
    }
}

/// Optional positive Telegram forum topic ID.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TelegramThreadId(NonZeroU32);

impl TelegramThreadId {
    /// Returns the topic ID.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl TryFrom<u32> for TelegramThreadId {
    type Error = InvalidDomainValue;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or_else(|| InvalidDomainValue::new("Telegram topic IDs must be positive"))
    }
}

/// Origin transport for a canonical bridge event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Concord/Armada.
    Armada,
    /// Telegram Bot API.
    Telegram,
}

/// One attributed reaction contributing to a cross-platform summary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReactionEntry {
    /// Stable source-platform actor identifier.
    pub reactor_id: String,
    /// Untrusted source-platform presentation name, when available.
    pub display_name: Option<String>,
    /// Unicode emoji or a safe textual custom-emoji shortcode.
    pub emoji: String,
    /// Number represented by this entry; greater than one is used for anonymous counts.
    pub count: u32,
}

/// Source reaction state change.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReactionChange {
    /// Replaces the complete known reaction set for the source message.
    Snapshot {
        /// Current reactions after the source-side change.
        entries: Vec<ReactionEntry>,
    },
    /// Replaces one known actor's selected Telegram reactions.
    ActorSelection {
        /// Stable source actor identifier.
        reactor_id: String,
        /// Current untrusted presentation name.
        display_name: Option<String>,
        /// Complete current supported emoji selection for that actor.
        emojis: Vec<String>,
    },
}

/// Normalized reaction update targeting a message known to the source transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BridgeReactionUpdate {
    /// Transport that observed the reaction.
    pub source: Source,
    /// Immutable or safely idempotent source update identifier.
    pub source_event_id: String,
    /// Configured route containing the target.
    pub route_id: RouteId,
    /// Transport-qualified message receiving the reaction.
    pub target_source_event_id: String,
    /// Complete snapshot or per-actor replacement.
    pub change: ReactionChange,
}

/// Stable sender identity separated from untrusted presentation fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum SenderIdentity {
    /// Authenticated inner Nostr author.
    Armada {
        /// Canonical `npub`.
        npub: String,
        /// Untrusted current display name.
        display_name: Option<String>,
    },
    /// Telegram user, anonymous administrator, or channel sender.
    Telegram {
        /// Stable numeric user ID when Telegram provides one.
        user_id: Option<u64>,
        /// Stable numeric sender-chat ID for anonymous/channel posts.
        sender_chat_id: Option<i64>,
        /// Untrusted mutable username.
        username: Option<String>,
        /// Untrusted mutable display name.
        display_name: Option<String>,
    },
}

/// Type of change represented by an inbound event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mutation {
    /// Newly created message.
    Create,
    /// Edited message.
    Edit,
    /// Deleted message.
    Delete,
    /// Expiring message.
    Expire,
}

/// Transport-qualified reference to an existing source message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceMessageRef {
    /// Source transport.
    pub source: Source,
    /// Immutable transport event/message ID.
    pub source_event_id: String,
}

/// Durable source locator needed to retrieve an attachment on retry.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum AttachmentLocator {
    /// Telegram Bot API file reference.
    Telegram {
        /// Downloadable Bot API file identifier.
        file_id: String,
    },
    /// Verified Concord attachment metadata. URLs and encryption material are
    /// sensitive and are only persisted inside the encrypted outbox payload.
    Armada {
        /// Concord content identifier.
        id: String,
        /// Attachment decryption key.
        key: String,
        /// Attachment decryption nonce.
        nonce: String,
        /// Original extension hint.
        extension: String,
        /// Primary encrypted Blossom URL.
        url: String,
        /// Encrypted Blossom mirror URLs.
        fallback_urls: Vec<String>,
        /// Optional plaintext hash for public legacy attachments.
        original_hash: Option<String>,
        /// Verified inner author used for SDK Blossom fallback discovery.
        author_npub: String,
    },
}

impl fmt::Debug for AttachmentLocator {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Telegram { .. } => formatter.write_str("Telegram([REDACTED])"),
            Self::Armada { .. } => formatter.write_str("Armada([REDACTED])"),
        }
    }
}

/// Transport-neutral attachment metadata and encrypted durable locator.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttachmentRef {
    /// Immutable source-side attachment reference.
    pub source_id: String,
    /// Untrusted source filename.
    pub file_name: Option<String>,
    /// Declared media type.
    pub media_type: Option<String>,
    /// Declared byte length.
    pub size_bytes: u64,
    /// Source-specific locator used by the corresponding adapter.
    pub locator: AttachmentLocator,
    /// Telegram album identifier, when supplied.
    pub media_group_id: Option<String>,
    /// One-based position among items observed in the same bounded batch.
    pub media_group_index: Option<u16>,
    /// Number of album items observed in the same bounded batch.
    pub media_group_total: Option<u16>,
}

/// Canonical transport-neutral event passed to routing and persistence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BridgeEvent {
    /// Origin transport.
    pub source: Source,
    /// Immutable, transport-namespaced source ID.
    pub source_event_id: String,
    /// Configured route.
    pub route_id: RouteId,
    /// Authenticated stable identity plus presentation metadata.
    pub sender: SenderIdentity,
    /// Milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Canonical visible message body.
    pub body: String,
    /// Transport-neutral source formatting when supplied by the adapter.
    pub rich_text: Option<RichText>,
    /// Zero or more source attachments.
    pub attachments: Vec<AttachmentRef>,
    /// Optional source-side reply target.
    pub reply_to: Option<SourceMessageRef>,
    /// Safe plaintext excerpt supplied by the source for an unmapped reply.
    pub reply_quote: Option<String>,
    /// Original source message affected by an edit, delete, or expiry event.
    pub mutation_target: Option<SourceMessageRef>,
    /// Message mutation type.
    pub mutation: Mutation,
    /// Absolute Unix millisecond at which a created mirror should be removed.
    pub expires_at_ms: Option<i64>,
}
