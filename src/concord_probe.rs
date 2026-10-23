//! Narrow, operator-gated Concord interoperability probe for P01.
//!
//! This module never creates an identity or accepts an invitation. It starts
//! `vector_sdk` with an explicitly supplied key and the SDK's manual invite
//! policy, then verifies relay-backed recovery of an already accepted
//! membership.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

use vector_sdk::{InvitePolicy, VectorBot};

use crate::error::Sensitive;

/// Immutable Concord route selected by canonical identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcordScope {
    community_id: String,
    channel_id: String,
}

impl ConcordScope {
    /// Creates an exact community/channel scope.
    #[must_use]
    pub fn new(community_id: impl Into<String>, channel_id: impl Into<String>) -> Self {
        Self {
            community_id: community_id.into(),
            channel_id: channel_id.into(),
        }
    }

    /// Returns true only when both canonical identifiers match.
    #[must_use]
    pub fn contains(&self, community_id: &str, channel_id: &str) -> bool {
        self.community_id == community_id && self.channel_id == channel_id
    }
}

/// Inputs for a fresh Concord interoperability probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcordProbeConfig {
    expected_npub: String,
    scope: ConcordScope,
    data_dir: PathBuf,
}

impl ConcordProbeConfig {
    /// Creates probe configuration from operator-reviewed identifiers.
    #[must_use]
    pub fn new(
        expected_npub: impl Into<String>,
        community_id: impl Into<String>,
        channel_id: impl Into<String>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            expected_npub: expected_npub.into(),
            scope: ConcordScope::new(community_id, channel_id),
            data_dir: data_dir.into(),
        }
    }
}

/// Membership state recovered from Concord relays.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredMembership {
    /// Canonical community identifier.
    pub community_id: String,
    /// Canonical channel identifier.
    pub channel_id: String,
    /// Whether the channel has an independent private key.
    pub is_private: bool,
    /// Whether the supplied identity holds the channel key.
    pub is_readable: bool,
}

/// Minimal authenticated message evidence used by the P01 live test.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedMessage {
    /// Inner Concord event identifier.
    pub event_id: String,
    /// Canonical community identifier resolved by the SDK.
    pub community_id: String,
    /// Canonical channel identifier used for the history query.
    pub channel_id: String,
    /// Authenticated inner author, when present.
    pub author_npub: Option<String>,
    /// Whether the SDK identifies the event as ours.
    pub is_mine: bool,
}

/// Connected SDK handle restricted to one exact route.
pub struct ConcordProbe {
    bot: VectorBot,
    scope: ConcordScope,
}

impl ConcordProbe {
    /// Connects with an operator-supplied identity and manual invite policy.
    ///
    /// # Errors
    ///
    /// Returns an error when SDK initialization fails or the supplied key does
    /// not derive the operator-reviewed `expected_npub`.
    pub async fn connect(
        nsec: &Sensitive,
        config: ConcordProbeConfig,
    ) -> Result<Self, ConcordProbeError> {
        let bot = VectorBot::builder()
            .nsec(nsec.expose().to_owned())
            .data_dir(&config.data_dir)
            .invite_policy(InvitePolicy::Manual)
            .build()
            .await
            .map_err(|source| ConcordProbeError::Sdk {
                operation: "connect Concord SDK",
                source,
            })?;

        if bot.npub() != config.expected_npub {
            return Err(ConcordProbeError::IdentityMismatch {
                expected: config.expected_npub,
                actual: bot.npub().to_owned(),
            });
        }

        Ok(Self {
            bot,
            scope: config.scope,
        })
    }

    /// Synchronizes relay-backed community state and verifies the exact channel.
    ///
    /// # Errors
    ///
    /// Returns an error when synchronization fails, the membership is absent,
    /// or the configured channel is absent from the recovered community.
    pub async fn recover_membership(&self) -> Result<RecoveredMembership, ConcordProbeError> {
        self.bot
            .sync_communities()
            .await
            .map_err(|source| ConcordProbeError::Sdk {
                operation: "synchronize Concord communities",
                source,
            })?;

        let communities = self.bot.communities().await;
        let community = communities
            .into_iter()
            .find(|community| community.id() == self.scope.community_id)
            .ok_or_else(|| ConcordProbeError::CommunityNotRecovered {
                community_id: self.scope.community_id.clone(),
            })?;
        let channels = community.channels().await;
        let channel = channels
            .into_iter()
            .find(|channel| channel.id() == self.scope.channel_id)
            .ok_or_else(|| ConcordProbeError::ChannelNotRecovered {
                community_id: self.scope.community_id.clone(),
                channel_id: self.scope.channel_id.clone(),
            })?;

        Ok(RecoveredMembership {
            community_id: self.scope.community_id.clone(),
            channel_id: self.scope.channel_id.clone(),
            is_private: channel.is_private(),
            is_readable: channel.is_readable(),
        })
    }

    /// Synchronizes bounded channel history and finds an exact text marker.
    ///
    /// # Errors
    ///
    /// Returns an error when the SDK cannot synchronize the configured channel.
    pub async fn find_message(
        &self,
        marker: &str,
        limit: usize,
    ) -> Result<Option<ObservedMessage>, ConcordProbeError> {
        let channel = self
            .bot
            .community(self.scope.community_id.clone())
            .channel(self.scope.channel_id.clone());
        channel
            .sync(limit)
            .await
            .map_err(|source| ConcordProbeError::Sdk {
                operation: "synchronize configured channel",
                source,
            })?;

        Ok(channel
            .history(limit)
            .await
            .into_iter()
            .find(|message| message.content == marker)
            .map(|message| ObservedMessage {
                event_id: message.id,
                community_id: self.scope.community_id.clone(),
                channel_id: self.scope.channel_id.clone(),
                author_npub: message.npub,
                is_mine: message.mine,
            }))
    }

    /// Sends an exact text marker to the configured channel.
    ///
    /// # Errors
    ///
    /// Returns an error when the SDK rejects or cannot publish the message.
    pub async fn send(&self, marker: &str) -> Result<String, ConcordProbeError> {
        self.bot
            .community(self.scope.community_id.clone())
            .channel(self.scope.channel_id.clone())
            .send(marker)
            .await
            .map_err(|source| ConcordProbeError::Sdk {
                operation: "send configured-channel marker",
                source,
            })
    }

    /// The SDK data directory used by this probe is controlled by its config.
    #[must_use]
    pub fn scope(&self) -> &ConcordScope {
        &self.scope
    }
}

/// Secret-safe failures from the interoperability probe.
#[derive(Debug)]
pub enum ConcordProbeError {
    /// The SDK failed while performing a named operation.
    Sdk {
        /// Safe operation label.
        operation: &'static str,
        /// SDK error, which must not contain the supplied `nsec`.
        source: vector_sdk::Error,
    },
    /// The key derives a different public identity than configured.
    IdentityMismatch {
        /// Operator-reviewed identity.
        expected: String,
        /// Identity derived by the SDK.
        actual: String,
    },
    /// Relay-backed state did not contain the configured community.
    CommunityNotRecovered {
        /// Missing canonical community ID.
        community_id: String,
    },
    /// The community did not contain the configured channel.
    ChannelNotRecovered {
        /// Recovered canonical community ID.
        community_id: String,
        /// Missing canonical channel ID.
        channel_id: String,
    },
}

impl Display for ConcordProbeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sdk { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::IdentityMismatch { expected, actual } => {
                write!(
                    formatter,
                    "configured npub {expected} does not match derived npub {actual}"
                )
            }
            Self::CommunityNotRecovered { community_id } => {
                write!(formatter, "community {community_id} was not recovered")
            }
            Self::ChannelNotRecovered {
                community_id,
                channel_id,
            } => write!(
                formatter,
                "channel {channel_id} was not recovered in community {community_id}"
            ),
        }
    }
}

impl Error for ConcordProbeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sdk { source, .. } => Some(source),
            Self::IdentityMismatch { .. }
            | Self::CommunityNotRecovered { .. }
            | Self::ChannelNotRecovered { .. } => None,
        }
    }
}
