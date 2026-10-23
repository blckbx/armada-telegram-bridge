//! Concord/Armada adapter with all `vector_sdk` details kept behind a port.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use vector_sdk::{InboundEventHandler, InvitePolicy, Message, VectorBot};

use crate::attachments::{
    DirectoryCleanupGuard, InspectedAttachment, sanitize_filename, set_directory_private,
    set_file_private, validate_declared_size,
};
use crate::config::Route;
use crate::domain::{
    AttachmentLocator, AttachmentRef, BridgeEvent, BridgeReactionUpdate, ChannelId, CommunityId,
    Mutation, ReactionChange, ReactionEntry, RouteId, SenderIdentity, Source,
};
use crate::error::Sensitive;

const MAX_HISTORY_PAGE: usize = 500;
const REACTION_PROFILE_TIMEOUT: Duration = Duration::from_secs(3);

/// Concord-specific projection of one configured route.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcordRoute {
    route_id: RouteId,
    community_id: CommunityId,
    channel_id: ChannelId,
}

impl ConcordRoute {
    /// Creates an exact community/channel route.
    #[must_use]
    pub const fn new(route_id: RouteId, community_id: CommunityId, channel_id: ChannelId) -> Self {
        Self {
            route_id,
            community_id,
            channel_id,
        }
    }

    /// Stable operator-defined route ID.
    #[must_use]
    pub const fn route_id(&self) -> &RouteId {
        &self.route_id
    }

    /// Exact canonical community ID.
    #[must_use]
    pub const fn community_id(&self) -> &CommunityId {
        &self.community_id
    }

    /// Exact canonical channel ID.
    #[must_use]
    pub const fn channel_id(&self) -> &ChannelId {
        &self.channel_id
    }
}

impl From<&Route> for ConcordRoute {
    fn from(route: &Route) -> Self {
        Self::new(
            route.id().clone(),
            route.community_id().clone(),
            route.channel_id().clone(),
        )
    }
}

/// Fail-closed readiness for one configured Concord route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConcordReadiness {
    /// The route has not completed recovery yet.
    Unchecked,
    /// Exact private channel membership and key material are available.
    Ready,
    /// The configured community is absent from recovered membership.
    MissingCommunity,
    /// The configured channel is absent from the exact community.
    MissingChannel,
    /// The channel is public, contrary to the bridge's private-room contract.
    NotPrivate,
    /// A private channel is known but its current key is unavailable.
    MissingKey,
    /// A key rotation was observed but could not be completed.
    IncompleteRekey,
    /// The bridge identity was removed from the community.
    Removed,
    /// The community was permanently dissolved.
    Dissolved,
    /// Relay-backed consensus could not be refreshed safely.
    StaleState,
}

/// Normalized Concord message and its stable history cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcordInboundMessage {
    /// Transport-neutral bridge event.
    pub event: BridgeEvent,
    /// Wire-stable SDK cursor to commit with the event.
    pub next_cursor: String,
}

/// One normalized live item produced by Concord.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConcordInbound {
    /// A new or edited channel message.
    Message(Box<ConcordInboundMessage>),
    /// The complete current reaction set for one channel message.
    Reaction(BridgeReactionUpdate),
}

/// Minimal profile presentation returned by the Concord port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcordProfile {
    /// Stable authenticated identity.
    pub npub: String,
    /// Best current untrusted display value.
    pub display_name: Option<String>,
}

/// Canonical receipt returned after publishing to Concord.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcordSendReceipt {
    /// Canonical signed inner event ID returned by the SDK.
    pub destination_event_id: String,
}

impl ConcordSendReceipt {
    /// Validates and wraps a canonical 32-byte hexadecimal event ID.
    ///
    /// # Errors
    ///
    /// Rejects pending IDs, wrapper IDs, and malformed SDK results.
    pub fn new(event_id: impl Into<String>) -> Result<Self, ConcordError> {
        let event_id = event_id.into();
        if !is_event_id(&event_id) {
            return Err(ConcordError::InvalidEventId);
        }
        Ok(Self {
            destination_event_id: event_id.to_ascii_lowercase(),
        })
    }
}

/// Transport-neutral Concord operations used by bridge orchestration.
#[async_trait]
pub trait ConcordPort: Send {
    /// Refreshes relay-backed membership and requires every route to be ready.
    async fn verify_ready(&mut self) -> Result<(), ConcordError>;

    /// Current fail-closed status for a route.
    fn readiness(&self, route_id: &RouteId) -> ConcordReadiness;

    /// Synchronizes bounded history strictly after an optional durable cursor.
    async fn catch_up(
        &mut self,
        route_id: &RouteId,
        after_cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConcordInboundMessage>, ConcordError>;

    /// Waits for the next relevant live message; `None` means listening stopped.
    async fn next_live(&mut self) -> Result<Option<ConcordInboundMessage>, ConcordError>;

    /// Waits for the next message or reaction update. Ports predating reaction
    /// support retain their message-only behavior through this default.
    async fn next_inbound(&mut self) -> Result<Option<ConcordInbound>, ConcordError> {
        Ok(self
            .next_live()
            .await?
            .map(|message| ConcordInbound::Message(Box::new(message))))
    }

    /// Returns current reaction snapshots from bounded synchronized history.
    async fn reaction_snapshots(
        &mut self,
        _route_id: &RouteId,
        _limit: usize,
    ) -> Result<Vec<BridgeReactionUpdate>, ConcordError> {
        Ok(Vec::new())
    }

    /// Publishes plaintext to one exact ready route.
    async fn send_text(
        &mut self,
        route_id: &RouteId,
        text: &str,
    ) -> Result<ConcordSendReceipt, ConcordError>;

    /// Publishes a native threaded reply to a mapped bridge-owned event.
    async fn send_reply(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
        text: &str,
    ) -> Result<ConcordSendReceipt, ConcordError>;

    /// Edits a mapped message previously created by the bridge identity.
    async fn edit_message(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
        text: &str,
    ) -> Result<(), ConcordError>;

    /// Deletes a mapped message previously created by the bridge identity.
    async fn delete_message(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
    ) -> Result<(), ConcordError>;

    /// Uses the SDK's bounded decrypt, integrity, Blossom fallback, and SSRF
    /// path, then writes the plaintext to an already-private staging file.
    async fn download_attachment(
        &mut self,
        attachment: &AttachmentRef,
        destination: &Path,
        maximum_bytes: u64,
    ) -> Result<(), ConcordError>;

    /// Encrypts and publishes one staged file through the Concord SDK.
    async fn send_attachment(
        &mut self,
        route_id: &RouteId,
        path: &Path,
        metadata: &InspectedAttachment,
    ) -> Result<ConcordSendReceipt, ConcordError>;

    /// Resolves presentation metadata without changing the stable identity.
    async fn profile(&mut self, npub: &str) -> Result<Option<ConcordProfile>, ConcordError>;
}

/// `vector_sdk 0.11.0` implementation of [`ConcordPort`].
pub struct ConcordAdapter {
    bot: VectorBot,
    own_npub: String,
    routes: Vec<ConcordRoute>,
    readiness: HashMap<RouteId, ConcordReadiness>,
    live_receiver: Option<mpsc::UnboundedReceiver<LiveSignal>>,
    live_task: Option<JoinHandle<()>>,
    pending_signal: Option<LiveSignal>,
    pending_live: VecDeque<ConcordInbound>,
    reaction_sequence: u64,
    attachment_cache_dir: std::path::PathBuf,
}

impl ConcordAdapter {
    /// Starts the SDK with the operator-supplied identity and manual invites.
    ///
    /// # Errors
    ///
    /// Returns a secret-safe error if SDK initialization fails or the derived
    /// identity does not match `expected_npub`.
    pub async fn connect(
        nsec: &Sensitive,
        expected_npub: &str,
        routes: Vec<ConcordRoute>,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self, ConcordError> {
        silence_sdk_logging();
        validate_routes(&routes)?;
        let data_dir = data_dir.as_ref();
        let attachment_cache_dir =
            std::env::temp_dir().join(format!("armada-telegram-bridge-sdk-{}", std::process::id()));
        std::fs::create_dir_all(&attachment_cache_dir).map_err(|_| ConcordError::TemporaryFile)?;
        set_directory_private(&attachment_cache_dir).map_err(|_| ConcordError::TemporaryFile)?;
        vector_sdk::vector_core::db::set_download_dir(attachment_cache_dir.clone());
        if vector_sdk::vector_core::db::get_download_dir() != attachment_cache_dir {
            return Err(ConcordError::InvalidConfiguration);
        }
        let bot = VectorBot::builder()
            .nsec(nsec.expose().to_owned())
            .data_dir(data_dir)
            .invite_policy(InvitePolicy::Manual)
            .build()
            .await
            .map_err(|error| ConcordError::Connect {
                reason: classify_connect_error(&error),
            })?;
        if bot.npub() != expected_npub {
            return Err(ConcordError::IdentityMismatch {
                expected: expected_npub.to_owned(),
                actual: bot.npub().to_owned(),
            });
        }
        let readiness = routes
            .iter()
            .map(|route| (route.route_id.clone(), ConcordReadiness::Unchecked))
            .collect();
        Ok(Self {
            own_npub: bot.npub().to_owned(),
            bot,
            routes,
            readiness,
            live_receiver: None,
            live_task: None,
            pending_signal: None,
            pending_live: VecDeque::new(),
            reaction_sequence: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                .unwrap_or(1),
            attachment_cache_dir,
        })
    }

    fn route(&self, route_id: &RouteId) -> Result<&ConcordRoute, ConcordError> {
        self.routes
            .iter()
            .find(|route| &route.route_id == route_id)
            .ok_or_else(|| ConcordError::UnknownRoute {
                route_id: route_id.clone(),
            })
    }

    fn require_ready(&self, route_id: &RouteId) -> Result<(), ConcordError> {
        let _ = self.route(route_id)?;
        let reason = self.readiness(route_id);
        if reason == ConcordReadiness::Ready {
            Ok(())
        } else {
            Err(ConcordError::RouteNotReady {
                route_id: route_id.clone(),
                reason,
            })
        }
    }

    async fn load_profile(&self, npub: &str) -> Option<vector_sdk::SlimProfile> {
        match self.bot.cached_profile(npub).await {
            Some(profile) => Some(profile),
            None => self.bot.fetch_profile(npub).await,
        }
    }

    async fn display_name(&self, npub: &str) -> Option<String> {
        self.load_profile(npub).await.and_then(profile_display_name)
    }

    async fn reaction_display_name(&self, npub: &str) -> Option<String> {
        if let Some(profile) = self.bot.cached_profile(npub).await {
            return profile_display_name(profile);
        }
        tokio::time::timeout(REACTION_PROFILE_TIMEOUT, self.bot.fetch_profile(npub))
            .await
            .ok()
            .flatten()
            .and_then(profile_display_name)
    }

    async fn normalize(
        &self,
        route: &ConcordRoute,
        channel_id: &str,
        is_group: bool,
        message: Message,
        mutation: Mutation,
    ) -> Result<Option<ConcordInboundMessage>, ConcordError> {
        if !is_group || channel_id != route.channel_id.as_str() || message.mine {
            return Ok(None);
        }
        let Some(author) = message.npub.clone() else {
            return Ok(None);
        };
        if author.is_empty() || author == self.own_npub {
            return Ok(None);
        }
        let display_name = self.display_name(&author).await;
        normalize_snapshot(
            route,
            channel_id,
            is_group,
            &self.own_npub,
            message,
            display_name,
            mutation,
        )
    }

    async fn normalize_reactions(
        &mut self,
        route: &ConcordRoute,
        message: &Message,
    ) -> Result<BridgeReactionUpdate, ConcordError> {
        if !is_event_id(&message.id) {
            return Err(ConcordError::InvalidEventId);
        }
        let mut entries = Vec::new();
        let mut display_names: HashMap<String, Option<String>> = HashMap::new();
        for (author_id, emoji) in supported_reaction_slots(message, &self.own_npub) {
            let display_name = if let Some(cached) = display_names.get(&author_id) {
                cached.clone()
            } else {
                // The live signal stays pending across cancellation, while the
                // bound prevents an unavailable profile relay from blocking
                // reaction delivery indefinitely.
                let loaded = self.reaction_display_name(&author_id).await;
                display_names.insert(author_id.clone(), loaded.clone());
                loaded
            };
            entries.push(ReactionEntry {
                reactor_id: author_id,
                display_name,
                emoji,
                count: 1,
            });
        }
        self.reaction_sequence = self.reaction_sequence.saturating_add(1);
        Ok(BridgeReactionUpdate {
            source: Source::Armada,
            source_event_id: format!(
                "armada:reaction:{}:{}",
                message.id.to_ascii_lowercase(),
                self.reaction_sequence
            ),
            route_id: route.route_id.clone(),
            target_source_event_id: format!("armada:{}", message.id.to_ascii_lowercase()),
            change: ReactionChange::Snapshot { entries },
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn next_inbound_event(&mut self) -> Result<Option<ConcordInbound>, ConcordError> {
        if let Some(event) = self.pending_live.pop_front() {
            return Ok(Some(event));
        }
        self.start_live();
        loop {
            if self.pending_signal.is_none() {
                self.pending_signal = self
                    .live_receiver
                    .as_mut()
                    .expect("live receiver is initialized immediately above")
                    .recv()
                    .await;
            }
            let signal = self.pending_signal.clone();
            match signal {
                Some(LiveSignal::Message {
                    channel_id,
                    message,
                }) => {
                    let Some(route) = self
                        .routes
                        .iter()
                        .find(|route| route.channel_id.as_str() == channel_id)
                        .cloned()
                    else {
                        self.pending_signal = None;
                        continue;
                    };
                    self.require_ready(&route.route_id)?;
                    let normalized = self
                        .normalize(&route, &channel_id, true, *message, Mutation::Create)
                        .await?;
                    self.pending_signal = None;
                    if let Some(message) = normalized {
                        return Ok(Some(ConcordInbound::Message(Box::new(message))));
                    }
                }
                Some(LiveSignal::Update {
                    channel_id,
                    message,
                }) => {
                    let Some(route) = self
                        .routes
                        .iter()
                        .find(|route| route.channel_id.as_str() == channel_id)
                        .cloned()
                    else {
                        self.pending_signal = None;
                        continue;
                    };
                    self.require_ready(&route.route_id)?;
                    let reaction = self.normalize_reactions(&route, &message).await?;
                    if message.edited
                        && let Some(edited) = self
                            .normalize(&route, &channel_id, true, *message, Mutation::Edit)
                            .await?
                    {
                        self.pending_signal = None;
                        self.pending_live
                            .push_back(ConcordInbound::Reaction(reaction));
                        return Ok(Some(ConcordInbound::Message(Box::new(edited))));
                    }
                    self.pending_signal = None;
                    return Ok(Some(ConcordInbound::Reaction(reaction)));
                }
                Some(LiveSignal::Removed { community_id }) => {
                    self.pending_signal = None;
                    self.mark_community(&community_id, ConcordReadiness::Removed);
                    return self.first_not_ready().map(|()| None);
                }
                Some(LiveSignal::Dissolved { community_id }) => {
                    self.pending_signal = None;
                    self.mark_community(&community_id, ConcordReadiness::Dissolved);
                    return self.first_not_ready().map(|()| None);
                }
                Some(LiveSignal::Keyed {
                    community_id,
                    channel_id,
                }) => {
                    for route in &self.routes {
                        if route.community_id.as_str() == community_id
                            && route.channel_id.as_str() == channel_id
                        {
                            self.readiness
                                .insert(route.route_id.clone(), ConcordReadiness::Ready);
                        }
                    }
                    self.pending_signal = None;
                }
                Some(LiveSignal::Refresh { community_id }) => {
                    if self
                        .routes
                        .iter()
                        .any(|route| route.community_id.as_str() == community_id)
                    {
                        self.verify_ready().await?;
                    }
                    self.pending_signal = None;
                }
                Some(LiveSignal::Stopped(clean)) => {
                    self.pending_signal = None;
                    for state in self.readiness.values_mut() {
                        *state = ConcordReadiness::StaleState;
                    }
                    if clean {
                        return Ok(None);
                    }
                    return Err(ConcordError::Sdk {
                        operation: "listen for Concord events",
                    });
                }
                None => return Ok(None),
            }
        }
    }

    fn start_live(&mut self) {
        if self.live_receiver.is_some() {
            return;
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        let handler = Arc::new(LiveHandler {
            sender: sender.clone(),
        });
        let bot = self.bot.clone();
        self.live_task = Some(tokio::spawn(async move {
            let result = bot.listen_with(handler).await;
            let _ = sender.send(LiveSignal::Stopped(result.is_ok()));
        }));
        self.live_receiver = Some(receiver);
    }

    fn mark_community(&mut self, community_id: &str, state: ConcordReadiness) {
        for route in &self.routes {
            if route.community_id.as_str() == community_id {
                self.readiness.insert(route.route_id.clone(), state);
            }
        }
    }
}

/// Bounded, secret-free reason for an SDK initialization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConcordConnectFailure {
    Database,
    Io,
    Nostr,
    Crypto,
    Network,
    State,
    Other,
}

fn classify_connect_error(error: &vector_sdk::VectorError) -> ConcordConnectFailure {
    use vector_sdk::VectorError;

    match error {
        VectorError::Db(_) => ConcordConnectFailure::Database,
        VectorError::Io(_) => ConcordConnectFailure::Io,
        VectorError::Nostr(_) => ConcordConnectFailure::Nostr,
        VectorError::Crypto(_) => ConcordConnectFailure::Crypto,
        VectorError::Network(_) => ConcordConnectFailure::Network,
        VectorError::NotInitialized(_) => ConcordConnectFailure::State,
        // Some vector-core database helpers currently erase their typed error
        // into `Other`. Inspect only for a fixed classification and never retain
        // or emit the underlying string, which can contain local paths.
        VectorError::Other(message) => {
            let message = message.to_ascii_lowercase();
            if ["permission denied", "read-only file system", "i/o error"]
                .iter()
                .any(|needle| message.contains(needle))
            {
                ConcordConnectFailure::Io
            } else if [
                "database",
                "sqlite",
                "schema",
                "migration",
                "profile directory",
                "account directory",
                "downgrade",
            ]
            .iter()
            .any(|needle| message.contains(needle))
            {
                ConcordConnectFailure::Database
            } else if ["encrypt", "decrypt", "cipher", "incorrect pin"]
                .iter()
                .any(|needle| message.contains(needle))
            {
                ConcordConnectFailure::Crypto
            } else {
                ConcordConnectFailure::Other
            }
        }
    }
}

fn supported_reaction_slots(message: &Message, own_npub: &str) -> Vec<(String, String)> {
    message
        .reactions
        .iter()
        .filter(|reaction| {
            !reaction.author_id.is_empty()
                && reaction.author_id != own_npub
                && reaction.author_id.len() <= 256
                && !reaction.author_id.chars().any(char::is_control)
                && !reaction.emoji.is_empty()
                && reaction.emoji.len() <= 128
                && reaction.emoji.chars().count() <= 32
                && !reaction.emoji.chars().any(char::is_control)
        })
        .map(|reaction| {
            let emoji = match reaction.emoji.as_str() {
                "+" => "👍".to_owned(),
                "-" => "👎".to_owned(),
                _ => reaction.emoji.clone(),
            };
            (reaction.author_id.clone(), emoji)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(256)
        .collect()
}

fn profile_display_name(profile: vector_sdk::SlimProfile) -> Option<String> {
    first_nonempty([profile.display_name, profile.name, profile.nickname])
}

fn silence_sdk_logging() {
    // vector-core's default warning sink includes full attachment URLs.
    // Bridge runtime diagnostics must never disclose private content locators.
    vector_sdk::vector_core::logging::set_log_level(vector_sdk::vector_core::logging::LEVEL_OFF);
}

impl Drop for ConcordAdapter {
    fn drop(&mut self) {
        if let Some(task) = self.live_task.take() {
            task.abort();
        }
        let _ = std::fs::remove_dir(&self.attachment_cache_dir);
    }
}

#[async_trait]
impl ConcordPort for ConcordAdapter {
    async fn verify_ready(&mut self) -> Result<(), ConcordError> {
        if self.bot.sync_communities().await.is_err() {
            for state in self.readiness.values_mut() {
                *state = ConcordReadiness::StaleState;
            }
            return self.first_not_ready();
        }

        let communities = self.bot.communities().await;
        for route in &self.routes {
            let Some(community) = communities
                .iter()
                .find(|community| community.id() == route.community_id.as_str())
            else {
                self.readiness
                    .insert(route.route_id.clone(), ConcordReadiness::MissingCommunity);
                continue;
            };
            if community.is_dissolved().await {
                self.readiness
                    .insert(route.route_id.clone(), ConcordReadiness::Dissolved);
                continue;
            }
            let channels = community.channels().await;
            let Some(channel) = channels
                .iter()
                .find(|channel| channel.id() == route.channel_id.as_str())
            else {
                self.readiness
                    .insert(route.route_id.clone(), ConcordReadiness::MissingChannel);
                continue;
            };
            let state = channel_readiness(channel.is_private(), channel.is_readable());
            self.readiness.insert(route.route_id.clone(), state);
        }
        self.first_not_ready()
    }

    fn readiness(&self, route_id: &RouteId) -> ConcordReadiness {
        self.readiness
            .get(route_id)
            .copied()
            .unwrap_or(ConcordReadiness::Unchecked)
    }

    async fn catch_up(
        &mut self,
        route_id: &RouteId,
        after_cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConcordInboundMessage>, ConcordError> {
        self.require_ready(route_id)?;
        if limit == 0 || limit > MAX_HISTORY_PAGE {
            return Err(ConcordError::InvalidHistoryLimit);
        }
        let cursor = after_cursor
            .map(str::parse::<vector_sdk::Cursor>)
            .transpose()
            .map_err(|_| ConcordError::InvalidCursor)?;
        let route = self.route(route_id)?.clone();
        let channel = self
            .bot
            .community(route.community_id.as_str().to_owned())
            .channel(route.channel_id.as_str().to_owned());
        if channel.sync(limit).await.is_err() {
            self.readiness
                .insert(route_id.clone(), ConcordReadiness::StaleState);
            return Err(ConcordError::RouteNotReady {
                route_id: route_id.clone(),
                reason: ConcordReadiness::StaleState,
            });
        }
        let messages = channel.history(limit).await;
        let mut normalized = Vec::new();
        for message in messages {
            let create_position = vector_sdk::Cursor::of(&message);
            let edit_at = message
                .edited
                .then(|| {
                    message
                        .edit_history
                        .as_ref()
                        .and_then(|history| history.last())
                        .map(|edit| edit.edited_at)
                })
                .flatten();
            if cursor
                .as_ref()
                .is_none_or(|cursor| create_position > *cursor)
                && let Some(created) = self
                    .normalize(
                        &route,
                        route.channel_id.as_str(),
                        true,
                        message.clone(),
                        Mutation::Create,
                    )
                    .await?
            {
                normalized.push(created);
            }
            if let Some(edit_at) = edit_at {
                let edit_position = vector_sdk::Cursor {
                    at_ms: edit_at,
                    id: message.id.clone(),
                };
                if cursor.as_ref().is_none_or(|cursor| edit_position > *cursor)
                    && let Some(edited) = self
                        .normalize(
                            &route,
                            route.channel_id.as_str(),
                            true,
                            message,
                            Mutation::Edit,
                        )
                        .await?
                {
                    normalized.push(edited);
                }
            }
        }
        normalized.sort_by(|left, right| {
            left.event
                .timestamp_ms
                .cmp(&right.event.timestamp_ms)
                .then_with(|| {
                    left.next_cursor
                        .split_once(':')
                        .map(|(_, id)| id)
                        .cmp(&right.next_cursor.split_once(':').map(|(_, id)| id))
                })
        });
        normalized.truncate(limit);
        Ok(normalized)
    }

    async fn next_live(&mut self) -> Result<Option<ConcordInboundMessage>, ConcordError> {
        loop {
            match self.next_inbound_event().await? {
                Some(ConcordInbound::Message(message)) => return Ok(Some(*message)),
                Some(ConcordInbound::Reaction(_)) => {}
                None => return Ok(None),
            }
        }
    }

    async fn next_inbound(&mut self) -> Result<Option<ConcordInbound>, ConcordError> {
        self.next_inbound_event().await
    }

    async fn reaction_snapshots(
        &mut self,
        route_id: &RouteId,
        limit: usize,
    ) -> Result<Vec<BridgeReactionUpdate>, ConcordError> {
        self.require_ready(route_id)?;
        if limit == 0 || limit > MAX_HISTORY_PAGE {
            return Err(ConcordError::InvalidHistoryLimit);
        }
        let route = self.route(route_id)?.clone();
        let channel = self
            .bot
            .community(route.community_id.as_str().to_owned())
            .channel(route.channel_id.as_str().to_owned());
        let messages = channel.history(limit).await;
        let mut snapshots = Vec::with_capacity(messages.len());
        for message in messages {
            snapshots.push(self.normalize_reactions(&route, &message).await?);
        }
        Ok(snapshots)
    }

    async fn send_text(
        &mut self,
        route_id: &RouteId,
        text: &str,
    ) -> Result<ConcordSendReceipt, ConcordError> {
        self.require_ready(route_id)?;
        let route = self.route(route_id)?.clone();
        let event_id = self
            .bot
            .community(route.community_id.as_str().to_owned())
            .channel(route.channel_id.as_str().to_owned())
            .send(text)
            .await
            .map_err(|_| ConcordError::Sdk {
                operation: "send Concord message",
            })?;
        ConcordSendReceipt::new(event_id)
    }

    async fn send_reply(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
        text: &str,
    ) -> Result<ConcordSendReceipt, ConcordError> {
        self.require_ready(route_id)?;
        if !is_event_id(destination_event_id) {
            return Err(ConcordError::InvalidMessageReference);
        }
        let route = self.route(route_id)?.clone();
        let event_id = self
            .bot
            .community(route.community_id.as_str().to_owned())
            .channel(route.channel_id.as_str().to_owned())
            .reply(destination_event_id, text)
            .await
            .map_err(|_| ConcordError::Sdk {
                operation: "send Concord reply",
            })?;
        ConcordSendReceipt::new(event_id)
    }

    async fn edit_message(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
        text: &str,
    ) -> Result<(), ConcordError> {
        self.require_ready(route_id)?;
        if !is_event_id(destination_event_id) {
            return Err(ConcordError::InvalidMessageReference);
        }
        let Some(message) = self.bot.message(destination_event_id).await else {
            return Ok(());
        };
        let route = self.route(route_id)?.clone();
        if message.chat_id != route.channel_id.as_str() || !message.message.mine {
            return Err(ConcordError::InvalidMessageReference);
        }
        self.bot
            .community(route.community_id.as_str().to_owned())
            .channel(route.channel_id.as_str().to_owned())
            .edit(destination_event_id, text)
            .await
            .map_err(|_| ConcordError::Sdk {
                operation: "edit Concord message",
            })
    }

    async fn delete_message(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
    ) -> Result<(), ConcordError> {
        self.require_ready(route_id)?;
        if !is_event_id(destination_event_id) {
            return Err(ConcordError::InvalidMessageReference);
        }
        let Some(message) = self.bot.message(destination_event_id).await else {
            return Ok(());
        };
        let route = self.route(route_id)?.clone();
        if message.chat_id != route.channel_id.as_str() || !message.message.mine {
            return Err(ConcordError::InvalidMessageReference);
        }
        self.bot
            .community(route.community_id.as_str().to_owned())
            .channel(route.channel_id.as_str().to_owned())
            .delete(destination_event_id)
            .await
            .map_err(|_| ConcordError::Sdk {
                operation: "delete Concord message",
            })
    }

    async fn download_attachment(
        &mut self,
        attachment: &AttachmentRef,
        destination: &Path,
        maximum_bytes: u64,
    ) -> Result<(), ConcordError> {
        validate_declared_size(attachment.size_bytes, maximum_bytes)
            .map_err(|_| ConcordError::AttachmentTooLarge)?;
        let AttachmentLocator::Armada {
            id,
            key,
            nonce,
            extension,
            url,
            fallback_urls,
            original_hash,
            author_npub,
        } = &attachment.locator
        else {
            return Err(ConcordError::InvalidAttachment);
        };
        let sdk_attachment = vector_sdk::Attachment {
            id: id.clone(),
            key: key.clone(),
            nonce: nonce.clone(),
            extension: extension.clone(),
            name: attachment.file_name.clone().unwrap_or_default(),
            url: url.clone(),
            size: attachment.size_bytes,
            original_hash: original_hash.clone(),
            fallback_urls: fallback_urls.clone(),
            ..vector_sdk::Attachment::default()
        };
        let maximum = usize::try_from(maximum_bytes)
            .unwrap_or(usize::MAX)
            .saturating_add(16);
        let bytes = self
            .bot
            .core()
            .download_attachment_within(&sdk_attachment, Some(author_npub), maximum)
            .await
            .map_err(|error| match error {
                vector_sdk::vector_core::DownloadError::TooLarge(_) => {
                    ConcordError::AttachmentTooLarge
                }
                vector_sdk::vector_core::DownloadError::Unusable(_) => {
                    ConcordError::AttachmentIntegrity
                }
                vector_sdk::vector_core::DownloadError::Unreachable(_) => {
                    ConcordError::AttachmentUnavailable
                }
            })?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
            return Err(ConcordError::AttachmentTooLarge);
        }
        tokio::fs::write(destination, bytes)
            .await
            .map_err(|_| ConcordError::TemporaryFile)
    }

    async fn send_attachment(
        &mut self,
        route_id: &RouteId,
        path: &Path,
        metadata: &InspectedAttachment,
    ) -> Result<ConcordSendReceipt, ConcordError> {
        self.require_ready(route_id)?;
        let route = self.route(route_id)?.clone();
        let parent = path.parent().ok_or(ConcordError::TemporaryFile)?;
        let publish_dir = tempfile::tempdir_in(parent).map_err(|_| ConcordError::TemporaryFile)?;
        let publish_name = sanitize_filename(Some(&metadata.file_name));
        let publish_path = publish_dir.path().join(publish_name);
        tokio::fs::copy(path, &publish_path)
            .await
            .map_err(|_| ConcordError::TemporaryFile)?;
        set_file_private(&publish_path).map_err(|_| ConcordError::TemporaryFile)?;
        let _cache_cleanup = DirectoryCleanupGuard::new(&self.attachment_cache_dir)
            .map_err(|_| ConcordError::TemporaryFile)?;
        let event_id = self
            .bot
            .community(route.community_id.as_str().to_owned())
            .channel(route.channel_id.as_str().to_owned())
            .send_file(&publish_path)
            .await
            .map_err(|_| ConcordError::Sdk {
                operation: "send Concord attachment",
            })?;
        ConcordSendReceipt::new(event_id)
    }

    async fn profile(&mut self, npub: &str) -> Result<Option<ConcordProfile>, ConcordError> {
        Ok(self.load_profile(npub).await.map(|profile| ConcordProfile {
            npub: npub.to_owned(),
            display_name: first_nonempty([profile.display_name, profile.name, profile.nickname]),
        }))
    }
}

impl ConcordAdapter {
    fn first_not_ready(&self) -> Result<(), ConcordError> {
        self.routes
            .iter()
            .find_map(|route| {
                let reason = self.readiness(&route.route_id);
                (reason != ConcordReadiness::Ready).then(|| ConcordError::RouteNotReady {
                    route_id: route.route_id.clone(),
                    reason,
                })
            })
            .map_or(Ok(()), Err)
    }
}

/// Secret-safe Concord adapter failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConcordError {
    /// Route definitions overlap or are otherwise unsafe.
    InvalidConfiguration,
    /// SDK initialization failed; the raw SDK error is deliberately discarded.
    Connect { reason: ConcordConnectFailure },
    /// A version-specific SDK operation failed; underlying details are omitted.
    Sdk {
        /// Safe operation label.
        operation: &'static str,
    },
    /// The supplied identity differs from the operator-reviewed identity.
    IdentityMismatch {
        /// Operator-reviewed identity.
        expected: String,
        /// Identity derived by the SDK.
        actual: String,
    },
    /// A route ID is not configured.
    UnknownRoute {
        /// Requested stable route ID.
        route_id: RouteId,
    },
    /// Exact route membership or cryptographic state is unsafe.
    RouteNotReady {
        /// Affected route.
        route_id: RouteId,
        /// Fail-closed state.
        reason: ConcordReadiness,
    },
    /// History limit is zero or exceeds the SDK page bound.
    InvalidHistoryLimit,
    /// Stored history cursor is malformed.
    InvalidCursor,
    /// SDK result is not a canonical signed inner event ID.
    InvalidEventId,
    /// SDK message timestamp exceeds the bridge representation.
    InvalidTimestamp,
    /// A declared or decrypted attachment exceeds the configured limit.
    AttachmentTooLarge,
    /// Downloaded bytes could not be decrypted or failed their declared hash.
    AttachmentIntegrity,
    /// Every attachment source was temporarily unreachable.
    AttachmentUnavailable,
    /// Attachment metadata cannot be used by this source adapter.
    InvalidAttachment,
    /// A durable destination event ID is malformed.
    InvalidMessageReference,
    /// Private temporary file access failed.
    TemporaryFile,
}

impl Display for ConcordError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("invalid Concord adapter configuration")
            }
            Self::Connect { reason } => {
                write!(formatter, "connect Concord SDK failed: {reason:?}")
            }
            Self::Sdk { operation } => write!(formatter, "{operation} failed"),
            Self::IdentityMismatch { expected, actual } => write!(
                formatter,
                "configured npub {expected} does not match derived npub {actual}"
            ),
            Self::UnknownRoute { route_id } => {
                write!(formatter, "unknown Concord route {route_id}")
            }
            Self::RouteNotReady { route_id, reason } => {
                write!(
                    formatter,
                    "Concord route {route_id} is not ready: {reason:?}"
                )
            }
            Self::InvalidHistoryLimit => formatter.write_str("invalid Concord history limit"),
            Self::InvalidCursor => formatter.write_str("invalid Concord history cursor"),
            Self::InvalidEventId => formatter.write_str("invalid Concord inner event ID"),
            Self::InvalidTimestamp => formatter.write_str("invalid Concord message timestamp"),
            Self::AttachmentTooLarge => {
                formatter.write_str("Concord attachment exceeds the configured limit")
            }
            Self::AttachmentIntegrity => {
                formatter.write_str("Concord attachment failed integrity verification")
            }
            Self::AttachmentUnavailable => {
                formatter.write_str("Concord attachment source is temporarily unavailable")
            }
            Self::InvalidAttachment => formatter.write_str("invalid Concord attachment"),
            Self::InvalidMessageReference => {
                formatter.write_str("invalid Concord message reference")
            }
            Self::TemporaryFile => formatter.write_str("Concord attachment staging failed"),
        }
    }
}

impl Error for ConcordError {}

#[derive(Clone)]
enum LiveSignal {
    Message {
        channel_id: String,
        message: Box<Message>,
    },
    Update {
        channel_id: String,
        message: Box<Message>,
    },
    Removed {
        community_id: String,
    },
    Dissolved {
        community_id: String,
    },
    Keyed {
        community_id: String,
        channel_id: String,
    },
    Refresh {
        community_id: String,
    },
    Stopped(bool),
}

struct LiveHandler {
    sender: mpsc::UnboundedSender<LiveSignal>,
}

impl InboundEventHandler for LiveHandler {
    fn on_reaction_received(&self, channel_id: &str, message: &Message) {
        let _ = self.sender.send(LiveSignal::Update {
            channel_id: channel_id.to_owned(),
            message: Box::new(message.clone()),
        });
    }

    fn on_community_message(&self, channel_id: &str, message: &Message, is_new: bool) {
        if is_new {
            let _ = self.sender.send(LiveSignal::Message {
                channel_id: channel_id.to_owned(),
                message: Box::new(message.clone()),
            });
        }
    }

    fn on_community_update(&self, channel_id: &str, _target_id: &str, message: &Message) {
        let _ = self.sender.send(LiveSignal::Update {
            channel_id: channel_id.to_owned(),
            message: Box::new(message.clone()),
        });
    }

    // `vector_sdk 0.11` intentionally does not expose whether a community
    // removal was an author delete or a moderator hide. The bridge defaults to
    // ignoring this ambiguous callback rather than deleting a mirror without
    // authenticated actor ownership.
    fn on_community_removed(&self, _channel_id: &str, _target_id: &str) {}

    fn on_community_self_removed(&self, community_id: &str) {
        let _ = self.sender.send(LiveSignal::Removed {
            community_id: community_id.to_owned(),
        });
    }

    fn on_channel_keyed(&self, community_id: &str, channel_id: &str, _backfilled: usize) {
        let _ = self.sender.send(LiveSignal::Keyed {
            community_id: community_id.to_owned(),
            channel_id: channel_id.to_owned(),
        });
    }

    fn on_community_refreshed(&self, community_id: &str) {
        let _ = self.sender.send(LiveSignal::Refresh {
            community_id: community_id.to_owned(),
        });
    }

    fn on_community_dissolved(&self, community_id: &str) {
        let _ = self.sender.send(LiveSignal::Dissolved {
            community_id: community_id.to_owned(),
        });
    }
}

fn normalize_snapshot(
    route: &ConcordRoute,
    channel_id: &str,
    is_group: bool,
    own_npub: &str,
    message: Message,
    display_name: Option<String>,
    mutation: Mutation,
) -> Result<Option<ConcordInboundMessage>, ConcordError> {
    if !is_group || channel_id != route.channel_id.as_str() || message.mine {
        return Ok(None);
    }
    let Some(author) = message.npub.clone().filter(|author| !author.is_empty()) else {
        return Ok(None);
    };
    if author == own_npub {
        return Ok(None);
    }
    if !is_event_id(&message.id) {
        return Err(ConcordError::InvalidEventId);
    }
    let event_at_ms = if mutation == Mutation::Edit {
        message
            .edit_history
            .as_ref()
            .and_then(|history| history.last())
            .map_or(message.at, |edit| edit.edited_at)
    } else {
        message.at
    };
    let timestamp_ms = i64::try_from(event_at_ms).map_err(|_| ConcordError::InvalidTimestamp)?;
    let cursor = vector_sdk::Cursor {
        at_ms: event_at_ms,
        id: message.id.clone(),
    }
    .to_string();
    let original_source_id = format!("armada:{}", message.id.to_ascii_lowercase());
    let source_event_id = if mutation == Mutation::Edit {
        format!(
            "armada:edit:{}:{timestamp_ms}",
            message.id.to_ascii_lowercase()
        )
    } else {
        original_source_id.clone()
    };
    let reply_to = (mutation == Mutation::Create && !message.replied_to.is_empty()).then(|| {
        crate::domain::SourceMessageRef {
            source: Source::Armada,
            source_event_id: format!("armada:{}", message.replied_to.to_ascii_lowercase()),
        }
    });
    let reply_quote = reply_to
        .as_ref()
        .and(message.replied_to_content.clone())
        .filter(|quote| !quote.is_empty());
    let mutation_target =
        (mutation != Mutation::Create).then_some(crate::domain::SourceMessageRef {
            source: Source::Armada,
            source_event_id: original_source_id,
        });
    let expires_at_ms = message
        .expiration
        .and_then(|seconds| seconds.checked_mul(1_000))
        .and_then(|milliseconds| i64::try_from(milliseconds).ok());
    let attachments = concord_attachments(&message, &author);
    Ok(Some(ConcordInboundMessage {
        event: BridgeEvent {
            source: Source::Armada,
            source_event_id,
            route_id: route.route_id.clone(),
            sender: SenderIdentity::Armada {
                npub: author,
                display_name,
            },
            timestamp_ms,
            body: message.content,
            rich_text: None,
            attachments,
            reply_to,
            reply_quote,
            mutation_target,
            mutation,
            expires_at_ms,
        },
        next_cursor: cursor,
    }))
}

fn concord_attachments(message: &Message, author: &str) -> Vec<AttachmentRef> {
    message
        .attachments
        .iter()
        .map(|attachment| AttachmentRef {
            source_id: attachment.id.clone(),
            file_name: (!attachment.name.is_empty()).then(|| attachment.name.clone()),
            media_type: media_type_from_extension(&attachment.extension).map(str::to_owned),
            size_bytes: if attachment.key.is_empty() {
                attachment.size
            } else {
                attachment.size.saturating_sub(16)
            },
            locator: AttachmentLocator::Armada {
                id: attachment.id.clone(),
                key: attachment.key.clone(),
                nonce: attachment.nonce.clone(),
                extension: attachment.extension.clone(),
                url: attachment.url.clone(),
                fallback_urls: attachment.fallback_urls.clone(),
                original_hash: attachment.original_hash.clone(),
                author_npub: author.to_owned(),
            },
            media_group_id: attachment.group_id.clone(),
            media_group_index: None,
            media_group_total: None,
        })
        .collect()
}

fn media_type_from_extension(extension: &str) -> Option<&'static str> {
    match extension.to_ascii_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "mp4" | "m4v" => Some("video/mp4"),
        "mp3" => Some("audio/mpeg"),
        "ogg" | "oga" | "opus" => Some("audio/ogg"),
        "pdf" => Some("application/pdf"),
        "zip" => Some("application/zip"),
        _ => None,
    }
}

fn validate_routes(routes: &[ConcordRoute]) -> Result<(), ConcordError> {
    if routes.is_empty() {
        return Err(ConcordError::InvalidConfiguration);
    }
    for (index, route) in routes.iter().enumerate() {
        if routes[..index].iter().any(|previous| {
            previous.route_id == route.route_id || previous.channel_id == route.channel_id
        }) {
            return Err(ConcordError::InvalidConfiguration);
        }
    }
    Ok(())
}

const fn channel_readiness(is_private: bool, is_readable: bool) -> ConcordReadiness {
    if !is_private {
        ConcordReadiness::NotPrivate
    } else if !is_readable {
        ConcordReadiness::MissingKey
    } else {
        ConcordReadiness::Ready
    }
}

fn first_nonempty(values: [String; 3]) -> Option<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
}

fn is_event_id(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMUNITY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CHANNEL: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const INNER_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn sdk_logging_is_forced_off_before_private_transport_use() {
        vector_sdk::vector_core::logging::set_log_level(
            vector_sdk::vector_core::logging::LEVEL_WARN,
        );
        silence_sdk_logging();
        assert_eq!(
            vector_sdk::vector_core::logging::log_level(),
            vector_sdk::vector_core::logging::LEVEL_OFF
        );
    }

    #[test]
    fn sdk_connect_errors_are_reduced_to_secret_free_categories() {
        use vector_sdk::VectorError;

        assert_eq!(
            classify_connect_error(&VectorError::Db("private path".to_owned())),
            ConcordConnectFailure::Database
        );
        assert_eq!(
            classify_connect_error(&VectorError::Other(
                "Failed to create profile directory /secret/location".to_owned()
            )),
            ConcordConnectFailure::Database
        );
        assert_eq!(
            classify_connect_error(&VectorError::Other(
                "Failed to create profile directory: permission denied at /secret/location"
                    .to_owned()
            )),
            ConcordConnectFailure::Io
        );
        let rendered = ConcordError::Connect {
            reason: ConcordConnectFailure::Database,
        }
        .to_string();
        assert_eq!(rendered, "connect Concord SDK failed: Database");
        assert!(!rendered.contains("secret"));
    }

    fn route() -> ConcordRoute {
        ConcordRoute::new(
            RouteId::try_from("alpha").expect("valid route"),
            CommunityId::try_from(COMMUNITY).expect("valid community"),
            ChannelId::try_from(CHANNEL).expect("valid channel"),
        )
    }

    fn message() -> Message {
        Message {
            id: INNER_ID.to_owned(),
            content: "hello".to_owned(),
            at: 1_234,
            npub: Some("npub1verifiedinner".to_owned()),
            wrapper_event_id: Some(
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            ),
            ..Message::default()
        }
    }

    #[test]
    fn exact_channel_uses_inner_id_and_verified_inner_author() {
        let normalized = normalize_snapshot(
            &route(),
            CHANNEL,
            true,
            "npub1bridge",
            message(),
            Some("Alice".to_owned()),
            Mutation::Create,
        )
        .expect("message should normalize")
        .expect("message should be relevant");

        assert_eq!(
            normalized.event.source_event_id,
            format!("armada:{INNER_ID}")
        );
        assert!(matches!(
            normalized.event.sender,
            SenderIdentity::Armada { npub, .. } if npub == "npub1verifiedinner"
        ));
        assert_eq!(normalized.next_cursor, format!("1234:{INNER_ID}"));
    }

    #[test]
    fn verified_sdk_attachments_become_redacted_durable_locators() {
        let mut source = message();
        source.attachments.push(vector_sdk::Attachment {
            id: "content-hash".to_owned(),
            key: "super-secret-key".to_owned(),
            nonce: "secret-nonce".to_owned(),
            extension: "PNG".to_owned(),
            name: "photo.png".to_owned(),
            url: "https://blossom.example/content-hash".to_owned(),
            size: 1_040,
            fallback_urls: vec!["https://mirror.example/content-hash".to_owned()],
            ..vector_sdk::Attachment::default()
        });
        let normalized = normalize_snapshot(
            &route(),
            CHANNEL,
            true,
            "npub1bridge",
            source,
            None,
            Mutation::Create,
        )
        .expect("attachment normalizes")
        .expect("message is relevant");

        let attachment = &normalized.event.attachments[0];
        assert_eq!(attachment.size_bytes, 1_024);
        assert_eq!(attachment.media_type.as_deref(), Some("image/png"));
        let debug = format!("{attachment:?}");
        assert!(!debug.contains("super-secret-key"));
        assert!(!debug.contains("secret-nonce"));
        assert!(!debug.contains("blossom.example"));
    }

    #[test]
    fn dms_other_channels_own_messages_and_missing_authors_are_rejected() {
        let route = route();
        assert!(
            normalize_snapshot(
                &route,
                "npub1dm",
                false,
                "npub1bridge",
                message(),
                None,
                Mutation::Create,
            )
            .expect("DM should be safely ignored")
            .is_none()
        );
        assert!(
            normalize_snapshot(
                &route,
                &"f".repeat(64),
                true,
                "npub1bridge",
                message(),
                None,
                Mutation::Create,
            )
            .expect("other channel should be safely ignored")
            .is_none()
        );
        let mut own = message();
        own.mine = true;
        assert!(
            normalize_snapshot(
                &route,
                CHANNEL,
                true,
                "npub1bridge",
                own,
                None,
                Mutation::Create,
            )
            .expect("own message should be safely ignored")
            .is_none()
        );
        let mut missing = message();
        missing.npub = None;
        assert!(
            normalize_snapshot(
                &route,
                CHANNEL,
                true,
                "npub1bridge",
                missing,
                None,
                Mutation::Create,
            )
            .expect("unattributed message should be safely ignored")
            .is_none()
        );
    }

    #[test]
    fn reconnect_normalization_keeps_the_same_namespaced_id_and_cursor() {
        let first = normalize_snapshot(
            &route(),
            CHANNEL,
            true,
            "npub1bridge",
            message(),
            None,
            Mutation::Create,
        )
        .expect("first normalization should work");
        let replay = normalize_snapshot(
            &route(),
            CHANNEL,
            true,
            "npub1bridge",
            message(),
            None,
            Mutation::Create,
        )
        .expect("replay normalization should work");
        assert_eq!(first, replay);
    }

    #[test]
    fn reaction_slots_filter_the_bridge_and_malformed_values_and_deduplicate() {
        let mut source = message();
        let reaction = |author_id: &str, emoji: &str| vector_sdk::Reaction {
            id: format!("{author_id}-{emoji}"),
            reference_id: INNER_ID.to_owned(),
            author_id: author_id.to_owned(),
            emoji: emoji.to_owned(),
            emoji_url: None,
        };
        source.reactions = vec![
            reaction("npub1alice", "+"),
            reaction("npub1alice", "+"),
            reaction("npub1bridge", "❤️"),
            reaction("npub1carol", "-"),
            reaction("npub1bob", ":party_parrot:"),
            reaction("npub1mallory", "\n"),
        ];

        assert_eq!(
            supported_reaction_slots(&source, "npub1bridge"),
            vec![
                ("npub1alice".to_owned(), "👍".to_owned()),
                ("npub1bob".to_owned(), ":party_parrot:".to_owned()),
                ("npub1carol".to_owned(), "👎".to_owned()),
            ]
        );
    }

    #[test]
    fn legacy_sdk_reaction_callback_enters_the_live_update_queue() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let handler = LiveHandler { sender };

        handler.on_reaction_received(CHANNEL, &message());

        assert!(matches!(
            receiver.try_recv().expect("reaction callback is forwarded"),
            LiveSignal::Update { channel_id, message }
                if channel_id == CHANNEL && message.id == INNER_ID
        ));
    }

    #[test]
    fn explicit_reply_edit_and_expiry_metadata_is_normalized() {
        let mut reply = message();
        reply.replied_to =
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned();
        reply.replied_to_content = Some("quoted".to_owned());
        reply.expiration = Some(9);
        let normalized = normalize_snapshot(
            &route(),
            CHANNEL,
            true,
            "npub1bridge",
            reply,
            None,
            Mutation::Create,
        )
        .expect("reply normalizes")
        .expect("reply is relevant");
        assert_eq!(
            normalized
                .event
                .reply_to
                .expect("reply target")
                .source_event_id,
            "armada:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        );
        assert_eq!(normalized.event.reply_quote.as_deref(), Some("quoted"));
        assert_eq!(normalized.event.expires_at_ms, Some(9_000));

        let mut edit = message();
        edit.apply_edit("fixed".to_owned(), 2_000, Vec::new());
        let normalized = normalize_snapshot(
            &route(),
            CHANNEL,
            true,
            "npub1bridge",
            edit,
            None,
            Mutation::Edit,
        )
        .expect("edit normalizes")
        .expect("edit is relevant");
        assert_eq!(normalized.event.mutation, Mutation::Edit);
        assert_eq!(normalized.event.body, "fixed");
        assert_eq!(normalized.event.timestamp_ms, 2_000);
        assert_eq!(normalized.next_cursor, format!("2000:{INNER_ID}"));
        assert_eq!(
            normalized
                .event
                .mutation_target
                .expect("mutation target")
                .source_event_id,
            format!("armada:{INNER_ID}")
        );
    }

    #[test]
    fn explicit_readability_is_the_private_channel_key_signal() {
        assert_eq!(channel_readiness(true, true), ConcordReadiness::Ready);
        assert_eq!(channel_readiness(true, false), ConcordReadiness::MissingKey);
        assert_eq!(channel_readiness(false, true), ConcordReadiness::NotPrivate);
    }
}
