//! Narrow Telegram Bot API adapter and transport-neutral port contract.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use crate::attachments::{InspectedAttachment, validate_declared_size};
use crate::config::Route;
use crate::domain::{
    AttachmentLocator, AttachmentRef, BridgeEvent, BridgeReactionUpdate, Mutation, ReactionChange,
    ReactionEntry, RouteId, SenderIdentity, Source, TelegramChatId, TelegramThreadId,
};
use crate::error::Sensitive;
use crate::rich_text::TelegramMessage;
use crate::rich_text::{TelegramEntity, TelegramEntityKind, from_telegram};

/// Telegram's hosted Bot API text-message limit after entity parsing.
pub const MAX_TEXT_CHARACTERS: usize = 4_096;
/// Telegram's hosted Bot API caption limit after entity parsing.
pub const MAX_CAPTION_CHARACTERS: usize = 1_024;
const DEFAULT_LONG_POLL_SECONDS: u64 = 30;
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 40;
const DEFAULT_API_BASE: &str = "https://api.telegram.org";

/// Telegram-specific projection of one configured static route.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramRoute {
    route_id: RouteId,
    chat_id: TelegramChatId,
    thread_id: Option<TelegramThreadId>,
}

impl TelegramRoute {
    /// Creates an exact Telegram group or group/topic route.
    #[must_use]
    pub const fn new(
        route_id: RouteId,
        chat_id: TelegramChatId,
        thread_id: Option<TelegramThreadId>,
    ) -> Self {
        Self {
            route_id,
            chat_id,
            thread_id,
        }
    }

    /// Stable route identifier.
    #[must_use]
    pub const fn route_id(&self) -> &RouteId {
        &self.route_id
    }

    /// Exact numeric group or supergroup identifier.
    #[must_use]
    pub const fn chat_id(&self) -> TelegramChatId {
        self.chat_id
    }

    /// Optional exact forum topic identifier.
    #[must_use]
    pub const fn thread_id(&self) -> Option<TelegramThreadId> {
        self.thread_id
    }
}

impl From<&Route> for TelegramRoute {
    fn from(route: &Route) -> Self {
        Self::new(
            route.id().clone(),
            route.telegram_chat_id(),
            route.telegram_thread_id(),
        )
    }
}

/// Identity returned by Telegram's `getMe` method.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramBotIdentity {
    /// Stable numeric bot ID.
    pub id: u64,
    /// Mutable public username, if present.
    pub username: Option<String>,
}

/// Normalized inbound event and the offset that must commit with it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundTelegramMessage {
    /// Transport-neutral message.
    pub event: BridgeEvent,
    /// First update ID not covered by this durable acceptance.
    pub next_offset: u64,
}

/// Summary of one bounded long-poll response.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PollReport {
    /// Updates returned by Telegram.
    pub observed: usize,
    /// Relevant messages durably accepted by the sink.
    pub accepted: usize,
    /// Irrelevant updates safely skipped.
    pub ignored: usize,
    /// Next offset after all successfully handled updates.
    pub next_offset: Option<u64>,
}

/// Canonical receipt for one Telegram send.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramSendReceipt {
    /// Telegram message ID within the destination chat.
    pub message_id: i64,
    /// Transport-namespaced destination identifier for message links.
    pub destination_message_id: String,
}

/// Read-only confirmation that the bot can resolve a configured chat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TelegramChatAccess {
    /// Exact configured group or supergroup ID returned by Telegram.
    pub chat_id: i64,
}

/// Validated caption text reserved for P09 attachment sends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptionText(String);

impl CaptionText {
    /// Returns the validated caption.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CaptionText {
    type Error = TelegramError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_length(&value, MAX_CAPTION_CHARACTERS, false)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for CaptionText {
    type Error = TelegramError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_owned())
    }
}

/// Deliberately detail-free durable-intake rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TelegramAcceptError;

impl Display for TelegramAcceptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("durable Telegram intake rejected the update")
    }
}

impl Error for TelegramAcceptError {}

/// Durable consumer called before a relevant update can be acknowledged.
#[async_trait]
pub trait TelegramUpdateSink: Send {
    /// Atomically accepts the event and `next_offset` into durable state.
    async fn accept(&mut self, message: InboundTelegramMessage) -> Result<(), TelegramAcceptError>;

    /// Atomically accepts a reaction change and `next_offset` into durable state.
    async fn accept_reaction(
        &mut self,
        _update: BridgeReactionUpdate,
        _next_offset: u64,
    ) -> Result<(), TelegramAcceptError> {
        Ok(())
    }
}

/// Transport contract used by bridge orchestration.
#[async_trait]
pub trait TelegramPort: Send {
    /// Seeds polling from the last offset committed with durable intake.
    fn set_next_offset(&mut self, next_offset: Option<u64>);

    /// Returns the current in-process polling offset.
    fn next_offset(&self) -> Option<u64>;

    /// Verifies the remote bot against the operator-reviewed numeric ID.
    async fn verify_identity(&mut self) -> Result<TelegramBotIdentity, TelegramError>;

    /// Confirms read-only access to the exact configured numeric chat.
    async fn verify_route_access(
        &mut self,
        route_id: &RouteId,
    ) -> Result<TelegramChatAccess, TelegramError>;

    /// Runs one bounded long poll and durably hands off relevant updates.
    async fn poll_once(
        &mut self,
        sink: &mut dyn TelegramUpdateSink,
    ) -> Result<PollReport, TelegramError>;

    /// Sends one already-rendered plaintext message to a configured route.
    async fn send_text(
        &mut self,
        route_id: &RouteId,
        text: &str,
    ) -> Result<TelegramSendReceipt, TelegramError> {
        self.send_rich_text(route_id, text, &[]).await
    }

    /// Sends one message with explicit Bot API UTF-16 entities.
    async fn send_rich_text(
        &mut self,
        route_id: &RouteId,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<TelegramSendReceipt, TelegramError>;

    /// Sends a native reply to one bridge-owned mapped Telegram message.
    async fn send_reply(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<TelegramSendReceipt, TelegramError>;

    /// Replaces the text of one bridge-owned mapped Telegram message.
    async fn edit_message(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<(), TelegramError>;

    /// Deletes one bridge-owned mapped Telegram message idempotently.
    async fn delete_message(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
    ) -> Result<(), TelegramError>;

    /// Streams one Telegram attachment into an already-private staging path.
    async fn download_attachment(
        &mut self,
        attachment: &AttachmentRef,
        destination: &Path,
        maximum_bytes: u64,
    ) -> Result<(), TelegramError>;

    /// Uploads one staged attachment, falling back to `sendDocument` when a
    /// specialized Telegram media method rejects otherwise-safe content.
    async fn send_attachment(
        &mut self,
        route_id: &RouteId,
        path: &Path,
        metadata: &InspectedAttachment,
        caption: Option<&TelegramMessage>,
    ) -> Result<TelegramSendReceipt, TelegramError>;
}

/// Direct HTTPS Bot API implementation of [`TelegramPort`].
pub struct TelegramAdapter {
    client: Client,
    api_base: String,
    token: Sensitive,
    expected_bot_id: u64,
    protect_content: bool,
    relay_other_bots: bool,
    routes: Vec<TelegramRoute>,
    paused_routes: HashSet<RouteId>,
    next_offset: Option<u64>,
}

impl TelegramAdapter {
    async fn send_rich_text_internal(
        &self,
        route_id: &RouteId,
        text: &str,
        entities: &[TelegramEntity],
        reply_to: Option<&str>,
    ) -> Result<TelegramSendReceipt, TelegramError> {
        validate_length(text, MAX_TEXT_CHARACTERS, true)?;
        from_telegram(text, entities).map_err(|_| TelegramError::InvalidEntities)?;
        if self.paused_routes.contains(route_id) {
            return Err(TelegramError::RoutePaused {
                route_id: route_id.clone(),
            });
        }
        let route = self
            .routes
            .iter()
            .find(|route| &route.route_id == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        let mut body = json!({
            "chat_id": route.chat_id.get(),
            "text": text,
            "protect_content": self.protect_content
        });
        if !entities.is_empty() {
            body["entities"] = json!(entities);
        }
        if let Some(thread_id) = route.thread_id {
            body["message_thread_id"] = json!(thread_id.get());
        }
        if let Some(destination) = reply_to {
            let message_id = telegram_message_id(route, destination)?;
            body["reply_parameters"] = json!({"message_id": message_id});
        }
        let sent: RawSentMessage = self.call("sendMessage", &body).await?;
        if sent.message_id <= 0 || sent.chat.id != route.chat_id.get() {
            return Err(TelegramError::MalformedResponse);
        }
        Ok(TelegramSendReceipt {
            message_id: sent.message_id,
            destination_message_id: format!("telegram:{}:{}", route.chat_id.get(), sent.message_id),
        })
    }

    /// Creates an adapter for Telegram's hosted Bot API.
    ///
    /// # Errors
    ///
    /// Fails if routes overlap or the HTTP client cannot be configured.
    pub fn new(
        token: &str,
        expected_bot_id: u64,
        protect_content: bool,
        relay_other_bots: bool,
        routes: Vec<TelegramRoute>,
    ) -> Result<Self, TelegramError> {
        Self::with_api_base(
            DEFAULT_API_BASE,
            token,
            expected_bot_id,
            protect_content,
            relay_other_bots,
            routes,
            Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECONDS),
        )
    }

    /// Creates an adapter against an alternate Bot API endpoint.
    ///
    /// This is public to support deterministic local protocol tests and a
    /// future local Bot API server deployment.
    ///
    /// # Errors
    ///
    /// Fails for a non-HTTP(S) base URL, overlapping routes, zero timeout, or
    /// HTTP client configuration failure.
    #[allow(clippy::too_many_arguments)]
    pub fn with_api_base(
        api_base: &str,
        token: &str,
        expected_bot_id: u64,
        protect_content: bool,
        relay_other_bots: bool,
        routes: Vec<TelegramRoute>,
        request_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        validate_routes(&routes)?;
        if request_timeout.is_zero() {
            return Err(TelegramError::InvalidConfiguration);
        }
        let parsed =
            reqwest::Url::parse(api_base).map_err(|_| TelegramError::InvalidConfiguration)?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(TelegramError::InvalidConfiguration);
        }
        let client = Client::builder()
            .timeout(request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| TelegramError::InvalidConfiguration)?;
        Ok(Self {
            client,
            api_base: api_base.trim_end_matches('/').to_owned(),
            token: Sensitive::new(token.to_owned()),
            expected_bot_id,
            protect_content,
            relay_other_bots,
            routes,
            paused_routes: HashSet::new(),
            next_offset: None,
        })
    }

    /// Seeds polling from a previously committed durable offset.
    pub const fn set_next_offset(&mut self, next_offset: Option<u64>) {
        self.next_offset = next_offset;
    }

    /// Current in-process offset, advanced only after safe handling.
    #[must_use]
    pub const fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    /// Whether migration detection has stopped one route.
    #[must_use]
    pub fn route_is_paused(&self, route_id: &RouteId) -> bool {
        self.paused_routes.contains(route_id)
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &'static str,
        body: &Value,
    ) -> Result<T, TelegramError> {
        let url = format!("{}/bot{}/{}", self.api_base, self.token.expose(), method);
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|error| classify_transport_error(&error))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| classify_transport_error(&error))?;

        if mutation_already_applied(method, status, &bytes) {
            return Err(TelegramError::MutationAlreadyApplied);
        }
        if !status.is_success() {
            return Err(classify_status(status, &bytes));
        }
        let envelope: ApiEnvelope<T> =
            serde_json::from_slice(&bytes).map_err(|_| TelegramError::MalformedResponse)?;
        if !envelope.ok {
            return Err(classify_api_error(envelope.error_code, envelope.parameters));
        }
        envelope.result.ok_or(TelegramError::MalformedResponse)
    }

    async fn call_multipart<T: DeserializeOwned>(
        &self,
        method: &'static str,
        form: Form,
    ) -> Result<T, TelegramError> {
        let url = format!("{}/bot{}/{}", self.api_base, self.token.expose(), method);
        let response = self
            .client
            .post(url)
            .multipart(form)
            .send()
            .await
            .map_err(|error| classify_transport_error(&error))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| classify_transport_error(&error))?;
        if !status.is_success() {
            return Err(classify_status(status, &bytes));
        }
        let envelope: ApiEnvelope<T> =
            serde_json::from_slice(&bytes).map_err(|_| TelegramError::MalformedResponse)?;
        if !envelope.ok {
            return Err(classify_api_error(envelope.error_code, envelope.parameters));
        }
        envelope.result.ok_or(TelegramError::MalformedResponse)
    }

    fn normalize_update(
        &self,
        raw: RawUpdate,
        album_position: Option<(u16, u16)>,
    ) -> Result<NormalizedUpdate, TelegramError> {
        let update_id = raw.update_id;
        if let Some(reaction) = raw.message_reaction {
            return self.normalize_reaction_update(update_id, reaction);
        }
        if let Some(reaction) = raw.message_reaction_count {
            return self.normalize_reaction_count_update(update_id, reaction);
        }
        let (message, mutation) = match (raw.message, raw.edited_message) {
            (Some(message), _) => (message, Mutation::Create),
            (None, Some(message)) => (message, Mutation::Edit),
            (None, None) => return Ok(NormalizedUpdate::Ignored { id: update_id }),
        };

        if let Some(new_chat_id) = message.migrate_to_chat_id {
            let route_ids = self.routes_for_chat(message.chat.id);
            if !route_ids.is_empty() {
                return Ok(NormalizedUpdate::Migration {
                    route_ids,
                    old_chat_id: message.chat.id,
                    new_chat_id,
                });
            }
        }
        if let Some(old_chat_id) = message.migrate_from_chat_id {
            let route_ids = self.routes_for_chat(old_chat_id);
            if !route_ids.is_empty() {
                return Ok(NormalizedUpdate::Migration {
                    route_ids,
                    old_chat_id,
                    new_chat_id: message.chat.id,
                });
            }
        }

        let Some(route) = self.routes.iter().find(|route| {
            route.chat_id.get() == message.chat.id
                && route
                    .thread_id
                    .is_none_or(|thread| Some(u64::from(thread.get())) == message.message_thread_id)
        }) else {
            return Ok(NormalizedUpdate::Ignored { id: update_id });
        };
        if self.paused_routes.contains(&route.route_id) {
            return Ok(NormalizedUpdate::Paused {
                route_id: route.route_id.clone(),
            });
        }

        let references = telegram_event_references(&message, mutation, update_id)?;

        let attachments = telegram_attachments(&message, album_position);
        let (body, raw_entities) = match (message.text, message.caption) {
            (Some(text), _) => (text, message.entities.unwrap_or_default()),
            (None, Some(caption)) => (caption, message.caption_entities.unwrap_or_default()),
            (None, None) if !attachments.is_empty() => (String::new(), Vec::new()),
            (None, None) => return Ok(NormalizedUpdate::Ignored { id: update_id }),
        };
        let entities = raw_entities
            .into_iter()
            .filter_map(RawMessageEntity::into_supported)
            .collect::<Vec<_>>();
        let rich_text =
            from_telegram(&body, &entities).map_err(|_| TelegramError::MalformedResponse)?;
        let Some(sender) = sender_identity(
            message.from,
            message.sender_chat,
            self.expected_bot_id,
            self.relay_other_bots,
        ) else {
            return Ok(NormalizedUpdate::Ignored { id: update_id });
        };
        Ok(NormalizedUpdate::Message(Box::new(
            InboundTelegramMessage {
                event: BridgeEvent {
                    source: Source::Telegram,
                    source_event_id: references.source_event_id,
                    route_id: route.route_id.clone(),
                    sender,
                    timestamp_ms: references.timestamp_ms,
                    body: rich_text.text.clone(),
                    rich_text: Some(rich_text),
                    attachments,
                    reply_to: references.reply_to,
                    reply_quote: references.reply_quote,
                    mutation_target: references.mutation_target,
                    mutation,
                    expires_at_ms: None,
                },
                next_offset: references.next_offset,
            },
        )))
    }

    fn normalize_reaction_update(
        &self,
        update_id: u64,
        reaction: RawMessageReaction,
    ) -> Result<NormalizedUpdate, TelegramError> {
        let Some(route) = self.routes.iter().find(|route| {
            route.chat_id.get() == reaction.chat.id
                && route.thread_id.is_none_or(|thread| {
                    Some(u64::from(thread.get())) == reaction.message_thread_id
                })
        }) else {
            return Ok(NormalizedUpdate::Ignored { id: update_id });
        };
        if self.paused_routes.contains(&route.route_id) {
            return Ok(NormalizedUpdate::Paused {
                route_id: route.route_id.clone(),
            });
        }
        let Some((reactor_id, display_name)) = reaction_actor(reaction.user, reaction.actor_chat)
        else {
            return Ok(NormalizedUpdate::Ignored { id: update_id });
        };
        if reactor_id == format!("telegram-user:{}", self.expected_bot_id) {
            return Ok(NormalizedUpdate::Ignored { id: update_id });
        }
        let emojis = reaction
            .new_reaction
            .into_iter()
            .filter_map(RawReactionType::unicode_emoji)
            .collect();
        Ok(NormalizedUpdate::Reaction {
            update: BridgeReactionUpdate {
                source: Source::Telegram,
                source_event_id: format!("telegram:reaction:{update_id}"),
                route_id: route.route_id.clone(),
                target_source_event_id: format!(
                    "telegram:{}:{}",
                    reaction.chat.id, reaction.message_id
                ),
                change: ReactionChange::ActorSelection {
                    reactor_id,
                    display_name,
                    emojis,
                },
            },
            next_offset: update_id
                .checked_add(1)
                .ok_or(TelegramError::MalformedResponse)?,
        })
    }

    fn normalize_reaction_count_update(
        &self,
        update_id: u64,
        reaction: RawMessageReactionCount,
    ) -> Result<NormalizedUpdate, TelegramError> {
        let Some(route) = self.routes.iter().find(|route| {
            route.chat_id.get() == reaction.chat.id
                && route.thread_id.is_none_or(|thread| {
                    Some(u64::from(thread.get())) == reaction.message_thread_id
                })
        }) else {
            return Ok(NormalizedUpdate::Ignored { id: update_id });
        };
        if self.paused_routes.contains(&route.route_id) {
            return Ok(NormalizedUpdate::Paused {
                route_id: route.route_id.clone(),
            });
        }
        let entries = reaction
            .reactions
            .into_iter()
            .filter_map(|reaction| {
                let emoji = reaction.kind.unicode_emoji()?;
                (reaction.total_count > 0).then(|| ReactionEntry {
                    reactor_id: format!("telegram-anonymous:{emoji}"),
                    display_name: None,
                    emoji,
                    count: reaction.total_count,
                })
            })
            .collect();
        Ok(NormalizedUpdate::Reaction {
            update: BridgeReactionUpdate {
                source: Source::Telegram,
                source_event_id: format!("telegram:reaction-count:{update_id}"),
                route_id: route.route_id.clone(),
                target_source_event_id: format!(
                    "telegram:{}:{}",
                    reaction.chat.id, reaction.message_id
                ),
                change: ReactionChange::Snapshot { entries },
            },
            next_offset: update_id
                .checked_add(1)
                .ok_or(TelegramError::MalformedResponse)?,
        })
    }

    fn routes_for_chat(&self, chat_id: i64) -> Vec<RouteId> {
        self.routes
            .iter()
            .filter(|route| route.chat_id.get() == chat_id)
            .map(|route| route.route_id.clone())
            .collect()
    }

    fn advance_past(&mut self, update_id: u64) -> Result<(), TelegramError> {
        let next = update_id
            .checked_add(1)
            .ok_or(TelegramError::MalformedResponse)?;
        self.next_offset = Some(self.next_offset.map_or(next, |current| current.max(next)));
        Ok(())
    }

    async fn upload_attachment_once(
        &self,
        route: &TelegramRoute,
        method: &'static str,
        file_field: &'static str,
        path: &Path,
        metadata: &InspectedAttachment,
        caption: Option<&TelegramMessage>,
    ) -> Result<TelegramSendReceipt, TelegramError> {
        let part = Part::file(path)
            .await
            .map_err(|_| TelegramError::TemporaryFile)?
            .file_name(metadata.file_name.clone())
            .mime_str(&metadata.media_type)
            .map_err(|_| TelegramError::InvalidAttachment)?;
        let mut form = Form::new()
            .text("chat_id", route.chat_id.get().to_string())
            .text("protect_content", self.protect_content.to_string())
            .part(file_field, part);
        if let Some(thread_id) = route.thread_id {
            form = form.text("message_thread_id", thread_id.get().to_string());
        }
        if let Some(caption) = caption {
            validate_length(&caption.text, MAX_CAPTION_CHARACTERS, false)?;
            from_telegram(&caption.text, &caption.entities)
                .map_err(|_| TelegramError::InvalidEntities)?;
            form = form.text("caption", caption.text.clone());
            if !caption.entities.is_empty() {
                form = form.text(
                    "caption_entities",
                    serde_json::to_string(&caption.entities)
                        .map_err(|_| TelegramError::InvalidEntities)?,
                );
            }
        }
        let sent: RawSentMessage = self.call_multipart(method, form).await?;
        if sent.message_id <= 0 || sent.chat.id != route.chat_id.get() {
            return Err(TelegramError::MalformedResponse);
        }
        Ok(TelegramSendReceipt {
            message_id: sent.message_id,
            destination_message_id: format!("telegram:{}:{}", route.chat_id.get(), sent.message_id),
        })
    }
}

#[async_trait]
impl TelegramPort for TelegramAdapter {
    fn set_next_offset(&mut self, next_offset: Option<u64>) {
        self.next_offset = next_offset;
    }

    fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    async fn verify_identity(&mut self) -> Result<TelegramBotIdentity, TelegramError> {
        let user: RawUser = self.call("getMe", &json!({})).await?;
        if !user.is_bot {
            return Err(TelegramError::MalformedResponse);
        }
        if user.id != self.expected_bot_id {
            return Err(TelegramError::IdentityMismatch {
                expected: self.expected_bot_id,
                actual: user.id,
            });
        }
        Ok(TelegramBotIdentity {
            id: user.id,
            username: user.username,
        })
    }

    async fn verify_route_access(
        &mut self,
        route_id: &RouteId,
    ) -> Result<TelegramChatAccess, TelegramError> {
        let route = self
            .routes
            .iter()
            .find(|route| &route.route_id == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        if self.paused_routes.contains(route_id) {
            return Err(TelegramError::RoutePaused {
                route_id: route_id.clone(),
            });
        }
        let chat: RawChat = self
            .call("getChat", &json!({"chat_id": route.chat_id.get()}))
            .await?;
        if chat.id != route.chat_id.get() {
            return Err(TelegramError::MalformedResponse);
        }
        Ok(TelegramChatAccess { chat_id: chat.id })
    }

    async fn poll_once(
        &mut self,
        sink: &mut dyn TelegramUpdateSink,
    ) -> Result<PollReport, TelegramError> {
        let mut request = json!({
            "timeout": DEFAULT_LONG_POLL_SECONDS,
            "limit": 100,
            "allowed_updates": [
                "message",
                "edited_message",
                "message_reaction",
                "message_reaction_count"
            ]
        });
        if let Some(offset) = self.next_offset {
            request["offset"] = json!(offset);
        }
        let updates: Vec<RawUpdate> = self.call("getUpdates", &request).await?;
        if updates
            .windows(2)
            .any(|pair| pair[0].update_id >= pair[1].update_id)
        {
            return Err(TelegramError::MalformedResponse);
        }
        let mut report = PollReport {
            observed: updates.len(),
            ..PollReport::default()
        };
        let album_positions = album_positions(&updates);
        for update in updates {
            let album_position = album_positions.get(&update.update_id).copied();
            match self.normalize_update(update, album_position)? {
                NormalizedUpdate::Ignored { id } => {
                    self.advance_past(id)?;
                    report.ignored += 1;
                }
                NormalizedUpdate::Message(message) => {
                    let next_offset = message.next_offset;
                    sink.accept(*message)
                        .await
                        .map_err(|_| TelegramError::AcceptanceFailed)?;
                    self.next_offset = Some(
                        self.next_offset
                            .map_or(next_offset, |current| current.max(next_offset)),
                    );
                    report.accepted += 1;
                }
                NormalizedUpdate::Reaction {
                    update,
                    next_offset,
                } => {
                    sink.accept_reaction(update, next_offset)
                        .await
                        .map_err(|_| TelegramError::AcceptanceFailed)?;
                    self.next_offset = Some(
                        self.next_offset
                            .map_or(next_offset, |current| current.max(next_offset)),
                    );
                    report.accepted += 1;
                }
                NormalizedUpdate::Migration {
                    route_ids,
                    old_chat_id,
                    new_chat_id,
                    ..
                } => {
                    self.paused_routes.extend(route_ids.iter().cloned());
                    return Err(TelegramError::ChatMigrated {
                        route_ids,
                        old_chat_id,
                        new_chat_id,
                    });
                }
                NormalizedUpdate::Paused { route_id } => {
                    return Err(TelegramError::RoutePaused { route_id });
                }
            }
        }
        report.next_offset = self.next_offset;
        Ok(report)
    }

    async fn send_rich_text(
        &mut self,
        route_id: &RouteId,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<TelegramSendReceipt, TelegramError> {
        self.send_rich_text_internal(route_id, text, entities, None)
            .await
    }

    async fn send_reply(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<TelegramSendReceipt, TelegramError> {
        self.send_rich_text_internal(route_id, text, entities, Some(destination_message_id))
            .await
    }

    async fn edit_message(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<(), TelegramError> {
        validate_length(text, MAX_TEXT_CHARACTERS, true)?;
        from_telegram(text, entities).map_err(|_| TelegramError::InvalidEntities)?;
        let route = self
            .routes
            .iter()
            .find(|route| &route.route_id == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        let message_id = telegram_message_id(route, destination_message_id)?;
        let mut body = json!({
            "chat_id": route.chat_id.get(),
            "message_id": message_id,
            "text": text
        });
        if !entities.is_empty() {
            body["entities"] = json!(entities);
        }
        match self.call::<RawSentMessage>("editMessageText", &body).await {
            Ok(_) | Err(TelegramError::MutationAlreadyApplied) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn delete_message(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
    ) -> Result<(), TelegramError> {
        let route = self
            .routes
            .iter()
            .find(|route| &route.route_id == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        let message_id = telegram_message_id(route, destination_message_id)?;
        let body = json!({"chat_id": route.chat_id.get(), "message_id": message_id});
        match self.call::<bool>("deleteMessage", &body).await {
            Ok(true) | Err(TelegramError::MutationAlreadyApplied) => Ok(()),
            Ok(false) => Err(TelegramError::MalformedResponse),
            Err(error) => Err(error),
        }
    }

    async fn download_attachment(
        &mut self,
        attachment: &AttachmentRef,
        destination: &Path,
        maximum_bytes: u64,
    ) -> Result<(), TelegramError> {
        validate_declared_size(attachment.size_bytes, maximum_bytes)
            .map_err(|_| TelegramError::AttachmentTooLarge)?;
        let AttachmentLocator::Telegram { file_id } = &attachment.locator else {
            return Err(TelegramError::InvalidAttachment);
        };
        let remote: RawRemoteFile = self.call("getFile", &json!({"file_id": file_id})).await?;
        if let Some(size) = remote.file_size {
            validate_declared_size(size, maximum_bytes)
                .map_err(|_| TelegramError::AttachmentTooLarge)?;
        }
        let file_path = remote.file_path.ok_or(TelegramError::MalformedResponse)?;
        if file_path.is_empty()
            || file_path.starts_with('/')
            || file_path.contains("..")
            || file_path.contains(['\\', '?', '#'])
        {
            return Err(TelegramError::MalformedResponse);
        }
        let url = format!(
            "{}/file/bot{}/{}",
            self.api_base,
            self.token.expose(),
            file_path
        );
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|error| classify_transport_error(&error))?;
        let status = response.status();
        if !status.is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|error| classify_transport_error(&error))?;
            return Err(classify_status(status, &bytes));
        }
        if response
            .content_length()
            .is_some_and(|length| length > maximum_bytes)
        {
            return Err(TelegramError::AttachmentTooLarge);
        }
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(destination)
            .await
            .map_err(|_| TelegramError::TemporaryFile)?;
        let mut written = 0_u64;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| classify_transport_error(&error))?
        {
            written = written
                .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
                .ok_or(TelegramError::AttachmentTooLarge)?;
            if written > maximum_bytes {
                return Err(TelegramError::AttachmentTooLarge);
            }
            file.write_all(&chunk)
                .await
                .map_err(|_| TelegramError::TemporaryFile)?;
        }
        file.flush().await.map_err(|_| TelegramError::TemporaryFile)
    }

    async fn send_attachment(
        &mut self,
        route_id: &RouteId,
        path: &Path,
        metadata: &InspectedAttachment,
        caption: Option<&TelegramMessage>,
    ) -> Result<TelegramSendReceipt, TelegramError> {
        if self.paused_routes.contains(route_id) {
            return Err(TelegramError::RoutePaused {
                route_id: route_id.clone(),
            });
        }
        let route = self
            .routes
            .iter()
            .find(|route| &route.route_id == route_id)
            .cloned()
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        let (method, field) = telegram_media_method(&metadata.media_type);
        let result = self
            .upload_attachment_once(&route, method, field, path, metadata, caption)
            .await;
        if method != "sendDocument"
            && matches!(result, Err(TelegramError::Terminal { status: 400 }))
        {
            return self
                .upload_attachment_once(&route, "sendDocument", "document", path, metadata, caption)
                .await;
        }
        result
    }
}

/// Secret-safe Telegram adapter failure classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelegramError {
    /// Static adapter settings are unusable or routes overlap.
    InvalidConfiguration,
    /// `getMe` returned a different numeric bot identity.
    IdentityMismatch {
        /// Operator-reviewed bot ID.
        expected: u64,
        /// Bot ID returned by Telegram.
        actual: u64,
    },
    /// Telegram requested an explicit delay.
    RateLimited {
        /// Required delay from `parameters.retry_after`.
        retry_after_seconds: u64,
    },
    /// A server-side HTTP failure can be retried.
    Retryable {
        /// HTTP or Bot API status.
        status: u16,
    },
    /// A client-side Bot API failure is terminal without intervention.
    Terminal {
        /// HTTP or Bot API status.
        status: u16,
    },
    /// The configured request deadline elapsed.
    Timeout,
    /// A transport failure occurred before a classified Bot API response.
    Transport,
    /// Telegram returned syntactically or structurally invalid data.
    MalformedResponse,
    /// Outbound entities do not align to the supplied text.
    InvalidEntities,
    /// The durable consumer rejected a relevant update.
    AcceptanceFailed,
    /// A configured chat migrated and every affected route was paused.
    ChatMigrated {
        /// All routes using the old numeric chat ID.
        route_ids: Vec<RouteId>,
        /// Configured group ID.
        old_chat_id: i64,
        /// Newly reported supergroup ID; not applied automatically.
        new_chat_id: i64,
    },
    /// The route has been paused after a safety event.
    RoutePaused {
        /// Paused route.
        route_id: RouteId,
    },
    /// No configured route has this stable ID.
    UnknownRoute {
        /// Requested route.
        route_id: RouteId,
    },
    /// Outbound text exceeds the Telegram limit.
    TextTooLong {
        /// Actual Unicode scalar count.
        actual: usize,
        /// Inclusive supported maximum.
        maximum: usize,
    },
    /// Outbound caption exceeds the Telegram limit.
    CaptionTooLong {
        /// Actual Unicode scalar count.
        actual: usize,
        /// Inclusive supported maximum.
        maximum: usize,
    },
    /// A declared or streamed file exceeds the configured limit.
    AttachmentTooLarge,
    /// Attachment metadata or media construction is invalid.
    InvalidAttachment,
    /// A durable destination ID does not belong to the configured route.
    InvalidMessageReference,
    /// Telegram confirms the mutation was already applied or its target is gone.
    MutationAlreadyApplied,
    /// Private temporary file access failed.
    TemporaryFile,
}

impl Display for TelegramError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("invalid Telegram adapter configuration")
            }
            Self::IdentityMismatch { expected, actual } => write!(
                formatter,
                "Telegram bot identity mismatch: expected {expected}, received {actual}"
            ),
            Self::RateLimited {
                retry_after_seconds,
            } => write!(
                formatter,
                "Telegram rate limited the request; retry after {retry_after_seconds} seconds"
            ),
            Self::Retryable { status } => {
                write!(formatter, "retryable Telegram failure ({status})")
            }
            Self::Terminal { status } => {
                write!(formatter, "terminal Telegram failure ({status})")
            }
            Self::Timeout => formatter.write_str("Telegram request timed out"),
            Self::Transport => formatter.write_str("Telegram transport failed"),
            Self::MalformedResponse => {
                formatter.write_str("Telegram returned a malformed response")
            }
            Self::InvalidEntities => formatter.write_str("invalid Telegram formatting entities"),
            Self::AcceptanceFailed => formatter.write_str("durable Telegram intake failed"),
            Self::ChatMigrated {
                old_chat_id,
                new_chat_id,
                ..
            } => write!(
                formatter,
                "Telegram chat migrated from {old_chat_id} to {new_chat_id}; affected routes are paused"
            ),
            Self::RoutePaused { route_id } => {
                write!(formatter, "Telegram route {route_id} is paused")
            }
            Self::UnknownRoute { route_id } => {
                write!(formatter, "unknown Telegram route {route_id}")
            }
            Self::TextTooLong { actual, maximum } => write!(
                formatter,
                "Telegram text has {actual} characters; maximum is {maximum}"
            ),
            Self::CaptionTooLong { actual, maximum } => write!(
                formatter,
                "Telegram caption has {actual} characters; maximum is {maximum}"
            ),
            Self::AttachmentTooLarge => {
                formatter.write_str("Telegram attachment exceeds the configured limit")
            }
            Self::InvalidAttachment => formatter.write_str("invalid Telegram attachment"),
            Self::InvalidMessageReference => {
                formatter.write_str("invalid Telegram message reference")
            }
            Self::MutationAlreadyApplied => {
                formatter.write_str("Telegram mutation already applied")
            }
            Self::TemporaryFile => formatter.write_str("Telegram attachment staging failed"),
        }
    }
}

impl Error for TelegramError {}

fn validate_routes(routes: &[TelegramRoute]) -> Result<(), TelegramError> {
    for (index, route) in routes.iter().enumerate() {
        if routes[..index].iter().any(|previous| {
            previous.route_id == route.route_id
                || (previous.chat_id == route.chat_id
                    && (previous.thread_id == route.thread_id
                        || previous.thread_id.is_none()
                        || route.thread_id.is_none()))
        }) {
            return Err(TelegramError::InvalidConfiguration);
        }
    }
    Ok(())
}

fn validate_length(text: &str, maximum: usize, is_message: bool) -> Result<(), TelegramError> {
    let actual = text.chars().count();
    if actual <= maximum {
        return Ok(());
    }
    if is_message {
        Err(TelegramError::TextTooLong { actual, maximum })
    } else {
        Err(TelegramError::CaptionTooLong { actual, maximum })
    }
}

fn telegram_message_id(
    route: &TelegramRoute,
    destination_message_id: &str,
) -> Result<i64, TelegramError> {
    let mut parts = destination_message_id.split(':');
    let (Some("telegram"), Some(chat_id), Some(message_id), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(TelegramError::InvalidMessageReference);
    };
    let chat_id = chat_id
        .parse::<i64>()
        .map_err(|_| TelegramError::InvalidMessageReference)?;
    let message_id = message_id
        .parse::<i64>()
        .map_err(|_| TelegramError::InvalidMessageReference)?;
    if chat_id != route.chat_id.get() || message_id <= 0 {
        return Err(TelegramError::InvalidMessageReference);
    }
    Ok(message_id)
}

fn telegram_media_method(media_type: &str) -> (&'static str, &'static str) {
    match media_type {
        "image/jpeg" | "image/png" => ("sendPhoto", "photo"),
        "image/gif" => ("sendAnimation", "animation"),
        "video/mp4" => ("sendVideo", "video"),
        "audio/mpeg" => ("sendAudio", "audio"),
        "audio/ogg" => ("sendVoice", "voice"),
        _ => ("sendDocument", "document"),
    }
}

struct TelegramEventReferences {
    source_event_id: String,
    timestamp_ms: i64,
    next_offset: u64,
    reply_to: Option<crate::domain::SourceMessageRef>,
    reply_quote: Option<String>,
    mutation_target: Option<crate::domain::SourceMessageRef>,
}

fn telegram_event_references(
    message: &RawMessage,
    mutation: Mutation,
    update_id: u64,
) -> Result<TelegramEventReferences, TelegramError> {
    if message.message_id <= 0 {
        return Err(TelegramError::MalformedResponse);
    }
    let timestamp_ms = message
        .date
        .checked_mul(1_000)
        .ok_or(TelegramError::MalformedResponse)?;
    let next_offset = update_id
        .checked_add(1)
        .ok_or(TelegramError::MalformedResponse)?;
    let source_message_id = format!("telegram:{}:{}", message.chat.id, message.message_id);
    let (reply_to, reply_quote) = if mutation == Mutation::Create {
        telegram_reply_context(message)
    } else {
        (None, None)
    };
    let (source_event_id, mutation_target) = if mutation == Mutation::Edit {
        (
            format!(
                "telegram:edit:{}:{}:{update_id}",
                message.chat.id, message.message_id
            ),
            Some(crate::domain::SourceMessageRef {
                source: Source::Telegram,
                source_event_id: source_message_id,
            }),
        )
    } else {
        (source_message_id, None)
    };
    Ok(TelegramEventReferences {
        source_event_id,
        timestamp_ms,
        next_offset,
        reply_to,
        reply_quote,
        mutation_target,
    })
}

fn telegram_reply_context(
    message: &RawMessage,
) -> (Option<crate::domain::SourceMessageRef>, Option<String>) {
    let Some(reply) = message.reply_to_message.as_deref() else {
        return (None, None);
    };
    if reply.chat.id != message.chat.id || reply.message_id <= 0 {
        return (None, None);
    }
    let quote = reply
        .text
        .as_deref()
        .or(reply.caption.as_deref())
        .map(|text| {
            text.chars()
                .filter(|character| {
                    !character.is_control()
                        && !matches!(
                            character,
                            '\u{061c}'
                                | '\u{200b}'
                                | '\u{200e}'..='\u{200f}'
                                | '\u{202a}'..='\u{202e}'
                                | '\u{2060}'..='\u{206f}'
                                | '\u{feff}'
                        )
                })
                .take(240)
                .collect::<String>()
        })
        .filter(|quote| !quote.is_empty());
    (
        Some(crate::domain::SourceMessageRef {
            source: Source::Telegram,
            source_event_id: format!("telegram:{}:{}", reply.chat.id, reply.message_id),
        }),
        quote,
    )
}

fn telegram_attachments(
    message: &RawMessage,
    album_position: Option<(u16, u16)>,
) -> Vec<AttachmentRef> {
    let selected = message
        .photo
        .as_ref()
        .and_then(|photos| {
            photos
                .iter()
                .max_by_key(|photo| photo.file_size.unwrap_or(0))
        })
        .map(|file| (file, "photo.jpg"))
        .or_else(|| message.document.as_ref().map(|file| (file, "document.bin")))
        .or_else(|| message.audio.as_ref().map(|file| (file, "audio.mp3")))
        .or_else(|| message.video.as_ref().map(|file| (file, "video.mp4")))
        .or_else(|| message.voice.as_ref().map(|file| (file, "voice.ogg")))
        .or_else(|| {
            message
                .animation
                .as_ref()
                .map(|file| (file, "animation.gif"))
        })
        .or_else(|| message.sticker.as_ref().map(|file| (file, "sticker.webp")));
    selected.map_or_else(Vec::new, |(file, fallback_name)| {
        vec![AttachmentRef {
            source_id: file.file_unique_id.clone(),
            file_name: Some(
                file.file_name
                    .clone()
                    .unwrap_or_else(|| fallback_name.to_owned()),
            ),
            media_type: file.mime_type.clone(),
            size_bytes: file.file_size.unwrap_or(0),
            locator: AttachmentLocator::Telegram {
                file_id: file.file_id.clone(),
            },
            media_group_id: message.media_group_id.clone(),
            media_group_index: album_position.map(|position| position.0),
            media_group_total: album_position.map(|position| position.1),
        }]
    })
}

fn album_positions(updates: &[RawUpdate]) -> HashMap<u64, (u16, u16)> {
    let mut groups: HashMap<&str, Vec<u64>> = HashMap::new();
    for update in updates {
        if let Some(group_id) = update
            .message
            .as_ref()
            .and_then(|message| message.media_group_id.as_deref())
        {
            groups.entry(group_id).or_default().push(update.update_id);
        }
    }
    let mut positions = HashMap::new();
    for update_ids in groups.into_values() {
        let Ok(total) = u16::try_from(update_ids.len()) else {
            continue;
        };
        for (index, update_id) in update_ids.into_iter().enumerate() {
            if let Ok(index) = u16::try_from(index + 1) {
                positions.insert(update_id, (index, total));
            }
        }
    }
    positions
}

fn sender_identity(
    from: Option<RawUser>,
    sender_chat: Option<RawSenderChat>,
    own_bot_id: u64,
    relay_other_bots: bool,
) -> Option<SenderIdentity> {
    if let Some(sender_chat) = sender_chat {
        return Some(SenderIdentity::Telegram {
            user_id: None,
            sender_chat_id: Some(sender_chat.id),
            username: sender_chat.username,
            display_name: sender_chat.title,
        });
    }
    let from = from?;
    if from.id == own_bot_id || (from.is_bot && !relay_other_bots) {
        return None;
    }
    let display_name = match from.last_name {
        Some(last_name) if !last_name.is_empty() => {
            Some(format!("{} {last_name}", from.first_name))
        }
        _ if from.first_name.is_empty() => None,
        _ => Some(from.first_name),
    };
    Some(SenderIdentity::Telegram {
        user_id: Some(from.id),
        sender_chat_id: None,
        username: from.username,
        display_name,
    })
}

fn reaction_actor(
    user: Option<RawUser>,
    actor_chat: Option<RawSenderChat>,
) -> Option<(String, Option<String>)> {
    if let Some(chat) = actor_chat {
        let name = chat
            .username
            .map(|username| format!("@{}", username.trim_start_matches('@')))
            .or(chat.title);
        return Some((format!("telegram-chat:{}", chat.id), name));
    }
    let user = user?;
    let name = user
        .username
        .map(|username| format!("@{}", username.trim_start_matches('@')))
        .or_else(|| match user.last_name {
            Some(last_name) if !last_name.is_empty() => {
                Some(format!("{} {last_name}", user.first_name))
            }
            _ if user.first_name.is_empty() => None,
            _ => Some(user.first_name),
        });
    Some((format!("telegram-user:{}", user.id), name))
}

fn classify_transport_error(error: &reqwest::Error) -> TelegramError {
    if error.is_timeout() {
        TelegramError::Timeout
    } else {
        TelegramError::Transport
    }
}

fn classify_status(status: StatusCode, body: &[u8]) -> TelegramError {
    if status == StatusCode::TOO_MANY_REQUESTS {
        let response: Result<ApiErrorBody, _> = serde_json::from_slice(body);
        return response
            .ok()
            .and_then(|response| response.parameters)
            .and_then(|parameters| parameters.retry_after)
            .map_or(TelegramError::MalformedResponse, |retry_after_seconds| {
                TelegramError::RateLimited {
                    retry_after_seconds,
                }
            });
    }
    if status.is_server_error() {
        TelegramError::Retryable {
            status: status.as_u16(),
        }
    } else {
        TelegramError::Terminal {
            status: status.as_u16(),
        }
    }
}

fn mutation_already_applied(method: &str, status: StatusCode, bytes: &[u8]) -> bool {
    if !matches!(method, "editMessageText" | "deleteMessage")
        || !(status == StatusCode::BAD_REQUEST || status.is_success())
    {
        return false;
    }
    let Ok(body) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    if body["ok"] != false || body["error_code"] != 400 {
        return false;
    }
    match (method, body["description"].as_str()) {
        ("editMessageText", Some(description)) => {
            description == "Bad Request: message to edit not found"
                || description == "Bad Request: message is not modified"
                || description.starts_with("Bad Request: message is not modified:")
        }
        ("deleteMessage", Some("Bad Request: message to delete not found")) => true,
        _ => false,
    }
}

fn classify_api_error(error_code: Option<u16>, parameters: Option<ApiParameters>) -> TelegramError {
    let status = error_code.unwrap_or(500);
    if status == 429 {
        return parameters
            .and_then(|parameters| parameters.retry_after)
            .map_or(TelegramError::MalformedResponse, |retry_after_seconds| {
                TelegramError::RateLimited {
                    retry_after_seconds,
                }
            });
    }
    if status >= 500 {
        TelegramError::Retryable { status }
    } else {
        TelegramError::Terminal { status }
    }
}

enum NormalizedUpdate {
    Ignored {
        id: u64,
    },
    Message(Box<InboundTelegramMessage>),
    Reaction {
        update: BridgeReactionUpdate,
        next_offset: u64,
    },
    Migration {
        route_ids: Vec<RouteId>,
        old_chat_id: i64,
        new_chat_id: i64,
    },
    Paused {
        route_id: RouteId,
    },
}

#[derive(Deserialize)]
struct ApiEnvelope<T> {
    ok: bool,
    result: Option<T>,
    error_code: Option<u16>,
    parameters: Option<ApiParameters>,
}

#[derive(Deserialize)]
struct ApiErrorBody {
    parameters: Option<ApiParameters>,
}

#[derive(Deserialize)]
struct ApiParameters {
    retry_after: Option<u64>,
}

#[derive(Deserialize)]
struct RawUpdate {
    update_id: u64,
    message: Option<RawMessage>,
    edited_message: Option<RawMessage>,
    message_reaction: Option<RawMessageReaction>,
    message_reaction_count: Option<RawMessageReactionCount>,
}

#[derive(Deserialize)]
struct RawMessageReaction {
    chat: RawChat,
    message_id: i64,
    user: Option<RawUser>,
    actor_chat: Option<RawSenderChat>,
    message_thread_id: Option<u64>,
    #[allow(dead_code)]
    old_reaction: Vec<RawReactionType>,
    new_reaction: Vec<RawReactionType>,
}

#[derive(Deserialize)]
struct RawMessageReactionCount {
    chat: RawChat,
    message_id: i64,
    message_thread_id: Option<u64>,
    reactions: Vec<RawReactionCount>,
}

#[derive(Deserialize)]
struct RawReactionCount {
    #[serde(rename = "type")]
    kind: RawReactionType,
    total_count: u32,
}

#[derive(Deserialize)]
struct RawReactionType {
    #[serde(rename = "type")]
    kind: String,
    emoji: Option<String>,
}

impl RawReactionType {
    fn unicode_emoji(self) -> Option<String> {
        (self.kind == "emoji").then_some(self.emoji).flatten()
    }
}

#[derive(Deserialize)]
struct RawMessage {
    message_id: i64,
    date: i64,
    chat: RawChat,
    message_thread_id: Option<u64>,
    from: Option<RawUser>,
    sender_chat: Option<RawSenderChat>,
    text: Option<String>,
    entities: Option<Vec<RawMessageEntity>>,
    caption: Option<String>,
    caption_entities: Option<Vec<RawMessageEntity>>,
    media_group_id: Option<String>,
    photo: Option<Vec<RawTelegramFile>>,
    document: Option<RawTelegramFile>,
    audio: Option<RawTelegramFile>,
    video: Option<RawTelegramFile>,
    voice: Option<RawTelegramFile>,
    animation: Option<RawTelegramFile>,
    sticker: Option<RawTelegramFile>,
    reply_to_message: Option<Box<RawMessage>>,
    migrate_to_chat_id: Option<i64>,
    migrate_from_chat_id: Option<i64>,
}

#[derive(Deserialize)]
struct RawTelegramFile {
    file_id: String,
    file_unique_id: String,
    file_size: Option<u64>,
    file_name: Option<String>,
    mime_type: Option<String>,
}

#[derive(Deserialize)]
struct RawMessageEntity {
    #[serde(rename = "type")]
    kind: String,
    offset: u32,
    length: u32,
    url: Option<String>,
    language: Option<String>,
}

impl RawMessageEntity {
    fn into_supported(self) -> Option<TelegramEntity> {
        let kind = match self.kind.as_str() {
            "bold" => TelegramEntityKind::Bold,
            "italic" => TelegramEntityKind::Italic,
            "underline" => TelegramEntityKind::Underline,
            "strikethrough" => TelegramEntityKind::Strikethrough,
            "spoiler" => TelegramEntityKind::Spoiler,
            "code" => TelegramEntityKind::Code,
            "pre" => TelegramEntityKind::Pre,
            "text_link" => TelegramEntityKind::TextLink,
            "url" => TelegramEntityKind::Url,
            "blockquote" => TelegramEntityKind::Blockquote,
            "expandable_blockquote" => TelegramEntityKind::ExpandableBlockquote,
            _ => return None,
        };
        Some(TelegramEntity {
            kind,
            offset: self.offset,
            length: self.length,
            url: self.url,
            language: self.language,
        })
    }
}

#[derive(Deserialize)]
struct RawUser {
    id: u64,
    is_bot: bool,
    first_name: String,
    last_name: Option<String>,
    username: Option<String>,
}

#[derive(Deserialize)]
struct RawSenderChat {
    id: i64,
    title: Option<String>,
    username: Option<String>,
}

#[derive(Deserialize)]
struct RawChat {
    id: i64,
}

#[derive(Deserialize)]
struct RawSentMessage {
    message_id: i64,
    chat: RawChat,
}

#[derive(Deserialize)]
struct RawRemoteFile {
    file_size: Option<u64>,
    file_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHAT_ID: i64 = -1_001_234;

    fn adapter() -> TelegramAdapter {
        TelegramAdapter::with_api_base(
            "http://127.0.0.1",
            "123456:TEST_ONLY_SECRET_TOKEN",
            123_456,
            true,
            false,
            vec![TelegramRoute::new(
                RouteId::try_from("alpha").expect("route"),
                TelegramChatId::try_from(CHAT_ID).expect("chat"),
                None,
            )],
            Duration::from_secs(1),
        )
        .expect("adapter")
    }

    fn unicode(emoji: &str) -> RawReactionType {
        RawReactionType {
            kind: "emoji".to_owned(),
            emoji: Some(emoji.to_owned()),
        }
    }

    #[test]
    fn reaction_normalization_preserves_public_identity_and_anonymous_counts() {
        let adapter = adapter();
        let public = adapter
            .normalize_reaction_update(
                90,
                RawMessageReaction {
                    chat: RawChat { id: CHAT_ID },
                    message_id: 190,
                    user: Some(RawUser {
                        id: 13,
                        is_bot: false,
                        first_name: "Alice".to_owned(),
                        last_name: None,
                        username: Some("alice".to_owned()),
                    }),
                    actor_chat: None,
                    message_thread_id: None,
                    old_reaction: Vec::new(),
                    new_reaction: vec![unicode("👍")],
                },
            )
            .expect("public reaction normalizes");
        assert!(matches!(
            public,
            NormalizedUpdate::Reaction {
                update: BridgeReactionUpdate {
                    change: ReactionChange::ActorSelection {
                        reactor_id,
                        display_name: Some(name),
                        emojis,
                    },
                    ..
                },
                next_offset: 91,
            } if reactor_id == "telegram-user:13" && name == "@alice" && emojis == ["👍"]
        ));

        let anonymous = adapter
            .normalize_reaction_count_update(
                91,
                RawMessageReactionCount {
                    chat: RawChat { id: CHAT_ID },
                    message_id: 190,
                    message_thread_id: None,
                    reactions: vec![RawReactionCount {
                        kind: unicode("❤️"),
                        total_count: 3,
                    }],
                },
            )
            .expect("anonymous reaction normalizes");
        assert!(matches!(
            anonymous,
            NormalizedUpdate::Reaction {
                update: BridgeReactionUpdate {
                    change: ReactionChange::Snapshot { entries },
                    ..
                },
                next_offset: 92,
            } if entries == [ReactionEntry {
                reactor_id: "telegram-anonymous:❤️".to_owned(),
                display_name: None,
                emoji: "❤️".to_owned(),
                count: 3,
            }]
        ));
    }
}
