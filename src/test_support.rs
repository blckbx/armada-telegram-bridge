//! Deterministic helpers shared by tests.

use std::collections::{HashMap, VecDeque};
use std::future::pending;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::attachments::InspectedAttachment;
use crate::clock::Clock;
use crate::concord::{
    ConcordError, ConcordInbound, ConcordInboundMessage, ConcordPort, ConcordProfile,
    ConcordReadiness, ConcordRoute, ConcordSendReceipt,
};
use crate::domain::{AttachmentLocator, AttachmentRef, BridgeReactionUpdate, RouteId};
use crate::rich_text::{TelegramEntity, TelegramMessage};
use crate::telegram::{
    InboundTelegramMessage, MAX_TEXT_CHARACTERS, PollReport, TelegramBotIdentity,
    TelegramChatAccess, TelegramError, TelegramPort, TelegramRoute, TelegramSendReceipt,
    TelegramUpdateSink,
};

/// Clock that always returns one configured time.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock {
    now: SystemTime,
}

impl FixedClock {
    /// Creates a clock fixed at `now`.
    #[must_use]
    pub const fn new(now: SystemTime) -> Self {
        Self { now }
    }
}

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.now
    }
}

/// Cloneable wall clock that tests can advance without sleeping.
#[derive(Clone, Debug)]
pub struct AdjustableClock {
    millis_since_epoch: Arc<AtomicU64>,
}

impl AdjustableClock {
    /// Creates a clock at one Unix millisecond timestamp.
    #[must_use]
    pub fn new(millis_since_epoch: u64) -> Self {
        Self {
            millis_since_epoch: Arc::new(AtomicU64::new(millis_since_epoch)),
        }
    }

    /// Advances wall time deterministically.
    pub fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.millis_since_epoch.fetch_add(millis, Ordering::SeqCst);
    }
}

impl Clock for AdjustableClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.millis_since_epoch.load(Ordering::SeqCst))
    }
}

/// Deterministic in-memory implementation of the Telegram port contract.
pub struct FakeTelegramPort {
    identity: TelegramBotIdentity,
    routes: Vec<TelegramRoute>,
    inbound: VecDeque<FakeTelegramInbound>,
    sent: Vec<(RouteId, String)>,
    sent_entities: Vec<Vec<TelegramEntity>>,
    replies: Vec<(RouteId, String, String)>,
    edits: Vec<(RouteId, String, String)>,
    deletions: Vec<(RouteId, String)>,
    attachment_bytes: HashMap<String, Vec<u8>>,
    sent_attachments: Vec<RecordedAttachment>,
    send_attempts: Vec<(RouteId, String)>,
    send_results: VecDeque<Result<(), TelegramError>>,
    send_hangs: bool,
    next_offset: Option<u64>,
    next_message_id: i64,
}

#[derive(Clone)]
enum FakeTelegramInbound {
    Message(InboundTelegramMessage),
    Reaction(BridgeReactionUpdate, u64),
}

impl FakeTelegramPort {
    /// Creates an empty fake with one fixed remote identity and route set.
    #[must_use]
    pub fn new(identity: TelegramBotIdentity, routes: Vec<TelegramRoute>) -> Self {
        Self {
            identity,
            routes,
            inbound: VecDeque::new(),
            sent: Vec::new(),
            sent_entities: Vec::new(),
            replies: Vec::new(),
            edits: Vec::new(),
            deletions: Vec::new(),
            attachment_bytes: HashMap::new(),
            sent_attachments: Vec::new(),
            send_attempts: Vec::new(),
            send_results: VecDeque::new(),
            send_hangs: false,
            next_offset: None,
            next_message_id: 1,
        }
    }

    /// Appends one normalized update to the next poll.
    pub fn push_inbound(&mut self, message: InboundTelegramMessage) {
        self.inbound
            .push_back(FakeTelegramInbound::Message(message));
    }

    /// Appends one normalized reaction update to the next poll.
    pub fn push_reaction(&mut self, update: BridgeReactionUpdate, next_offset: u64) {
        self.inbound
            .push_back(FakeTelegramInbound::Reaction(update, next_offset));
    }

    /// Recorded outbound route and plaintext calls.
    #[must_use]
    pub fn sent(&self) -> &[(RouteId, String)] {
        &self.sent
    }

    /// Formatting entities recorded alongside successful outbound sends.
    #[must_use]
    pub fn sent_entities(&self) -> &[Vec<TelegramEntity>] {
        &self.sent_entities
    }

    /// Native replies recorded as route, target, and text.
    #[must_use]
    pub fn replies(&self) -> &[(RouteId, String, String)] {
        &self.replies
    }

    /// Bridge-owned edits recorded as route, target, and text.
    #[must_use]
    pub fn edits(&self) -> &[(RouteId, String, String)] {
        &self.edits
    }

    /// Bridge-owned deletions recorded as route and target.
    #[must_use]
    pub fn deletions(&self) -> &[(RouteId, String)] {
        &self.deletions
    }

    /// Registers bytes returned for one source attachment ID.
    pub fn set_attachment(&mut self, source_id: impl Into<String>, bytes: Vec<u8>) {
        self.attachment_bytes.insert(source_id.into(), bytes);
    }

    /// Successful attachment uploads recorded by the fake.
    #[must_use]
    pub fn sent_attachments(&self) -> &[RecordedAttachment] {
        &self.sent_attachments
    }

    /// Scripts one destination result before the fake's normal success.
    pub fn push_send_result(&mut self, result: Result<(), TelegramError>) {
        self.send_results.push_back(result);
    }

    /// Makes subsequent destination sends remain pending until cancelled.
    pub const fn set_send_hangs(&mut self, send_hangs: bool) {
        self.send_hangs = send_hangs;
    }

    /// All attempted sends, including failed and cancelled attempts.
    #[must_use]
    pub fn send_attempts(&self) -> &[(RouteId, String)] {
        &self.send_attempts
    }
}

#[async_trait]
impl TelegramPort for FakeTelegramPort {
    fn set_next_offset(&mut self, next_offset: Option<u64>) {
        self.next_offset = next_offset;
    }

    fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    async fn verify_identity(&mut self) -> Result<TelegramBotIdentity, TelegramError> {
        Ok(self.identity.clone())
    }

    async fn verify_route_access(
        &mut self,
        route_id: &RouteId,
    ) -> Result<TelegramChatAccess, TelegramError> {
        self.routes
            .iter()
            .find(|route| route.route_id() == route_id)
            .map(|route| TelegramChatAccess {
                chat_id: route.chat_id().get(),
            })
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })
    }

    async fn poll_once(
        &mut self,
        sink: &mut dyn TelegramUpdateSink,
    ) -> Result<PollReport, TelegramError> {
        let observed = self.inbound.len();
        let mut accepted = 0;
        let mut ignored = 0;
        while let Some(item) = self.inbound.pop_front() {
            let next_offset = match &item {
                FakeTelegramInbound::Message(message) => message.next_offset,
                FakeTelegramInbound::Reaction(_, next_offset) => *next_offset,
            };
            if self
                .next_offset
                .is_some_and(|committed| next_offset <= committed)
            {
                ignored += 1;
                continue;
            }
            let accepted_result = match &item {
                FakeTelegramInbound::Message(message) => sink.accept(message.clone()).await,
                FakeTelegramInbound::Reaction(update, next_offset) => {
                    sink.accept_reaction(update.clone(), *next_offset).await
                }
            };
            if accepted_result.is_err() {
                self.inbound.push_front(item);
                return Err(TelegramError::AcceptanceFailed);
            }
            self.next_offset = Some(
                self.next_offset
                    .map_or(next_offset, |current| current.max(next_offset)),
            );
            accepted += 1;
        }
        Ok(PollReport {
            observed,
            accepted,
            ignored,
            next_offset: self.next_offset,
        })
    }

    async fn send_rich_text(
        &mut self,
        route_id: &RouteId,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<TelegramSendReceipt, TelegramError> {
        self.send_attempts.push((route_id.clone(), text.to_owned()));
        if self.send_hangs {
            pending::<()>().await;
        }
        if let Some(result) = self.send_results.pop_front() {
            result?;
        }
        let actual = text.chars().count();
        if actual > MAX_TEXT_CHARACTERS {
            return Err(TelegramError::TextTooLong {
                actual,
                maximum: MAX_TEXT_CHARACTERS,
            });
        }
        let route = self
            .routes
            .iter()
            .find(|route| route.route_id() == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        let message_id = self.next_message_id;
        self.next_message_id += 1;
        self.sent.push((route_id.clone(), text.to_owned()));
        self.sent_entities.push(entities.to_vec());
        Ok(TelegramSendReceipt {
            message_id,
            destination_message_id: format!("telegram:{}:{message_id}", route.chat_id().get()),
        })
    }

    async fn send_reply(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Result<TelegramSendReceipt, TelegramError> {
        let receipt = self.send_rich_text(route_id, text, entities).await?;
        self.replies.push((
            route_id.clone(),
            destination_message_id.to_owned(),
            text.to_owned(),
        ));
        Ok(receipt)
    }

    async fn edit_message(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
        text: &str,
        _entities: &[TelegramEntity],
    ) -> Result<(), TelegramError> {
        self.routes
            .iter()
            .find(|route| route.route_id() == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        self.edits.push((
            route_id.clone(),
            destination_message_id.to_owned(),
            text.to_owned(),
        ));
        Ok(())
    }

    async fn delete_message(
        &mut self,
        route_id: &RouteId,
        destination_message_id: &str,
    ) -> Result<(), TelegramError> {
        self.routes
            .iter()
            .find(|route| route.route_id() == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        self.deletions
            .push((route_id.clone(), destination_message_id.to_owned()));
        Ok(())
    }

    async fn download_attachment(
        &mut self,
        attachment: &AttachmentRef,
        destination: &Path,
        maximum_bytes: u64,
    ) -> Result<(), TelegramError> {
        if !matches!(attachment.locator, AttachmentLocator::Telegram { .. }) {
            return Err(TelegramError::InvalidAttachment);
        }
        let bytes = self
            .attachment_bytes
            .get(&attachment.source_id)
            .ok_or(TelegramError::InvalidAttachment)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
            return Err(TelegramError::AttachmentTooLarge);
        }
        tokio::fs::write(destination, bytes)
            .await
            .map_err(|_| TelegramError::TemporaryFile)
    }

    async fn send_attachment(
        &mut self,
        route_id: &RouteId,
        path: &Path,
        metadata: &InspectedAttachment,
        caption: Option<&TelegramMessage>,
    ) -> Result<TelegramSendReceipt, TelegramError> {
        self.send_attempts
            .push((route_id.clone(), "[attachment]".to_owned()));
        if self.send_hangs {
            pending::<()>().await;
        }
        if let Some(result) = self.send_results.pop_front() {
            result?;
        }
        let route = self
            .routes
            .iter()
            .find(|route| route.route_id() == route_id)
            .ok_or_else(|| TelegramError::UnknownRoute {
                route_id: route_id.clone(),
            })?;
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| TelegramError::TemporaryFile)?;
        let message_id = self.next_message_id;
        self.next_message_id += 1;
        self.sent_attachments.push(RecordedAttachment {
            route_id: route_id.clone(),
            bytes,
            metadata: metadata.clone(),
            caption: caption.cloned(),
        });
        Ok(TelegramSendReceipt {
            message_id,
            destination_message_id: format!("telegram:{}:{message_id}", route.chat_id().get()),
        })
    }
}

/// One attachment recorded by an in-memory destination port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedAttachment {
    /// Destination route.
    pub route_id: RouteId,
    /// Exact staged bytes.
    pub bytes: Vec<u8>,
    /// Sniffed metadata supplied to the destination.
    pub metadata: InspectedAttachment,
    /// Optional formatted Telegram caption.
    pub caption: Option<TelegramMessage>,
}

/// Deterministic in-memory implementation of the Concord port contract.
pub struct FakeConcordPort {
    routes: Vec<ConcordRoute>,
    readiness: HashMap<RouteId, ConcordReadiness>,
    history: Vec<ConcordInboundMessage>,
    live: VecDeque<ConcordInbound>,
    reaction_history: Vec<BridgeReactionUpdate>,
    profiles: HashMap<String, ConcordProfile>,
    sent: Vec<(RouteId, String)>,
    replies: Vec<(RouteId, String, String)>,
    edits: Vec<(RouteId, String, String)>,
    deletions: Vec<(RouteId, String)>,
    send_attempts: Vec<(RouteId, String)>,
    send_results: VecDeque<Result<(), ConcordError>>,
    send_hangs: bool,
    next_event_id: u64,
    attachment_bytes: HashMap<String, Vec<u8>>,
    download_results: VecDeque<Result<(), ConcordError>>,
    sent_attachments: Vec<RecordedAttachment>,
}

impl FakeConcordPort {
    /// Creates an empty fail-closed fake for exact configured routes.
    #[must_use]
    pub fn new(routes: Vec<ConcordRoute>) -> Self {
        let readiness = routes
            .iter()
            .map(|route| (route.route_id().clone(), ConcordReadiness::Unchecked))
            .collect();
        Self {
            routes,
            readiness,
            history: Vec::new(),
            live: VecDeque::new(),
            reaction_history: Vec::new(),
            profiles: HashMap::new(),
            sent: Vec::new(),
            replies: Vec::new(),
            edits: Vec::new(),
            deletions: Vec::new(),
            send_attempts: Vec::new(),
            send_results: VecDeque::new(),
            send_hangs: false,
            next_event_id: 1,
            attachment_bytes: HashMap::new(),
            download_results: VecDeque::new(),
            sent_attachments: Vec::new(),
        }
    }

    /// Sets one route's scripted readiness.
    pub fn set_readiness(&mut self, route_id: RouteId, readiness: ConcordReadiness) {
        self.readiness.insert(route_id, readiness);
    }

    /// Replaces chronological catch-up results.
    pub fn set_history(&mut self, history: Vec<ConcordInboundMessage>) {
        self.history = history;
    }

    /// Queues one normalized live event.
    pub fn push_live(&mut self, message: ConcordInboundMessage) {
        self.live
            .push_back(ConcordInbound::Message(Box::new(message)));
    }

    /// Queues one normalized live reaction update.
    pub fn push_reaction(&mut self, update: BridgeReactionUpdate) {
        self.live.push_back(ConcordInbound::Reaction(update));
    }

    /// Replaces reaction snapshots returned during startup reconciliation.
    pub fn set_reaction_history(&mut self, updates: Vec<BridgeReactionUpdate>) {
        self.reaction_history = updates;
    }

    /// Stores one profile result.
    pub fn set_profile(&mut self, profile: ConcordProfile) {
        self.profiles.insert(profile.npub.clone(), profile);
    }

    /// Recorded outbound route and plaintext calls.
    #[must_use]
    pub fn sent(&self) -> &[(RouteId, String)] {
        &self.sent
    }

    /// Native replies recorded as route, target, and text.
    #[must_use]
    pub fn replies(&self) -> &[(RouteId, String, String)] {
        &self.replies
    }

    /// Bridge-owned edits recorded as route, target, and text.
    #[must_use]
    pub fn edits(&self) -> &[(RouteId, String, String)] {
        &self.edits
    }

    /// Bridge-owned deletions recorded as route and target.
    #[must_use]
    pub fn deletions(&self) -> &[(RouteId, String)] {
        &self.deletions
    }

    /// Registers bytes returned for one Concord attachment ID.
    pub fn set_attachment(&mut self, source_id: impl Into<String>, bytes: Vec<u8>) {
        self.attachment_bytes.insert(source_id.into(), bytes);
    }

    /// Scripts one source attachment download result.
    pub fn push_download_result(&mut self, result: Result<(), ConcordError>) {
        self.download_results.push_back(result);
    }

    /// Successful attachment publications recorded by the fake.
    #[must_use]
    pub fn sent_attachments(&self) -> &[RecordedAttachment] {
        &self.sent_attachments
    }

    /// Scripts one destination result before the fake's normal success.
    pub fn push_send_result(&mut self, result: Result<(), ConcordError>) {
        self.send_results.push_back(result);
    }

    /// Makes subsequent destination sends remain pending until cancelled.
    pub const fn set_send_hangs(&mut self, send_hangs: bool) {
        self.send_hangs = send_hangs;
    }

    /// All attempted sends, including failed and cancelled attempts.
    #[must_use]
    pub fn send_attempts(&self) -> &[(RouteId, String)] {
        &self.send_attempts
    }

    fn route(&self, route_id: &RouteId) -> Result<&ConcordRoute, ConcordError> {
        self.routes
            .iter()
            .find(|route| route.route_id() == route_id)
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
}

#[async_trait]
impl ConcordPort for FakeConcordPort {
    async fn verify_ready(&mut self) -> Result<(), ConcordError> {
        for route in &self.routes {
            let reason = self.readiness(route.route_id());
            if reason != ConcordReadiness::Ready {
                return Err(ConcordError::RouteNotReady {
                    route_id: route.route_id().clone(),
                    reason,
                });
            }
        }
        Ok(())
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
        if limit == 0 || limit > 500 {
            return Err(ConcordError::InvalidHistoryLimit);
        }
        let start = match after_cursor {
            Some(cursor) => self
                .history
                .iter()
                .position(|message| message.next_cursor == cursor)
                .map_or(0, |index| index + 1),
            None => 0,
        };
        Ok(self
            .history
            .iter()
            .skip(start)
            .filter(|message| &message.event.route_id == route_id)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn next_live(&mut self) -> Result<Option<ConcordInboundMessage>, ConcordError> {
        while let Some(inbound) = self.live.pop_front() {
            let ConcordInbound::Message(message) = inbound else {
                continue;
            };
            if self.readiness(&message.event.route_id) != ConcordReadiness::Ready {
                return Err(ConcordError::RouteNotReady {
                    route_id: message.event.route_id.clone(),
                    reason: self.readiness(&message.event.route_id),
                });
            }
            if self.route(&message.event.route_id).is_ok() {
                return Ok(Some(*message));
            }
        }
        Ok(None)
    }

    async fn next_inbound(&mut self) -> Result<Option<ConcordInbound>, ConcordError> {
        while let Some(inbound) = self.live.pop_front() {
            let route_id = match &inbound {
                ConcordInbound::Message(message) => &message.event.route_id,
                ConcordInbound::Reaction(update) => &update.route_id,
            };
            if self.readiness(route_id) != ConcordReadiness::Ready {
                return Err(ConcordError::RouteNotReady {
                    route_id: route_id.clone(),
                    reason: self.readiness(route_id),
                });
            }
            if self.route(route_id).is_ok() {
                return Ok(Some(inbound));
            }
        }
        Ok(None)
    }

    async fn reaction_snapshots(
        &mut self,
        route_id: &RouteId,
        limit: usize,
    ) -> Result<Vec<BridgeReactionUpdate>, ConcordError> {
        self.require_ready(route_id)?;
        if limit == 0 || limit > 500 {
            return Err(ConcordError::InvalidHistoryLimit);
        }
        Ok(self
            .reaction_history
            .iter()
            .filter(|update| &update.route_id == route_id)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn send_text(
        &mut self,
        route_id: &RouteId,
        text: &str,
    ) -> Result<ConcordSendReceipt, ConcordError> {
        self.require_ready(route_id)?;
        self.send_attempts.push((route_id.clone(), text.to_owned()));
        if self.send_hangs {
            pending::<()>().await;
        }
        if let Some(result) = self.send_results.pop_front() {
            result?;
        }
        self.sent.push((route_id.clone(), text.to_owned()));
        let event_id = format!("{:064x}", self.next_event_id);
        self.next_event_id += 1;
        ConcordSendReceipt::new(event_id)
    }

    async fn send_reply(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
        text: &str,
    ) -> Result<ConcordSendReceipt, ConcordError> {
        let receipt = self.send_text(route_id, text).await?;
        self.replies.push((
            route_id.clone(),
            destination_event_id.to_owned(),
            text.to_owned(),
        ));
        Ok(receipt)
    }

    async fn edit_message(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
        text: &str,
    ) -> Result<(), ConcordError> {
        self.require_ready(route_id)?;
        self.edits.push((
            route_id.clone(),
            destination_event_id.to_owned(),
            text.to_owned(),
        ));
        Ok(())
    }

    async fn delete_message(
        &mut self,
        route_id: &RouteId,
        destination_event_id: &str,
    ) -> Result<(), ConcordError> {
        self.require_ready(route_id)?;
        self.deletions
            .push((route_id.clone(), destination_event_id.to_owned()));
        Ok(())
    }

    async fn download_attachment(
        &mut self,
        attachment: &AttachmentRef,
        destination: &Path,
        maximum_bytes: u64,
    ) -> Result<(), ConcordError> {
        if let Some(result) = self.download_results.pop_front() {
            result?;
        }
        if !matches!(attachment.locator, AttachmentLocator::Armada { .. }) {
            return Err(ConcordError::InvalidAttachment);
        }
        let bytes = self
            .attachment_bytes
            .get(&attachment.source_id)
            .ok_or(ConcordError::InvalidAttachment)?;
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
        self.send_attempts
            .push((route_id.clone(), "[attachment]".to_owned()));
        if self.send_hangs {
            pending::<()>().await;
        }
        if let Some(result) = self.send_results.pop_front() {
            result?;
        }
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| ConcordError::TemporaryFile)?;
        self.sent_attachments.push(RecordedAttachment {
            route_id: route_id.clone(),
            bytes,
            metadata: metadata.clone(),
            caption: None,
        });
        let event_id = format!("{:064x}", self.next_event_id);
        self.next_event_id += 1;
        ConcordSendReceipt::new(event_id)
    }

    async fn profile(&mut self, npub: &str) -> Result<Option<ConcordProfile>, ConcordError> {
        Ok(self.profiles.get(npub).cloned())
    }
}
