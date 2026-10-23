//! Durable bidirectional plaintext bridge orchestration.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::attachments::{
    AttachmentError, MAX_ATTACHMENTS_PER_MESSAGE, PrivateTempFile, validate_declared_size,
};
use crate::clock::Clock;
use crate::concord::{ConcordError, ConcordInbound, ConcordInboundMessage, ConcordPort};
use crate::config::{BridgeConfig, FirstStartupPolicy};
use crate::domain::{
    AttachmentLocator, AttachmentRef, BridgeEvent, BridgeReactionUpdate, Mutation, ReactionChange,
    ReactionEntry, RouteId, SenderIdentity, Source,
};
use crate::rich_text::{
    RichText, TelegramMessage, parse_armada_markdown, prepend_plain, render_armada_markdown,
    split_for_telegram, to_telegram,
};
use crate::store::{
    AcceptOutcome, DeliveryAttemptState, DeliveryDirection, DeliveryState, JobId, NewDelivery,
    RouteState, Store, StoreError,
};
use crate::telegram::{
    InboundTelegramMessage, MAX_CAPTION_CHARACTERS, MAX_TEXT_CHARACTERS, PollReport,
    TelegramAcceptError, TelegramError, TelegramPort, TelegramUpdateSink,
};

const CONCORD_HISTORY_PAGE_LIMIT: usize = 500;
const DEFAULT_MAX_PENDING_JOBS: usize = 500;
const DEFAULT_MAX_CATCHUP_AGE_MS: i64 = 24 * 60 * 60 * 1_000;
const DEFAULT_MAX_FILE_BYTES: u64 = 20_000_000;
const TELEGRAM_PAYLOAD_PREFIX: &[u8] = b"ATB_RT1\n";
const ARMADA_PAYLOAD_PREFIX: &[u8] = b"ATB_A1\n";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeliveryOperation {
    #[default]
    Create,
    Edit,
    Delete,
    ReactionSummary,
}

#[derive(Clone, Deserialize, Serialize)]
struct ReactionDeliveryPayload {
    message_job_id: i64,
    destination_target_id: String,
    change: ReactionChange,
}

#[derive(Clone, Deserialize, Serialize)]
struct TelegramDeliveryPayload {
    chunks: Vec<TelegramMessage>,
    #[serde(default)]
    attachments: Vec<AttachmentRef>,
    #[serde(default)]
    operation: DeliveryOperation,
    #[serde(default)]
    target_destination_ids: Vec<String>,
    #[serde(default)]
    reply_destination_id: Option<String>,
    #[serde(default)]
    expires_at_ms: Option<i64>,
    #[serde(default)]
    reaction: Option<ReactionDeliveryPayload>,
}

#[derive(Deserialize, Serialize)]
struct ArmadaDeliveryPayload {
    text: String,
    attachments: Vec<AttachmentRef>,
    #[serde(default)]
    operation: DeliveryOperation,
    #[serde(default)]
    target_destination_ids: Vec<String>,
    #[serde(default)]
    reply_destination_id: Option<String>,
    #[serde(default)]
    reaction: Option<ReactionDeliveryPayload>,
}

struct DispatchSuccess {
    destination_ids: String,
    creates_link: bool,
    expires_at_ms: Option<i64>,
}

/// Durable retry timing applied independently to each route/direction stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Initial retry delay before deterministic jitter.
    pub base_delay: Duration,
    /// Maximum exponential delay before deterministic jitter.
    pub max_delay: Duration,
    /// Maximum failed attempts before a dead letter.
    pub max_attempts: u32,
    /// Symmetric deterministic jitter percentage from 0 through 50.
    pub jitter_percent: u8,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_mins(5),
            max_attempts: 8,
            jitter_percent: 20,
        }
    }
}

/// Static values needed by the service after adapter construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BridgeServiceSettings {
    routes: Vec<RouteId>,
    own_armada_npub: String,
    telegram_bot_id: u64,
    first_startup: FirstStartupPolicy,
    max_catchup_events: usize,
    max_catchup_age_ms: i64,
    max_text_bytes: usize,
    max_file_bytes: u64,
    attachment_temp_root: PathBuf,
    max_pending_jobs: usize,
    retry_policy: RetryPolicy,
}

impl BridgeServiceSettings {
    /// Creates settings for deterministic orchestration and adapter tests.
    #[must_use]
    pub fn new(
        routes: Vec<RouteId>,
        own_armada_npub: impl Into<String>,
        telegram_bot_id: u64,
        max_catchup_events: usize,
        max_text_bytes: usize,
    ) -> Self {
        Self {
            routes,
            own_armada_npub: own_armada_npub.into(),
            telegram_bot_id,
            first_startup: FirstStartupPolicy::NewMessagesOnly,
            max_catchup_events,
            max_catchup_age_ms: DEFAULT_MAX_CATCHUP_AGE_MS,
            max_text_bytes,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            attachment_temp_root: std::env::temp_dir()
                .join("armada-telegram-bridge")
                .join("attachments"),
            max_pending_jobs: DEFAULT_MAX_PENDING_JOBS,
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Overrides P09 file size and private staging location.
    #[must_use]
    pub fn with_attachment_policy(
        mut self,
        max_file_bytes: u64,
        attachment_temp_root: PathBuf,
    ) -> Self {
        self.max_file_bytes = max_file_bytes;
        self.attachment_temp_root = attachment_temp_root;
        self
    }

    /// Overrides P07 queue, recovery-age, and retry policy settings.
    #[must_use]
    pub fn with_delivery_policy(
        mut self,
        max_pending_jobs: usize,
        max_catchup_age: Duration,
        retry_policy: RetryPolicy,
    ) -> Self {
        self.max_pending_jobs = max_pending_jobs;
        self.max_catchup_age_ms = i64::try_from(max_catchup_age.as_millis()).unwrap_or(i64::MAX);
        self.retry_policy = retry_policy;
        self
    }

    /// Derives non-secret orchestration settings from validated configuration.
    ///
    /// # Errors
    ///
    /// Fails only if a platform cannot represent a validated limit as `usize`.
    pub fn from_config(config: &BridgeConfig) -> Result<Self, ServiceError> {
        Ok(Self {
            routes: config
                .routes()
                .iter()
                .map(|route| route.id().clone())
                .collect(),
            own_armada_npub: config.nostr().expected_npub().to_owned(),
            telegram_bot_id: config.telegram().expected_bot_id(),
            first_startup: config.first_startup(),
            max_catchup_events: usize::try_from(config.limits().max_catchup_events().get())
                .map_err(|_| ServiceError::InvalidSettings)?,
            max_catchup_age_ms: i64::try_from(
                config.limits().max_catchup_age_hours().get() * 60 * 60 * 1_000,
            )
            .map_err(|_| ServiceError::InvalidSettings)?,
            max_text_bytes: usize::try_from(config.limits().max_text_bytes().get())
                .map_err(|_| ServiceError::InvalidSettings)?,
            max_file_bytes: config.limits().max_file_bytes().get(),
            attachment_temp_root: crate::attachments::temp_root(config.data_dir()),
            max_pending_jobs: usize::try_from(config.limits().max_pending_jobs().get())
                .map_err(|_| ServiceError::InvalidSettings)?,
            retry_policy: RetryPolicy::default(),
        })
    }

    fn route_is_configured(&self, route_id: &RouteId) -> bool {
        self.routes.iter().any(|configured| configured == route_id)
    }
}

/// Result of routing one normalized input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntakeOutcome {
    /// A new durable destination job was created.
    Enqueued,
    /// The immutable source event was already known.
    Duplicate,
    /// The normalized event failed a service-level routing rule.
    Ignored,
}

/// Summary of startup recovery and first-run baselining.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StartupReport {
    /// Armada history messages durably enqueued during restart catch-up.
    pub armada_enqueued: usize,
    /// Whether an initial Telegram backlog was deliberately discarded.
    pub telegram_baselined: bool,
}

/// Work performed by one scheduler pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DispatchReport {
    /// Destination receipts committed.
    pub delivered: usize,
    /// Retryable failures durably scheduled.
    pub retried: usize,
    /// Terminal or exhausted failures moved to dead letter.
    pub dead_lettered: usize,
}

/// Result of a bounded graceful drain.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Jobs delivered during shutdown.
    pub delivered: usize,
    /// Jobs still durable after the deadline/drain.
    pub remaining: u64,
    /// Whether the supplied Tokio deadline elapsed during a send.
    pub timed_out: bool,
}

/// One-process orchestration over transport ports and the durable store.
pub struct BridgeService<T, C, K> {
    store: Store,
    telegram: T,
    concord: C,
    clock: K,
    settings: BridgeServiceSettings,
    health: Option<crate::operations::Health>,
}

impl<T, C, K> BridgeService<T, C, K>
where
    T: TelegramPort,
    C: ConcordPort,
    K: Clock,
{
    /// Creates a service without contacting either transport.
    #[must_use]
    pub const fn new(
        store: Store,
        telegram: T,
        concord: C,
        clock: K,
        settings: BridgeServiceSettings,
    ) -> Self {
        Self {
            store,
            telegram,
            concord,
            clock,
            settings,
            health: None,
        }
    }

    /// Attaches content-free operational reporting before startup.
    pub fn set_health(&mut self, health: crate::operations::Health) {
        self.health = Some(health);
    }

    fn publish_health(&self) -> Result<(), ServiceError> {
        if let Some(health) = &self.health {
            health.update(
                self.is_ready()?,
                self.store.pending_job_count()?,
                u64::try_from(self.store.dead_letters()?.len()).unwrap_or(u64::MAX),
            );
        }
        Ok(())
    }

    /// Verifies both transports, restores cursors, catches up, and establishes
    /// first-run baselines without relaying pre-existing messages.
    ///
    /// # Errors
    ///
    /// Fails closed when configuration, persistence, or either adapter fails.
    pub async fn initialize(&mut self) -> Result<StartupReport, ServiceError> {
        self.validate_settings()?;
        for route_id in &self.settings.routes {
            self.store.register_route(route_id)?;
            if self.store.route_state(route_id)? != RouteState::Active {
                return Err(ServiceError::RouteQuarantined(route_id.clone()));
            }
        }

        let identity = self.telegram.verify_identity().await?;
        if identity.id != self.settings.telegram_bot_id {
            return Err(ServiceError::TelegramIdentityMismatch);
        }
        for route_id in &self.settings.routes {
            self.telegram.verify_route_access(route_id).await?;
        }
        if let Err(error) = self.concord.verify_ready().await {
            self.persist_concord_safety(&error, now_ms(&self.clock)?)?;
            return Err(error.into());
        }

        let now_ms = now_ms(&self.clock)?;
        let mut report = StartupReport::default();
        let mut restored_telegram_offset = None;
        for route_id in &self.settings.routes {
            if let Some(cursor) = self
                .store
                .cursor(route_id, DeliveryDirection::TelegramToArmada)?
            {
                let offset = cursor
                    .parse::<u64>()
                    .map_err(|_| ServiceError::InvalidTelegramCursor)?;
                restored_telegram_offset = Some(
                    restored_telegram_offset.map_or(offset, |current: u64| current.max(offset)),
                );
            }
        }
        self.telegram.set_next_offset(restored_telegram_offset);

        for route_id in self.settings.routes.clone() {
            report.armada_enqueued += self.recover_armada_route(&route_id, now_ms).await?;
        }

        if restored_telegram_offset.is_none()
            && self.settings.first_startup == FirstStartupPolicy::NewMessagesOnly
        {
            let mut sink = BaselineTelegramSink;
            let baseline = self.telegram.poll_once(&mut sink).await?;
            if let Some(offset) = baseline.next_offset {
                self.checkpoint_telegram_stream(offset, now_ms)?;
            }
            report.telegram_baselined = baseline.observed > 0;
        }
        Ok(report)
    }

    async fn recover_armada_route(
        &mut self,
        route_id: &RouteId,
        now_ms: i64,
    ) -> Result<usize, ServiceError> {
        let initial_cursor = self
            .store
            .cursor(route_id, DeliveryDirection::ArmadaToTelegram)?;
        let first_start = initial_cursor.is_none();
        let mut after_cursor = initial_cursor;
        let mut remaining = self.settings.max_catchup_events;
        let mut enqueued = 0;
        let oldest_allowed = now_ms.saturating_sub(self.settings.max_catchup_age_ms);

        while remaining > 0 {
            let page_limit = remaining.min(CONCORD_HISTORY_PAGE_LIMIT);
            let history = match self
                .concord
                .catch_up(route_id, after_cursor.as_deref(), page_limit)
                .await
            {
                Ok(history) => history,
                Err(error) => {
                    self.persist_concord_safety(&error, now_ms)?;
                    return Err(error.into());
                }
            };
            if history.is_empty() {
                break;
            }
            let observed = history.len();
            for message in history {
                after_cursor = Some(message.next_cursor.clone());
                remaining -= 1;
                if (first_start
                    && self.settings.first_startup == FirstStartupPolicy::NewMessagesOnly)
                    || message.event.timestamp_ms < oldest_allowed
                {
                    self.store.checkpoint_cursor(
                        route_id,
                        DeliveryDirection::ArmadaToTelegram,
                        &message.next_cursor,
                        now_ms,
                    )?;
                } else if self.accept_armada(message)? == IntakeOutcome::Enqueued {
                    enqueued += 1;
                }
            }
            if observed < page_limit {
                break;
            }
        }
        Ok(enqueued)
    }

    /// Polls Telegram once and atomically queues every relevant normalized
    /// message before the adapter advances past it.
    ///
    /// # Errors
    ///
    /// Returns a transport, durable-state, cursor, or clock failure.
    pub async fn poll_telegram_once(&mut self) -> Result<PollReport, ServiceError> {
        let now_ms = now_ms(&self.clock)?;
        let (telegram, store) = (&mut self.telegram, &mut self.store);
        let mut sink = DurableTelegramSink {
            store,
            settings: &self.settings,
            now_ms,
            failure: None,
        };
        let polled = telegram.poll_once(&mut sink).await;
        if let Some(error) = sink.failure {
            return Err(error.into());
        }
        let report = match polled {
            Ok(report) => report,
            Err(error) => {
                self.persist_telegram_safety(&error, now_ms)?;
                return Err(error.into());
            }
        };
        if let Some(offset) = report.next_offset {
            self.checkpoint_telegram_stream(offset, now_ms)?;
        }
        Ok(report)
    }

    /// Accepts at most one relevant live Armada message.
    ///
    /// # Errors
    ///
    /// Returns a Concord, durable-state, or clock failure.
    pub async fn receive_armada_once(&mut self) -> Result<Option<IntakeOutcome>, ServiceError> {
        let Some(inbound) = self.concord.next_inbound().await? else {
            return Ok(None);
        };
        Ok(Some(match inbound {
            ConcordInbound::Message(message) => self.accept_armada(*message)?,
            ConcordInbound::Reaction(update) => self.accept_reaction(update)?,
        }))
    }

    /// Reconciles current Armada reaction snapshots after recovered messages
    /// have been delivered and therefore have durable cross-platform links.
    ///
    /// # Errors
    /// Returns a Concord, durable-state, payload-encoding, or clock failure.
    pub async fn recover_armada_reactions(&mut self) -> Result<usize, ServiceError> {
        let mut enqueued = 0;
        for route_id in self.settings.routes.clone() {
            let snapshots = self
                .concord
                .reaction_snapshots(
                    &route_id,
                    self.settings
                        .max_catchup_events
                        .min(CONCORD_HISTORY_PAGE_LIMIT),
                )
                .await?;
            for update in snapshots {
                if self.accept_reaction(update)? == IntakeOutcome::Enqueued {
                    enqueued += 1;
                }
            }
        }
        Ok(enqueued)
    }

    /// Routes one normalized Armada message into the durable outbox.
    ///
    /// # Errors
    ///
    /// Returns a durable-state or clock failure.
    pub fn accept_armada(
        &mut self,
        message: ConcordInboundMessage,
    ) -> Result<IntakeOutcome, ServiceError> {
        let now_ms = now_ms(&self.clock)?;
        Ok(accept_event(
            &mut self.store,
            &self.settings,
            message.event,
            DeliveryDirection::ArmadaToTelegram,
            Some(message.next_cursor),
            now_ms,
        )?)
    }

    /// Aggregates one mapped source reaction change into a durable summary job.
    ///
    /// Reactions targeting messages outside this bridge's durable link set are
    /// ignored; the bridge never guesses a cross-platform target.
    ///
    /// # Errors
    /// Returns a durable-state, payload-encoding, or clock failure.
    pub fn accept_reaction(
        &mut self,
        update: BridgeReactionUpdate,
    ) -> Result<IntakeOutcome, ServiceError> {
        let now_ms = now_ms(&self.clock)?;
        Ok(accept_reaction_update(
            &mut self.store,
            &self.settings,
            update,
            None,
            now_ms,
        )?)
    }

    /// Sends every currently eligible stream head while preserving FIFO order.
    ///
    /// # Errors
    ///
    /// Returns a destination, durable-state, payload, or clock failure.
    pub async fn dispatch_pending(&mut self) -> Result<usize, ServiceError> {
        Ok(self.dispatch_ready().await?.delivered)
    }

    /// Runs one failure-isolated scheduler pass across every route/direction.
    ///
    /// # Errors
    ///
    /// Returns only durable-state, payload, or clock failures. Destination
    /// failures are classified and persisted as retry waits or dead letters.
    pub async fn dispatch_ready(&mut self) -> Result<DispatchReport, ServiceError> {
        let mut report = DispatchReport::default();
        self.dispatch_due_deletions().await?;
        for route_id in self.settings.routes.clone() {
            if self.store.route_state(&route_id)? != RouteState::Active {
                continue;
            }
            add_dispatch_report(
                &mut report,
                self.dispatch_direction(&route_id, DeliveryDirection::ArmadaToTelegram)
                    .await?,
            );
            add_dispatch_report(
                &mut report,
                self.dispatch_direction(&route_id, DeliveryDirection::TelegramToArmada)
                    .await?,
            );
        }
        self.publish_health()?;
        Ok(report)
    }

    /// Runs live intake in both directions and dispatches after each completed
    /// source operation. Retry policy and graceful shutdown are added in P07.
    ///
    /// # Errors
    ///
    /// Returns immediately when a transport, durable-state, payload, cursor,
    /// listener, or clock operation fails.
    pub async fn run(&mut self) -> Result<(), ServiceError> {
        loop {
            enum SourceResult {
                Telegram(Result<PollReport, TelegramError>, Option<StoreError>),
                Armada(Box<Result<Option<ConcordInbound>, ConcordError>>),
            }

            let now_ms = now_ms(&self.clock)?;
            let source_result = {
                let (telegram, concord, store) =
                    (&mut self.telegram, &mut self.concord, &mut self.store);
                let mut sink = DurableTelegramSink {
                    store,
                    settings: &self.settings,
                    now_ms,
                    failure: None,
                };
                tokio::select! {
                    polled = telegram.poll_once(&mut sink) => {
                        SourceResult::Telegram(polled, sink.failure)
                    }
                    message = concord.next_inbound() => SourceResult::Armada(Box::new(message)),
                }
            };

            match source_result {
                SourceResult::Telegram(polled, failure) => {
                    if let Some(error) = failure {
                        return Err(error.into());
                    }
                    let polled = match polled {
                        Ok(polled) => polled,
                        Err(error) => {
                            self.persist_telegram_safety(&error, now_ms)?;
                            return Err(error.into());
                        }
                    };
                    if let Some(offset) = polled.next_offset {
                        self.checkpoint_telegram_stream(offset, now_ms)?;
                    }
                }
                SourceResult::Armada(message) => {
                    let Some(message) = (*message)? else {
                        return Err(ServiceError::ArmadaListenerStopped);
                    };
                    match message {
                        ConcordInbound::Message(message) => {
                            self.accept_armada(*message)?;
                        }
                        ConcordInbound::Reaction(update) => {
                            self.accept_reaction(update)?;
                        }
                    }
                }
            }
            self.dispatch_ready().await?;
        }
    }

    async fn dispatch_direction(
        &mut self,
        route_id: &RouteId,
        direction: DeliveryDirection,
    ) -> Result<DispatchReport, ServiceError> {
        let mut report = DispatchReport::default();
        loop {
            let Some(job) = self
                .store
                .pending_jobs(route_id, direction, 1)?
                .into_iter()
                .next()
            else {
                break;
            };
            let now_ms = now_ms(&self.clock)?;
            if job.delivery_state == DeliveryState::DeadLetter
                || (job.delivery_state == DeliveryState::RetryWait
                    && job.next_attempt_at_ms > now_ms)
            {
                break;
            }
            let result = match direction {
                DeliveryDirection::ArmadaToTelegram => {
                    let payload = decode_telegram_payload(&job.payload)?;
                    self.dispatch_to_telegram(route_id, payload).await
                }
                DeliveryDirection::TelegramToArmada => {
                    let payload = decode_armada_payload(&job.payload)?;
                    self.dispatch_to_armada(route_id, payload).await
                }
            };
            match result {
                Ok(success) => {
                    if success.creates_link {
                        self.store.mark_delivered_with_expiry(
                            job.id,
                            &success.destination_ids,
                            now_ms,
                            success.expires_at_ms,
                        )?;
                    } else {
                        self.store.mark_mutation_delivered(job.id, now_ms)?;
                    }
                    report.delivered += 1;
                }
                Err(DestinationFailure::Store(error)) => return Err(error.into()),
                Err(failure) => {
                    let disposition = classify_failure(&failure);
                    let attempt = job.attempt_count.saturating_add(1);
                    if disposition.retryable && attempt < self.settings.retry_policy.max_attempts {
                        let delay_ms = disposition.override_delay_ms.unwrap_or_else(|| {
                            retry_delay_ms(job.id, attempt, self.settings.retry_policy)
                        });
                        let next_attempt_at_ms =
                            now_ms.saturating_add(i64::try_from(delay_ms).unwrap_or(i64::MAX));
                        self.store.schedule_retry(
                            job.id,
                            next_attempt_at_ms,
                            disposition.error_class,
                            disposition.possible_duplicate,
                            now_ms,
                        )?;
                        report.retried += 1;
                    } else {
                        self.store.mark_dead_letter(
                            job.id,
                            disposition.error_class,
                            disposition.possible_duplicate,
                            now_ms,
                        )?;
                        report.dead_lettered += 1;
                    }
                    if let Some(reason) = quarantine_reason(&failure) {
                        self.store.set_route_state(
                            route_id,
                            RouteState::Quarantined,
                            Some(reason),
                            now_ms,
                        )?;
                    }
                    break;
                }
            }
        }
        Ok(report)
    }

    async fn dispatch_to_telegram(
        &mut self,
        route_id: &RouteId,
        mut payload: TelegramDeliveryPayload,
    ) -> Result<DispatchSuccess, DestinationFailure> {
        if payload.operation == DeliveryOperation::ReactionSummary {
            let reaction = payload
                .reaction
                .as_ref()
                .ok_or(DestinationFailure::InvalidPayload)?;
            return self
                .dispatch_reaction_summary_to_telegram(route_id, reaction)
                .await;
        }
        if payload.operation != DeliveryOperation::Create {
            return self.dispatch_telegram_mutation(route_id, &payload).await;
        }
        let mut staged_attachments = Vec::with_capacity(payload.attachments.len());
        let mut staged_bytes = 0_u64;
        let mut integrity_failures = Vec::new();
        for attachment in &payload.attachments {
            let staged = PrivateTempFile::new(
                &self.settings.attachment_temp_root,
                attachment.file_name.as_deref(),
            )
            .map_err(DestinationFailure::Attachment)?;
            let downloaded = self
                .concord
                .download_attachment(attachment, staged.path(), self.settings.max_file_bytes)
                .await;
            if matches!(downloaded, Err(ConcordError::AttachmentIntegrity)) {
                integrity_failures.push(unavailable_attachment_kind(attachment));
                continue;
            }
            downloaded.map_err(DestinationFailure::Concord)?;
            let metadata = staged
                .inspect(self.settings.max_file_bytes)
                .map_err(DestinationFailure::Attachment)?;
            staged_bytes = staged_bytes.saturating_add(metadata.size_bytes);
            if staged_bytes > self.settings.max_file_bytes {
                return Err(DestinationFailure::Attachment(AttachmentError::TooLarge));
            }
            staged_attachments.push((staged, metadata));
        }
        if !integrity_failures.is_empty() {
            append_attachment_unavailable_notice(&mut payload.chunks, &integrity_failures);
        }
        let caption = (staged_attachments.len() == 1
            && payload.chunks.len() == 1
            && payload.reply_destination_id.is_none()
            && payload.chunks[0].text.chars().count() <= MAX_CAPTION_CHARACTERS)
            .then(|| payload.chunks[0].clone());
        let mut destination_ids = Vec::new();
        if caption.is_none() || staged_attachments.is_empty() {
            for (index, chunk) in payload.chunks.iter().enumerate() {
                let receipt = if index == 0 {
                    if let Some(reply_to) = payload.reply_destination_id.as_deref() {
                        self.telegram
                            .send_reply(route_id, reply_to, &chunk.text, &chunk.entities)
                            .await
                    } else {
                        self.telegram
                            .send_rich_text(route_id, &chunk.text, &chunk.entities)
                            .await
                    }
                } else {
                    self.telegram
                        .send_rich_text(route_id, &chunk.text, &chunk.entities)
                        .await
                }
                .map_err(DestinationFailure::Telegram)?;
                destination_ids.push(receipt.destination_message_id);
            }
        }
        for (index, (staged, metadata)) in staged_attachments.iter().enumerate() {
            let receipt = self
                .telegram
                .send_attachment(
                    route_id,
                    staged.path(),
                    metadata,
                    (index == 0).then_some(caption.as_ref()).flatten(),
                )
                .await
                .map_err(DestinationFailure::Telegram)?;
            destination_ids.push(receipt.destination_message_id);
        }
        Ok(DispatchSuccess {
            destination_ids: destination_ids.join(","),
            creates_link: true,
            expires_at_ms: payload.expires_at_ms,
        })
    }

    async fn dispatch_telegram_mutation(
        &mut self,
        route_id: &RouteId,
        payload: &TelegramDeliveryPayload,
    ) -> Result<DispatchSuccess, DestinationFailure> {
        if payload.operation == DeliveryOperation::Delete {
            for destination_id in &payload.target_destination_ids {
                self.telegram
                    .delete_message(route_id, destination_id)
                    .await
                    .map_err(DestinationFailure::Telegram)?;
            }
        } else {
            let Some(chunk) = payload.chunks.first() else {
                return Err(DestinationFailure::InvalidPayload);
            };
            let Some((first, remaining)) = payload.target_destination_ids.split_first() else {
                return Err(DestinationFailure::InvalidPayload);
            };
            self.telegram
                .edit_message(route_id, first, &chunk.text, &chunk.entities)
                .await
                .map_err(DestinationFailure::Telegram)?;
            for obsolete in remaining {
                self.telegram
                    .delete_message(route_id, obsolete)
                    .await
                    .map_err(DestinationFailure::Telegram)?;
            }
        }
        Ok(DispatchSuccess {
            destination_ids: payload.target_destination_ids.join(","),
            creates_link: false,
            expires_at_ms: None,
        })
    }

    async fn dispatch_to_armada(
        &mut self,
        route_id: &RouteId,
        payload: ArmadaDeliveryPayload,
    ) -> Result<DispatchSuccess, DestinationFailure> {
        if payload.operation == DeliveryOperation::ReactionSummary {
            let reaction = payload
                .reaction
                .as_ref()
                .ok_or(DestinationFailure::InvalidPayload)?;
            return self
                .dispatch_reaction_summary_to_armada(route_id, reaction)
                .await;
        }
        if payload.operation == DeliveryOperation::Delete {
            for destination_id in &payload.target_destination_ids {
                self.concord
                    .delete_message(route_id, destination_id)
                    .await
                    .map_err(DestinationFailure::Concord)?;
            }
            return Ok(DispatchSuccess {
                destination_ids: payload.target_destination_ids.join(","),
                creates_link: false,
                expires_at_ms: None,
            });
        }
        if payload.operation == DeliveryOperation::Edit {
            let Some(first) = payload.target_destination_ids.first() else {
                return Err(DestinationFailure::InvalidPayload);
            };
            self.concord
                .edit_message(route_id, first, &payload.text)
                .await
                .map_err(DestinationFailure::Concord)?;
            for obsolete in payload.target_destination_ids.iter().skip(1) {
                self.concord
                    .delete_message(route_id, obsolete)
                    .await
                    .map_err(DestinationFailure::Concord)?;
            }
            return Ok(DispatchSuccess {
                destination_ids: payload.target_destination_ids.join(","),
                creates_link: false,
                expires_at_ms: None,
            });
        }
        let mut staged_attachments = Vec::with_capacity(payload.attachments.len());
        let mut staged_bytes = 0_u64;
        for attachment in &payload.attachments {
            let staged = PrivateTempFile::new(
                &self.settings.attachment_temp_root,
                attachment.file_name.as_deref(),
            )
            .map_err(DestinationFailure::Attachment)?;
            self.telegram
                .download_attachment(attachment, staged.path(), self.settings.max_file_bytes)
                .await
                .map_err(DestinationFailure::Telegram)?;
            let metadata = staged
                .inspect(self.settings.max_file_bytes)
                .map_err(DestinationFailure::Attachment)?;
            staged_bytes = staged_bytes.saturating_add(metadata.size_bytes);
            if staged_bytes > self.settings.max_file_bytes {
                return Err(DestinationFailure::Attachment(AttachmentError::TooLarge));
            }
            staged_attachments.push((staged, metadata));
        }
        let mut destination_ids = Vec::new();
        if !payload.text.is_empty() {
            let receipt = if let Some(reply_to) = payload.reply_destination_id.as_deref() {
                self.concord
                    .send_reply(route_id, reply_to, &payload.text)
                    .await
            } else {
                self.concord.send_text(route_id, &payload.text).await
            }
            .map_err(DestinationFailure::Concord)?;
            destination_ids.push(receipt.destination_event_id);
        }
        for (staged, metadata) in &staged_attachments {
            let receipt = self
                .concord
                .send_attachment(route_id, staged.path(), metadata)
                .await
                .map_err(DestinationFailure::Concord)?;
            destination_ids.push(receipt.destination_event_id);
        }
        Ok(DispatchSuccess {
            destination_ids: destination_ids.join(","),
            creates_link: true,
            expires_at_ms: None,
        })
    }

    async fn dispatch_reaction_summary_to_telegram(
        &mut self,
        route_id: &RouteId,
        payload: &ReactionDeliveryPayload,
    ) -> Result<DispatchSuccess, DestinationFailure> {
        let message_job_id = JobId::try_from(payload.message_job_id)
            .map_err(|_| DestinationFailure::InvalidPayload)?;
        let state = self
            .store
            .apply_reaction_change(
                message_job_id,
                DeliveryDirection::ArmadaToTelegram,
                &payload.destination_target_id,
                &payload.change,
            )
            .map_err(DestinationFailure::Store)?;
        let desired = render_reaction_summary(DeliveryDirection::ArmadaToTelegram, &state.entries);
        let destination_id = match (state.destination_summary_id.as_deref(), desired.as_deref()) {
            (None, None) => None,
            (None, Some(text)) => {
                let receipt = self
                    .telegram
                    .send_reply(route_id, &state.destination_target_id, text, &[])
                    .await
                    .map_err(DestinationFailure::Telegram)?;
                self.store
                    .set_reaction_summary_projection(
                        message_job_id,
                        DeliveryDirection::ArmadaToTelegram,
                        Some(&receipt.destination_message_id),
                        text,
                    )
                    .map_err(DestinationFailure::Store)?;
                Some(receipt.destination_message_id)
            }
            (Some(summary_id), Some(text)) if state.rendered_text != text => {
                match self
                    .telegram
                    .edit_message(route_id, summary_id, text, &[])
                    .await
                {
                    Ok(()) | Err(TelegramError::MutationAlreadyApplied) => {}
                    Err(error) => return Err(DestinationFailure::Telegram(error)),
                }
                self.store
                    .set_reaction_summary_projection(
                        message_job_id,
                        DeliveryDirection::ArmadaToTelegram,
                        Some(summary_id),
                        text,
                    )
                    .map_err(DestinationFailure::Store)?;
                Some(summary_id.to_owned())
            }
            (Some(summary_id), Some(_)) => Some(summary_id.to_owned()),
            (Some(summary_id), None) => {
                match self.telegram.delete_message(route_id, summary_id).await {
                    Ok(()) | Err(TelegramError::MutationAlreadyApplied) => {}
                    Err(error) => return Err(DestinationFailure::Telegram(error)),
                }
                self.store
                    .set_reaction_summary_projection(
                        message_job_id,
                        DeliveryDirection::ArmadaToTelegram,
                        None,
                        "",
                    )
                    .map_err(DestinationFailure::Store)?;
                None
            }
        };
        Ok(DispatchSuccess {
            destination_ids: destination_id.unwrap_or_default(),
            creates_link: false,
            expires_at_ms: None,
        })
    }

    async fn dispatch_reaction_summary_to_armada(
        &mut self,
        route_id: &RouteId,
        payload: &ReactionDeliveryPayload,
    ) -> Result<DispatchSuccess, DestinationFailure> {
        let message_job_id = JobId::try_from(payload.message_job_id)
            .map_err(|_| DestinationFailure::InvalidPayload)?;
        let state = self
            .store
            .apply_reaction_change(
                message_job_id,
                DeliveryDirection::TelegramToArmada,
                &payload.destination_target_id,
                &payload.change,
            )
            .map_err(DestinationFailure::Store)?;
        let desired = render_reaction_summary(DeliveryDirection::TelegramToArmada, &state.entries);
        let destination_id = match (state.destination_summary_id.as_deref(), desired.as_deref()) {
            (None, None) => None,
            (None, Some(text)) => {
                let receipt = self
                    .concord
                    .send_reply(route_id, &state.destination_target_id, text)
                    .await
                    .map_err(DestinationFailure::Concord)?;
                self.store
                    .set_reaction_summary_projection(
                        message_job_id,
                        DeliveryDirection::TelegramToArmada,
                        Some(&receipt.destination_event_id),
                        text,
                    )
                    .map_err(DestinationFailure::Store)?;
                Some(receipt.destination_event_id)
            }
            (Some(summary_id), Some(text)) if state.rendered_text != text => {
                self.concord
                    .edit_message(route_id, summary_id, text)
                    .await
                    .map_err(DestinationFailure::Concord)?;
                self.store
                    .set_reaction_summary_projection(
                        message_job_id,
                        DeliveryDirection::TelegramToArmada,
                        Some(summary_id),
                        text,
                    )
                    .map_err(DestinationFailure::Store)?;
                Some(summary_id.to_owned())
            }
            (Some(summary_id), Some(_)) => Some(summary_id.to_owned()),
            (Some(summary_id), None) => {
                self.concord
                    .delete_message(route_id, summary_id)
                    .await
                    .map_err(DestinationFailure::Concord)?;
                self.store
                    .set_reaction_summary_projection(
                        message_job_id,
                        DeliveryDirection::TelegramToArmada,
                        None,
                        "",
                    )
                    .map_err(DestinationFailure::Store)?;
                None
            }
        };
        Ok(DispatchSuccess {
            destination_ids: destination_id.unwrap_or_default(),
            creates_link: false,
            expires_at_ms: None,
        })
    }

    async fn dispatch_due_deletions(&mut self) -> Result<(), ServiceError> {
        let now_ms = now_ms(&self.clock)?;
        for deletion in self.store.due_deletions(now_ms)? {
            let mut completed = true;
            for destination_id in deletion.destination_message_id.split(',') {
                if destination_id.is_empty()
                    || self
                        .telegram
                        .delete_message(&deletion.route_id, destination_id)
                        .await
                        .is_err()
                {
                    completed = false;
                    break;
                }
            }
            if completed {
                self.store.complete_deletion(deletion.id, now_ms)?;
            }
        }
        Ok(())
    }

    /// Whether all routes are active and the durable queue has admission room.
    ///
    /// # Errors
    ///
    /// Returns a durable-state query failure.
    pub fn is_ready(&self) -> Result<bool, ServiceError> {
        if self.store.pending_job_count()?
            >= u64::try_from(self.settings.max_pending_jobs)
                .map_err(|_| ServiceError::InvalidSettings)?
            || !self.store.dead_letters()?.is_empty()
        {
            return Ok(false);
        }
        for route_id in &self.settings.routes {
            if self.store.route_state(route_id)? != RouteState::Active {
                return Ok(false);
            }
            for direction in [
                DeliveryDirection::ArmadaToTelegram,
                DeliveryDirection::TelegramToArmada,
            ] {
                if self
                    .store
                    .pending_jobs(route_id, direction, 1)?
                    .first()
                    .is_some_and(|job| job.delivery_state == DeliveryState::RetryWait)
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Drains immediately eligible work until completion, no progress, or the
    /// supplied Tokio deadline. Intake must already be stopped by the caller.
    ///
    /// # Errors
    ///
    /// Returns a durable-state, payload, or clock failure.
    pub async fn shutdown_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<ShutdownReport, ServiceError> {
        let pending_before = self.store.pending_job_count()?;
        let drain = async {
            let mut delivered = 0;
            loop {
                let report = self.dispatch_ready().await?;
                delivered += report.delivered;
                if report.delivered == 0 {
                    let Some(delay) = self.next_retry_delay()? else {
                        break;
                    };
                    tokio::time::sleep(delay).await;
                }
            }
            Ok::<usize, ServiceError>(delivered)
        };
        let (drained, timed_out) = match tokio::time::timeout_at(deadline, drain).await {
            Ok(result) => (Some(result?), false),
            Err(_) => (None, true),
        };
        let remaining = self.store.pending_job_count()?;
        let delivered = drained.unwrap_or_else(|| {
            usize::try_from(pending_before.saturating_sub(remaining)).unwrap_or(usize::MAX)
        });
        Ok(ShutdownReport {
            delivered,
            remaining,
            timed_out,
        })
    }

    fn next_retry_delay(&self) -> Result<Option<Duration>, ServiceError> {
        let now_ms = now_ms(&self.clock)?;
        let mut minimum = None;
        for route_id in &self.settings.routes {
            if self.store.route_state(route_id)? != RouteState::Active {
                continue;
            }
            for direction in [
                DeliveryDirection::ArmadaToTelegram,
                DeliveryDirection::TelegramToArmada,
            ] {
                let Some(job) = self
                    .store
                    .pending_jobs(route_id, direction, 1)?
                    .into_iter()
                    .next()
                else {
                    continue;
                };
                if job.delivery_state != DeliveryState::RetryWait {
                    continue;
                }
                let delay_ms = job.next_attempt_at_ms.saturating_sub(now_ms).max(0);
                let delay = Duration::from_millis(u64::try_from(delay_ms).unwrap_or(u64::MAX));
                minimum = Some(minimum.map_or(delay, |current: Duration| current.min(delay)));
            }
        }
        Ok(minimum)
    }

    /// Retries an inspectable dead letter immediately.
    ///
    /// # Errors
    ///
    /// Returns an error unless the job is dead-lettered or the clock/store fails.
    pub fn retry_dead_letter(&mut self, job_id: JobId) -> Result<(), ServiceError> {
        self.store.retry_dead_letter(job_id, now_ms(&self.clock)?)?;
        Ok(())
    }

    /// Lists dead letters without exposing message plaintext.
    ///
    /// # Errors
    ///
    /// Returns a durable-state query failure.
    pub fn dead_letters(&self) -> Result<Vec<DeliveryAttemptState>, ServiceError> {
        Ok(self.store.dead_letters()?)
    }

    /// Quarantines a route until explicit operator resume.
    ///
    /// # Errors
    ///
    /// Returns a durable-state or clock failure.
    pub fn quarantine_route(
        &mut self,
        route_id: &RouteId,
        reason: &str,
    ) -> Result<(), ServiceError> {
        self.store.set_route_state(
            route_id,
            RouteState::Quarantined,
            Some(reason),
            now_ms(&self.clock)?,
        )?;
        Ok(())
    }

    /// Resumes an operator-reviewed route.
    ///
    /// # Errors
    ///
    /// Returns a durable-state or clock failure.
    pub fn resume_route(&mut self, route_id: &RouteId) -> Result<(), ServiceError> {
        self.store
            .set_route_state(route_id, RouteState::Active, None, now_ms(&self.clock)?)?;
        Ok(())
    }

    fn checkpoint_telegram_stream(&mut self, offset: u64, now_ms: i64) -> Result<(), StoreError> {
        for route_id in &self.settings.routes {
            self.store.checkpoint_cursor(
                route_id,
                DeliveryDirection::TelegramToArmada,
                &offset.to_string(),
                now_ms,
            )?;
        }
        Ok(())
    }

    fn persist_telegram_safety(
        &mut self,
        error: &TelegramError,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        match error {
            TelegramError::ChatMigrated { route_ids, .. } => {
                for route_id in route_ids {
                    self.store.set_route_state(
                        route_id,
                        RouteState::Quarantined,
                        Some("telegram_chat_migrated"),
                        now_ms,
                    )?;
                }
            }
            TelegramError::RoutePaused { route_id } => self.store.set_route_state(
                route_id,
                RouteState::Quarantined,
                Some("telegram_route_paused"),
                now_ms,
            )?,
            _ => {}
        }
        Ok(())
    }

    fn persist_concord_safety(
        &mut self,
        error: &ConcordError,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if let ConcordError::RouteNotReady { route_id, .. } = error {
            self.store.set_route_state(
                route_id,
                RouteState::Quarantined,
                Some("concord_route_not_ready"),
                now_ms,
            )?;
        }
        Ok(())
    }

    fn validate_settings(&self) -> Result<(), ServiceError> {
        if self.settings.routes.is_empty()
            || self.settings.own_armada_npub.is_empty()
            || self.settings.telegram_bot_id == 0
            || self.settings.max_catchup_events == 0
            || self.settings.max_catchup_age_ms <= 0
            || self.settings.max_text_bytes == 0
            || self.settings.max_file_bytes == 0
            || self.settings.attachment_temp_root.as_os_str().is_empty()
            || self.settings.max_pending_jobs == 0
            || self.settings.retry_policy.base_delay.is_zero()
            || self.settings.retry_policy.max_delay < self.settings.retry_policy.base_delay
            || self.settings.retry_policy.max_attempts == 0
            || self.settings.retry_policy.jitter_percent > 50
        {
            return Err(ServiceError::InvalidSettings);
        }
        Ok(())
    }

    /// Exposes the store for diagnostics and deterministic integration tests.
    #[must_use]
    pub const fn store(&self) -> &Store {
        &self.store
    }

    /// Exposes the Telegram port for diagnostics and deterministic tests.
    #[must_use]
    pub const fn telegram(&self) -> &T {
        &self.telegram
    }

    /// Mutable port access for deterministic harnesses and lifecycle wiring.
    #[doc(hidden)]
    pub const fn telegram_mut(&mut self) -> &mut T {
        &mut self.telegram
    }

    /// Exposes the Concord port for diagnostics and deterministic tests.
    #[must_use]
    pub const fn concord(&self) -> &C {
        &self.concord
    }

    /// Mutable port access for deterministic harnesses and lifecycle wiring.
    #[doc(hidden)]
    pub const fn concord_mut(&mut self) -> &mut C {
        &mut self.concord
    }
}

struct BaselineTelegramSink;

enum DestinationFailure {
    Telegram(TelegramError),
    Concord(ConcordError),
    Attachment(AttachmentError),
    Store(StoreError),
    InvalidPayload,
}

struct FailureDisposition {
    retryable: bool,
    error_class: &'static str,
    override_delay_ms: Option<u64>,
    possible_duplicate: bool,
}

fn classify_failure(failure: &DestinationFailure) -> FailureDisposition {
    match failure {
        DestinationFailure::Telegram(TelegramError::RateLimited {
            retry_after_seconds,
        }) => FailureDisposition {
            retryable: true,
            error_class: "telegram_rate_limited",
            override_delay_ms: Some(retry_after_seconds.saturating_mul(1_000)),
            possible_duplicate: false,
        },
        DestinationFailure::Telegram(TelegramError::Retryable { .. }) => FailureDisposition {
            retryable: true,
            error_class: "telegram_retryable",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::Telegram(TelegramError::Timeout) => FailureDisposition {
            retryable: true,
            error_class: "telegram_timeout",
            override_delay_ms: None,
            possible_duplicate: true,
        },
        DestinationFailure::Telegram(TelegramError::Transport) => FailureDisposition {
            retryable: true,
            error_class: "telegram_transport",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::Concord(ConcordError::Sdk { .. }) => FailureDisposition {
            retryable: true,
            error_class: "concord_sdk",
            override_delay_ms: None,
            possible_duplicate: true,
        },
        DestinationFailure::Concord(ConcordError::AttachmentUnavailable) => FailureDisposition {
            retryable: true,
            error_class: "attachment_source_unavailable",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::Attachment(AttachmentError::TemporaryStorage) => FailureDisposition {
            retryable: true,
            error_class: "attachment_staging",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::Attachment(_) => FailureDisposition {
            retryable: false,
            error_class: "attachment_terminal",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::InvalidPayload => FailureDisposition {
            retryable: false,
            error_class: "mutation_payload",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::Telegram(_) => FailureDisposition {
            retryable: false,
            error_class: "telegram_terminal",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::Concord(_) => FailureDisposition {
            retryable: false,
            error_class: "concord_terminal",
            override_delay_ms: None,
            possible_duplicate: false,
        },
        DestinationFailure::Store(_) => unreachable!("store failures leave the dispatcher"),
    }
}

fn quarantine_reason(failure: &DestinationFailure) -> Option<&'static str> {
    match failure {
        DestinationFailure::Telegram(TelegramError::ChatMigrated { .. }) => {
            Some("telegram_chat_migrated")
        }
        DestinationFailure::Telegram(TelegramError::RoutePaused { .. }) => {
            Some("telegram_route_paused")
        }
        DestinationFailure::Concord(ConcordError::RouteNotReady { .. }) => {
            Some("concord_route_not_ready")
        }
        _ => None,
    }
}

/// Calculates capped exponential backoff with stable per-job/attempt jitter.
#[must_use]
pub fn retry_delay_ms(job_id: JobId, attempt: u32, policy: RetryPolicy) -> u64 {
    let base = u64::try_from(policy.base_delay.as_millis()).unwrap_or(u64::MAX);
    let cap = u64::try_from(policy.max_delay.as_millis()).unwrap_or(u64::MAX);
    let shift = attempt.saturating_sub(1).min(63);
    let exponential = base.saturating_mul(1_u64 << shift).min(cap);
    let span = exponential.saturating_mul(u64::from(policy.jitter_percent)) / 100;
    if span == 0 {
        return exponential;
    }
    let mut seed = u64::from_ne_bytes(job_id.get().to_ne_bytes())
        ^ u64::from(attempt).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    seed ^= seed >> 30;
    seed = seed.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    seed ^= seed >> 27;
    let width = span.saturating_mul(2).saturating_add(1);
    let offset = i128::from(seed % width) - i128::from(span);
    u64::try_from((i128::from(exponential) + offset).max(0))
        .unwrap_or(u64::MAX)
        .min(cap)
}

fn add_dispatch_report(total: &mut DispatchReport, addition: DispatchReport) {
    total.delivered += addition.delivered;
    total.retried += addition.retried;
    total.dead_lettered += addition.dead_lettered;
}

#[async_trait]
impl TelegramUpdateSink for BaselineTelegramSink {
    async fn accept(
        &mut self,
        _message: InboundTelegramMessage,
    ) -> Result<(), TelegramAcceptError> {
        Ok(())
    }
}

struct DurableTelegramSink<'a> {
    store: &'a mut Store,
    settings: &'a BridgeServiceSettings,
    now_ms: i64,
    failure: Option<StoreError>,
}

#[async_trait]
impl TelegramUpdateSink for DurableTelegramSink<'_> {
    async fn accept(&mut self, message: InboundTelegramMessage) -> Result<(), TelegramAcceptError> {
        let result = accept_event(
            self.store,
            self.settings,
            message.event,
            DeliveryDirection::TelegramToArmada,
            Some(message.next_offset.to_string()),
            self.now_ms,
        );
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                self.failure = Some(error);
                Err(TelegramAcceptError)
            }
        }
    }

    async fn accept_reaction(
        &mut self,
        update: BridgeReactionUpdate,
        next_offset: u64,
    ) -> Result<(), TelegramAcceptError> {
        let result = accept_reaction_update(
            self.store,
            self.settings,
            update,
            Some(next_offset.to_string()),
            self.now_ms,
        );
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                self.failure = Some(error);
                Err(TelegramAcceptError)
            }
        }
    }
}

fn accept_event(
    store: &mut Store,
    settings: &BridgeServiceSettings,
    event: BridgeEvent,
    direction: DeliveryDirection,
    cursor: Option<String>,
    now_ms: i64,
) -> Result<IntakeOutcome, StoreError> {
    if !settings.route_is_configured(&event.route_id) {
        return Ok(IntakeOutcome::Ignored);
    }
    let author_id = stable_sender_id(&event);
    let base_supported = source_matches_direction(event.source, direction)
        && event.body.len() <= settings.max_text_bytes
        && source_id_matches(event.source, &event.source_event_id)
        && sender_is_relayable(&event, settings)
        && author_id.is_some();
    let operation = match event.mutation {
        Mutation::Create => DeliveryOperation::Create,
        Mutation::Edit => DeliveryOperation::Edit,
        Mutation::Delete | Mutation::Expire => DeliveryOperation::Delete,
    };
    let create_supported = operation == DeliveryOperation::Create
        && (!event.body.is_empty() || !event.attachments.is_empty())
        && attachments_are_relayable(&event, settings)
        && event.expires_at_ms.is_none_or(|expiry| expiry > now_ms);
    let mutation_supported = operation != DeliveryOperation::Create
        && event.attachments.is_empty()
        && (operation == DeliveryOperation::Delete || !event.body.is_empty());

    let mut target_destination_ids = Vec::new();
    if mutation_supported {
        let target = event.mutation_target.as_ref().filter(|target| {
            target.source == event.source
                && source_id_matches(target.source, &target.source_event_id)
        });
        if let Some(link) = target
            .map(|target| store.message_link(&event.route_id, direction, &target.source_event_id))
            .transpose()?
            .flatten()
            .filter(|link| Some(link.source_author_id.as_str()) == author_id.as_deref())
            && (operation != DeliveryOperation::Edit || editable_link(store, &link, direction)?)
        {
            target_destination_ids = link
                .destination_message_id
                .split(',')
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .collect();
        }
    }

    let reply_destination_id = if operation == DeliveryOperation::Create {
        event
            .reply_to
            .as_ref()
            .filter(|reply| {
                reply.source == event.source
                    && source_id_matches(reply.source, &reply.source_event_id)
            })
            .map(|reply| store.message_link(&event.route_id, direction, &reply.source_event_id))
            .transpose()?
            .flatten()
            .and_then(|link| {
                link.destination_message_id
                    .split(',')
                    .next()
                    .map(str::to_owned)
            })
    } else {
        None
    };

    let supported = base_supported
        && (create_supported || (mutation_supported && !target_destination_ids.is_empty()));
    let Some(payload) = supported
        .then(|| {
            render_delivery_payload(
                &event,
                operation,
                target_destination_ids,
                reply_destination_id,
            )
        })
        .flatten()
    else {
        if let Some(cursor) = cursor {
            store.checkpoint_cursor(&event.route_id, direction, &cursor, now_ms)?;
        }
        return Ok(IntakeOutcome::Ignored);
    };

    let mut delivery = NewDelivery::new(
        event.route_id,
        direction,
        event.source_event_id,
        payload,
        now_ms,
    )
    .with_source_author(author_id.expect("supported events have a stable author"));
    if let Some(cursor) = cursor {
        delivery = delivery.with_cursor(cursor);
    }
    Ok(
        match store.accept_inbound_bounded(&delivery, settings.max_pending_jobs)? {
            AcceptOutcome::Enqueued(_) => IntakeOutcome::Enqueued,
            AcceptOutcome::Duplicate(_) => IntakeOutcome::Duplicate,
        },
    )
}

#[allow(clippy::too_many_lines)]
fn accept_reaction_update(
    store: &mut Store,
    settings: &BridgeServiceSettings,
    mut update: BridgeReactionUpdate,
    cursor: Option<String>,
    now_ms: i64,
) -> Result<IntakeOutcome, StoreError> {
    if !settings.route_is_configured(&update.route_id)
        || !source_id_matches(update.source, &update.source_event_id)
        || !source_id_matches(update.source, &update.target_source_event_id)
    {
        return Ok(IntakeOutcome::Ignored);
    }
    match &mut update.change {
        ReactionChange::Snapshot { entries } => {
            entries.retain(|entry| {
                !(update.source == Source::Armada && entry.reactor_id == settings.own_armada_npub
                    || update.source == Source::Telegram
                        && entry.reactor_id
                            == format!("telegram-user:{}", settings.telegram_bot_id))
            });
        }
        ReactionChange::ActorSelection { reactor_id, .. }
            if update.source == Source::Telegram
                && reactor_id == &format!("telegram-user:{}", settings.telegram_bot_id) =>
        {
            return Ok(IntakeOutcome::Ignored);
        }
        ReactionChange::ActorSelection { .. } => {}
    }
    if !reaction_change_is_relayable(&update.change) {
        return Ok(IntakeOutcome::Ignored);
    }

    let direction = match update.source {
        Source::Armada => DeliveryDirection::ArmadaToTelegram,
        Source::Telegram => DeliveryDirection::TelegramToArmada,
    };
    let opposite = match direction {
        DeliveryDirection::ArmadaToTelegram => DeliveryDirection::TelegramToArmada,
        DeliveryDirection::TelegramToArmada => DeliveryDirection::ArmadaToTelegram,
    };
    let direct = store.message_link(&update.route_id, direction, &update.target_source_event_id)?;
    let (link, destination_target_id) = if let Some(link) = direct {
        let Some(destination) = link
            .destination_message_id
            .split(',')
            .find(|id| !id.is_empty())
            .map(str::to_owned)
        else {
            return Ok(IntakeOutcome::Ignored);
        };
        (link, destination)
    } else {
        let target_destination_id =
            source_id_for_destination_lookup(update.source, &update.target_source_event_id);
        let Some(link) =
            store.message_link_by_destination(&update.route_id, opposite, target_destination_id)?
        else {
            return Ok(IntakeOutcome::Ignored);
        };
        let destination = source_id_for_destination_lookup(
            match direction {
                DeliveryDirection::ArmadaToTelegram => Source::Telegram,
                DeliveryDirection::TelegramToArmada => Source::Armada,
            },
            &link.source_message_id,
        )
        .to_owned();
        (link, destination)
    };

    let reaction = ReactionDeliveryPayload {
        message_job_id: link.job_id.get(),
        destination_target_id,
        change: update.change,
    };
    let payload = match direction {
        DeliveryDirection::ArmadaToTelegram => encode_telegram_payload(&TelegramDeliveryPayload {
            chunks: Vec::new(),
            attachments: Vec::new(),
            operation: DeliveryOperation::ReactionSummary,
            target_destination_ids: Vec::new(),
            reply_destination_id: None,
            expires_at_ms: None,
            reaction: Some(reaction),
        }),
        DeliveryDirection::TelegramToArmada => encode_armada_payload(&ArmadaDeliveryPayload {
            text: String::new(),
            attachments: Vec::new(),
            operation: DeliveryOperation::ReactionSummary,
            target_destination_ids: Vec::new(),
            reply_destination_id: None,
            reaction: Some(reaction),
        }),
    }
    .map_err(|_| StoreError::InvalidInput("reaction payload"))?;
    let mut delivery = NewDelivery::new(
        update.route_id,
        direction,
        update.source_event_id,
        payload,
        now_ms,
    )
    .with_source_author("reaction-summary");
    if let Some(cursor) = cursor {
        delivery = delivery.with_cursor(cursor);
    }
    Ok(
        match store.accept_inbound_bounded(&delivery, settings.max_pending_jobs)? {
            AcceptOutcome::Enqueued(_) => IntakeOutcome::Enqueued,
            AcceptOutcome::Duplicate(_) => IntakeOutcome::Duplicate,
        },
    )
}

fn source_id_for_destination_lookup(source: Source, source_event_id: &str) -> &str {
    match source {
        Source::Armada => source_event_id
            .strip_prefix("armada:")
            .unwrap_or(source_event_id),
        Source::Telegram => source_event_id,
    }
}

fn reaction_change_is_relayable(change: &ReactionChange) -> bool {
    let valid_emoji = |emoji: &str| {
        !emoji.is_empty()
            && emoji.len() <= 128
            && emoji.chars().count() <= 32
            && !emoji.chars().any(char::is_control)
    };
    let valid_actor = |id: &str, name: Option<&str>| {
        !id.is_empty()
            && id.len() <= 256
            && !id.chars().any(char::is_control)
            && name.is_none_or(|name| {
                !name.is_empty() && name.len() <= 256 && !name.chars().any(char::is_control)
            })
    };
    match change {
        ReactionChange::Snapshot { entries } => {
            entries.len() <= 256
                && entries.iter().all(|entry| {
                    valid_actor(&entry.reactor_id, entry.display_name.as_deref())
                        && valid_emoji(&entry.emoji)
                        && entry.count > 0
                })
        }
        ReactionChange::ActorSelection {
            reactor_id,
            display_name,
            emojis,
        } => {
            emojis.len() <= 16
                && valid_actor(reactor_id, display_name.as_deref())
                && emojis.iter().all(|emoji| valid_emoji(emoji))
        }
    }
}

fn source_matches_direction(source: Source, direction: DeliveryDirection) -> bool {
    matches!(
        (source, direction),
        (Source::Armada, DeliveryDirection::ArmadaToTelegram)
            | (Source::Telegram, DeliveryDirection::TelegramToArmada)
    )
}

fn editable_link(
    store: &Store,
    link: &crate::store::MessageLink,
    direction: DeliveryDirection,
) -> Result<bool, StoreError> {
    if link.destination_message_id.contains(',') {
        return Ok(false);
    }
    let Some(payload) = store.job(link.job_id)?.and_then(|job| job.payload) else {
        return Ok(false);
    };
    Ok(match direction {
        DeliveryDirection::ArmadaToTelegram => decode_telegram_payload(&payload)
            .is_ok_and(|payload| payload.attachments.is_empty() && payload.chunks.len() == 1),
        DeliveryDirection::TelegramToArmada => {
            decode_armada_payload(&payload).is_ok_and(|payload| payload.attachments.is_empty())
        }
    })
}

fn stable_sender_id(event: &BridgeEvent) -> Option<String> {
    let canonical = match (&event.source, &event.sender) {
        (Source::Armada, SenderIdentity::Armada { npub, .. }) if !npub.is_empty() => {
            Some(format!("armada:{npub}"))
        }
        (
            Source::Telegram,
            SenderIdentity::Telegram {
                user_id,
                sender_chat_id,
                ..
            },
        ) => user_id
            .map(|id| format!("telegram-user:{id}"))
            .or_else(|| sender_chat_id.map(|id| format!("telegram-chat:{id}"))),
        _ => None,
    }?;
    Some(format!("{:x}", Sha256::digest(canonical.as_bytes())))
}

fn source_id_matches(source: Source, source_event_id: &str) -> bool {
    match source {
        Source::Armada => source_event_id.starts_with("armada:"),
        Source::Telegram => source_event_id.starts_with("telegram:"),
    }
}

fn sender_is_relayable(event: &BridgeEvent, settings: &BridgeServiceSettings) -> bool {
    match (&event.source, &event.sender) {
        (Source::Armada, SenderIdentity::Armada { npub, .. }) => {
            !npub.is_empty() && npub != &settings.own_armada_npub
        }
        (
            Source::Telegram,
            SenderIdentity::Telegram {
                user_id,
                sender_chat_id,
                ..
            },
        ) => user_id.map_or_else(
            || sender_chat_id.is_some(),
            |id| id != settings.telegram_bot_id,
        ),
        _ => false,
    }
}

fn attachments_are_relayable(event: &BridgeEvent, settings: &BridgeServiceSettings) -> bool {
    if event.attachments.len() > MAX_ATTACHMENTS_PER_MESSAGE {
        return false;
    }
    let mut source_ids = std::collections::HashSet::with_capacity(event.attachments.len());
    let mut declared_total = 0_u64;
    event.attachments.iter().all(|attachment| {
        declared_total = declared_total.saturating_add(attachment.size_bytes);
        declared_total <= settings.max_file_bytes
            && !attachment.source_id.is_empty()
            && source_ids.insert(attachment.source_id.as_str())
            && validate_declared_size(attachment.size_bytes, settings.max_file_bytes).is_ok()
            && matches!(
                (&event.source, &attachment.locator),
                (Source::Armada, AttachmentLocator::Armada { .. })
                    | (Source::Telegram, AttachmentLocator::Telegram { .. })
            )
    })
}

fn unavailable_attachment_kind(attachment: &AttachmentRef) -> &'static str {
    let media_type = attachment.media_type.as_deref().unwrap_or_default();
    let file_name_is_gif = attachment
        .file_name
        .as_deref()
        .and_then(|name| name.rsplit_once('.'))
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("gif"));
    let locator_is_gif = matches!(
        &attachment.locator,
        AttachmentLocator::Armada { extension, .. } if extension.eq_ignore_ascii_case("gif")
    );
    if media_type.eq_ignore_ascii_case("image/gif") || file_name_is_gif || locator_is_gif {
        "GIF"
    } else if media_type.starts_with("image/") {
        "Photo"
    } else if media_type.starts_with("audio/") {
        "Audio"
    } else if media_type.starts_with("video/") {
        "Video"
    } else {
        "File"
    }
}

fn append_attachment_unavailable_notice(chunks: &mut Vec<TelegramMessage>, kinds: &[&str]) {
    let label = if kinds.len() == 1 {
        format!("[{} could not be transferred]", kinds[0])
    } else {
        format!("[{} attachments could not be transferred]", kinds.len())
    };
    let suffix = format!("\n{label}");
    if let Some(last) = chunks.last_mut()
        && last.text.chars().count() + suffix.chars().count() <= MAX_TEXT_CHARACTERS
    {
        last.text.push_str(&suffix);
    } else {
        chunks.push(TelegramMessage {
            text: label,
            entities: Vec::new(),
        });
    }
}

fn render_reaction_summary(
    direction: DeliveryDirection,
    entries: &[ReactionEntry],
) -> Option<String> {
    if entries.is_empty() {
        return None;
    }
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for entry in entries {
        let mut emoji = entry.emoji.clone();
        let mut actor = clean_header(entry.display_name.as_deref()).unwrap_or_else(|| {
            if entry.reactor_id.starts_with("telegram-anonymous:") {
                "anonymous".to_owned()
            } else {
                short_stable_id(&entry.reactor_id)
            }
        });
        if entry.count > 1 {
            actor = if actor == "anonymous" {
                format!("{} anonymous", entry.count)
            } else {
                format!("{actor} ×{}", entry.count)
            };
        }
        if direction == DeliveryDirection::TelegramToArmada {
            emoji = escape_armada_header(&emoji);
            actor = escape_armada_header(&actor);
        }
        grouped.entry(emoji).or_default().push(actor);
    }
    let mut rendered = match direction {
        DeliveryDirection::ArmadaToTelegram => "Reactions from Armada: ".to_owned(),
        DeliveryDirection::TelegramToArmada => "Reactions: ".to_owned(),
    };
    for (index, (emoji, mut actors)) in grouped.into_iter().enumerate() {
        actors.sort();
        actors.dedup();
        let segment = format!(
            "{}{emoji} {}",
            if index == 0 { "" } else { " · " },
            actors.join(", ")
        );
        if rendered.chars().count() + segment.chars().count() > MAX_TEXT_CHARACTERS - 2 {
            rendered.push('…');
            break;
        }
        rendered.push_str(&segment);
    }
    Some(rendered)
}

/// Produces the P06 plaintext attribution header and body.
#[must_use]
pub fn render_attributed(event: &BridgeEvent) -> Option<String> {
    let header = attribution_header(event)?;
    Some(format!("{header}\n{}", event.body))
}

fn attribution_header(event: &BridgeEvent) -> Option<String> {
    match &event.sender {
        SenderIdentity::Armada { npub, display_name } if event.source == Source::Armada => {
            let name =
                clean_header(display_name.as_deref()).unwrap_or_else(|| "Armada user".into());
            Some(format!("⛵ {name} ({})", short_stable_id(npub)))
        }
        SenderIdentity::Telegram {
            user_id,
            sender_chat_id,
            username,
            display_name,
        } if event.source == Source::Telegram => {
            // A stable Telegram identity is required for attribution and
            // deduplication, but its numeric ID is an internal detail and is
            // intentionally not exposed in the Armada message header.
            if user_id.is_none() && sender_chat_id.is_none() {
                return None;
            }
            let name = clean_header(username.as_deref())
                .map(|name| format!("@{}", name.trim_start_matches('@')))
                .or_else(|| clean_header(display_name.as_deref()))
                .unwrap_or_else(|| "Telegram user".into());
            let name = escape_armada_header(&name);
            Some(format!("✈️ {name}"))
        }
        _ => None,
    }
}

fn render_delivery_payload(
    event: &BridgeEvent,
    operation: DeliveryOperation,
    target_destination_ids: Vec<String>,
    reply_destination_id: Option<String>,
) -> Option<Vec<u8>> {
    if operation == DeliveryOperation::Delete {
        return match event.source {
            Source::Armada => encode_telegram_payload(&TelegramDeliveryPayload {
                chunks: Vec::new(),
                attachments: Vec::new(),
                operation,
                target_destination_ids,
                reply_destination_id: None,
                expires_at_ms: None,
                reaction: None,
            })
            .ok(),
            Source::Telegram => encode_armada_payload(&ArmadaDeliveryPayload {
                text: String::new(),
                attachments: Vec::new(),
                operation,
                target_destination_ids,
                reply_destination_id: None,
                reaction: None,
            })
            .ok(),
        };
    }
    let mut header = attribution_header(event)?;
    if let Some((index, total)) = event
        .attachments
        .first()
        .and_then(|attachment| Some((attachment.media_group_index?, attachment.media_group_total?)))
    {
        let _ = write!(header, "\n(album {index}/{total})");
    }
    let reply_prefix = (event.reply_to.is_some() && reply_destination_id.is_none())
        .then(|| fallback_reply_prefix(event));
    match event.source {
        Source::Armada => {
            let mut body = parse_armada_markdown(&event.body);
            if let Some(prefix) = reply_prefix {
                body = prepend_plain(&prefix, &body);
            }
            let header_characters = header.chars().count() + 1;
            let body_limit = MAX_TEXT_CHARACTERS
                .saturating_sub(header_characters)
                .saturating_sub(32)
                .max(1);
            let mut body_chunks = split_for_telegram(&body, body_limit);
            if body_chunks.is_empty() {
                body_chunks.push(RichText::plain(""));
            }
            let total = body_chunks.len();
            let chunks = body_chunks
                .iter()
                .enumerate()
                .map(|(index, chunk)| {
                    let prefix = if total == 1 {
                        format!("{header}\n")
                    } else {
                        format!("{header}\n(part {}/{total})\n", index + 1)
                    };
                    to_telegram(&prepend_plain(&prefix, chunk))
                })
                .collect::<Vec<_>>();
            if operation == DeliveryOperation::Edit && chunks.len() != 1 {
                return None;
            }
            encode_telegram_payload(&TelegramDeliveryPayload {
                chunks,
                attachments: event.attachments.clone(),
                operation,
                target_destination_ids,
                reply_destination_id,
                expires_at_ms: event.expires_at_ms,
                reaction: None,
            })
            .ok()
        }
        Source::Telegram => {
            let mut body = event
                .rich_text
                .clone()
                .filter(|rich| rich.text == event.body)
                .unwrap_or_else(|| RichText::plain(event.body.clone()));
            if let Some(prefix) = reply_prefix {
                body = prepend_plain(&prefix, &body);
            }
            encode_armada_payload(&ArmadaDeliveryPayload {
                text: format!("{header}\n{}", render_armada_markdown(&body)),
                attachments: event.attachments.clone(),
                operation,
                target_destination_ids,
                reply_destination_id,
                reaction: None,
            })
            .ok()
        }
    }
}

fn fallback_reply_prefix(event: &BridgeEvent) -> String {
    let quote = event.reply_quote.as_deref().map_or_else(
        || "earlier message".to_owned(),
        |quote| {
            let cleaned: String = quote
                .chars()
                .map(|character| {
                    if character.is_control() || is_invisible_override(character) {
                        ' '
                    } else {
                        character
                    }
                })
                .take(160)
                .collect();
            let cleaned = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
            if cleaned.is_empty() {
                "earlier message".to_owned()
            } else {
                format!("“{cleaned}”")
            }
        },
    );
    format!("↪ {quote}\n")
}

fn encode_telegram_payload(
    payload: &TelegramDeliveryPayload,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut encoded = TELEGRAM_PAYLOAD_PREFIX.to_vec();
    encoded.extend(serde_json::to_vec(payload)?);
    Ok(encoded)
}

fn decode_telegram_payload(payload: &[u8]) -> Result<TelegramDeliveryPayload, ServiceError> {
    if let Some(json) = payload.strip_prefix(TELEGRAM_PAYLOAD_PREFIX) {
        let decoded: TelegramDeliveryPayload =
            serde_json::from_slice(json).map_err(|_| ServiceError::InvalidPayload)?;
        if decoded.operation == DeliveryOperation::ReactionSummary {
            if decoded.reaction.is_none() || !decoded.chunks.is_empty() {
                return Err(ServiceError::InvalidPayload);
            }
        } else if decoded.reaction.is_some()
            || (decoded.chunks.is_empty() && decoded.operation != DeliveryOperation::Delete)
        {
            return Err(ServiceError::InvalidPayload);
        }
        return Ok(decoded);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ServiceError::InvalidPayload)?;
    Ok(TelegramDeliveryPayload {
        chunks: vec![TelegramMessage {
            text: text.to_owned(),
            entities: Vec::new(),
        }],
        attachments: Vec::new(),
        operation: DeliveryOperation::Create,
        target_destination_ids: Vec::new(),
        reply_destination_id: None,
        expires_at_ms: None,
        reaction: None,
    })
}

fn encode_armada_payload(payload: &ArmadaDeliveryPayload) -> Result<Vec<u8>, serde_json::Error> {
    let mut encoded = ARMADA_PAYLOAD_PREFIX.to_vec();
    encoded.extend(serde_json::to_vec(payload)?);
    Ok(encoded)
}

fn decode_armada_payload(payload: &[u8]) -> Result<ArmadaDeliveryPayload, ServiceError> {
    if let Some(json) = payload.strip_prefix(ARMADA_PAYLOAD_PREFIX) {
        let decoded: ArmadaDeliveryPayload =
            serde_json::from_slice(json).map_err(|_| ServiceError::InvalidPayload)?;
        if (decoded.operation == DeliveryOperation::ReactionSummary) != decoded.reaction.is_some() {
            return Err(ServiceError::InvalidPayload);
        }
        return Ok(decoded);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ServiceError::InvalidPayload)?;
    Ok(ArmadaDeliveryPayload {
        text: text.to_owned(),
        attachments: Vec::new(),
        operation: DeliveryOperation::Create,
        target_destination_ids: Vec::new(),
        reply_destination_id: None,
        reaction: None,
    })
}

fn escape_armada_header(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(
            character,
            '\\' | '`' | '*' | '_' | '~' | '[' | ']' | '(' | ')' | '<' | '>' | '|'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn clean_header(value: Option<&str>) -> Option<String> {
    let normalized: String = value?
        .chars()
        .map(|character| {
            if character.is_control() || is_invisible_override(character) {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(128)
        .collect();
    (!normalized.is_empty()).then_some(normalized)
}

const fn is_invisible_override(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{feff}'
    )
}

fn short_stable_id(value: &str) -> String {
    const PREFIX: usize = 12;
    const SUFFIX: usize = 4;
    let characters: Vec<char> = value.chars().collect();
    if characters.len() <= PREFIX + SUFFIX + 1 {
        return value.to_owned();
    }
    let start: String = characters.iter().take(PREFIX).collect();
    let end: String = characters.iter().skip(characters.len() - SUFFIX).collect();
    format!("{start}…{end}")
}

fn now_ms(clock: &impl Clock) -> Result<i64, ServiceError> {
    let duration = clock
        .now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::ClockBeforeEpoch)?;
    i64::try_from(duration.as_millis()).map_err(|_| ServiceError::ClockOverflow)
}

/// Secret-safe orchestration failure.
#[derive(Debug)]
pub enum ServiceError {
    /// Static service settings violate required invariants.
    InvalidSettings,
    /// The port returned an identity other than its configured bot ID.
    TelegramIdentityMismatch,
    /// A durable Telegram cursor was not an unsigned integer.
    InvalidTelegramCursor,
    /// A stored plaintext payload was not valid UTF-8.
    InvalidPayload,
    /// Durable intake stopped before acknowledging because the queue is full.
    QueueSaturated,
    /// A persisted route safety event requires explicit operator resume.
    RouteQuarantined(RouteId),
    /// The Concord live stream ended unexpectedly.
    ArmadaListenerStopped,
    /// The clock is earlier than the Unix epoch.
    ClockBeforeEpoch,
    /// The current timestamp cannot fit the database representation.
    ClockOverflow,
    /// Durable-state operation failed.
    Store(StoreError),
    /// Telegram adapter operation failed.
    Telegram(TelegramError),
    /// Concord adapter operation failed.
    Concord(ConcordError),
}

impl Display for ServiceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSettings => formatter.write_str("invalid bridge service settings"),
            Self::TelegramIdentityMismatch => {
                formatter.write_str("Telegram port returned an unexpected bot identity")
            }
            Self::InvalidTelegramCursor => formatter.write_str("invalid durable Telegram cursor"),
            Self::InvalidPayload => formatter.write_str("invalid durable plaintext payload"),
            Self::QueueSaturated => formatter.write_str("durable pending queue is saturated"),
            Self::RouteQuarantined(route_id) => {
                write!(formatter, "route {route_id} is quarantined")
            }
            Self::ArmadaListenerStopped => {
                formatter.write_str("Armada live message listener stopped unexpectedly")
            }
            Self::ClockBeforeEpoch | Self::ClockOverflow => {
                formatter.write_str("system clock is outside the supported range")
            }
            Self::Store(error) => write!(formatter, "durable bridge state failed: {error}"),
            Self::Telegram(error) => write!(formatter, "Telegram bridge operation failed: {error}"),
            Self::Concord(error) => write!(formatter, "Armada bridge operation failed: {error}"),
        }
    }
}

impl Error for ServiceError {}

impl From<StoreError> for ServiceError {
    fn from(error: StoreError) -> Self {
        if matches!(error, StoreError::QueueFull) {
            Self::QueueSaturated
        } else {
            Self::Store(error)
        }
    }
}

impl From<TelegramError> for ServiceError {
    fn from(error: TelegramError) -> Self {
        Self::Telegram(error)
    }
}

impl From<ConcordError> for ServiceError {
    fn from(error: ConcordError) -> Self {
        Self::Concord(error)
    }
}
