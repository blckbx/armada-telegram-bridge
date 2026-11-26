//! Transactional SQLite inbox/outbox with encrypted queued payloads.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rusqlite::{Connection, OptionalExtension, params};
use tempfile::NamedTempFile;
use zeroize::{Zeroize, Zeroizing};

use crate::domain::{ReactionChange, ReactionEntry, RouteId};

const DATABASE_FILE: &str = "bridge.sqlite3";
const SPOOL_KEY_FILE: &str = "spool.key";
const SPOOL_KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 24;
const SCHEMA_VERSION: u32 = 5;

const MIGRATION_1: &str = r"
CREATE TABLE IF NOT EXISTS routes (
    route_id TEXT PRIMARY KEY NOT NULL,
    created_at_ms INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE IF NOT EXISTS inbox_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_event_id TEXT NOT NULL UNIQUE,
    route_id TEXT NOT NULL REFERENCES routes(route_id) ON DELETE RESTRICT,
    direction TEXT NOT NULL CHECK (direction IN ('armada_to_telegram', 'telegram_to_armada')),
    received_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS outbox_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inbox_event_id INTEGER NOT NULL UNIQUE REFERENCES inbox_events(id) ON DELETE RESTRICT,
    route_id TEXT NOT NULL REFERENCES routes(route_id) ON DELETE RESTRICT,
    direction TEXT NOT NULL CHECK (direction IN ('armada_to_telegram', 'telegram_to_armada')),
    payload_cipher BLOB,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered')),
    created_at_ms INTEGER NOT NULL,
    delivered_at_ms INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS outbox_pending_fifo
    ON outbox_jobs(route_id, direction, status, id);

CREATE TABLE IF NOT EXISTS cursors (
    route_id TEXT NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
    direction TEXT NOT NULL CHECK (direction IN ('armada_to_telegram', 'telegram_to_armada')),
    cursor TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (route_id, direction)
) STRICT;

CREATE TABLE IF NOT EXISTS message_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL UNIQUE REFERENCES outbox_jobs(id) ON DELETE RESTRICT,
    route_id TEXT NOT NULL REFERENCES routes(route_id) ON DELETE RESTRICT,
    direction TEXT NOT NULL CHECK (direction IN ('armada_to_telegram', 'telegram_to_armada')),
    source_message_id TEXT NOT NULL,
    destination_message_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE (route_id, direction, source_message_id),
    UNIQUE (route_id, direction, destination_message_id)
) STRICT;

CREATE TABLE IF NOT EXISTS cached_profiles (
    transport TEXT NOT NULL,
    stable_id TEXT NOT NULL,
    display_name TEXT,
    username TEXT,
    refreshed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (transport, stable_id)
) STRICT;

CREATE TABLE IF NOT EXISTS route_state (
    route_id TEXT PRIMARY KEY NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'paused', 'quarantined')),
    reason TEXT,
    updated_at_ms INTEGER NOT NULL DEFAULT 0
) STRICT;

PRAGMA user_version = 1;
";

const MIGRATION_2: &str = r"
CREATE TABLE IF NOT EXISTS job_delivery_state (
    job_id INTEGER PRIMARY KEY NOT NULL REFERENCES outbox_jobs(id) ON DELETE CASCADE,
    state TEXT NOT NULL DEFAULT 'ready'
        CHECK (state IN ('ready', 'retry_wait', 'dead_letter', 'delivered')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
    last_error_class TEXT,
    possible_duplicate INTEGER NOT NULL DEFAULT 0 CHECK (possible_duplicate IN (0, 1)),
    updated_at_ms INTEGER NOT NULL DEFAULT 0
) STRICT;

INSERT OR IGNORE INTO job_delivery_state(job_id, state)
SELECT id, CASE status WHEN 'delivered' THEN 'delivered' ELSE 'ready' END
FROM outbox_jobs;

CREATE INDEX IF NOT EXISTS job_delivery_ready
    ON job_delivery_state(state, next_attempt_at_ms, job_id);

PRAGMA user_version = 2;
";

const MIGRATION_3: &str = r"
ALTER TABLE inbox_events ADD COLUMN source_author_id TEXT NOT NULL DEFAULT '';
ALTER TABLE message_links ADD COLUMN source_author_id TEXT NOT NULL DEFAULT '';

CREATE TABLE scheduled_deletions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL UNIQUE REFERENCES outbox_jobs(id) ON DELETE CASCADE,
    route_id TEXT NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
    destination_message_id TEXT NOT NULL,
    due_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
) STRICT;

CREATE INDEX scheduled_deletions_due
    ON scheduled_deletions(completed_at_ms, due_at_ms, id);

PRAGMA user_version = 3;
";

const MIGRATION_4: &str = r"
CREATE TABLE skipped_jobs (
    job_id INTEGER PRIMARY KEY NOT NULL REFERENCES outbox_jobs(id) ON DELETE CASCADE,
    skipped_at_ms INTEGER NOT NULL,
    prior_error_class TEXT,
    possible_duplicate INTEGER NOT NULL CHECK (possible_duplicate IN (0, 1))
) STRICT;

PRAGMA user_version = 4;
";

const MIGRATION_5: &str = r"
CREATE TABLE reaction_entries (
    message_job_id INTEGER NOT NULL REFERENCES outbox_jobs(id) ON DELETE CASCADE,
    direction TEXT NOT NULL CHECK (direction IN ('armada_to_telegram', 'telegram_to_armada')),
    reactor_id TEXT NOT NULL,
    display_name TEXT,
    emoji TEXT NOT NULL,
    reaction_count INTEGER NOT NULL CHECK (reaction_count > 0),
    PRIMARY KEY (message_job_id, direction, reactor_id, emoji)
) STRICT;

CREATE TABLE reaction_summaries (
    message_job_id INTEGER NOT NULL REFERENCES outbox_jobs(id) ON DELETE CASCADE,
    direction TEXT NOT NULL CHECK (direction IN ('armada_to_telegram', 'telegram_to_armada')),
    destination_target_id TEXT NOT NULL,
    destination_summary_id TEXT,
    rendered_text TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (message_job_id, direction)
) STRICT;

PRAGMA user_version = 5;
";

type StoredJobRow = (String, String, String, Option<Vec<u8>>, Option<i64>, String);

/// Ordered delivery direction for a route.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeliveryDirection {
    /// Concord/Armada source to Telegram destination.
    ArmadaToTelegram,
    /// Telegram source to Concord/Armada destination.
    TelegramToArmada,
}

impl DeliveryDirection {
    const fn as_database_value(self) -> &'static str {
        match self {
            Self::ArmadaToTelegram => "armada_to_telegram",
            Self::TelegramToArmada => "telegram_to_armada",
        }
    }

    fn from_database_value(value: &str) -> Result<Self, StoreError> {
        match value {
            "armada_to_telegram" => Ok(Self::ArmadaToTelegram),
            "telegram_to_armada" => Ok(Self::TelegramToArmada),
            _ => Err(StoreError::CorruptData("unknown delivery direction")),
        }
    }
}

/// Durable outbox job identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobId(i64);

impl JobId {
    /// Returns the underlying SQLite row identifier.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for JobId {
    type Error = StoreError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(StoreError::InvalidInput("job ID"));
        }
        Ok(Self(value))
    }
}

/// Input applied atomically to the inbox, outbox, and optional cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewDelivery {
    route_id: RouteId,
    direction: DeliveryDirection,
    source_event_id: String,
    payload: Vec<u8>,
    cursor: Option<String>,
    received_at_ms: i64,
    source_author_id: String,
}

impl NewDelivery {
    /// Creates a destination work item for one immutable source event.
    #[must_use]
    pub fn new(
        route_id: RouteId,
        direction: DeliveryDirection,
        source_event_id: impl Into<String>,
        payload: Vec<u8>,
        received_at_ms: i64,
    ) -> Self {
        Self {
            route_id,
            direction,
            source_event_id: source_event_id.into(),
            payload,
            cursor: None,
            received_at_ms,
            source_author_id: String::new(),
        }
    }

    /// Associates the authenticated stable source author with this event.
    #[must_use]
    pub fn with_source_author(mut self, source_author_id: impl Into<String>) -> Self {
        self.source_author_id = source_author_id.into();
        self
    }

    /// Advances the source cursor in the same transaction as this event.
    #[must_use]
    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }
}

/// Result of applying an immutable inbound event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptOutcome {
    /// A new inbox event and job were committed.
    Enqueued(JobId),
    /// The source event was already durably accepted.
    Duplicate(JobId),
}

impl AcceptOutcome {
    /// Returns the existing or newly created job identifier.
    #[must_use]
    pub const fn job_id(self) -> JobId {
        match self {
            Self::Enqueued(id) | Self::Duplicate(id) => id,
        }
    }
}

/// Durable delivery state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStatus {
    /// Awaiting destination acknowledgement.
    Pending,
    /// Destination acknowledgement and message link committed.
    Delivered,
}

/// Scheduler state attached to one durable outbox job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryState {
    /// Eligible for an immediate destination attempt.
    Ready,
    /// Waiting until a durable retry timestamp.
    RetryWait,
    /// Terminal failure requiring an explicit operator retry.
    DeadLetter,
    /// Destination receipt was committed.
    Delivered,
}

impl DeliveryState {
    fn from_database_value(value: &str) -> Result<Self, StoreError> {
        match value {
            "ready" => Ok(Self::Ready),
            "retry_wait" => Ok(Self::RetryWait),
            "dead_letter" => Ok(Self::DeadLetter),
            "delivered" => Ok(Self::Delivered),
            _ => Err(StoreError::CorruptData("unknown delivery state")),
        }
    }
}

impl JobStatus {
    fn from_database_value(value: &str) -> Result<Self, StoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "delivered" => Ok(Self::Delivered),
            _ => Err(StoreError::CorruptData("unknown job status")),
        }
    }
}

/// Pending job returned to an ordered dispatcher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxJob {
    /// Durable identifier.
    pub id: JobId,
    /// Static route.
    pub route_id: RouteId,
    /// Ordered delivery direction.
    pub direction: DeliveryDirection,
    /// Immutable namespaced source ID.
    pub source_event_id: String,
    /// Decrypted destination payload.
    pub payload: Vec<u8>,
    /// Creation timestamp in milliseconds.
    pub created_at_ms: i64,
    /// Durable scheduler state.
    pub delivery_state: DeliveryState,
    /// Number of failed destination attempts already recorded.
    pub attempt_count: u32,
    /// Earliest wall-clock millisecond for another attempt.
    pub next_attempt_at_ms: i64,
    /// Whether an earlier ambiguous failure may already have delivered.
    pub possible_duplicate: bool,
}

/// Inspectable scheduler metadata for a pending or completed job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryAttemptState {
    /// Durable job identifier.
    pub job_id: JobId,
    /// Current scheduler state.
    pub state: DeliveryState,
    /// Number of recorded failures.
    pub attempt_count: u32,
    /// Next eligible attempt timestamp.
    pub next_attempt_at_ms: i64,
    /// Stable secret-free failure classification.
    pub last_error_class: Option<String>,
    /// An ambiguous send outcome may have reached the destination.
    pub possible_duplicate: bool,
}

/// Persisted route safety state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteState {
    /// Intake and delivery are allowed.
    Active,
    /// Operator-paused route.
    Paused,
    /// Safety failure requires explicit operator recovery.
    Quarantined,
}

impl RouteState {
    const fn as_database_value(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Quarantined => "quarantined",
        }
    }

    fn from_database_value(value: &str) -> Result<Self, StoreError> {
        match value {
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            "quarantined" => Ok(Self::Quarantined),
            _ => Err(StoreError::CorruptData("unknown route state")),
        }
    }
}

/// Job inspection result; delivered payload may have been erased.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredJob {
    /// Durable identifier.
    pub id: JobId,
    /// Delivery state.
    pub status: JobStatus,
    /// Decrypted payload when it remains inside retention.
    pub payload: Option<Vec<u8>>,
    /// Delivery timestamp, if acknowledged.
    pub delivered_at_ms: Option<i64>,
}

/// Durable source-to-destination message mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageLink {
    /// Namespaced source message/event ID.
    pub source_message_id: String,
    /// Canonical destination message/event ID.
    pub destination_message_id: String,
    /// Authenticated stable author that created the source message.
    pub source_author_id: String,
    /// Original delivery job for checking whether an edit is representable.
    pub job_id: JobId,
}

/// Durable reaction entries plus the current destination summary projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactionSummaryState {
    /// Message at the destination beneath which the summary belongs.
    pub destination_target_id: String,
    /// Existing bridge-owned summary message, when one has been created.
    pub destination_summary_id: Option<String>,
    /// Last text acknowledged by the destination.
    pub rendered_text: String,
    /// Current source reactions in stable order.
    pub entries: Vec<ReactionEntry>,
}

/// Durable best-effort deletion of an expiring Telegram mirror.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledDeletion {
    /// Durable task identifier.
    pub id: i64,
    /// Route containing the mirror.
    pub route_id: RouteId,
    /// One or more comma-separated canonical destination IDs.
    pub destination_message_id: String,
    /// Absolute Unix millisecond deadline.
    pub due_at_ms: i64,
}

/// Row counts used by diagnostics and invariant tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreCounts {
    /// Number of accepted source events.
    pub inbox_events: u64,
    /// Number of durable jobs.
    pub outbox_jobs: u64,
}

/// SQLite-backed delivery state.
pub struct Store {
    connection: Connection,
    spool_key: Zeroizing<[u8; SPOOL_KEY_BYTES]>,
}

impl Store {
    /// Opens or creates the database and independent encrypted-spool key.
    ///
    /// # Errors
    ///
    /// Fails closed for unsafe/malformed key material, migration failures, or
    /// SQLite configuration errors.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let data_dir = data_dir.as_ref();
        create_data_directory(data_dir)?;
        if data_dir.join(DATABASE_FILE).exists() && !data_dir.join(SPOOL_KEY_FILE).exists() {
            return Err(StoreError::CorruptData(
                "existing database is missing its spool key",
            ));
        }
        let spool_key = load_or_create_spool_key(data_dir)?;
        let database_path = data_dir.join(DATABASE_FILE);
        let connection = Connection::open(&database_path)?;
        restrict_database_permissions(&database_path)?;
        configure_connection(&connection)?;
        migrate(&connection)?;
        Ok(Self {
            connection,
            spool_key,
        })
    }

    /// Registers a configured route and its initial active state.
    ///
    /// # Errors
    ///
    /// Returns a database error if the transaction cannot commit.
    pub fn register_route(&mut self, route_id: &RouteId) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO routes(route_id) VALUES (?1)",
            [route_id.as_str()],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO route_state(route_id) VALUES (?1)",
            [route_id.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically deduplicates an inbound event, creates a job, and advances
    /// its source cursor.
    ///
    /// # Errors
    ///
    /// Returns an error without committing any component if validation,
    /// encryption, a foreign-key check, or another database operation fails.
    pub fn accept_inbound(&mut self, delivery: &NewDelivery) -> Result<AcceptOutcome, StoreError> {
        validate_delivery(delivery)?;
        let associated_data = associated_data(
            &delivery.route_id,
            delivery.direction,
            &delivery.source_event_id,
        );
        let cipher = encrypt(&self.spool_key, &delivery.payload, &associated_data)?;
        let transaction = self.connection.transaction()?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO inbox_events
             (source_event_id, route_id, direction, received_at_ms, source_author_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                delivery.source_event_id,
                delivery.route_id.as_str(),
                delivery.direction.as_database_value(),
                delivery.received_at_ms,
                delivery.source_author_id
            ],
        )?;

        let outcome = if inserted == 0 {
            let (job_id, existing_route, existing_direction): (i64, String, String) = transaction
                .query_row(
                "SELECT o.id, i.route_id, i.direction
                     FROM inbox_events i
                     JOIN outbox_jobs o ON o.inbox_event_id = i.id
                     WHERE i.source_event_id = ?1",
                [&delivery.source_event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            if existing_route != delivery.route_id.as_str()
                || existing_direction != delivery.direction.as_database_value()
            {
                return Err(StoreError::SourceEventCollision);
            }
            AcceptOutcome::Duplicate(JobId(job_id))
        } else {
            let inbox_id = transaction.last_insert_rowid();
            transaction.execute(
                "INSERT INTO outbox_jobs
                 (inbox_event_id, route_id, direction, payload_cipher, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    inbox_id,
                    delivery.route_id.as_str(),
                    delivery.direction.as_database_value(),
                    cipher,
                    delivery.received_at_ms
                ],
            )?;
            let job_id = JobId(transaction.last_insert_rowid());
            transaction.execute(
                "INSERT INTO job_delivery_state(job_id, updated_at_ms) VALUES (?1, ?2)",
                params![job_id.get(), delivery.received_at_ms],
            )?;
            AcceptOutcome::Enqueued(job_id)
        };

        if let Some(cursor) = &delivery.cursor {
            transaction.execute(
                "INSERT INTO cursors(route_id, direction, cursor, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(route_id, direction) DO UPDATE SET
                   cursor = excluded.cursor,
                   updated_at_ms = excluded.updated_at_ms",
                params![
                    delivery.route_id.as_str(),
                    delivery.direction.as_database_value(),
                    cursor,
                    delivery.received_at_ms
                ],
            )?;
        }
        transaction.commit()?;
        Ok(outcome)
    }

    /// Accepts an inbound event only while the global pending queue remains
    /// below its configured bound. Deduplicated inputs remain admissible.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::QueueFull`] without advancing the cursor when a
    /// new event would exceed the bound, or any normal intake failure.
    pub fn accept_inbound_bounded(
        &mut self,
        delivery: &NewDelivery,
        max_pending_jobs: usize,
    ) -> Result<AcceptOutcome, StoreError> {
        if max_pending_jobs == 0 {
            return Err(StoreError::InvalidInput("pending job limit"));
        }
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM inbox_events WHERE source_event_id = ?1)",
            [&delivery.source_event_id],
            |row| row.get(0),
        )?;
        if !exists && self.pending_job_count()? >= max_pending_jobs as u64 {
            return Err(StoreError::QueueFull);
        }
        self.accept_inbound(delivery)
    }

    /// Returns pending jobs in insertion order for one route/direction.
    ///
    /// # Errors
    ///
    /// Returns an error for database corruption or authentication failure while
    /// decrypting a payload.
    pub fn pending_jobs(
        &self,
        route_id: &RouteId,
        direction: DeliveryDirection,
        limit: usize,
    ) -> Result<Vec<OutboxJob>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit).map_err(|_| StoreError::InvalidInput("job limit"))?;
        let mut statement = self.connection.prepare(
            "SELECT o.id, i.source_event_id, o.payload_cipher, o.created_at_ms,
                    d.state, d.attempt_count, d.next_attempt_at_ms, d.possible_duplicate
             FROM outbox_jobs o
             JOIN inbox_events i ON i.id = o.inbox_event_id
             JOIN job_delivery_state d ON d.job_id = o.id
             WHERE o.route_id = ?1 AND o.direction = ?2 AND o.status = 'pending'
             ORDER BY o.id ASC
             LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![route_id.as_str(), direction.as_database_value(), limit],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u32>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, bool>(7)?,
                ))
            },
        )?;

        let mut jobs = Vec::new();
        for row in rows {
            let (
                id,
                source_event_id,
                cipher,
                created_at_ms,
                delivery_state,
                attempt_count,
                next_attempt_at_ms,
                possible_duplicate,
            ) = row?;
            let aad = associated_data(route_id, direction, &source_event_id);
            jobs.push(OutboxJob {
                id: JobId(id),
                route_id: route_id.clone(),
                direction,
                source_event_id,
                payload: decrypt(&self.spool_key, &cipher, &aad)?,
                created_at_ms,
                delivery_state: DeliveryState::from_database_value(&delivery_state)?,
                attempt_count,
                next_attempt_at_ms,
                possible_duplicate,
            });
        }
        Ok(jobs)
    }

    /// Atomically records the destination receipt and marks a job delivered.
    ///
    /// # Errors
    ///
    /// Returns an error when the job is missing, not pending, the receipt is
    /// empty, or a unique message-link constraint fails. No partial state is
    /// committed.
    pub fn mark_delivered(
        &mut self,
        job_id: JobId,
        destination_message_id: &str,
        delivered_at_ms: i64,
    ) -> Result<(), StoreError> {
        self.mark_delivered_with_expiry(job_id, destination_message_id, delivered_at_ms, None)
    }

    /// Commits a create receipt and optionally schedules deletion of its
    /// Telegram mirror in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::mark_delivered`].
    pub fn mark_delivered_with_expiry(
        &mut self,
        job_id: JobId,
        destination_message_id: &str,
        delivered_at_ms: i64,
        expires_at_ms: Option<i64>,
    ) -> Result<(), StoreError> {
        if destination_message_id.is_empty() {
            return Err(StoreError::InvalidInput("destination message ID"));
        }
        let transaction = self.connection.transaction()?;
        let row: Option<(String, String, String, String, String)> = transaction
            .query_row(
                "SELECT o.route_id, o.direction, i.source_event_id, o.status,
                        i.source_author_id
                 FROM outbox_jobs o
                 JOIN inbox_events i ON i.id = o.inbox_event_id
                 WHERE o.id = ?1",
                [job_id.get()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((route_id, direction, source_message_id, status, source_author_id)) = row else {
            return Err(StoreError::JobNotFound(job_id));
        };
        if status != "pending" {
            return Err(StoreError::InvalidJobState(job_id));
        }

        transaction.execute(
            "INSERT INTO message_links
             (job_id, route_id, direction, source_message_id, destination_message_id,
              created_at_ms, source_author_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                job_id.get(),
                &route_id,
                &direction,
                source_message_id,
                destination_message_id,
                delivered_at_ms,
                source_author_id
            ],
        )?;
        if direction == DeliveryDirection::ArmadaToTelegram.as_database_value()
            && let Some(due_at_ms) = expires_at_ms
        {
            transaction.execute(
                "INSERT INTO scheduled_deletions
                     (job_id, route_id, destination_message_id, due_at_ms)
                     VALUES (?1, ?2, ?3, ?4)",
                params![job_id.get(), route_id, destination_message_id, due_at_ms],
            )?;
        }
        let changed = transaction.execute(
            "UPDATE outbox_jobs
             SET status = 'delivered', delivered_at_ms = ?2
             WHERE id = ?1 AND status = 'pending'",
            params![job_id.get(), delivered_at_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidJobState(job_id));
        }
        transaction.execute(
            "UPDATE job_delivery_state
             SET state = 'delivered', next_attempt_at_ms = 0, updated_at_ms = ?2
             WHERE job_id = ?1",
            params![job_id.get(), delivered_at_ms],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Marks a mutation job delivered without creating another source link.
    ///
    /// # Errors
    ///
    /// Fails unless the job exists and is still pending.
    pub fn mark_mutation_delivered(
        &mut self,
        job_id: JobId,
        delivered_at_ms: i64,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        let changed = transaction.execute(
            "UPDATE outbox_jobs
             SET status = 'delivered', delivered_at_ms = ?2
             WHERE id = ?1 AND status = 'pending'",
            params![job_id.get(), delivered_at_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidJobState(job_id));
        }
        transaction.execute(
            "UPDATE job_delivery_state
             SET state = 'delivered', next_attempt_at_ms = 0, updated_at_ms = ?2
             WHERE job_id = ?1",
            params![job_id.get(), delivered_at_ms],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Records a retryable destination failure and its next eligible time.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid metadata, a missing/non-pending job, or a
    /// database failure.
    pub fn schedule_retry(
        &mut self,
        job_id: JobId,
        next_attempt_at_ms: i64,
        error_class: &str,
        possible_duplicate: bool,
        updated_at_ms: i64,
    ) -> Result<(), StoreError> {
        validate_error_class(error_class)?;
        let changed = self.connection.execute(
            "UPDATE job_delivery_state
             SET state = 'retry_wait',
                 attempt_count = attempt_count + 1,
                 next_attempt_at_ms = ?2,
                 last_error_class = ?3,
                 possible_duplicate = MAX(possible_duplicate, ?4),
                 updated_at_ms = ?5
             WHERE job_id = ?1
               AND EXISTS (
                   SELECT 1 FROM outbox_jobs o
                   WHERE o.id = job_delivery_state.job_id AND o.status = 'pending'
               )",
            params![
                job_id.get(),
                next_attempt_at_ms,
                error_class,
                possible_duplicate,
                updated_at_ms
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidJobState(job_id));
        }
        Ok(())
    }

    /// Moves a terminal destination failure to the inspectable dead letter.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid metadata, a missing/non-pending job, or a
    /// database failure.
    pub fn mark_dead_letter(
        &mut self,
        job_id: JobId,
        error_class: &str,
        possible_duplicate: bool,
        updated_at_ms: i64,
    ) -> Result<(), StoreError> {
        validate_error_class(error_class)?;
        let changed = self.connection.execute(
            "UPDATE job_delivery_state
             SET state = 'dead_letter',
                 attempt_count = attempt_count + 1,
                 next_attempt_at_ms = 0,
                 last_error_class = ?2,
                 possible_duplicate = MAX(possible_duplicate, ?3),
                 updated_at_ms = ?4
             WHERE job_id = ?1
               AND EXISTS (
                   SELECT 1 FROM outbox_jobs o
                   WHERE o.id = job_delivery_state.job_id AND o.status = 'pending'
               )",
            params![job_id.get(), error_class, possible_duplicate, updated_at_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidJobState(job_id));
        }
        Ok(())
    }

    /// Makes one dead-lettered job immediately eligible for operator retry.
    ///
    /// # Errors
    ///
    /// Returns an error unless the job is currently dead-lettered.
    pub fn retry_dead_letter(
        &mut self,
        job_id: JobId,
        updated_at_ms: i64,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE job_delivery_state
             SET state = 'ready', attempt_count = 0,
                 next_attempt_at_ms = ?2, updated_at_ms = ?2
             WHERE job_id = ?1 AND state = 'dead_letter'",
            params![job_id.get(), updated_at_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidJobState(job_id));
        }
        Ok(())
    }

    /// Explicitly discards one dead-lettered destination delivery and records
    /// an audit row so later FIFO work may proceed.
    ///
    /// The source event and metadata remain for deduplication, while encrypted
    /// payload plaintext is erased. No destination message link is created.
    ///
    /// # Errors
    ///
    /// Returns an error unless the job is currently dead-lettered.
    pub fn skip_dead_letter(
        &mut self,
        job_id: JobId,
        skipped_at_ms: i64,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        let inserted = transaction.execute(
            "INSERT INTO skipped_jobs
             (job_id, skipped_at_ms, prior_error_class, possible_duplicate)
             SELECT job_id, ?2, last_error_class, possible_duplicate
             FROM job_delivery_state
             WHERE job_id = ?1 AND state = 'dead_letter'",
            params![job_id.get(), skipped_at_ms],
        )?;
        if inserted != 1 {
            return Err(StoreError::InvalidJobState(job_id));
        }
        let updated = transaction.execute(
            "UPDATE outbox_jobs
             SET status = 'delivered', delivered_at_ms = ?2,
                 payload_cipher = NULL
             WHERE id = ?1 AND status = 'pending'",
            params![job_id.get(), skipped_at_ms],
        )?;
        if updated != 1 {
            return Err(StoreError::InvalidJobState(job_id));
        }
        transaction.execute(
            "UPDATE job_delivery_state
             SET state = 'delivered', next_attempt_at_ms = 0, updated_at_ms = ?2
             WHERE job_id = ?1",
            params![job_id.get(), skipped_at_ms],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Number of explicitly skipped destination deliveries retained for audit.
    ///
    /// # Errors
    /// Returns a database query failure.
    pub fn skipped_job_count(&self) -> Result<u64, StoreError> {
        Ok(self
            .connection
            .query_row("SELECT COUNT(*) FROM skipped_jobs", [], |row| row.get(0))?)
    }

    /// Reads scheduler metadata for one job.
    ///
    /// # Errors
    ///
    /// Returns an error for database corruption or query failure.
    pub fn delivery_attempt_state(
        &self,
        job_id: JobId,
    ) -> Result<Option<DeliveryAttemptState>, StoreError> {
        self.connection
            .query_row(
                "SELECT state, attempt_count, next_attempt_at_ms,
                        last_error_class, possible_duplicate
                 FROM job_delivery_state WHERE job_id = ?1",
                [job_id.get()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, bool>(4)?,
                    ))
                },
            )
            .optional()?
            .map(
                |(
                    state,
                    attempt_count,
                    next_attempt_at_ms,
                    last_error_class,
                    possible_duplicate,
                )| {
                    Ok(DeliveryAttemptState {
                        job_id,
                        state: DeliveryState::from_database_value(&state)?,
                        attempt_count,
                        next_attempt_at_ms,
                        last_error_class,
                        possible_duplicate,
                    })
                },
            )
            .transpose()
    }

    /// Lists dead letters without decrypting or exposing their message body.
    ///
    /// # Errors
    ///
    /// Returns an error for database corruption or query failure.
    pub fn dead_letters(&self) -> Result<Vec<DeliveryAttemptState>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT job_id, attempt_count, next_attempt_at_ms,
                    last_error_class, possible_duplicate
             FROM job_delivery_state WHERE state = 'dead_letter' ORDER BY job_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(DeliveryAttemptState {
                job_id: JobId(row.get(0)?),
                state: DeliveryState::DeadLetter,
                attempt_count: row.get(1)?,
                next_attempt_at_ms: row.get(2)?,
                last_error_class: row.get(3)?,
                possible_duplicate: row.get(4)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Counts all pending jobs, including retry waits and dead letters.
    ///
    /// # Errors
    ///
    /// Returns a database error if the count cannot be read.
    pub fn pending_job_count(&self) -> Result<u64, StoreError> {
        Ok(self.connection.query_row(
            "SELECT COUNT(*) FROM outbox_jobs WHERE status = 'pending'",
            [],
            |row| row.get(0),
        )?)
    }

    /// Changes the persisted safety state for one configured route.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty reason, unknown route, or database failure.
    pub fn set_route_state(
        &mut self,
        route_id: &RouteId,
        state: RouteState,
        reason: Option<&str>,
        updated_at_ms: i64,
    ) -> Result<(), StoreError> {
        if reason == Some("") {
            return Err(StoreError::InvalidInput("route-state reason"));
        }
        let changed = self.connection.execute(
            "UPDATE route_state SET state = ?2, reason = ?3, updated_at_ms = ?4
             WHERE route_id = ?1",
            params![
                route_id.as_str(),
                state.as_database_value(),
                reason,
                updated_at_ms
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::UnknownRoute(route_id.clone()));
        }
        Ok(())
    }

    /// Reads one configured route's persisted safety state.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown route, corruption, or query failure.
    pub fn route_state(&self, route_id: &RouteId) -> Result<RouteState, StoreError> {
        let value = self
            .connection
            .query_row(
                "SELECT state FROM route_state WHERE route_id = ?1",
                [route_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::UnknownRoute(route_id.clone()))?;
        RouteState::from_database_value(&value)
    }

    /// Reads the committed source cursor for one ordered stream.
    ///
    /// # Errors
    ///
    /// Returns a database error if the query cannot complete.
    pub fn cursor(
        &self,
        route_id: &RouteId,
        direction: DeliveryDirection,
    ) -> Result<Option<String>, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT cursor FROM cursors WHERE route_id = ?1 AND direction = ?2",
                params![route_id.as_str(), direction.as_database_value()],
                |row| row.get(0),
            )
            .optional()?)
    }

    /// Records a source baseline when there is deliberately no relay job.
    ///
    /// This is used on first startup and when an unsupported but safely
    /// consumed source event must advance its stream. The route must already
    /// be registered.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty cursor or a database failure.
    pub fn checkpoint_cursor(
        &mut self,
        route_id: &RouteId,
        direction: DeliveryDirection,
        cursor: &str,
        updated_at_ms: i64,
    ) -> Result<(), StoreError> {
        if cursor.is_empty() {
            return Err(StoreError::InvalidInput("cursor"));
        }
        self.connection.execute(
            "INSERT INTO cursors(route_id, direction, cursor, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(route_id, direction) DO UPDATE SET
               cursor = excluded.cursor,
               updated_at_ms = excluded.updated_at_ms",
            params![
                route_id.as_str(),
                direction.as_database_value(),
                cursor,
                updated_at_ms
            ],
        )?;
        Ok(())
    }

    /// Looks up the durable destination mapping for a source message.
    ///
    /// # Errors
    ///
    /// Returns a database error if the query cannot complete.
    pub fn message_link(
        &self,
        route_id: &RouteId,
        direction: DeliveryDirection,
        source_message_id: &str,
    ) -> Result<Option<MessageLink>, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT source_message_id, destination_message_id, source_author_id, job_id
                 FROM message_links
                 WHERE route_id = ?1 AND direction = ?2 AND source_message_id = ?3",
                params![
                    route_id.as_str(),
                    direction.as_database_value(),
                    source_message_id
                ],
                |row| {
                    Ok(MessageLink {
                        source_message_id: row.get(0)?,
                        destination_message_id: row.get(1)?,
                        source_author_id: row.get(2)?,
                        job_id: JobId(row.get(3)?),
                    })
                },
            )
            .optional()?)
    }

    /// Looks up a message link containing one exact destination ID.
    ///
    /// Multi-part mirrors store comma-separated destination IDs; every element
    /// remains independently resolvable for reactions.
    ///
    /// # Errors
    /// Returns a database error if the query cannot complete.
    pub fn message_link_by_destination(
        &self,
        route_id: &RouteId,
        direction: DeliveryDirection,
        destination_message_id: &str,
    ) -> Result<Option<MessageLink>, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT source_message_id, destination_message_id, source_author_id, job_id
                 FROM message_links
                 WHERE route_id = ?1 AND direction = ?2
                   AND instr(',' || destination_message_id || ',', ',' || ?3 || ',') > 0",
                params![
                    route_id.as_str(),
                    direction.as_database_value(),
                    destination_message_id
                ],
                |row| {
                    Ok(MessageLink {
                        source_message_id: row.get(0)?,
                        destination_message_id: row.get(1)?,
                        source_author_id: row.get(2)?,
                        job_id: JobId(row.get(3)?),
                    })
                },
            )
            .optional()?)
    }

    /// Applies one reaction snapshot/delta and returns the desired summary state.
    ///
    /// Reapplying the same update is idempotent, which makes destination retries
    /// safe after the source state was committed.
    ///
    /// # Errors
    /// Returns an error for malformed reaction data, missing message links, or
    /// database failures.
    pub fn apply_reaction_change(
        &mut self,
        message_job_id: JobId,
        direction: DeliveryDirection,
        destination_target_id: &str,
        change: &ReactionChange,
    ) -> Result<ReactionSummaryState, StoreError> {
        if destination_target_id.is_empty() {
            return Err(StoreError::InvalidInput("reaction destination target"));
        }
        let transaction = self.connection.transaction()?;
        let message_exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM message_links WHERE job_id = ?1)",
            [message_job_id.get()],
            |row| row.get(0),
        )?;
        if !message_exists {
            return Err(StoreError::JobNotFound(message_job_id));
        }
        match change {
            ReactionChange::Snapshot { entries } => {
                transaction.execute(
                    "DELETE FROM reaction_entries
                     WHERE message_job_id = ?1 AND direction = ?2",
                    params![message_job_id.get(), direction.as_database_value()],
                )?;
                for entry in entries {
                    validate_reaction_entry(entry)?;
                    insert_reaction_entry(&transaction, message_job_id, direction, entry)?;
                }
            }
            ReactionChange::ActorSelection {
                reactor_id,
                display_name,
                emojis,
            } => {
                validate_reactor(reactor_id, display_name.as_deref())?;
                transaction.execute(
                    "DELETE FROM reaction_entries
                     WHERE message_job_id = ?1 AND direction = ?2 AND reactor_id = ?3",
                    params![
                        message_job_id.get(),
                        direction.as_database_value(),
                        reactor_id
                    ],
                )?;
                for emoji in emojis {
                    let entry = ReactionEntry {
                        reactor_id: reactor_id.clone(),
                        display_name: display_name.clone(),
                        emoji: emoji.clone(),
                        count: 1,
                    };
                    validate_reaction_entry(&entry)?;
                    insert_reaction_entry(&transaction, message_job_id, direction, &entry)?;
                }
            }
        }
        transaction.execute(
            "INSERT INTO reaction_summaries
             (message_job_id, direction, destination_target_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(message_job_id, direction) DO UPDATE SET
               destination_target_id = excluded.destination_target_id",
            params![
                message_job_id.get(),
                direction.as_database_value(),
                destination_target_id
            ],
        )?;
        let state = reaction_summary_state(&transaction, message_job_id, direction)?;
        transaction.commit()?;
        Ok(state)
    }

    /// Records the destination state after a reaction summary create/edit/delete.
    ///
    /// # Errors
    /// Returns an error unless the reaction projection exists.
    pub fn set_reaction_summary_projection(
        &mut self,
        message_job_id: JobId,
        direction: DeliveryDirection,
        destination_summary_id: Option<&str>,
        rendered_text: &str,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE reaction_summaries
             SET destination_summary_id = ?3, rendered_text = ?4
             WHERE message_job_id = ?1 AND direction = ?2",
            params![
                message_job_id.get(),
                direction.as_database_value(),
                destination_summary_id,
                rendered_text
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::JobNotFound(message_job_id));
        }
        Ok(())
    }

    /// Returns incomplete expiry deletions due at or before `now_ms`.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed durable rows or a database failure.
    pub fn due_deletions(&self, now_ms: i64) -> Result<Vec<ScheduledDeletion>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, route_id, destination_message_id, due_at_ms
             FROM scheduled_deletions
             WHERE completed_at_ms IS NULL AND due_at_ms <= ?1
             ORDER BY due_at_ms, id",
        )?;
        let rows = statement.query_map([now_ms], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let mut deletions = Vec::new();
        for row in rows {
            let (id, route_id, destination_message_id, due_at_ms) = row?;
            deletions.push(ScheduledDeletion {
                id,
                route_id: RouteId::try_from(route_id)
                    .map_err(|_| StoreError::CorruptData("invalid scheduled-deletion route"))?,
                destination_message_id,
                due_at_ms,
            });
        }
        Ok(deletions)
    }

    /// Completes one best-effort expiry deletion idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable task is missing or already completed.
    pub fn complete_deletion(&mut self, id: i64, completed_at_ms: i64) -> Result<(), StoreError> {
        if id <= 0 {
            return Err(StoreError::InvalidInput("scheduled deletion ID"));
        }
        let changed = self.connection.execute(
            "UPDATE scheduled_deletions SET completed_at_ms = ?2
             WHERE id = ?1 AND completed_at_ms IS NULL",
            params![id, completed_at_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidInput("scheduled deletion state"));
        }
        Ok(())
    }

    /// Reads one job for diagnostics or retention verification.
    ///
    /// # Errors
    ///
    /// Returns an error for database corruption or payload authentication
    /// failure.
    pub fn job(&self, job_id: JobId) -> Result<Option<StoredJob>, StoreError> {
        let row: Option<StoredJobRow> = self
            .connection
            .query_row(
                "SELECT o.route_id, o.direction, i.source_event_id,
                        o.payload_cipher, o.delivered_at_ms, o.status
                 FROM outbox_jobs o
                 JOIN inbox_events i ON i.id = o.inbox_event_id
                 WHERE o.id = ?1",
                [job_id.get()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((route, direction, source_event_id, payload_cipher, delivered_at_ms, status)) =
            row
        else {
            return Ok(None);
        };
        let route_id = RouteId::try_from(route)
            .map_err(|_| StoreError::CorruptData("invalid stored route ID"))?;
        let direction = DeliveryDirection::from_database_value(&direction)?;
        let payload = payload_cipher
            .map(|cipher| {
                decrypt(
                    &self.spool_key,
                    &cipher,
                    &associated_data(&route_id, direction, &source_event_id),
                )
            })
            .transpose()?;
        Ok(Some(StoredJob {
            id: job_id,
            status: JobStatus::from_database_value(&status)?,
            payload,
            delivered_at_ms,
        }))
    }

    /// Erases encrypted payload columns for delivered jobs at or before a
    /// retention cutoff while preserving message links and delivery metadata.
    ///
    /// # Errors
    ///
    /// Returns a database error if cleanup cannot commit.
    pub fn cleanup_delivered_payloads(&self, cutoff_ms: i64) -> Result<usize, StoreError> {
        Ok(self.connection.execute(
            "UPDATE outbox_jobs SET payload_cipher = NULL
             WHERE status = 'delivered'
               AND delivered_at_ms <= ?1
               AND payload_cipher IS NOT NULL",
            [cutoff_ms],
        )?)
    }

    /// Returns inbox/outbox counts without exposing message content.
    ///
    /// # Errors
    ///
    /// Returns a database error if either count cannot be read.
    pub fn counts(&self) -> Result<StoreCounts, StoreError> {
        let inbox_events =
            self.connection
                .query_row("SELECT COUNT(*) FROM inbox_events", [], |row| row.get(0))?;
        let outbox_jobs =
            self.connection
                .query_row("SELECT COUNT(*) FROM outbox_jobs", [], |row| row.get(0))?;
        Ok(StoreCounts {
            inbox_events,
            outbox_jobs,
        })
    }

    /// Current migration version.
    ///
    /// # Errors
    ///
    /// Returns a database error if the pragma cannot be read.
    pub fn schema_version(&self) -> Result<u32, StoreError> {
        Ok(self
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    /// Active SQLite journal mode.
    ///
    /// # Errors
    ///
    /// Returns a database error if the pragma cannot be read.
    pub fn journal_mode(&self) -> Result<String, StoreError> {
        Ok(self
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))?)
    }

    /// Whether foreign-key enforcement is active for this connection.
    ///
    /// # Errors
    ///
    /// Returns a database error if the pragma cannot be read.
    pub fn foreign_keys_enabled(&self) -> Result<bool, StoreError> {
        let enabled: i64 = self
            .connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
        Ok(enabled == 1)
    }

    /// User-table names for migration diagnostics.
    ///
    /// # Errors
    ///
    /// Returns a database error if the schema query cannot complete.
    pub fn table_names(&self) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT name FROM sqlite_schema
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )?;
        let names = statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }
}

/// Secret-safe persistence failure.
#[derive(Debug)]
pub enum StoreError {
    /// Filesystem operation failed.
    Io {
        /// Safe operation label.
        operation: &'static str,
        /// Affected path.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// SQLite rejected an operation.
    Database(rusqlite::Error),
    /// Spool-key path is not a regular, non-symlink file.
    UnsafeSpoolKeyFile {
        /// Rejected path.
        path: PathBuf,
    },
    /// Spool key is accessible by group or other users.
    UnsafeSpoolKeyPermissions {
        /// Rejected path.
        path: PathBuf,
        /// Observed Unix mode.
        mode: u32,
    },
    /// Spool key is not exactly 32 bytes.
    MalformedSpoolKey,
    /// AEAD encryption or authentication failed.
    Crypto,
    /// Public input is empty or out of range.
    InvalidInput(&'static str),
    /// Configured pending-job capacity has been reached.
    QueueFull,
    /// Requested route does not exist in durable configuration state.
    UnknownRoute(RouteId),
    /// A duplicate source ID was presented for a different route/direction.
    SourceEventCollision,
    /// Requested job does not exist.
    JobNotFound(JobId),
    /// Requested transition is invalid for the current job state.
    InvalidJobState(JobId),
    /// Stored data violates an internal invariant.
    CorruptData(&'static str),
}

impl From<rusqlite::Error> for StoreError {
    fn from(source: rusqlite::Error) -> Self {
        Self::Database(source)
    }
}

impl Display for StoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
            Self::Database(source) => write!(formatter, "database operation failed: {source}"),
            Self::UnsafeSpoolKeyFile { path } => write!(
                formatter,
                "spool key {} must be a regular non-symlink file",
                path.display()
            ),
            Self::UnsafeSpoolKeyPermissions { path, mode } => write!(
                formatter,
                "spool key {} has unsafe mode {:04o}; use 0600 or 0400",
                path.display(),
                mode & 0o777
            ),
            Self::MalformedSpoolKey => {
                formatter.write_str("spool key must contain exactly 32 bytes")
            }
            Self::Crypto => formatter.write_str("encrypted spool payload failed authentication"),
            Self::InvalidInput(field) => write!(formatter, "invalid {field}"),
            Self::QueueFull => formatter.write_str("pending job queue is full"),
            Self::UnknownRoute(route_id) => write!(formatter, "unknown route {route_id}"),
            Self::SourceEventCollision => {
                formatter.write_str("source event ID collides with another route or direction")
            }
            Self::JobNotFound(job_id) => write!(formatter, "job {} was not found", job_id.get()),
            Self::InvalidJobState(job_id) => {
                write!(formatter, "job {} is not pending", job_id.get())
            }
            Self::CorruptData(reason) => write!(formatter, "corrupt store data: {reason}"),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Database(source) => Some(source),
            _ => None,
        }
    }
}

fn validate_delivery(delivery: &NewDelivery) -> Result<(), StoreError> {
    if delivery.source_event_id.is_empty() {
        return Err(StoreError::InvalidInput("source event ID"));
    }
    if delivery.cursor.as_deref() == Some("") {
        return Err(StoreError::InvalidInput("source cursor"));
    }
    Ok(())
}

fn validate_error_class(error_class: &str) -> Result<(), StoreError> {
    if error_class.is_empty()
        || error_class.len() > 64
        || !error_class
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(StoreError::InvalidInput("error class"));
    }
    Ok(())
}

fn configure_connection(connection: &Connection) -> Result<(), StoreError> {
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL;")?;
    let mode: String = connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if mode != "wal" {
        return Err(StoreError::CorruptData("WAL mode was not enabled"));
    }
    Ok(())
}

fn migrate(connection: &Connection) -> Result<(), StoreError> {
    let mut current: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if current == 0 {
        connection.execute_batch(&format!("BEGIN IMMEDIATE;{MIGRATION_1}COMMIT;"))?;
        current = 1;
    }
    if current == 1 {
        connection.execute_batch(&format!("BEGIN IMMEDIATE;{MIGRATION_2}COMMIT;"))?;
        current = 2;
    }
    if current == 2 {
        connection.execute_batch(&format!("BEGIN IMMEDIATE;{MIGRATION_3}COMMIT;"))?;
        current = 3;
    }
    if current == 3 {
        connection.execute_batch(&format!("BEGIN IMMEDIATE;{MIGRATION_4}COMMIT;"))?;
        current = 4;
    }
    if current == 4 {
        connection.execute_batch(&format!("BEGIN IMMEDIATE;{MIGRATION_5}COMMIT;"))?;
        current = 5;
    }
    if current != SCHEMA_VERSION {
        return Err(StoreError::CorruptData(
            "unsupported database schema version",
        ));
    }
    Ok(())
}

fn validate_reactor(reactor_id: &str, display_name: Option<&str>) -> Result<(), StoreError> {
    if reactor_id.is_empty()
        || reactor_id.len() > 256
        || reactor_id.chars().any(char::is_control)
        || display_name.is_some_and(|name| {
            name.is_empty() || name.len() > 256 || name.chars().any(char::is_control)
        })
    {
        return Err(StoreError::InvalidInput("reaction actor"));
    }
    Ok(())
}

fn validate_reaction_entry(entry: &ReactionEntry) -> Result<(), StoreError> {
    validate_reactor(&entry.reactor_id, entry.display_name.as_deref())?;
    if entry.emoji.is_empty()
        || entry.emoji.len() > 128
        || entry.emoji.chars().count() > 32
        || entry.emoji.chars().any(char::is_control)
        || entry.count == 0
    {
        return Err(StoreError::InvalidInput("reaction"));
    }
    Ok(())
}

fn insert_reaction_entry(
    transaction: &rusqlite::Transaction<'_>,
    message_job_id: JobId,
    direction: DeliveryDirection,
    entry: &ReactionEntry,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO reaction_entries
         (message_job_id, direction, reactor_id, display_name, emoji, reaction_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            message_job_id.get(),
            direction.as_database_value(),
            entry.reactor_id,
            entry.display_name,
            entry.emoji,
            entry.count
        ],
    )?;
    Ok(())
}

fn reaction_summary_state(
    transaction: &rusqlite::Transaction<'_>,
    message_job_id: JobId,
    direction: DeliveryDirection,
) -> Result<ReactionSummaryState, StoreError> {
    let (destination_target_id, destination_summary_id, rendered_text) = transaction.query_row(
        "SELECT destination_target_id, destination_summary_id, rendered_text
         FROM reaction_summaries WHERE message_job_id = ?1 AND direction = ?2",
        params![message_job_id.get(), direction.as_database_value()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let mut statement = transaction.prepare(
        "SELECT reactor_id, display_name, emoji, reaction_count
         FROM reaction_entries
         WHERE message_job_id = ?1 AND direction = ?2
         ORDER BY emoji, display_name, reactor_id",
    )?;
    let rows = statement.query_map(
        params![message_job_id.get(), direction.as_database_value()],
        |row| {
            Ok(ReactionEntry {
                reactor_id: row.get(0)?,
                display_name: row.get(1)?,
                emoji: row.get(2)?,
                count: row.get(3)?,
            })
        },
    )?;
    let entries = rows.collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    Ok(ReactionSummaryState {
        destination_target_id,
        destination_summary_id,
        rendered_text,
        entries,
    })
}

fn associated_data(
    route_id: &RouteId,
    direction: DeliveryDirection,
    source_event_id: &str,
) -> Vec<u8> {
    format!(
        "{}\0{}\0{}",
        route_id.as_str(),
        direction.as_database_value(),
        source_event_id
    )
    .into_bytes()
}

fn encrypt(
    key: &[u8; SPOOL_KEY_BYTES],
    plaintext: &[u8],
    associated_data: &[u8],
) -> Result<Vec<u8>, StoreError> {
    let cipher = XChaCha20Poly1305::new_from_slice(key).map_err(|_| StoreError::Crypto)?;
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let encrypted = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: associated_data,
            },
        )
        .map_err(|_| StoreError::Crypto)?;
    let mut stored = Vec::with_capacity(NONCE_BYTES + encrypted.len());
    stored.extend_from_slice(&nonce);
    stored.extend_from_slice(&encrypted);
    Ok(stored)
}

fn decrypt(
    key: &[u8; SPOOL_KEY_BYTES],
    stored: &[u8],
    associated_data: &[u8],
) -> Result<Vec<u8>, StoreError> {
    if stored.len() <= NONCE_BYTES {
        return Err(StoreError::Crypto);
    }
    let cipher = XChaCha20Poly1305::new_from_slice(key).map_err(|_| StoreError::Crypto)?;
    cipher
        .decrypt(
            XNonce::from_slice(&stored[..NONCE_BYTES]),
            Payload {
                msg: &stored[NONCE_BYTES..],
                aad: associated_data,
            },
        )
        .map_err(|_| StoreError::Crypto)
}

fn create_data_directory(path: &Path) -> Result<(), StoreError> {
    let did_exist = path.exists();
    fs::create_dir_all(path).map_err(|source| StoreError::Io {
        operation: "create data directory",
        path: path.to_path_buf(),
        source,
    })?;
    if !did_exist {
        set_owner_only_permissions(path, 0o700, "restrict data directory")?;
    }
    Ok(())
}

fn load_or_create_spool_key(
    data_dir: &Path,
) -> Result<Zeroizing<[u8; SPOOL_KEY_BYTES]>, StoreError> {
    let path = data_dir.join(SPOOL_KEY_FILE);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => load_spool_key(&path, &metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_spool_key(data_dir, &path)
        }
        Err(source) => Err(StoreError::Io {
            operation: "inspect spool key",
            path,
            source,
        }),
    }
}

fn create_spool_key(
    data_dir: &Path,
    key_path: &Path,
) -> Result<Zeroizing<[u8; SPOOL_KEY_BYTES]>, StoreError> {
    let mut generated = XChaCha20Poly1305::generate_key(&mut OsRng);
    let mut key = Zeroizing::new([0_u8; SPOOL_KEY_BYTES]);
    key.copy_from_slice(&generated);
    generated.zeroize();

    let mut temporary = NamedTempFile::new_in(data_dir).map_err(|source| StoreError::Io {
        operation: "create temporary spool key",
        path: data_dir.to_path_buf(),
        source,
    })?;
    set_owner_only_permissions(temporary.path(), 0o600, "restrict temporary spool key")?;
    temporary
        .write_all(key.as_ref())
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| StoreError::Io {
            operation: "write temporary spool key",
            path: temporary.path().to_path_buf(),
            source,
        })?;

    match fs::hard_link(temporary.path(), key_path) {
        Ok(()) => {
            sync_directory(data_dir)?;
            Ok(key)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            key.zeroize();
            let metadata = fs::symlink_metadata(key_path).map_err(|source| StoreError::Io {
                operation: "inspect concurrently created spool key",
                path: key_path.to_path_buf(),
                source,
            })?;
            load_spool_key(key_path, &metadata)
        }
        Err(source) => Err(StoreError::Io {
            operation: "publish spool key",
            path: key_path.to_path_buf(),
            source,
        }),
    }
}

fn load_spool_key(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<Zeroizing<[u8; SPOOL_KEY_BYTES]>, StoreError> {
    if !metadata.file_type().is_file() {
        return Err(StoreError::UnsafeSpoolKeyFile {
            path: path.to_path_buf(),
        });
    }
    check_spool_key_permissions(path, metadata)?;
    if metadata.len() != SPOOL_KEY_BYTES as u64 {
        return Err(StoreError::MalformedSpoolKey);
    }
    let mut bytes = Zeroizing::new(fs::read(path).map_err(|source| StoreError::Io {
        operation: "read spool key",
        path: path.to_path_buf(),
        source,
    })?);
    if bytes.len() != SPOOL_KEY_BYTES {
        return Err(StoreError::MalformedSpoolKey);
    }
    let mut key = Zeroizing::new([0_u8; SPOOL_KEY_BYTES]);
    key.copy_from_slice(&bytes);
    bytes.zeroize();
    Ok(key)
}

#[cfg(unix)]
fn check_spool_key_permissions(path: &Path, metadata: &fs::Metadata) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode() & 0o777;
    if mode != 0o600 && mode != 0o400 {
        return Err(StoreError::UnsafeSpoolKeyPermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_spool_key_permissions(_path: &Path, _metadata: &fs::Metadata) -> Result<(), StoreError> {
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_permissions(
    path: &Path,
    mode: u32,
    operation: &'static str,
) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|source| StoreError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(not(unix))]
fn set_owner_only_permissions(
    _path: &Path,
    _mode: u32,
    _operation: &'static str,
) -> Result<(), StoreError> {
    Ok(())
}

fn restrict_database_permissions(path: &Path) -> Result<(), StoreError> {
    set_owner_only_permissions(path, 0o600, "restrict database")
}

fn sync_directory(path: &Path) -> Result<(), StoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StoreError::Io {
            operation: "synchronize data directory",
            path: path.to_path_buf(),
            source,
        })
}
