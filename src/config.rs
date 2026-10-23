//! Offline loading and validation for the static bridge configuration.

use std::collections::HashSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vector_sdk::nostr::{FromBech32, Keys, PublicKey, ToBech32};
use zeroize::Zeroize;

use crate::domain::{ChannelId, CommunityId, RouteId, TelegramChatId, TelegramThreadId};
use crate::error::Sensitive;

const CONFIG_VERSION: u32 = 1;
const MAX_DOTENV_BYTES: u64 = 65_536;
const MAX_TEXT_BYTES: u64 = 1_000_000;
const MAX_FILE_BYTES: u64 = 20_000_000;
const MAX_PENDING_JOBS: u64 = 100_000;
const MAX_CATCHUP_AGE_HOURS: u64 = 24 * 30;
const MAX_CATCHUP_EVENTS: u64 = 100_000;

/// Validated static bridge configuration, including redacting secret wrappers.
#[derive(Debug, Serialize)]
pub struct BridgeConfig {
    version: u32,
    data_dir: PathBuf,
    health_listen: std::net::SocketAddr,
    nostr: NostrConfig,
    telegram: TelegramConfig,
    routes: Vec<Route>,
    limits: Limits,
    first_startup: FirstStartupPolicy,
    fingerprint: ConfigFingerprint,
}

impl BridgeConfig {
    /// Loads TOML plus the adjacent `.env` without network access.
    ///
    /// Relative data paths and `.env` are resolved from the configuration file's
    /// parent directory rather than the process working directory.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed configuration, an unsafe `.env`,
    /// identity mismatches, or unsupported values.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let source = fs::read_to_string(path).map_err(|source| ConfigError::ConfigIo {
            path: path.to_path_buf(),
            source,
        })?;
        let raw: RawConfig = toml::from_str(&source).map_err(|_| ConfigError::Toml)?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        Self::validate(raw, base)
    }

    fn validate(raw: RawConfig, base: &Path) -> Result<Self, ConfigError> {
        if raw.version != CONFIG_VERSION {
            return Err(ConfigError::UnsupportedVersion(raw.version));
        }
        if raw.routes.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "routes",
                reason: "at least one route is required",
            });
        }

        let data_dir = resolve_path(base, raw.data_dir);
        let dotenv_path = base.join(".env");
        let (nsec, token) = read_dotenv(&dotenv_path)?;
        let expected_npub = validate_nostr_identity(&nsec, raw.nostr.expected_npub)?;
        validate_telegram_token(&token, raw.telegram.expected_bot_id)?;

        let mut route_ids = HashSet::with_capacity(raw.routes.len());
        let mut routes = Vec::with_capacity(raw.routes.len());
        for raw_route in raw.routes {
            let id = RouteId::try_from(raw_route.name).map_err(|_| ConfigError::InvalidField {
                field: "routes.name",
                reason: "must be a stable lowercase route ID, not a display name",
            })?;
            if !route_ids.insert(id.clone()) {
                return Err(ConfigError::DuplicateRouteId { route_id: id });
            }
            let community_id = CommunityId::try_from(raw_route.community_id).map_err(|_| {
                ConfigError::InvalidField {
                    field: "routes.community_id",
                    reason: "must be a canonical 64-character hexadecimal ID",
                }
            })?;
            let channel_id = ChannelId::try_from(raw_route.channel_id).map_err(|_| {
                ConfigError::InvalidField {
                    field: "routes.channel_id",
                    reason: "must be a canonical 64-character hexadecimal ID",
                }
            })?;
            let telegram_chat_id =
                TelegramChatId::try_from(raw_route.telegram_chat_id).map_err(|_| {
                    ConfigError::InvalidField {
                        field: "routes.telegram_chat_id",
                        reason: "must be a negative numeric Telegram group ID",
                    }
                })?;
            let telegram_thread_id = raw_route
                .telegram_thread_id
                .map(TelegramThreadId::try_from)
                .transpose()
                .map_err(|_| ConfigError::InvalidField {
                    field: "routes.telegram_thread_id",
                    reason: "must be a positive numeric Telegram topic ID",
                })?;
            routes.push(Route {
                id,
                community_id,
                channel_id,
                telegram_chat_id,
                telegram_thread_id,
            });
        }

        let limits = Limits::try_from(raw.limits)?;
        let nostr = NostrConfig {
            expected_npub,
            nsec,
        };
        let telegram = TelegramConfig {
            expected_bot_id: raw.telegram.expected_bot_id,
            mode: raw.telegram.mode,
            protect_content: raw.telegram.protect_content,
            relay_other_bots: raw.telegram.relay_other_bots,
            token,
        };
        let first_startup = raw.startup.first_run;
        let fingerprint = fingerprint(
            raw.version,
            &data_dir,
            &nostr,
            &telegram,
            &routes,
            &limits,
            first_startup,
            raw.health_listen,
        );

        Ok(Self {
            version: raw.version,
            data_dir,
            health_listen: raw.health_listen,
            nostr,
            telegram,
            routes,
            limits,
            first_startup,
            fingerprint,
        })
    }

    /// Configuration schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Absolute or configuration-relative state directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Address for content-free health and metrics endpoints; loopback by default.
    #[must_use]
    pub const fn health_listen(&self) -> std::net::SocketAddr {
        self.health_listen
    }

    /// Validated Nostr identity settings.
    #[must_use]
    pub const fn nostr(&self) -> &NostrConfig {
        &self.nostr
    }

    /// Validated Telegram settings.
    #[must_use]
    pub const fn telegram(&self) -> &TelegramConfig {
        &self.telegram
    }

    /// Exact static routes.
    #[must_use]
    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    /// Validated resource limits.
    #[must_use]
    pub const fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Policy used when no durable cursor exists yet.
    #[must_use]
    pub const fn first_startup(&self) -> FirstStartupPolicy {
        self.first_startup
    }

    /// Stable hash of non-secret effective configuration.
    #[must_use]
    pub const fn fingerprint(&self) -> &ConfigFingerprint {
        &self.fingerprint
    }
}

/// Validated Nostr credentials and expected public identity.
#[derive(Debug, Serialize)]
pub struct NostrConfig {
    expected_npub: String,
    nsec: Sensitive,
}

impl NostrConfig {
    /// Secret key usable only by the Concord adapter.
    #[must_use]
    pub const fn nsec(&self) -> &Sensitive {
        &self.nsec
    }

    /// Operator-reviewed public identity derived from the secret key.
    #[must_use]
    pub fn expected_npub(&self) -> &str {
        &self.expected_npub
    }
}

/// Telegram delivery mode. P02 deliberately supports no webhook listener.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelegramMode {
    /// Poll Telegram with a bounded long-poll request.
    #[default]
    LongPolling,
}

/// Validated Telegram credentials and intake policies.
#[derive(Debug, Serialize)]
pub struct TelegramConfig {
    expected_bot_id: u64,
    mode: TelegramMode,
    protect_content: bool,
    relay_other_bots: bool,
    token: Sensitive,
}

impl TelegramConfig {
    /// Bot token usable only by the Telegram adapter.
    #[must_use]
    pub const fn token(&self) -> &Sensitive {
        &self.token
    }

    /// Numeric Bot API identity.
    #[must_use]
    pub const fn expected_bot_id(&self) -> u64 {
        self.expected_bot_id
    }

    /// Telegram update delivery mode.
    #[must_use]
    pub const fn mode(&self) -> TelegramMode {
        self.mode
    }

    /// Whether outbound Telegram content is protected from forwarding/saving.
    #[must_use]
    pub const fn protect_content(&self) -> bool {
        self.protect_content
    }

    /// Whether messages authored by other bots may enter the bridge.
    #[must_use]
    pub const fn relay_other_bots(&self) -> bool {
        self.relay_other_bots
    }
}

/// First-start behavior when the bridge has no durable cursor.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FirstStartupPolicy {
    /// Record a baseline and relay only subsequently observed messages.
    #[default]
    NewMessagesOnly,
}

/// One exact Armada channel to Telegram group/topic mapping.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Route {
    id: RouteId,
    community_id: CommunityId,
    channel_id: ChannelId,
    telegram_chat_id: TelegramChatId,
    telegram_thread_id: Option<TelegramThreadId>,
}

impl Route {
    /// Operator-defined stable route ID.
    #[must_use]
    pub const fn id(&self) -> &RouteId {
        &self.id
    }

    /// Canonical Concord community ID.
    #[must_use]
    pub const fn community_id(&self) -> &CommunityId {
        &self.community_id
    }

    /// Canonical Concord channel ID.
    #[must_use]
    pub const fn channel_id(&self) -> &ChannelId {
        &self.channel_id
    }

    /// Numeric Telegram group/supergroup ID.
    #[must_use]
    pub const fn telegram_chat_id(&self) -> TelegramChatId {
        self.telegram_chat_id
    }

    /// Optional numeric Telegram forum topic ID.
    #[must_use]
    pub const fn telegram_thread_id(&self) -> Option<TelegramThreadId> {
        self.telegram_thread_id
    }
}

/// A positive, upper-bounded operational limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ValidatedLimit<const MAX: u64>(NonZeroU64);

impl<const MAX: u64> ValidatedLimit<MAX> {
    fn new(field: &'static str, value: u64) -> Result<Self, ConfigError> {
        if value == 0 || value > MAX {
            return Err(ConfigError::InvalidLimit {
                field,
                value,
                maximum: MAX,
            });
        }
        Ok(Self(
            NonZeroU64::new(value).expect("nonzero limit checked immediately above"),
        ))
    }

    /// Returns the validated value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Validated resource and catch-up bounds.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Limits {
    max_text_bytes: ValidatedLimit<MAX_TEXT_BYTES>,
    max_file_bytes: ValidatedLimit<MAX_FILE_BYTES>,
    max_pending_jobs: ValidatedLimit<MAX_PENDING_JOBS>,
    max_catchup_age_hours: ValidatedLimit<MAX_CATCHUP_AGE_HOURS>,
    max_catchup_events: ValidatedLimit<MAX_CATCHUP_EVENTS>,
}

impl Limits {
    /// Maximum normalized text payload.
    #[must_use]
    pub const fn max_text_bytes(&self) -> ValidatedLimit<MAX_TEXT_BYTES> {
        self.max_text_bytes
    }

    /// Maximum attachment size accepted by the MVP.
    #[must_use]
    pub const fn max_file_bytes(&self) -> ValidatedLimit<MAX_FILE_BYTES> {
        self.max_file_bytes
    }

    /// Per-process durable pending-job bound.
    #[must_use]
    pub const fn max_pending_jobs(&self) -> ValidatedLimit<MAX_PENDING_JOBS> {
        self.max_pending_jobs
    }

    /// Maximum restart catch-up age.
    #[must_use]
    pub const fn max_catchup_age_hours(&self) -> ValidatedLimit<MAX_CATCHUP_AGE_HOURS> {
        self.max_catchup_age_hours
    }

    /// Maximum restart catch-up event count.
    #[must_use]
    pub const fn max_catchup_events(&self) -> ValidatedLimit<MAX_CATCHUP_EVENTS> {
        self.max_catchup_events
    }
}

impl TryFrom<RawLimits> for Limits {
    type Error = ConfigError;

    fn try_from(raw: RawLimits) -> Result<Self, Self::Error> {
        Ok(Self {
            max_text_bytes: ValidatedLimit::new("limits.max_text_bytes", raw.max_text_bytes)?,
            max_file_bytes: ValidatedLimit::new("limits.max_file_bytes", raw.max_file_bytes)?,
            max_pending_jobs: ValidatedLimit::new("limits.max_pending_jobs", raw.max_pending_jobs)?,
            max_catchup_age_hours: ValidatedLimit::new(
                "limits.max_catchup_age_hours",
                raw.max_catchup_age_hours,
            )?,
            max_catchup_events: ValidatedLimit::new(
                "limits.max_catchup_events",
                raw.max_catchup_events,
            )?,
        })
    }
}

/// SHA-256 of effective non-secret configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ConfigFingerprint(String);

impl Display for ConfigFingerprint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Secret-safe offline configuration failure.
#[derive(Debug)]
pub enum ConfigError {
    /// Configuration file could not be read.
    ConfigIo {
        /// Configuration path.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// TOML could not be decoded.
    Toml,
    /// Configuration version is unsupported.
    UnsupportedVersion(u32),
    /// A public field failed validation.
    InvalidField {
        /// Stable field path.
        field: &'static str,
        /// Secret-free validation guidance.
        reason: &'static str,
    },
    /// A configured limit is zero or excessive.
    InvalidLimit {
        /// Stable field path.
        field: &'static str,
        /// Rejected numeric value.
        value: u64,
        /// Inclusive supported maximum.
        maximum: u64,
    },
    /// Two routes use the same stable ID.
    DuplicateRouteId {
        /// Duplicated route ID.
        route_id: RouteId,
    },
    /// Dotenv file could not be inspected or read.
    DotenvIo {
        /// Dotenv path; never its contents.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// Dotenv path is not a regular, non-symlink file.
    UnsafeDotenvFile {
        /// Rejected path.
        path: PathBuf,
    },
    /// Dotenv file is accessible to group or other users.
    UnsafeDotenvPermissions {
        /// Rejected path.
        path: PathBuf,
        /// Observed Unix mode.
        mode: u32,
    },
    /// Dotenv file is unreasonably large.
    DotenvTooLarge {
        /// Rejected path.
        path: PathBuf,
    },
    /// Dotenv syntax is unsupported or a required key is duplicated.
    InvalidDotenv,
    /// A required credential is absent from `.env`.
    MissingDotenvKey {
        /// Required public variable name.
        name: &'static str,
    },
    /// Nostr secret key is malformed; raw input is intentionally omitted.
    InvalidNsec,
    /// Expected public identity is malformed.
    InvalidExpectedNpub,
    /// Nostr secret does not derive the expected public identity.
    IdentityMismatch {
        /// Operator-reviewed public identity.
        expected: String,
        /// Public identity derived offline.
        derived: String,
    },
    /// Telegram token is malformed; raw input is intentionally omitted.
    InvalidTelegramToken,
    /// Token prefix and configured bot identity disagree.
    TelegramBotIdMismatch {
        /// Operator-reviewed bot ID.
        expected: u64,
        /// Bot ID encoded in the token prefix.
        token_bot_id: u64,
    },
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigIo { path, source } => {
                write!(
                    formatter,
                    "cannot read configuration {}: {source}",
                    path.display()
                )
            }
            Self::Toml => formatter.write_str("invalid configuration TOML"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported configuration version {version}")
            }
            Self::InvalidField { field, reason } => write!(formatter, "invalid {field}: {reason}"),
            Self::InvalidLimit {
                field,
                value,
                maximum,
            } => write!(
                formatter,
                "invalid {field}: {value} must be between 1 and {maximum}"
            ),
            Self::DuplicateRouteId { route_id } => {
                write!(formatter, "duplicate route ID {route_id}")
            }
            Self::DotenvIo { path, source } => {
                write!(
                    formatter,
                    "cannot read dotenv file {}: {source}",
                    path.display()
                )
            }
            Self::UnsafeDotenvFile { path } => write!(
                formatter,
                "dotenv path {} must be a regular non-symlink file",
                path.display()
            ),
            Self::UnsafeDotenvPermissions { path, mode } => write!(
                formatter,
                "dotenv file {} has unsafe mode {:04o}; use 0600 or 0400",
                path.display(),
                mode & 0o777
            ),
            Self::DotenvTooLarge { path } => {
                write!(
                    formatter,
                    "dotenv file {} exceeds 65536 bytes",
                    path.display()
                )
            }
            Self::InvalidDotenv => formatter.write_str("invalid .env syntax"),
            Self::MissingDotenvKey { name } => {
                write!(formatter, ".env must define {name}")
            }
            Self::InvalidNsec => formatter.write_str("ARMADA_NSEC must contain a valid nsec"),
            Self::InvalidExpectedNpub => {
                formatter.write_str("nostr.expected_npub must contain a valid canonical npub")
            }
            Self::IdentityMismatch { .. } => {
                formatter.write_str("configured npub does not match the supplied key")
            }
            Self::InvalidTelegramToken => {
                formatter.write_str("TELEGRAM_BOT_TOKEN must contain a valid Telegram bot token")
            }
            Self::TelegramBotIdMismatch {
                expected,
                token_bot_id,
            } => write!(
                formatter,
                "configured Telegram bot ID {expected} does not match token bot ID {token_bot_id}"
            ),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigIo { source, .. } | Self::DotenvIo { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    version: u32,
    data_dir: PathBuf,
    #[serde(default = "default_health_listen")]
    health_listen: std::net::SocketAddr,
    nostr: RawNostrConfig,
    telegram: RawTelegramConfig,
    routes: Vec<RawRoute>,
    #[serde(default)]
    limits: RawLimits,
    #[serde(default)]
    startup: RawStartup,
}

fn default_health_listen() -> std::net::SocketAddr {
    std::net::SocketAddr::from(([127, 0, 0, 1], 9090))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNostrConfig {
    expected_npub: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTelegramConfig {
    expected_bot_id: u64,
    #[serde(default)]
    mode: TelegramMode,
    #[serde(default = "default_true")]
    protect_content: bool,
    #[serde(default)]
    relay_other_bots: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRoute {
    name: String,
    community_id: String,
    channel_id: String,
    telegram_chat_id: i64,
    telegram_thread_id: Option<u32>,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
struct RawLimits {
    max_text_bytes: u64,
    max_file_bytes: u64,
    max_pending_jobs: u64,
    max_catchup_age_hours: u64,
    max_catchup_events: u64,
}

impl Default for RawLimits {
    fn default() -> Self {
        Self {
            max_text_bytes: 60_000,
            max_file_bytes: 20_000_000,
            max_pending_jobs: 500,
            max_catchup_age_hours: 24,
            max_catchup_events: 1_000,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawStartup {
    first_run: FirstStartupPolicy,
}

const fn default_true() -> bool {
    true
}

fn resolve_path(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn read_dotenv(path: &Path) -> Result<(Sensitive, Sensitive), ConfigError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| ConfigError::DotenvIo {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_file() {
        return Err(ConfigError::UnsafeDotenvFile {
            path: path.to_path_buf(),
        });
    }
    if metadata.len() > MAX_DOTENV_BYTES {
        return Err(ConfigError::DotenvTooLarge {
            path: path.to_path_buf(),
        });
    }
    check_dotenv_permissions(path, &metadata)?;

    let mut raw = fs::read_to_string(path).map_err(|source| ConfigError::DotenvIo {
        path: path.to_path_buf(),
        source,
    })?;
    let result = parse_dotenv_secrets(&raw);
    raw.zeroize();
    result
}

fn parse_dotenv_secrets(raw: &str) -> Result<(Sensitive, Sensitive), ConfigError> {
    let mut nsec = None;
    let mut token = None;

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or(ConfigError::InvalidDotenv)?;
        let key = key.trim();
        let value = parse_dotenv_value(value.trim())?;
        let destination = match key {
            "ARMADA_NSEC" => &mut nsec,
            "TELEGRAM_BOT_TOKEN" => &mut token,
            _ => continue,
        };
        if destination.is_some() {
            return Err(ConfigError::InvalidDotenv);
        }
        *destination = Some(Sensitive::new(value.to_owned()));
    }

    let nsec = nsec.ok_or(ConfigError::MissingDotenvKey {
        name: "ARMADA_NSEC",
    })?;
    let token = token.ok_or(ConfigError::MissingDotenvKey {
        name: "TELEGRAM_BOT_TOKEN",
    })?;
    Ok((nsec, token))
}

fn parse_dotenv_value(value: &str) -> Result<&str, ConfigError> {
    if let Some(value) = value.strip_prefix('"') {
        return value.strip_suffix('"').ok_or(ConfigError::InvalidDotenv);
    }
    if let Some(value) = value.strip_prefix('\'') {
        return value.strip_suffix('\'').ok_or(ConfigError::InvalidDotenv);
    }
    if value.ends_with('"') || value.ends_with('\'') {
        return Err(ConfigError::InvalidDotenv);
    }
    Ok(value)
}

#[cfg(unix)]
fn check_dotenv_permissions(path: &Path, metadata: &fs::Metadata) -> Result<(), ConfigError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(ConfigError::UnsafeDotenvPermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_dotenv_permissions(_path: &Path, _metadata: &fs::Metadata) -> Result<(), ConfigError> {
    Ok(())
}

fn validate_nostr_identity(nsec: &Sensitive, expected: String) -> Result<String, ConfigError> {
    if !nsec.expose().starts_with("nsec1") {
        return Err(ConfigError::InvalidNsec);
    }
    let keys = Keys::parse(nsec.expose()).map_err(|_| ConfigError::InvalidNsec)?;
    if !expected.starts_with("npub1") {
        return Err(ConfigError::InvalidExpectedNpub);
    }
    let expected_key =
        PublicKey::from_bech32(&expected).map_err(|_| ConfigError::InvalidExpectedNpub)?;
    let canonical_expected = expected_key
        .to_bech32()
        .map_err(|_| ConfigError::InvalidExpectedNpub)?;
    if expected != canonical_expected {
        return Err(ConfigError::InvalidExpectedNpub);
    }
    let derived = keys
        .public_key()
        .to_bech32()
        .map_err(|_| ConfigError::InvalidNsec)?;
    if derived != expected {
        return Err(ConfigError::IdentityMismatch { expected, derived });
    }
    Ok(canonical_expected)
}

fn validate_telegram_token(token: &Sensitive, expected_bot_id: u64) -> Result<(), ConfigError> {
    if expected_bot_id == 0 {
        return Err(ConfigError::InvalidField {
            field: "telegram.expected_bot_id",
            reason: "must be positive",
        });
    }
    let (prefix, secret) = token
        .expose()
        .split_once(':')
        .ok_or(ConfigError::InvalidTelegramToken)?;
    let token_bot_id = prefix
        .parse::<u64>()
        .map_err(|_| ConfigError::InvalidTelegramToken)?;
    if token_bot_id == 0
        || !(20..=128).contains(&secret.len())
        || !secret
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(ConfigError::InvalidTelegramToken);
    }
    if token_bot_id != expected_bot_id {
        return Err(ConfigError::TelegramBotIdMismatch {
            expected: expected_bot_id,
            token_bot_id,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn fingerprint(
    version: u32,
    data_dir: &Path,
    nostr: &NostrConfig,
    telegram: &TelegramConfig,
    routes: &[Route],
    limits: &Limits,
    first_startup: FirstStartupPolicy,
    health_listen: std::net::SocketAddr,
) -> ConfigFingerprint {
    let mut hasher = Sha256::new();
    hash_part(&mut hasher, &version.to_string());
    hash_part(&mut hasher, &data_dir.to_string_lossy());
    hash_part(&mut hasher, &health_listen.to_string());
    hash_part(&mut hasher, &nostr.expected_npub);
    hash_part(&mut hasher, &telegram.expected_bot_id.to_string());
    hash_part(&mut hasher, &format!("{:?}", telegram.mode));
    hash_part(&mut hasher, &telegram.protect_content.to_string());
    hash_part(&mut hasher, &telegram.relay_other_bots.to_string());
    for route in routes {
        hash_part(&mut hasher, route.id.as_str());
        hash_part(&mut hasher, route.community_id.as_str());
        hash_part(&mut hasher, route.channel_id.as_str());
        hash_part(&mut hasher, &route.telegram_chat_id.get().to_string());
        hash_part(
            &mut hasher,
            &route
                .telegram_thread_id
                .map_or_else(String::new, |id| id.get().to_string()),
        );
    }
    for value in [
        limits.max_text_bytes.get(),
        limits.max_file_bytes.get(),
        limits.max_pending_jobs.get(),
        limits.max_catchup_age_hours.get(),
        limits.max_catchup_events.get(),
    ] {
        hash_part(&mut hasher, &value.to_string());
    }
    hash_part(&mut hasher, &format!("{first_startup:?}"));
    ConfigFingerprint(format!("{:x}", hasher.finalize()))
}

fn hash_part(hasher: &mut Sha256, value: &str) {
    hasher.update(value.len().to_le_bytes());
    hasher.update(value.as_bytes());
}
