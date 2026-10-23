//! Content-free operations interfaces. No transport or user strings are accepted.

use std::fs::{File, OpenOptions};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// An advisory OS lock; retained files are harmless after process exit.
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    /// Locks one local data directory, including across processes.
    ///
    /// # Errors
    /// Returns an error for unsafe paths or an already-running instance.
    pub fn acquire(directory: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(directory)?;
            let metadata = std::fs::symlink_metadata(directory)?;
            if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
                return Err(io::Error::other(
                    "data directory must be private and not a symlink",
                ));
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .custom_flags(
                    i32::try_from(rustix::fs::OFlags::NOFOLLOW.bits()).map_err(io::Error::other)?,
                )
                .open(directory.join("instance.lock"))?;
            let metadata = file.metadata()?;
            if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
                return Err(io::Error::other(
                    "instance lock must be a private regular file",
                ));
            }
            rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
            Ok(Self { _file: file })
        }
        #[cfg(not(unix))]
        {
            let _ = directory;
            Err(io::Error::other("instance locking requires Unix"))
        }
    }
}

#[derive(Default)]
struct Snapshot {
    ready: bool,
    pending: u64,
    dead: u64,
    refreshed: Option<Instant>,
}

/// Shared health snapshot. Stale observations fail readiness closed after 90s.
#[derive(Clone, Default)]
pub struct Health(Arc<Mutex<Snapshot>>, Arc<AtomicBool>);

impl Health {
    /// Permanently withdraws readiness before draining, even if work completes.
    pub fn stop(&self) {
        self.1.store(true, Ordering::Relaxed);
    }
    /// Publishes only aggregate operational values, never route or user labels.
    pub fn update(&self, ready: bool, pending: u64, dead: u64) {
        if let Ok(mut snapshot) = self.0.lock() {
            *snapshot = Snapshot {
                ready,
                pending,
                dead,
                refreshed: Some(Instant::now()),
            };
        }
    }

    /// Returns a content-free status and response body for an exact path.
    #[must_use]
    pub fn response(&self, path: &str) -> (u16, String) {
        let Ok(snapshot) = self.0.lock() else {
            return (503, "unavailable\n".into());
        };
        let ready = !self.1.load(Ordering::Relaxed)
            && snapshot.ready
            && snapshot
                .refreshed
                .is_some_and(|at| at.elapsed() < Duration::from_secs(90));
        match path {
            "/live" => (200, "live\n".into()),
            "/ready" => {
                if ready {
                    (200, "ready\n".into())
                } else {
                    (503, "not ready\n".into())
                }
            }
            "/metrics" => (
                200,
                format!(
                    "# TYPE bridge_ready gauge\nbridge_ready {}\n# TYPE bridge_pending_jobs gauge\nbridge_pending_jobs {}\n# TYPE bridge_dead_letters gauge\nbridge_dead_letters {}\n",
                    u8::from(ready),
                    snapshot.pending,
                    snapshot.dead
                ),
            ),
            _ => (404, "not found\n".into()),
        }
    }

    /// Starts a bounded HTTP server. Bind to a trusted interface only.
    ///
    /// # Errors
    /// Returns socket binding or address inspection errors.
    pub async fn serve(&self, address: SocketAddr) -> io::Result<HealthServer> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let health = self.clone();
        let task = tokio::spawn(async move {
            let slots = Arc::new(Semaphore::new(16));
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    continue;
                };
                let health = health.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = tokio::time::timeout(Duration::from_secs(2), async {
                        let mut request = Vec::new();
                        let mut buffer = [0_u8; 512];
                        while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                            let count = stream.read(&mut buffer).await?;
                            if count == 0 || request.len() + count > 2048 { return Ok::<(), io::Error>(()); }
                            request.extend_from_slice(&buffer[..count]);
                        }
                        let line = std::str::from_utf8(&request).unwrap_or("").lines().next().unwrap_or("");
                        let mut parts = line.split_whitespace();
                        let method = parts.next();
                        let path = parts.next().unwrap_or("");
                        let (status, body) = if method == Some("GET") { health.response(path) } else { (405, "method not allowed\n".into()) };
                        let response = format!("HTTP/1.1 {status} Status\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n{body}", body.len());
                        stream.write_all(response.as_bytes()).await?;
                        stream.shutdown().await
                    }).await;
                });
            }
        });
        Ok(HealthServer { address, task })
    }
}

/// Holds the listener task for the lifetime of the running service.
pub struct HealthServer {
    address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl HealthServer {
    /// Actual bound address (supports port zero in tests).
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }
}
impl Drop for HealthServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Fixed event vocabulary prevents arbitrary content from entering default logs.
#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEvent {
    Starting,
    Ready,
    Shutdown,
    RuntimeFailed,
    LockUnavailable,
    HealthBindFailed,
    StoreFailed,
    ArmadaConnectFailed,
    ArmadaConnectDatabaseFailed,
    ArmadaConnectIoFailed,
    ArmadaConnectNostrFailed,
    ArmadaConnectCryptoFailed,
    ArmadaConnectNetworkFailed,
    ArmadaConnectStateFailed,
    ArmadaConnectSdkFailed,
    StartupFailed,
    StartupTelegramConflict,
    StartupTelegramAuthenticationFailed,
    StartupTelegramAccessFailed,
    StartupTelegramIdentityFailed,
    StartupTelegramTransportFailed,
    StartupArmadaCommunityMissing,
    StartupArmadaChannelMissing,
    StartupArmadaChannelNotPrivate,
    StartupArmadaChannelKeyMissing,
    StartupArmadaRekeyIncomplete,
    StartupArmadaMembershipRemoved,
    StartupArmadaCommunityDissolved,
    StartupArmadaStateStale,
    StartupArmadaSdkFailed,
    StartupRouteQuarantined,
    StartupStateFailed,
    DoctorPassed,
    DoctorFailed,
}

/// Emits JSON from a fixed event name and numeric aggregate only.
#[must_use]
pub fn structured_log(event: LogEvent, count: u64) -> String {
    serde_json::json!({ "event": event, "count": count }).to_string()
}

/// Waits for SIGTERM or Ctrl-C without exiting before the caller drains work.
///
/// # Errors
/// Returns signal handler registration errors.
pub async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

/// Checks local state metadata without opening the SDK/database or writing files.
/// Missing state is valid before the first start. This does not certify live access.
///
/// # Errors
/// Returns an error for inaccessible, incomplete, or unsafe existing state.
pub fn doctor(config: &crate::config::BridgeConfig) -> io::Result<()> {
    let directory = config.data_dir();
    let metadata = match std::fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Err(io::Error::other("unsafe state directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::other("state directory is not private"));
        }
    }
    let database = directory.join("bridge.sqlite3");
    let key = directory.join("spool.key");
    if database.exists() != key.exists() {
        return Err(io::Error::other(
            "incomplete state: database and spool key must stay together",
        ));
    }
    for path in [database, key] {
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(io::Error::other("unsafe state file"));
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if metadata.permissions().mode() & 0o077 != 0 {
                        return Err(io::Error::other("state file is not private"));
                    }
                }
                if path.file_name().is_some_and(|name| name == "spool.key") && metadata.len() != 32
                {
                    return Err(io::Error::other("invalid spool key size"));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
