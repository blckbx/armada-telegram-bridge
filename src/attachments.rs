//! Bounded attachment staging and untrusted metadata normalization.

use std::collections::HashSet;
use std::error::Error;
use std::ffi::OsString;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};

use tempfile::{Builder, NamedTempFile};

/// Maximum attachment count accepted from one source message.
pub const MAX_ATTACHMENTS_PER_MESSAGE: usize = 10;
const SNIFF_BYTES: usize = 512;

/// A classified, safely named staged attachment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectedAttachment {
    /// Sanitized user-facing filename.
    pub file_name: String,
    /// Media type detected from file bytes.
    pub media_type: String,
    /// Actual bytes on disk.
    pub size_bytes: u64,
}

/// Attachment validation or staging failure without sensitive source details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachmentError {
    /// Declared or actual content exceeds the configured bound.
    TooLarge,
    /// The source supplied too many attachments.
    TooMany,
    /// Temporary storage could not be safely prepared or inspected.
    TemporaryStorage,
    /// A bridge-controlled remote URL is unsafe.
    UnsafeUrl,
}

impl Display for AttachmentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge => formatter.write_str("attachment exceeds the configured limit"),
            Self::TooMany => formatter.write_str("message has too many attachments"),
            Self::TemporaryStorage => formatter.write_str("private attachment staging failed"),
            Self::UnsafeUrl => formatter.write_str("attachment URL is not safe for remote fetch"),
        }
    }
}

impl Error for AttachmentError {}

/// A randomized mode-`0600` temporary file removed automatically on drop.
pub struct PrivateTempFile {
    file: NamedTempFile,
    display_name: String,
}

/// Removes files an SDK operation adds to one bridge-owned cache directory,
/// including when its future is cancelled or unwinds.
pub(crate) struct DirectoryCleanupGuard {
    directory: PathBuf,
    initial: HashSet<OsString>,
}

impl DirectoryCleanupGuard {
    pub(crate) fn new(directory: &Path) -> Result<Self, AttachmentError> {
        fs::create_dir_all(directory).map_err(|_| AttachmentError::TemporaryStorage)?;
        set_directory_private(directory)?;
        let initial = directory_entries(directory)?;
        Ok(Self {
            directory: directory.to_owned(),
            initial,
        })
    }
}

impl Drop for DirectoryCleanupGuard {
    fn drop(&mut self) {
        let Ok(entries) = fs::read_dir(&self.directory) else {
            return;
        };
        for entry in entries.flatten() {
            if !self.initial.contains(&entry.file_name()) {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

fn directory_entries(directory: &Path) -> Result<HashSet<OsString>, AttachmentError> {
    fs::read_dir(directory)
        .map_err(|_| AttachmentError::TemporaryStorage)?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|_| AttachmentError::TemporaryStorage)
        })
        .collect()
}

impl PrivateTempFile {
    /// Creates a private staging file under the configured bridge temp root.
    ///
    /// # Errors
    ///
    /// Fails if the directory or private file cannot be created safely.
    pub fn new(root: &Path, untrusted_name: Option<&str>) -> Result<Self, AttachmentError> {
        fs::create_dir_all(root).map_err(|_| AttachmentError::TemporaryStorage)?;
        set_directory_private(root)?;
        let display_name = sanitize_filename(untrusted_name);
        let suffix = Path::new(&display_name)
            .extension()
            .and_then(|extension| extension.to_str())
            .map_or_else(String::new, |extension| format!(".{extension}"));
        let file = Builder::new()
            .prefix("transfer-")
            .suffix(&suffix)
            .tempfile_in(root)
            .map_err(|_| AttachmentError::TemporaryStorage)?;
        set_file_private(file.path())?;
        Ok(Self { file, display_name })
    }

    /// Random internal path. Never use it as user-facing metadata.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.file.path()
    }

    /// Sanitized original filename for destination metadata.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Validates actual length and classifies the leading bytes.
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be inspected or exceeds `maximum_bytes`.
    pub fn inspect(&self, maximum_bytes: u64) -> Result<InspectedAttachment, AttachmentError> {
        let size_bytes = self
            .file
            .as_file()
            .metadata()
            .map_err(|_| AttachmentError::TemporaryStorage)?
            .len();
        if size_bytes > maximum_bytes {
            return Err(AttachmentError::TooLarge);
        }
        let mut bytes = [0_u8; SNIFF_BYTES];
        let mut reader =
            fs::File::open(self.path()).map_err(|_| AttachmentError::TemporaryStorage)?;
        let read = reader
            .read(&mut bytes)
            .map_err(|_| AttachmentError::TemporaryStorage)?;
        Ok(InspectedAttachment {
            file_name: self.display_name.clone(),
            media_type: sniff_media_type(&bytes[..read]).to_owned(),
            size_bytes,
        })
    }
}

/// Rejects oversized declarations before any network access.
///
/// A zero declaration means the source did not supply a size and must be
/// checked while streaming.
///
/// # Errors
///
/// Returns [`AttachmentError::TooLarge`] above `maximum_bytes`.
pub fn validate_declared_size(size_bytes: u64, maximum_bytes: u64) -> Result<(), AttachmentError> {
    if size_bytes > maximum_bytes {
        Err(AttachmentError::TooLarge)
    } else {
        Ok(())
    }
}

/// Reduces an untrusted filename to a portable, non-hidden basename.
#[must_use]
pub fn sanitize_filename(value: Option<&str>) -> String {
    let candidate = value
        .unwrap_or("attachment.bin")
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("attachment.bin");
    let mut safe = String::new();
    for character in candidate.chars() {
        if safe.len() >= 120 {
            break;
        }
        if character.is_control() || is_directional_control(character) {
            continue;
        }
        if matches!(
            character,
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
        ) {
            safe.push('_');
        } else {
            safe.push(character);
        }
    }
    let safe = safe.trim_matches([' ', '.']);
    if safe.is_empty() || safe == "." || safe == ".." {
        "attachment.bin".to_owned()
    } else {
        safe.to_owned()
    }
}

/// Classifies common safe media formats from magic bytes.
#[must_use]
pub fn sniff_media_type(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.starts_with(b"%PDF-") {
        "application/pdf"
    } else if bytes.starts_with(b"OggS") {
        "audio/ogg"
    } else if bytes.starts_with(b"ID3") || bytes.starts_with(b"\xff\xfb") {
        "audio/mpeg"
    } else if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        "video/mp4"
    } else if bytes.starts_with(b"\x1aE\xdf\xa3") {
        "video/webm"
    } else if bytes.starts_with(b"PK\x03\x04") {
        "application/zip"
    } else {
        "application/octet-stream"
    }
}

/// Validates an attacker-controlled URL before bridge-owned HTTP fetching.
///
/// # Errors
///
/// Rejects non-HTTP schemes, credentials, localhost names, and literal private,
/// loopback, link-local, multicast, documentation, or unspecified addresses.
pub fn validate_remote_url(value: &str) -> Result<reqwest::Url, AttachmentError> {
    let url = reqwest::Url::parse(value).map_err(|_| AttachmentError::UnsafeUrl)?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(AttachmentError::UnsafeUrl);
    }
    let host = url.host_str().ok_or(AttachmentError::UnsafeUrl)?;
    match host.trim_matches(['[', ']']).parse::<IpAddr>() {
        Err(_) => {
            let domain = host.trim_end_matches('.').to_ascii_lowercase();
            let final_label = domain.rsplit('.').next().unwrap_or_default();
            if domain == "localhost"
                || domain.ends_with(".localhost")
                || matches!(final_label, "local" | "internal")
            {
                return Err(AttachmentError::UnsafeUrl);
            }
        }
        Ok(IpAddr::V4(address)) => {
            if unsafe_ipv4(address) {
                return Err(AttachmentError::UnsafeUrl);
            }
        }
        Ok(IpAddr::V6(address)) => {
            if unsafe_ipv6(address) {
                return Err(AttachmentError::UnsafeUrl);
            }
        }
    }
    Ok(url)
}

fn unsafe_ipv4(address: Ipv4Addr) -> bool {
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_multicast()
        || address.is_unspecified()
        || address.is_broadcast()
        || address.is_documentation()
        || address.octets()[0] == 0
        || address.octets()[0] >= 240
}

fn unsafe_ipv6(address: Ipv6Addr) -> bool {
    address.is_loopback()
        || address.is_multicast()
        || address.is_unspecified()
        || address.is_unique_local()
        || address.is_unicast_link_local()
        || matches!(address.to_ipv4_mapped(), Some(mapped) if unsafe_ipv4(mapped))
}

const fn is_directional_control(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200b}'
            | '\u{200e}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{feff}'
    )
}

#[cfg(unix)]
pub(crate) fn set_directory_private(path: &Path) -> Result<(), AttachmentError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| AttachmentError::TemporaryStorage)
}

#[cfg(not(unix))]
pub(crate) fn set_directory_private(_path: &Path) -> Result<(), AttachmentError> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn set_file_private(path: &Path) -> Result<(), AttachmentError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|_| AttachmentError::TemporaryStorage)
}

#[cfg(not(unix))]
pub(crate) fn set_file_private(_path: &Path) -> Result<(), AttachmentError> {
    Ok(())
}

/// Returns a stable bridge temp root beneath a data directory.
#[must_use]
pub fn temp_root(data_dir: &Path) -> PathBuf {
    data_dir.join("tmp").join("attachments")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdk_cache_guard_preserves_old_files_and_removes_new_files_on_drop() {
        let root = tempfile::tempdir().expect("temporary root");
        let old = root.path().join("old");
        fs::write(&old, b"old").expect("old fixture writes");
        let new = root.path().join("new");
        {
            let _guard = DirectoryCleanupGuard::new(root.path()).expect("guard starts");
            fs::write(&new, b"plaintext").expect("new fixture writes");
        }
        assert!(old.exists());
        assert!(!new.exists());
    }
}
