//! Application errors and secret-safe presentation types.

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

use serde::{Serialize, Serializer};
use zeroize::Zeroize;

const REDACTED: &str = "[REDACTED]";

/// A string whose normal formatting never reveals its contents.
pub struct Sensitive(String);

impl Sensitive {
    /// Wraps a sensitive value.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// Deliberately exposes the value to code that needs to use the secret.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Display for Sensitive {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED)
    }
}

impl Debug for Sensitive {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("Sensitive([REDACTED])")
    }
}

impl Serialize for Sensitive {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(REDACTED)
    }
}

impl Drop for Sensitive {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Errors that can cross the application boundary and be shown to operators.
#[derive(Debug)]
pub enum AppError {
    /// No configuration path was supplied.
    MissingConfig,
    /// Command-line arguments were syntactically invalid.
    InvalidArguments(String),
    /// An operation failed and its diagnostic detail is sensitive.
    SensitiveFailure {
        /// Safe description of the operation.
        operation: &'static str,
        /// Detail that must never be rendered directly.
        detail: Sensitive,
    },
}

impl AppError {
    /// Creates an error whose diagnostic detail remains redacted.
    #[must_use]
    pub fn sensitive_failure(operation: &'static str, detail: Sensitive) -> Self {
        Self::SensitiveFailure { operation, detail }
    }

    /// Process exit code associated with this error.
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::MissingConfig | Self::InvalidArguments(_) => 2,
            Self::SensitiveFailure { .. } => 1,
        }
    }
}

impl Display for AppError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfig => formatter.write_str("missing required --config <PATH>"),
            Self::InvalidArguments(message) => formatter.write_str(message),
            Self::SensitiveFailure { operation, detail } => {
                write!(formatter, "{operation}: {detail}")
            }
        }
    }
}

impl Error for AppError {}
