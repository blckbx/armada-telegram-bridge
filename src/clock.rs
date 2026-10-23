//! Time boundary used to keep scheduling tests deterministic.

use std::time::SystemTime;

/// Source of wall-clock time.
pub trait Clock: Send + Sync {
    /// Returns the current wall-clock time.
    fn now(&self) -> SystemTime;
}

/// Production wall-clock source.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}
