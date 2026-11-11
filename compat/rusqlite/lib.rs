//! Compatibility for vector-core 0.10's rusqlite 0.38 requirement.
//!
//! All implementation and types come from the patched upstream release.
//! Feature forwarding is limited to the features used by this dependency graph.
#![forbid(unsafe_code)]

pub use rusqlite_patched::*;
