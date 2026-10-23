//! Core library for the Armada Telegram bridge.

pub mod attachments;
pub mod cli;
pub mod clock;
pub mod concord;
pub mod concord_probe;
pub mod config;
pub mod domain;
pub mod error;
pub mod operations;
pub mod rich_text;
pub mod service;
pub mod store;
pub mod telegram;

#[doc(hidden)]
pub mod test_support;
