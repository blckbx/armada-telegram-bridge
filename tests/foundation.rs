use std::error::Error;
use std::time::{Duration, UNIX_EPOCH};

use armada_telegram_bridge::clock::Clock;
use armada_telegram_bridge::error::{AppError, Sensitive};
use armada_telegram_bridge::test_support::FixedClock;

#[test]
fn fixed_clock_always_returns_the_configured_time() {
    let instant = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let clock = FixedClock::new(instant);

    assert_eq!(clock.now(), instant);
    assert_eq!(clock.now(), instant);
}

#[test]
fn sensitive_values_are_redacted_in_display_debug_and_errors() {
    let raw_secret = "nsec1synthetic-secret";
    let secret = Sensitive::new(raw_secret.to_owned());

    assert_eq!(secret.expose(), raw_secret);
    assert_eq!(secret.to_string(), "[REDACTED]");
    assert_eq!(format!("{secret:?}"), "Sensitive([REDACTED])");

    let error = AppError::sensitive_failure("load identity", secret);
    let rendered = error.to_string();
    assert!(rendered.contains("load identity"));
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains(raw_secret));
    assert!(!format!("{error:?}").contains(raw_secret));
    assert!(error.source().is_none());
}
