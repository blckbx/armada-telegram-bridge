//! Command-line parsing kept separate from process exit handling.

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

use crate::domain::RouteId;
use crate::error::AppError;
use crate::store::JobId;

/// Help displayed by the bridge binary.
pub const HELP: &str = "Armada Telegram Bridge\n\nUsage:\n  armada-telegram-bridge --config <PATH>\n  armada-telegram-bridge doctor --config <PATH>\n  armada-telegram-bridge dead-letters --config <PATH>\n  armada-telegram-bridge retry --config <PATH> --job <ID>\n  armada-telegram-bridge skip --config <PATH> --job <ID> --acknowledge-loss\n  armada-telegram-bridge resume --config <PATH> --route <ROUTE>\n\nDoctor validates local configuration/state metadata offline without writes.\nSkip permanently omits one dead-lettered destination delivery and requires an explicit loss acknowledgement.\n\nOptions:\n  --config <PATH>       Path to the bridge configuration\n  --job <ID>            Positive durable outbox job ID\n  --route <ROUTE>       Stable configured route ID\n  --acknowledge-loss    Confirm that a skipped mirror will not be delivered\n  -h, --help            Print help\n";

/// A validated command-line action.
#[derive(Debug, Eq, PartialEq)]
pub enum Action {
    /// Display command-line help.
    Help,
    /// Start the bridge with a configuration path.
    Start { config: PathBuf },
    /// Validate local configuration and state metadata without writes or network.
    Doctor { config: PathBuf },
    /// List secret-free dead-letter metadata.
    DeadLetters { config: PathBuf },
    /// Make one dead letter immediately eligible for retry.
    Retry { config: PathBuf, job_id: JobId },
    /// Permanently omit one dead-lettered delivery after explicit acknowledgement.
    Skip { config: PathBuf, job_id: JobId },
    /// Resume one operator-reviewed quarantined route.
    Resume { config: PathBuf, route_id: RouteId },
}

/// Parses process arguments without reading environment variables or secrets.
///
/// # Errors
///
/// Returns [`AppError::MissingConfig`] when no arguments are supplied and
/// [`AppError::InvalidArguments`] when the argument shape is unsupported.
pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Action, AppError> {
    let mut args = args.into_iter();

    let Some(first) = args.next() else {
        return Err(AppError::MissingConfig);
    };

    match first.to_str() {
        Some("-h" | "--help") => {
            reject_extra_arguments(args)?;
            Ok(Action::Help)
        }
        Some("--config") => {
            let config = required_value(&mut args, "--config")?;
            reject_extra_arguments(args)?;
            Ok(Action::Start {
                config: PathBuf::from(config),
            })
        }
        Some("dead-letters" | "doctor") => {
            expect_flag(&mut args, "--config")?;
            let config = PathBuf::from(required_value(&mut args, "--config")?);
            reject_extra_arguments(args)?;
            if first == "doctor" {
                Ok(Action::Doctor { config })
            } else {
                Ok(Action::DeadLetters { config })
            }
        }
        Some("retry") => {
            expect_flag(&mut args, "--config")?;
            let config = PathBuf::from(required_value(&mut args, "--config")?);
            expect_flag(&mut args, "--job")?;
            let raw = required_value(&mut args, "--job")?;
            let value = raw
                .to_str()
                .ok_or_else(|| AppError::InvalidArguments("--job must be UTF-8".to_owned()))?
                .parse::<i64>()
                .map_err(|_| AppError::InvalidArguments("--job must be positive".to_owned()))?;
            let job_id = JobId::try_from(value)
                .map_err(|_| AppError::InvalidArguments("--job must be positive".to_owned()))?;
            reject_extra_arguments(args)?;
            Ok(Action::Retry { config, job_id })
        }
        Some("skip") => {
            expect_flag(&mut args, "--config")?;
            let config = PathBuf::from(required_value(&mut args, "--config")?);
            expect_flag(&mut args, "--job")?;
            let raw = required_value(&mut args, "--job")?;
            let job_id = parse_job_id(&raw)?;
            expect_flag(&mut args, "--acknowledge-loss")?;
            reject_extra_arguments(args)?;
            Ok(Action::Skip { config, job_id })
        }
        Some("resume") => {
            expect_flag(&mut args, "--config")?;
            let config = PathBuf::from(required_value(&mut args, "--config")?);
            expect_flag(&mut args, "--route")?;
            let raw = required_value(&mut args, "--route")?;
            let route_id = raw
                .to_str()
                .ok_or_else(|| AppError::InvalidArguments("--route must be UTF-8".to_owned()))
                .and_then(|value| {
                    RouteId::try_from(value).map_err(|_| {
                        AppError::InvalidArguments("--route is not a valid route ID".to_owned())
                    })
                })?;
            reject_extra_arguments(args)?;
            Ok(Action::Resume { config, route_id })
        }
        _ => Err(AppError::InvalidArguments(
            "expected --config <PATH>, an operator command, or --help".to_owned(),
        )),
    }
}

fn parse_job_id(raw: &OsStr) -> Result<JobId, AppError> {
    let value = raw
        .to_str()
        .ok_or_else(|| AppError::InvalidArguments("--job must be UTF-8".to_owned()))?
        .parse::<i64>()
        .map_err(|_| AppError::InvalidArguments("--job must be positive".to_owned()))?;
    JobId::try_from(value)
        .map_err(|_| AppError::InvalidArguments("--job must be positive".to_owned()))
}

fn required_value(
    args: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> Result<OsString, AppError> {
    args.next()
        .ok_or_else(|| AppError::InvalidArguments(format!("{flag} requires a value")))
}

fn expect_flag(args: &mut impl Iterator<Item = OsString>, expected: &str) -> Result<(), AppError> {
    let actual = args
        .next()
        .ok_or_else(|| AppError::InvalidArguments(format!("expected {expected}")))?;
    if actual == expected {
        Ok(())
    } else {
        Err(AppError::InvalidArguments(format!("expected {expected}")))
    }
}

fn reject_extra_arguments(mut args: impl Iterator<Item = OsString>) -> Result<(), AppError> {
    if args.next().is_some() {
        return Err(AppError::InvalidArguments(
            "unexpected extra argument".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use super::{Action, parse};
    use crate::error::AppError;

    #[test]
    fn parses_config_path() {
        let args = [OsString::from("--config"), OsString::from("bridge.toml")];

        assert_eq!(
            parse(args).expect("valid arguments should parse"),
            Action::Start {
                config: PathBuf::from("bridge.toml")
            }
        );
    }

    #[test]
    fn rejects_extra_arguments() {
        let args = [OsString::from("--help"), OsString::from("unexpected")];

        assert!(matches!(parse(args), Err(AppError::InvalidArguments(_))));
    }

    #[test]
    fn parses_operator_recovery_commands() {
        assert_eq!(
            parse([
                OsString::from("dead-letters"),
                OsString::from("--config"),
                OsString::from("bridge.toml"),
            ])
            .expect("dead-letter command parses"),
            Action::DeadLetters {
                config: PathBuf::from("bridge.toml")
            }
        );
        assert!(matches!(
            parse([
                OsString::from("retry"),
                OsString::from("--config"),
                OsString::from("bridge.toml"),
                OsString::from("--job"),
                OsString::from("7"),
            ]),
            Ok(Action::Retry { job_id, .. }) if job_id.get() == 7
        ));
        assert!(matches!(
            parse([
                OsString::from("skip"),
                OsString::from("--config"),
                OsString::from("bridge.toml"),
                OsString::from("--job"),
                OsString::from("7"),
                OsString::from("--acknowledge-loss"),
            ]),
            Ok(Action::Skip { job_id, .. }) if job_id.get() == 7
        ));
        assert!(
            parse([
                OsString::from("skip"),
                OsString::from("--config"),
                OsString::from("bridge.toml"),
                OsString::from("--job"),
                OsString::from("7"),
            ])
            .is_err()
        );
        assert!(matches!(
            parse([
                OsString::from("resume"),
                OsString::from("--config"),
                OsString::from("bridge.toml"),
                OsString::from("--route"),
                OsString::from("alpha"),
            ]),
            Ok(Action::Resume { route_id, .. }) if route_id.as_str() == "alpha"
        ));
    }
}
