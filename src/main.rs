use std::env;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use armada_telegram_bridge::cli::{self, Action};
use armada_telegram_bridge::clock::SystemClock;
use armada_telegram_bridge::concord::{
    ConcordAdapter, ConcordConnectFailure, ConcordError, ConcordReadiness, ConcordRoute,
};
use armada_telegram_bridge::config::BridgeConfig;
use armada_telegram_bridge::operations::{self, Health, InstanceLock, LogEvent, structured_log};
use armada_telegram_bridge::service::{BridgeService, BridgeServiceSettings, ServiceError};
use armada_telegram_bridge::store::{JobId, RouteState, Store};
use armada_telegram_bridge::telegram::{TelegramAdapter, TelegramError, TelegramRoute};

#[tokio::main]
async fn main() -> ExitCode {
    match cli::parse(env::args_os().skip(1)) {
        Ok(Action::Help) => {
            print!("{}", cli::HELP);
            ExitCode::SUCCESS
        }
        Ok(Action::Start { config }) => match BridgeConfig::load(config) {
            Ok(config) => {
                if run(config).await.is_ok() {
                    ExitCode::SUCCESS
                } else {
                    eprintln!("{}", structured_log(LogEvent::RuntimeFailed, 0));
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("error: {error}");
                ExitCode::from(2)
            }
        },
        Ok(Action::Doctor { config }) => {
            if BridgeConfig::load(config)
                .map_err(|_| ())
                .and_then(|config| operations::doctor(&config).map_err(|_| ()))
                .is_ok()
            {
                println!("{}", structured_log(LogEvent::DoctorPassed, 0));
                ExitCode::SUCCESS
            } else {
                eprintln!("{}", structured_log(LogEvent::DoctorFailed, 0));
                ExitCode::FAILURE
            }
        }
        Ok(Action::DeadLetters { config }) => operator_result(list_dead_letters(&config)),
        Ok(Action::Retry { config, job_id }) => operator_result(retry_job(&config, job_id)),
        Ok(Action::Skip { config, job_id }) => operator_result(skip_job(&config, job_id)),
        Ok(Action::Resume { config, route_id }) => {
            operator_result(resume_route(&config, &route_id))
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}

fn operator_result(result: Result<(), Box<dyn std::error::Error>>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn open_operator_store(
    config_path: &std::path::Path,
) -> Result<(Store, InstanceLock), Box<dyn std::error::Error>> {
    let config = BridgeConfig::load(config_path)?;
    let lock = InstanceLock::acquire(config.data_dir())?;
    Ok((Store::open(config.data_dir())?, lock))
}

fn list_dead_letters(config_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let (store, _lock) = open_operator_store(config_path)?;
    for dead in store.dead_letters()? {
        println!(
            "job={} attempts={} class={} possible_duplicate={}",
            dead.job_id.get(),
            dead.attempt_count,
            dead.last_error_class.as_deref().unwrap_or("unknown"),
            dead.possible_duplicate
        );
    }
    Ok(())
}

fn retry_job(
    config_path: &std::path::Path,
    job_id: JobId,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut store, _lock) = open_operator_store(config_path)?;
    store.retry_dead_letter(job_id, system_now_ms()?)?;
    Ok(())
}

fn skip_job(
    config_path: &std::path::Path,
    job_id: JobId,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut store, _lock) = open_operator_store(config_path)?;
    store.skip_dead_letter(job_id, system_now_ms()?)?;
    println!("skipped dead-letter job {}", job_id.get());
    Ok(())
}

fn resume_route(
    config_path: &std::path::Path,
    route_id: &armada_telegram_bridge::domain::RouteId,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut store, _lock) = open_operator_store(config_path)?;
    store.set_route_state(route_id, RouteState::Active, None, system_now_ms()?)?;
    Ok(())
}

fn system_now_ms() -> Result<i64, Box<dyn std::error::Error>> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(i64::try_from(duration.as_millis())?)
}

async fn run(config: BridgeConfig) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = InstanceLock::acquire(config.data_dir())
        .inspect_err(|_| log_failure(LogEvent::LockUnavailable))?;
    let health = Health::default();
    let _health_server = health
        .serve(config.health_listen())
        .await
        .inspect_err(|_| log_failure(LogEvent::HealthBindFailed))?;
    eprintln!("{}", structured_log(LogEvent::Starting, 0));
    let mut signal = tokio::spawn(operations::shutdown_signal());
    let store =
        Store::open(config.data_dir()).inspect_err(|_| log_failure(LogEvent::StoreFailed))?;
    let telegram_routes = config.routes().iter().map(TelegramRoute::from).collect();
    let concord_routes = config.routes().iter().map(ConcordRoute::from).collect();
    let telegram = TelegramAdapter::new(
        config.telegram().token().expose(),
        config.telegram().expected_bot_id(),
        config.telegram().protect_content(),
        config.telegram().relay_other_bots(),
        telegram_routes,
    )?;
    let connecting = ConcordAdapter::connect(
        config.nostr().nsec(),
        config.nostr().expected_npub(),
        concord_routes,
        config.data_dir().join("concord"),
    );
    let concord = tokio::select! {
        result = connecting => result.inspect_err(|error| log_failure(armada_connect_event(error)))?,
        result = &mut signal => { result??; return Ok(()); }
    };
    let settings = BridgeServiceSettings::from_config(&config)?;
    let mut service = BridgeService::new(store, telegram, concord, SystemClock, settings);
    service.set_health(health.clone());
    tokio::select! {
        result = service.initialize() => { result.inspect_err(|error| log_failure(startup_event(error)))?; },
        result = &mut signal => { result??; return Ok(()); }
    }
    eprintln!("{}", structured_log(LogEvent::Ready, 0));
    tokio::select! {
        result = async {
            service.dispatch_pending().await?;
            service.recover_armada_reactions().await?;
            service.dispatch_pending().await?;
            service.run().await
        } => result?,
        result = &mut signal => {
            result??;
            health.stop();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let report = service.shutdown_until(deadline).await?;
            eprintln!("{}", structured_log(LogEvent::Shutdown, report.remaining));
        }
    }
    Ok(())
}

fn log_failure(event: LogEvent) {
    eprintln!("{}", structured_log(event, 0));
}

fn armada_connect_event(error: &ConcordError) -> LogEvent {
    match error {
        ConcordError::Connect { reason } => match reason {
            ConcordConnectFailure::Database => LogEvent::ArmadaConnectDatabaseFailed,
            ConcordConnectFailure::Io => LogEvent::ArmadaConnectIoFailed,
            ConcordConnectFailure::Nostr => LogEvent::ArmadaConnectNostrFailed,
            ConcordConnectFailure::Crypto => LogEvent::ArmadaConnectCryptoFailed,
            ConcordConnectFailure::Network => LogEvent::ArmadaConnectNetworkFailed,
            ConcordConnectFailure::State => LogEvent::ArmadaConnectStateFailed,
            ConcordConnectFailure::Other => LogEvent::ArmadaConnectSdkFailed,
        },
        ConcordError::TemporaryFile => LogEvent::ArmadaConnectIoFailed,
        ConcordError::IdentityMismatch { .. } => LogEvent::ArmadaConnectNostrFailed,
        ConcordError::InvalidConfiguration => LogEvent::ArmadaConnectStateFailed,
        _ => LogEvent::ArmadaConnectFailed,
    }
}

fn startup_event(error: &ServiceError) -> LogEvent {
    match error {
        ServiceError::TelegramIdentityMismatch
        | ServiceError::Telegram(TelegramError::IdentityMismatch { .. }) => {
            LogEvent::StartupTelegramIdentityFailed
        }
        ServiceError::Telegram(TelegramError::Terminal { status: 409 }) => {
            LogEvent::StartupTelegramConflict
        }
        ServiceError::Telegram(TelegramError::Terminal { status: 401 }) => {
            LogEvent::StartupTelegramAuthenticationFailed
        }
        ServiceError::Telegram(TelegramError::Terminal { status: 400 | 403 }) => {
            LogEvent::StartupTelegramAccessFailed
        }
        ServiceError::Telegram(
            TelegramError::RateLimited { .. }
            | TelegramError::Retryable { .. }
            | TelegramError::Timeout
            | TelegramError::Transport,
        ) => LogEvent::StartupTelegramTransportFailed,
        ServiceError::Concord(ConcordError::RouteNotReady { reason, .. }) => match reason {
            ConcordReadiness::MissingCommunity => LogEvent::StartupArmadaCommunityMissing,
            ConcordReadiness::MissingChannel => LogEvent::StartupArmadaChannelMissing,
            ConcordReadiness::MissingKey => LogEvent::StartupArmadaChannelKeyMissing,
            ConcordReadiness::IncompleteRekey => LogEvent::StartupArmadaRekeyIncomplete,
            ConcordReadiness::Removed => LogEvent::StartupArmadaMembershipRemoved,
            ConcordReadiness::Dissolved => LogEvent::StartupArmadaCommunityDissolved,
            ConcordReadiness::StaleState | ConcordReadiness::Unchecked => {
                LogEvent::StartupArmadaStateStale
            }
            ConcordReadiness::Ready => LogEvent::StartupFailed,
        },
        ServiceError::Concord(ConcordError::Sdk { .. }) => LogEvent::StartupArmadaSdkFailed,
        ServiceError::RouteQuarantined(_) => LogEvent::StartupRouteQuarantined,
        ServiceError::Store(_)
        | ServiceError::InvalidTelegramCursor
        | ServiceError::QueueSaturated => LogEvent::StartupStateFailed,
        _ => LogEvent::StartupFailed,
    }
}
