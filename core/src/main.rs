use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use dlmm_core::actor::block_processor::{PricingSettings, ProcessorSettings};
use dlmm_core::actor::live_supervisor::{self, SupervisorError};
use dlmm_core::actor::messages::{FillerMessage, PriceFeedMessage, ProcessorMessage};
use dlmm_core::actor::range_filler::{ArchiveLane, FillerError};
use dlmm_core::actor::{block_processor, price_feed, range_filler};
use dlmm_core::domain::amounts::QuoteAllowlist;
use dlmm_core::domain::config::{
    ChannelCapacity, RpsMax, SLOT_MILLISECONDS_OBSERVED, TransactionVersionMax,
};
use dlmm_core::domain::error::{RpcError, StoreError};
use dlmm_core::domain::job::ArchiveRouting;
use dlmm_core::domain::price::PriceSource;
use dlmm_core::domain::projection::{ProjectionName, ProjectionState};
use dlmm_core::gateway::binance::{BinanceGateway, KLINES_PATH};
use dlmm_core::gateway::geyser::{GeyserError, GeyserGateway, GeyserToken};
use dlmm_core::gateway::rpc::{Endpoint, RequestLane, RpcGateway, lane_share};
use dlmm_core::store::{read_projection_states, rebuild_projection};
use reqwest::Url;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use thiserror::Error;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

// "DLMM" in ASCII: one indexer per database.
const ADVISORY_LOCK_KEY: i64 = 0x444c_4d4d;
const DATABASE_CONNECTION_COUNT_MAX: u32 = 8;
const RPC_RPS_MAX_DEFAULT: u32 = 10;
// The archive is paced by its latency (ARCHIVE_IN_FLIGHT_MAX), so its limiter only caps bursts
// of fast answers such as skipped slots.
const ARCHIVE_RPS_MAX_DEFAULT: u32 = 20;
const BINANCE_DATA_API_URL_DEFAULT: &str = "https://data-api.binance.vision";
// Configurable sources were misconfigurable; onboarding a market is a code change.
const PRICE_SOURCE_MARKET: PriceSource = PriceSource::Binance;
// Compose gives 30 s between SIGTERM and SIGKILL; the live sources, the filler and the drain
// share it. The supervisor stops two sources of up to 2 s each one after the other, so 6 s
// leaves it room before it is aborted (which aborts its sources too).
const SUPERVISOR_STOP_TIMEOUT: Duration = Duration::from_secs(6);
const FILLER_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESSOR_STOP_TIMEOUT: Duration = Duration::from_secs(16);
// The rebuild runs one page transaction at a time; a spare connection covers the state update.
const REBUILD_CONNECTION_COUNT_MAX: u32 = 2;
const INSTANCE_LOCK_CHECK_INTERVAL: Duration = Duration::from_secs(30);
// A check that hangs (a half-open TCP session) is as good as a lost lock: nothing proves this
// session still holds it.
const INSTANCE_LOCK_CHECK_TIMEOUT: Duration = Duration::from_secs(10);
// One permit each for the live tail and the filler, which own separate shares of the quota.
const RPS_MAX_MIN: u32 = 2;
// The tail fetches every block (one getBlock per observed slot, about 3.7 a second) plus one
// getBlocks per 2 s tick and an occasional getSlot; one spare request a second covers those,
// and below this live share the tail falls behind the chain.
const LIVE_RPS_MIN: u32 = 1000_u64.div_ceil(SLOT_MILLISECONDS_OBSERVED) as u32 + 1;
// Totals above this are surely enough, so the search for the smallest one stops here.
const RPC_RPS_MAX_SEARCH_MAX: u32 = 100;

#[derive(Debug, Parser)]
#[command(name = "indexer", about = "Meteora DLMM swap indexer")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run migrations, then the ingest, processing and price actors until SIGTERM.
    Run,
    /// Rebuild one projection (pool_volume_1h, pool_volume_1d, pool_stats) from the swap log.
    /// Offline only: refuses to start while an indexer holds the lock.
    RebuildProjection { name: ProjectionName },
}

#[derive(Debug, Error)]
enum ConfigError {
    #[error("{name} is not set")]
    Missing { name: &'static str },
    #[error("{name} is invalid: {reason}")]
    Invalid { name: &'static str, reason: String },
}

#[derive(Debug, Error)]
enum RunError {
    #[error("configuration: {0}")]
    Config(#[from] ConfigError),
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("rpc client: {0}")]
    Rpc(#[from] RpcError),
    #[error(
        "another indexer holds the lock (advisory lock {ADVISORY_LOCK_KEY:#x} on this database)"
    )]
    AnotherInstance,
    #[error(
        "advisory lock {ADVISORY_LOCK_KEY:#x} lost ({reason}); exiting so a second indexer cannot run beside this one"
    )]
    InstanceLockLost { reason: String },
    #[error("range filler: {0}")]
    Filler(#[from] FillerError),
    #[error("geyser: {0}")]
    Geyser(#[from] GeyserError),
    #[error("live supervisor: {0}")]
    Supervisor(#[from] SupervisorError),
    #[error("block processor: {0}")]
    Processor(#[from] StoreError),
    #[error("projection rebuild: {0}")]
    Rebuild(StoreError),
    #[error(
        "projection {name} is mid-rebuild; finish it with `indexer rebuild-projection {name}` before running the indexer"
    )]
    ProjectionBuilding { name: ProjectionName },
    #[error("{actor} panicked or was cancelled")]
    Task { actor: &'static str },
    #[error("signal handler: {0}")]
    Signal(#[from] std::io::Error),
}

#[derive(Debug)]
struct Config {
    db_dsn: String,
    rpc_url: Url,
    rpc_rps_max: RpsMax,
    // One ceiling for both mappers: a version is supported across every source or not at all.
    transaction_version_max: TransactionVersionMax,
    price_klines_url: Url,
    // None means the RPC tail is the only live source.
    geyser_url: Option<Url>,
    geyser_x_token: Option<GeyserToken>,
    // None means the provider fills every hole, however old.
    archive_rpc_url: Option<Url>,
    archive_rps_max: RpsMax,
}

#[tokio::main]
async fn main() -> ExitCode {
    // A missing .env is normal in containers, where compose sets the environment.
    let _ = dotenvy::dotenv();
    init_tracing();
    let arguments = Arguments::parse();
    let result = match arguments.command {
        Command::Run => run().await,
        Command::RebuildProjection { name } => rebuild(name).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "indexer_stopped");
            eprintln!("indexer: {error}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match std::env::var("LOG_FORMAT").as_deref() {
        Ok("pretty") => builder.pretty().init(),
        _ => builder.json().init(),
    }
}

fn read_config() -> Result<Config, ConfigError> {
    let rpc_url = parse_url("RPC_URL", &required("RPC_URL")?)?;
    let rpc_rps_max = match optional("RPC_RPS_MAX") {
        None => RPC_RPS_MAX_DEFAULT,
        Some(text) => parse_number("RPC_RPS_MAX", &text)?,
    };
    if rpc_rps_max < RPS_MAX_MIN {
        return Err(invalid(
            "RPC_RPS_MAX",
            "must be at least 2 (one for the live tail, one for the filler)",
        ));
    }
    let archive_rps_max = match optional("ARCHIVE_RPS_MAX") {
        None => ARCHIVE_RPS_MAX_DEFAULT,
        Some(text) => parse_number("ARCHIVE_RPS_MAX", &text)?,
    };
    if archive_rps_max < RPS_MAX_MIN {
        return Err(invalid(
            "ARCHIVE_RPS_MAX",
            "must be at least 2 (the shared limiter keeps a live share it never uses)",
        ));
    }
    let binance_data_api_url =
        optional("BINANCE_DATA_API_URL").unwrap_or_else(|| BINANCE_DATA_API_URL_DEFAULT.to_owned());
    Ok(Config {
        db_dsn: required("DB_DSN")?,
        rpc_url,
        rpc_rps_max: RpsMax::new(rpc_rps_max),
        transaction_version_max: transaction_version_max("TRANSACTION_VERSION_MAX")?,
        price_klines_url: price_klines_url(&binance_data_api_url)?,
        geyser_url: optional("GEYSER_URL")
            .map(|text| parse_url("GEYSER_URL", &text))
            .transpose()?,
        geyser_x_token: optional("GEYSER_X_TOKEN")
            .map(|text| {
                GeyserToken::new(text)
                    .map_err(|error| invalid("GEYSER_X_TOKEN", &error.to_string()))
            })
            .transpose()?,
        archive_rpc_url: optional("ARCHIVE_RPC_URL")
            .map(|text| parse_url("ARCHIVE_RPC_URL", &text))
            .transpose()?,
        archive_rps_max: RpsMax::new(archive_rps_max),
    })
}

// A ceiling above what the mappers parse would let unparsed layouts through.
fn transaction_version_max(name: &'static str) -> Result<TransactionVersionMax, ConfigError> {
    let version = match optional(name) {
        None => TransactionVersionMax::SUPPORTED.get(),
        Some(text) => parse_number(name, &text)?,
    };
    if version > TransactionVersionMax::SUPPORTED.get() {
        return Err(invalid(
            name,
            "above the highest transaction version the mapper supports",
        ));
    }
    Ok(TransactionVersionMax::new(version))
}

fn price_klines_url(base_text: &str) -> Result<Url, ConfigError> {
    const NAME: &str = "BINANCE_DATA_API_URL";
    let base_url = parse_url(NAME, base_text)?;
    if !matches!(base_url.scheme(), "http" | "https") {
        return Err(invalid(NAME, "must be an http or https URL"));
    }
    base_url
        .join(KLINES_PATH)
        .map_err(|error| invalid(NAME, &error.to_string()))
}

fn optional(name: &'static str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn required(name: &'static str) -> Result<String, ConfigError> {
    optional(name).ok_or(ConfigError::Missing { name })
}

fn invalid(name: &'static str, reason: &str) -> ConfigError {
    ConfigError::Invalid {
        name,
        reason: reason.to_owned(),
    }
}

fn parse_url(name: &'static str, text: &str) -> Result<Url, ConfigError> {
    Url::parse(text).map_err(|error| invalid(name, &error.to_string()))
}

fn parse_number<T: std::str::FromStr>(name: &'static str, text: &str) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    text.parse()
        .map_err(|error: T::Err| invalid(name, &error.to_string()))
}

// The lock lives as long as this connection, so the caller keeps it open until exit and
// checks it with `instance_lock_lost`.
async fn take_instance_lock(connect_options: &PgConnectOptions) -> Result<PgConnection, RunError> {
    let mut connection = PgConnection::connect_with(connect_options).await?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .fetch_one(&mut connection)
        .await?;
    if !locked {
        return Err(RunError::AnotherInstance);
    }
    Ok(connection)
}

// Resolves only when the lock is gone: the connection failed (the server dropped the session
// and with it the lock) or pg_locks no longer lists it for this session.
async fn instance_lock_lost(connection: &mut PgConnection) -> String {
    let mut check = tokio::time::interval(INSTANCE_LOCK_CHECK_INTERVAL);
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        check.tick().await;
        // A bigint advisory key is stored as classid (high half) and objid (low half).
        let held = tokio::time::timeout(
            INSTANCE_LOCK_CHECK_TIMEOUT,
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                 SELECT 1 FROM pg_locks
                 WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
                   AND ((classid::bigint << 32) | objid::bigint) = $1)",
            )
            .bind(ADVISORY_LOCK_KEY)
            .fetch_one(&mut *connection),
        )
        .await;
        match held {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => return "no longer held by this session".to_owned(),
            Ok(Err(error)) => return format!("lock connection failed: {error}"),
            Err(_) => return "lock check timed out".to_owned(),
        }
    }
}

struct Actors {
    supervisor: JoinHandle<Result<(), SupervisorError>>,
    supervisor_stop: watch::Sender<bool>,
    filler: JoinHandle<Result<(), FillerError>>,
    processor: JoinHandle<Result<(), StoreError>>,
    filler_sender: mpsc::Sender<FillerMessage>,
    price_sender: mpsc::Sender<PriceFeedMessage>,
    live_sender: mpsc::Sender<ProcessorMessage>,
    // Held so the fill channel stays open until the processor has drained it.
    _fill_sender: mpsc::Sender<ProcessorMessage>,
}

async fn run() -> Result<(), RunError> {
    let config = read_config()?;
    let connect_options: PgConnectOptions = config.db_dsn.parse()?;
    let mut instance_lock = take_instance_lock(&connect_options).await?;
    let database = PgPoolOptions::new()
        .max_connections(DATABASE_CONNECTION_COUNT_MAX)
        .connect_with(connect_options)
        .await?;
    sqlx::migrate!("../migrations").run(&database).await?;
    tracing::info!("migrations_applied");
    refuse_unfinished_rebuild(&database).await?;
    let gateway = Arc::new(RpcGateway::new(
        config.rpc_url.clone(),
        Endpoint::Provider,
        config.rpc_rps_max,
        config.transaction_version_max,
    )?);
    warn_if_live_share_low(&gateway);
    let geyser_gateway = config
        .geyser_url
        .clone()
        .map(|url| {
            GeyserGateway::new(
                url,
                config.geyser_x_token.clone(),
                config.transaction_version_max,
            )
        })
        .transpose()?;
    log_live_source(&config);
    let archive_gateway = config
        .archive_rpc_url
        .clone()
        .map(|url| {
            RpcGateway::new(
                url,
                Endpoint::Archive,
                config.archive_rps_max,
                config.transaction_version_max,
            )
        })
        .transpose()?;
    let actors = spawn_actors(&config, database, gateway, archive_gateway, geyser_gateway);
    supervise(actors, &mut instance_lock).await
}

// Live blocks add their deltas to every projection, including one a killed rebuild left
// building; the resumed rebuild would then replay those swaps a second time.
async fn refuse_unfinished_rebuild(database: &PgPool) -> Result<(), RunError> {
    let states = read_projection_states(database)
        .await
        .map_err(RunError::Processor)?;
    match states
        .into_iter()
        .find(|(_, state)| *state == ProjectionState::Building)
    {
        Some((name, _)) => Err(RunError::ProjectionBuilding { name }),
        None => Ok(()),
    }
}

fn log_live_source(config: &Config) {
    match &config.geyser_url {
        Some(url) => tracing::info!(
            host = url.host_str().unwrap_or_default(),
            x_token_set = config.geyser_x_token.is_some(),
            "live_source_geyser"
        ),
        None => tracing::info!("live_source_rpc_tail"),
    }
}

async fn rebuild(name: ProjectionName) -> Result<(), RunError> {
    let connect_options: PgConnectOptions = required("DB_DSN")?.parse()?;
    rebuild_with_lock(connect_options, name).await
}

// Same lock as `run`: a rebuild beside a live indexer would add live deltas to a table it is
// truncating and replaying.
async fn rebuild_with_lock(
    connect_options: PgConnectOptions,
    name: ProjectionName,
) -> Result<(), RunError> {
    let _instance_lock = take_instance_lock(&connect_options).await?;
    let database = PgPoolOptions::new()
        .max_connections(REBUILD_CONNECTION_COUNT_MAX)
        .connect_with(connect_options)
        .await?;
    tracing::info!(projection = %name, "projection_rebuild_started");
    let summary = rebuild_projection(&database, name)
        .await
        .map_err(RunError::Rebuild)?;
    tracing::info!(
        projection = %name,
        start = ?summary.start,
        row_count = summary.row_count,
        page_count = summary.page_count,
        "projection_rebuilt"
    );
    Ok(())
}

// The smallest RPC_RPS_MAX whose live share keeps up with the chain.
fn rpc_rps_max_needed() -> u32 {
    (RPS_MAX_MIN..=RPC_RPS_MAX_SEARCH_MAX)
        .find(|total| lane_share(RpsMax::new(*total)).live.get() >= LIVE_RPS_MIN)
        .unwrap_or(RPC_RPS_MAX_SEARCH_MAX)
}

// Allowed, because a lagging demo on a public endpoint is still useful, but never silent.
fn warn_if_live_share_low(gateway: &RpcGateway) {
    let live_share = gateway.rps_max(RequestLane::Live).get();
    if live_share < LIVE_RPS_MIN {
        tracing::warn!(
            live_rps_max = live_share,
            live_rps_min = LIVE_RPS_MIN,
            rpc_rps_max_needed = rpc_rps_max_needed(),
            consequence = "the tail will lag the chain and leave the skipped stretches to fill \
                           jobs; raise RPC_RPS_MAX",
            "live_share_below_chain_rate"
        );
    }
}

// The archive's window is read by the filler beside live, not before it: its two calls take
// half a minute on faithful-cli. Until it is known nothing is opened or walked.
fn spawn_actors(
    config: &Config,
    database: PgPool,
    gateway: Arc<RpcGateway>,
    archive_gateway: Option<RpcGateway>,
    geyser_gateway: Option<GeyserGateway>,
) -> Actors {
    let (routing_sender, routing) = watch::channel(match archive_gateway {
        Some(_) => ArchiveRouting::Pending,
        None => ArchiveRouting::Ready(None),
    });
    let archive = archive_gateway.map(|gateway| ArchiveLane {
        gateway: Arc::new(gateway),
        routing: routing_sender,
    });
    let (live_sender, live_receiver) = mpsc::channel(ChannelCapacity::LIVE_BLOCKS.get());
    let (fill_sender, fill_receiver) = mpsc::channel(ChannelCapacity::FILL_BLOCKS.get());
    let (filler_sender, filler_receiver) = mpsc::channel(ChannelCapacity::NUDGES.get());
    let (price_sender, price_receiver) = mpsc::channel(ChannelCapacity::PRICE_FEED_MESSAGES.get());
    let (supervisor_stop, supervisor_stop_receiver) = watch::channel(false);
    let supervisor = tokio::spawn(live_supervisor::run(
        database.clone(),
        Arc::clone(&gateway),
        geyser_gateway,
        live_sender.clone(),
        supervisor_stop_receiver,
    ));
    let filler = tokio::spawn(range_filler::run(
        database.clone(),
        Arc::clone(&gateway),
        archive,
        routing.clone(),
        filler_receiver,
        fill_sender.clone(),
    ));
    let processor = tokio::spawn(block_processor::run(
        database.clone(),
        gateway,
        ProcessorSettings {
            pricing: PricingSettings {
                allowlist: QuoteAllowlist::mainnet(),
                market: PRICE_SOURCE_MARKET,
            },
        },
        routing,
        live_receiver,
        fill_receiver,
        filler_sender.clone(),
    ));
    spawn_price_feed(
        database,
        config.price_klines_url.clone(),
        price_receiver,
        fill_sender.clone(),
    );
    Actors {
        supervisor,
        supervisor_stop,
        filler,
        processor,
        filler_sender,
        price_sender,
        live_sender,
        _fill_sender: fill_sender,
    }
}

// Prices are late-bound, so a price feed failure (or panic) degrades USD figures without
// stopping ingestion; it is logged and the indexer keeps running.
fn spawn_price_feed(
    database: PgPool,
    klines_url: Url,
    receiver: mpsc::Receiver<PriceFeedMessage>,
    processor_sender: mpsc::Sender<ProcessorMessage>,
) {
    let feed = tokio::spawn(async move {
        let gateway = BinanceGateway::new(klines_url)?;
        price_feed::run(database, gateway, receiver, processor_sender).await
    });
    tokio::spawn(async move {
        match feed.await {
            Ok(Ok(())) => tracing::info!("price_feed_stopped"),
            Ok(Err(error)) => tracing::error!(%error, "price_feed_failed"),
            Err(join_error) => tracing::error!(%join_error, "price_feed_panicked"),
        }
    });
}

enum StopCause {
    Signal,
    InstanceLockLost(String),
    SupervisorStopped(Result<Result<(), SupervisorError>, tokio::task::JoinError>),
    FillerStopped(Result<Result<(), FillerError>, tokio::task::JoinError>),
    ProcessorStopped(Result<Result<(), StoreError>, tokio::task::JoinError>),
}

async fn supervise(mut actors: Actors, instance_lock: &mut PgConnection) -> Result<(), RunError> {
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let cause = tokio::select! {
        _ = terminate.recv() => StopCause::Signal,
        _ = interrupt.recv() => StopCause::Signal,
        reason = instance_lock_lost(instance_lock) => StopCause::InstanceLockLost(reason),
        joined = &mut actors.supervisor => StopCause::SupervisorStopped(joined),
        joined = &mut actors.filler => StopCause::FillerStopped(joined),
        joined = &mut actors.processor => StopCause::ProcessorStopped(joined),
    };
    tracing::info!("shutdown_started");
    // Best effort: a full or closed channel means the actor is already stopping.
    let _ = actors.price_sender.try_send(PriceFeedMessage::Shutdown);
    // Producers stop before the processor, so its Shutdown is behind every block they sent.
    match cause {
        StopCause::Signal => {
            let supervisor = stop_supervisor(&mut actors).await;
            let filler = stop_filler(&mut actors).await;
            let processor = stop_processor(&mut actors).await;
            supervisor.and(filler).and(processor)
        }
        // Stop cleanly first: what is queued is still ours to commit and is idempotent anyway.
        StopCause::InstanceLockLost(reason) => {
            tracing::error!(%reason, "instance_lock_lost");
            let supervisor = stop_supervisor(&mut actors).await;
            let filler = stop_filler(&mut actors).await;
            let processor = stop_processor(&mut actors).await;
            supervisor
                .and(filler)
                .and(processor)
                .and(Err(RunError::InstanceLockLost { reason }))
        }
        StopCause::SupervisorStopped(joined) => {
            let filler = stop_filler(&mut actors).await;
            let processor = stop_processor(&mut actors).await;
            flatten("live supervisor", joined)
                .and(filler)
                .and(processor)
        }
        StopCause::FillerStopped(joined) => {
            let supervisor = stop_supervisor(&mut actors).await;
            let processor = stop_processor(&mut actors).await;
            flatten("range filler", joined)
                .and(supervisor)
                .and(processor)
        }
        StopCause::ProcessorStopped(joined) => {
            actors.supervisor.abort();
            actors.filler.abort();
            flatten("block processor", joined)
        }
    }
}

// The supervisor stops its own sources (each within 2 s) before it returns; aborting it on a
// timeout aborts them as well.
async fn stop_supervisor(actors: &mut Actors) -> Result<(), RunError> {
    // An error means the supervisor already exited and dropped its receiver.
    let _ = actors.supervisor_stop.send(true);
    match tokio::time::timeout(SUPERVISOR_STOP_TIMEOUT, &mut actors.supervisor).await {
        Ok(joined) => flatten("live supervisor", joined),
        Err(_) => {
            tracing::warn!("live_supervisor_stop_timed_out");
            actors.supervisor.abort();
            Ok(())
        }
    }
}

async fn stop_filler(actors: &mut Actors) -> Result<(), RunError> {
    let _ = actors.filler_sender.try_send(FillerMessage::Shutdown);
    match tokio::time::timeout(FILLER_STOP_TIMEOUT, &mut actors.filler).await {
        Ok(joined) => flatten("range filler", joined),
        Err(_) => {
            tracing::warn!("range_filler_stop_timed_out");
            actors.filler.abort();
            Ok(())
        }
    }
}

async fn stop_processor(actors: &mut Actors) -> Result<(), RunError> {
    // Sent after the filler stopped, so every block it produced is ahead of this message.
    if actors
        .live_sender
        .send(ProcessorMessage::Shutdown)
        .await
        .is_err()
    {
        tracing::warn!("block_processor_already_stopped");
    }
    match tokio::time::timeout(PROCESSOR_STOP_TIMEOUT, &mut actors.processor).await {
        Ok(joined) => flatten("block processor", joined),
        Err(_) => Err(RunError::Task {
            actor: "block processor",
        }),
    }
}

fn flatten<E: Into<RunError>>(
    actor: &'static str,
    joined: Result<Result<(), E>, tokio::task::JoinError>,
) -> Result<(), RunError> {
    match joined {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err(RunError::Task { actor }),
    }
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::{Connection, PgConnection};

    use super::{
        ADVISORY_LOCK_KEY, RunError, rebuild_with_lock, refuse_unfinished_rebuild,
        rpc_rps_max_needed,
    };
    use dlmm_core::domain::projection::ProjectionName;
    use sqlx::PgPool;

    // At the observed 0.27 s slots the live tail needs five requests a second, which a total of
    // seven yields (60 percent, rounded up); the README's practical minimum is this number.
    #[test]
    fn live_share_keeps_up_with_the_chain_from_seven_requests_a_second() {
        assert_eq!(rpc_rps_max_needed(), 7);
    }

    // A rebuild killed mid-replay blocks the indexer, and the message names the command that
    // finishes it.
    #[sqlx::test(migrations = "../migrations")]
    async fn run_refuses_while_a_projection_is_building(database: PgPool) {
        assert!(refuse_unfinished_rebuild(&database).await.is_ok());
        sqlx::query("UPDATE projection SET state = 'building' WHERE name = 'pool_stats'")
            .execute(&database)
            .await
            .expect("interrupted rebuild");
        let refused = refuse_unfinished_rebuild(&database).await;
        let Err(error @ RunError::ProjectionBuilding { .. }) = refused else {
            panic!("expected a refusal, got {refused:?}");
        };
        assert!(
            error
                .to_string()
                .contains("indexer rebuild-projection pool_stats"),
            "{error}"
        );
    }

    // A rebuild beside a running indexer refuses before touching the projection.
    #[sqlx::test(migrations = "../migrations")]
    async fn rebuild_refuses_while_another_indexer_holds_the_lock(
        _pool_options: PgPoolOptions,
        connect_options: PgConnectOptions,
    ) {
        let mut indexer = PgConnection::connect_with(&connect_options)
            .await
            .expect("indexer connection");
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(ADVISORY_LOCK_KEY)
            .fetch_one(&mut indexer)
            .await
            .expect("take the lock");
        assert!(locked);
        // A rebuild that ran would end live again, so updated_at is what shows it was touched.
        let projection_row_sql =
            "SELECT row_to_json(p)::text FROM projection p WHERE name = 'pool_volume_1d'";
        let before: String = sqlx::query_scalar(projection_row_sql)
            .fetch_one(&mut indexer)
            .await
            .expect("projection row");

        let refused = rebuild_with_lock(connect_options, ProjectionName::PoolVolume1d).await;
        assert!(matches!(refused, Err(RunError::AnotherInstance)));
        let after: String = sqlx::query_scalar(projection_row_sql)
            .fetch_one(&mut indexer)
            .await
            .expect("projection row");
        assert_eq!(after, before);
    }
}
