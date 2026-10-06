use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use dlmm_core::domain::config::{RpsMax, TransactionVersionMax};
use dlmm_core::gateway::rpc::{Endpoint, RpcGateway, Url};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

const LISTEN_ADDRESS_DEFAULT: &str = "0.0.0.0:8080";
const DATABASE_CONNECTION_COUNT_MAX: u32 = 10;
const DATABASE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
// The indexer's default and minimum for RPC_RPS_MAX; the API spends it only on the bisection a
// backfill request runs.
const RPC_RPS_MAX_DEFAULT: u32 = 10;
const RPC_RPS_MAX_MIN: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug)]
struct ApiConfig {
    db_dsn: String,
    listen_address: SocketAddr,
    // None answers POST /v1/backfills with 503; every read route works without it.
    rpc_url: Option<Url>,
    rpc_rps_max: RpsMax,
}

#[derive(Debug, thiserror::Error)]
enum ConfigError {
    #[error("DB_DSN is unset")]
    DbDsnMissing,
    #[error("API_LISTEN_ADDRESS {value:?} is not a socket address")]
    ListenAddressInvalid { value: String },
    #[error("RPC_URL is not a URL: {reason}")]
    RpcUrlInvalid { reason: String },
    #[error("RPC_RPS_MAX must be an integer of at least {RPC_RPS_MAX_MIN}")]
    RpcRpsMaxInvalid,
}

fn log_format_from_environment() -> LogFormat {
    match std::env::var("LOG_FORMAT").as_deref() {
        Ok("pretty") => LogFormat::Pretty,
        _ => LogFormat::Json,
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match log_format_from_environment() {
        LogFormat::Json => builder.json().init(),
        LogFormat::Pretty => builder.pretty().init(),
    }
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn config_from_environment() -> Result<ApiConfig, ConfigError> {
    // Reads, plus one INSERT into slot_range_job for a backfill and one UPDATE of it for a
    // cancel; a read-only role here serves every read and refuses both.
    let db_dsn = std::env::var("DB_DSN").map_err(|_| ConfigError::DbDsnMissing)?;
    let rpc_url = optional("RPC_URL")
        .map(|text| {
            Url::parse(&text).map_err(|error| ConfigError::RpcUrlInvalid {
                reason: error.to_string(),
            })
        })
        .transpose()?;
    let rpc_rps_max = match optional("RPC_RPS_MAX") {
        None => RPC_RPS_MAX_DEFAULT,
        Some(text) => text.parse().map_err(|_| ConfigError::RpcRpsMaxInvalid)?,
    };
    if rpc_rps_max < RPC_RPS_MAX_MIN {
        return Err(ConfigError::RpcRpsMaxInvalid);
    }
    let listen_text =
        std::env::var("API_LISTEN_ADDRESS").unwrap_or_else(|_| LISTEN_ADDRESS_DEFAULT.to_string());
    let listen_address = listen_text
        .parse()
        .map_err(|_| ConfigError::ListenAddressInvalid { value: listen_text })?;
    Ok(ApiConfig {
        db_dsn,
        listen_address,
        rpc_url,
        rpc_rps_max: RpsMax::new(rpc_rps_max),
    })
}

async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "sigterm_handler_unavailable");
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        _ = interrupt => {},
        () = terminate => {},
    }
    tracing::info!("api_shutting_down");
}

async fn serve(config: ApiConfig) -> std::io::Result<()> {
    // Lazy so the API starts while the database is down and health answers 503 meanwhile.
    let database = PgPoolOptions::new()
        .max_connections(DATABASE_CONNECTION_COUNT_MAX)
        .acquire_timeout(DATABASE_ACQUIRE_TIMEOUT)
        .connect_lazy(&config.db_dsn)
        .map_err(std::io::Error::other)?;
    // Only getBlocks and getBlockTime, never getBlock, so the version ceiling is never sent.
    let backfill_gateway = config
        .rpc_url
        .map(|url| {
            RpcGateway::new(
                url,
                Endpoint::Provider,
                config.rpc_rps_max,
                TransactionVersionMax::SUPPORTED,
            )
        })
        .transpose()
        .map_err(std::io::Error::other)?
        .map(Arc::new);
    let listener = tokio::net::TcpListener::bind(config.listen_address).await?;
    tracing::info!(
        listen_address = %config.listen_address,
        backfill_enabled = backfill_gateway.is_some(),
        "api_listening"
    );
    axum::serve(listener, dlmm_api::router(database, backfill_gateway))
        .with_graceful_shutdown(shutdown_signal())
        .await
}

#[tokio::main]
async fn main() -> ExitCode {
    // A missing .env is normal inside containers, where compose supplies the environment.
    let _ = dotenvy::dotenv();
    init_tracing();
    let config = match config_from_environment() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(%error, "api_configuration_invalid");
            return ExitCode::FAILURE;
        }
    };
    match serve(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "api_stopped");
            ExitCode::FAILURE
        }
    }
}
