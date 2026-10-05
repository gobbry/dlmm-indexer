use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use dlmm_core::domain::ids::{PoolAddress, UnixSeconds};
use dlmm_core::domain::job::BackfillInsert;
use dlmm_core::domain::query::{Bucket, PoolMetadata, ProjectionRead, RowCountMax};
use dlmm_core::gateway::rpc::{RequestLane, RpcGateway};
use dlmm_core::store::{
    insert_backfill_job, read_health, read_lowest_coverage_start, read_pool_metadata,
    read_pool_volume, read_pools, read_recent_swaps,
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::error::{ApiError, TimeParameter, store_failure};
use crate::range::align_range;
use crate::render::{
    BackfillBody, HealthBody, PoolListBody, SwapListBody, VolumeBody, render_health, render_pool,
    render_swap, render_volume,
};

const POOL_LIMIT_DEFAULT: u32 = 50;
const POOL_LIMIT_MAX: u32 = 500;
const SWAP_LIMIT_DEFAULT: u32 = 20;
const SWAP_LIMIT_MAX: u32 = 500;

#[derive(Clone)]
struct ApiState {
    database: PgPool,
    // None when the API was started without RPC_URL: backfills answer 503.
    backfill_gateway: Option<Arc<RpcGateway>>,
}

// Every parameter is read as text so a bad value becomes the envelope, not axum's plain text.
#[derive(Debug, Deserialize)]
struct VolumeQuery {
    bucket: Option<String>,
    from: Option<String>,
    to: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LimitQuery {
    limit: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BackfillRequest {
    from: String,
}

pub fn router(database: PgPool, backfill_gateway: Option<Arc<RpcGateway>>) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/pools", get(pools))
        .route("/v1/pools/{pool}/volume", get(volume))
        .route("/v1/pools/{pool}/swaps", get(swaps))
        .route("/v1/backfills", post(backfill))
        .fallback(not_found)
        .with_state(ApiState {
            database,
            backfill_gateway,
        })
}

async fn not_found() -> ApiError {
    ApiError::NotFound
}

fn parse_pool(text: &str) -> Result<PoolAddress, ApiError> {
    text.parse().map_err(|_| ApiError::InvalidPool)
}

fn parse_bucket(text: Option<&str>) -> Result<Bucket, ApiError> {
    match text {
        Some("hour") => Ok(Bucket::Hour),
        Some("day") => Ok(Bucket::Day),
        _ => Err(ApiError::InvalidBucket),
    }
}

fn parse_time(text: Option<&str>, parameter: TimeParameter) -> Result<DateTime<Utc>, ApiError> {
    let text = text.ok_or(ApiError::InvalidTime { parameter })?;
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| ApiError::InvalidTime { parameter })
}

fn parse_limit(text: Option<&str>, default: u32, limit_max: u32) -> Result<RowCountMax, ApiError> {
    debug_assert!(default >= 1);
    debug_assert!(default <= limit_max);
    let Some(text) = text else {
        return Ok(RowCountMax::new(default));
    };
    match text.parse::<u32>() {
        Ok(limit) if (1..=limit_max).contains(&limit) => Ok(RowCountMax::new(limit)),
        _ => Err(ApiError::InvalidLimit { limit_max }),
    }
}

// Existence and labels never wait on a rebuild; only the table a request serves from may.
async fn existing_pool(state: &ApiState, pool: PoolAddress) -> Result<PoolMetadata, ApiError> {
    read_pool_metadata(&state.database, pool)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::PoolNotFound)
}

// A table mid-rebuild holds a prefix of the log; serving it would understate volume.
fn live_rows<T>(read: ProjectionRead<T>) -> Result<T, ApiError> {
    match read {
        ProjectionRead::Live(rows) => Ok(rows),
        ProjectionRead::Rebuilding(projection) => Err(ApiError::ProjectionRebuilding {
            projection: projection.table_name(),
        }),
    }
}

// The body is read as text, whatever its content type, so `curl -d` works without a header and
// a malformed one gets the envelope.
fn parse_backfill_from(body: &str, now: UnixSeconds) -> Result<UnixSeconds, ApiError> {
    let request: BackfillRequest =
        serde_json::from_str(body).map_err(|_| ApiError::InvalidBackfillBody)?;
    let from = DateTime::parse_from_rfc3339(&request.from)
        .map(|time| UnixSeconds::new(time.timestamp()))
        .map_err(|_| ApiError::InvalidBackfillBody)?;
    if from > now {
        return Err(ApiError::BackfillFromInFuture);
    }
    Ok(from)
}

// Cheap refusals come before the bisection, which costs tens of RPC calls. The job itself is
// one INSERT that re-reads the lowest range, so a refusal here is only an early answer.
async fn backfill(
    State(state): State<ApiState>,
    body: String,
) -> Result<(StatusCode, Json<BackfillBody>), ApiError> {
    let from = parse_backfill_from(&body, UnixSeconds::new(Utc::now().timestamp()))?;
    let gateway = state
        .backfill_gateway
        .as_ref()
        .ok_or(ApiError::BackfillUnavailable)?;
    let lowest_coverage_start = read_lowest_coverage_start(&state.database)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::NothingIndexedYet)?;
    let from_slot = gateway
        .slot_at_or_after(from, RequestLane::Live)
        .await
        .map_err(|error| {
            tracing::warn!(%error, from = from.get(), "backfill_slot_search_failed");
            ApiError::RpcUnavailable
        })?
        // The finalized tip is still before `from`, so it is above every indexed block too.
        .ok_or(ApiError::BackfillFromAfterCoverage)?;
    if from_slot >= lowest_coverage_start {
        return Err(ApiError::BackfillFromAfterCoverage);
    }
    match insert_backfill_job(&state.database, from_slot)
        .await
        .map_err(store_failure)?
    {
        BackfillInsert::Inserted { id, range } => {
            tracing::info!(
                job_id = id.get(),
                from = from.get(),
                start_slot = range.start.get(),
                end_slot = range.end_inclusive.get(),
                "backfill_job_inserted"
            );
            Ok((
                StatusCode::ACCEPTED,
                Json(BackfillBody {
                    job_id: id.get(),
                    start_slot: range.start.get(),
                    end_slot: range.end_inclusive.get(),
                }),
            ))
        }
        BackfillInsert::NothingIndexedYet => Err(ApiError::NothingIndexedYet),
        BackfillInsert::FromAtOrAboveCoverage { .. } => Err(ApiError::BackfillFromAfterCoverage),
        BackfillInsert::OverlapsJob => Err(ApiError::BackfillOverlapsJob),
    }
}

async fn health(State(state): State<ApiState>) -> Result<Json<HealthBody>, ApiError> {
    let health = read_health(&state.database).await.map_err(store_failure)?;
    let now = UnixSeconds::new(Utc::now().timestamp());
    Ok(Json(render_health(&health, now)))
}

async fn pools(
    State(state): State<ApiState>,
    query: Result<Query<LimitQuery>, QueryRejection>,
) -> Result<Json<PoolListBody>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::InvalidQuery)?;
    let limit = parse_limit(query.limit.as_deref(), POOL_LIMIT_DEFAULT, POOL_LIMIT_MAX)?;
    let pools = live_rows(
        read_pools(&state.database, limit)
            .await
            .map_err(store_failure)?,
    )?;
    Ok(Json(PoolListBody {
        pools: pools.iter().map(render_pool).collect(),
    }))
}

async fn swaps(
    State(state): State<ApiState>,
    Path(pool_text): Path<String>,
    query: Result<Query<LimitQuery>, QueryRejection>,
) -> Result<Json<SwapListBody>, ApiError> {
    let pool = parse_pool(&pool_text)?;
    // Swaps come from the log, not a projection, so a rebuild does not hold them back.
    existing_pool(&state, pool).await?;
    let Query(query) = query.map_err(|_| ApiError::InvalidQuery)?;
    let limit = parse_limit(query.limit.as_deref(), SWAP_LIMIT_DEFAULT, SWAP_LIMIT_MAX)?;
    let swaps = read_recent_swaps(&state.database, pool, limit)
        .await
        .map_err(store_failure)?;
    Ok(Json(SwapListBody {
        pool: pool.to_string(),
        swaps: swaps.iter().map(render_swap).collect(),
    }))
}

// Validation order is the contract: pool, existence, bucket, times, alignment, range, cap.
async fn volume(
    State(state): State<ApiState>,
    Path(pool_text): Path<String>,
    query: Result<Query<VolumeQuery>, QueryRejection>,
) -> Result<Json<VolumeBody>, ApiError> {
    let pool = existing_pool(&state, parse_pool(&pool_text)?).await?;
    let Query(query) = query.map_err(|_| ApiError::InvalidQuery)?;
    let bucket = parse_bucket(query.bucket.as_deref())?;
    let from = parse_time(query.from.as_deref(), TimeParameter::From)?;
    let to = parse_time(query.to.as_deref(), TimeParameter::To)?;
    let range = align_range(from, to, bucket)?;
    let buckets = live_rows(
        read_pool_volume(&state.database, pool.address, range)
            .await
            .map_err(store_failure)?,
    )?;
    Ok(Json(render_volume(&pool, range, &buckets)))
}
