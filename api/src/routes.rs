use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::header::{CONTENT_TYPE, HeaderName};
use axum::http::{Method, StatusCode};
use axum::routing::{MethodRouter, delete, get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use dlmm_core::domain::ids::{JobId, PoolAddress, UnixSeconds};
use dlmm_core::domain::job::{BackfillInsert, CancelOutcome};
use dlmm_core::domain::query::{
    Bucket, PageLimit, PageOffset, PoolMetadata, ProjectionRead, RowCountMax, SwapCursor,
};
use dlmm_core::gateway::rpc::{RequestLane, RpcGateway};
use dlmm_core::store::{
    cancel_job, insert_backfill_job, read_health, read_job, read_jobs, read_lowest_coverage_start,
    read_pool_metadata, read_pool_summary, read_pool_volume, read_pools, read_swaps,
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::error::{ApiError, TimeParameter, store_failure};
use crate::range::align_range;
use crate::render::{
    BackfillBody, CancelBody, HealthBody, JobBody, JobListBody, PoolBody, PoolListBody,
    SwapListBody, VolumeBody, render_cancel, render_health, render_job, render_pool,
    render_pool_page, render_swap_page, render_volume,
};

const OPENAPI_DOCUMENT: &str = include_str!("../openapi.json");
const JOB_LIST_COUNT_MAX: RowCountMax = RowCountMax::new(50);

#[derive(Clone)]
struct ApiState {
    database: PgPool,
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
struct PoolPageQuery {
    limit: Option<String>,
    offset: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SwapPageQuery {
    limit: Option<String>,
    before: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BackfillRequest {
    from: String,
}

// The one list of what the API serves: the router is built from it and only from it, and
// api/openapi.json is checked against it, so a route cannot ship without its contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Health,
    PoolList,
    Pool,
    PoolVolume,
    PoolSwaps,
    BackfillCreate,
    BackfillList,
    BackfillRead,
    BackfillCancel,
    OpenApi,
}

pub const ROUTES: [Route; 10] = [
    Route::Health,
    Route::PoolList,
    Route::Pool,
    Route::PoolVolume,
    Route::PoolSwaps,
    Route::BackfillCreate,
    Route::BackfillList,
    Route::BackfillRead,
    Route::BackfillCancel,
    Route::OpenApi,
];

impl Route {
    pub const fn method(self) -> Method {
        match self {
            Self::BackfillCreate => Method::POST,
            Self::BackfillCancel => Method::DELETE,
            Self::Health
            | Self::PoolList
            | Self::Pool
            | Self::PoolVolume
            | Self::PoolSwaps
            | Self::BackfillList
            | Self::BackfillRead
            | Self::OpenApi => Method::GET,
        }
    }

    pub const fn path(self) -> &'static str {
        match self {
            Self::Health => "/v1/health",
            Self::PoolList => "/v1/pools",
            Self::Pool => "/v1/pools/{pool}",
            Self::PoolVolume => "/v1/pools/{pool}/volume",
            Self::PoolSwaps => "/v1/pools/{pool}/swaps",
            Self::BackfillCreate | Self::BackfillList => "/v1/backfills",
            Self::BackfillRead | Self::BackfillCancel => "/v1/backfills/{job_id}",
            Self::OpenApi => "/openapi.json",
        }
    }

    fn handler(self) -> MethodRouter<ApiState> {
        let handler = match self {
            Self::Health => get(health),
            Self::PoolList => get(pools),
            Self::Pool => get(pool_summary),
            Self::PoolVolume => get(volume),
            Self::PoolSwaps => get(swaps),
            Self::BackfillCreate => post(backfill),
            Self::BackfillList => get(backfills),
            Self::BackfillRead => get(backfill_job),
            Self::BackfillCancel => delete(cancel_backfill),
            Self::OpenApi => get(openapi),
        };
        debug_assert!(self.path().starts_with('/'));
        handler
    }
}

// axum merges two routes on one path and panics on a repeated method, so a duplicate entry in
// ROUTES fails at startup.
pub fn router(database: PgPool, backfill_gateway: Option<Arc<RpcGateway>>) -> Router {
    ROUTES
        .into_iter()
        .fold(Router::new(), |router, route| {
            router.route(route.path(), route.handler())
        })
        .fallback(not_found)
        .with_state(ApiState {
            database,
            backfill_gateway,
        })
}

async fn openapi() -> ([(HeaderName, &'static str); 1], &'static str) {
    ([(CONTENT_TYPE, "application/json")], OPENAPI_DOCUMENT)
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

// One spelling per number, as for the cursor: u32's parser also takes a leading '+', and a
// leading zero would make two requests for one page.
fn parse_canonical_u32(text: &str) -> Option<u32> {
    let digits_only = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    let leading_zero = text.len() > 1 && text.starts_with('0');
    if !digits_only || leading_zero {
        return None;
    }
    text.parse().ok()
}

fn parse_limit(text: Option<&str>) -> Result<PageLimit, ApiError> {
    let Some(text) = text else {
        return Ok(PageLimit::DEFAULT);
    };
    parse_canonical_u32(text)
        .and_then(PageLimit::new)
        .ok_or(ApiError::InvalidLimit {
            limit_max: PageLimit::MAX,
        })
}

fn parse_offset(text: Option<&str>) -> Result<PageOffset, ApiError> {
    let Some(text) = text else {
        return Ok(PageOffset::default());
    };
    parse_canonical_u32(text)
        .map(PageOffset::new)
        .ok_or(ApiError::InvalidOffset)
}

fn parse_cursor(text: Option<&str>) -> Result<Option<SwapCursor>, ApiError> {
    text.map(|text| text.parse().map_err(|_| ApiError::InvalidCursor))
        .transpose()
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

fn parse_job_id(text: &str) -> Result<JobId, ApiError> {
    match text.parse::<i64>() {
        Ok(id) if id > 0 => Ok(JobId::new(id)),
        _ => Err(ApiError::InvalidJobId),
    }
}

async fn backfills(State(state): State<ApiState>) -> Result<Json<JobListBody>, ApiError> {
    let jobs = read_jobs(&state.database, JOB_LIST_COUNT_MAX)
        .await
        .map_err(store_failure)?;
    Ok(Json(JobListBody {
        jobs: jobs.iter().map(render_job).collect(),
    }))
}

async fn backfill_job(
    State(state): State<ApiState>,
    Path(job_id_text): Path<String>,
) -> Result<Json<JobBody>, ApiError> {
    let job_id = parse_job_id(&job_id_text)?;
    let job = read_job(&state.database, job_id)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::JobNotFound)?;
    Ok(Json(render_job(&job)))
}

// A cancel blocks the job with reason `cancelled` instead of deleting it: a blocked job keeps
// owning its hole, so the reconciler reopens nothing over it. Resuming is deleting that row.
async fn cancel_backfill(
    State(state): State<ApiState>,
    Path(job_id_text): Path<String>,
) -> Result<Json<CancelBody>, ApiError> {
    let job_id = parse_job_id(&job_id_text)?;
    match cancel_job(&state.database, job_id)
        .await
        .map_err(store_failure)?
    {
        CancelOutcome::Cancelled {
            id,
            range,
            next_slot,
        } => {
            tracing::info!(
                job_id = id.get(),
                start_slot = range.start.get(),
                end_slot = range.end_inclusive.get(),
                next_slot = next_slot.get(),
                "backfill_job_cancelled"
            );
            Ok(Json(render_cancel(id, range, next_slot)))
        }
        CancelOutcome::NotFound => Err(ApiError::JobNotFound),
        CancelOutcome::AlreadyCompleted => Err(ApiError::JobAlreadyCompleted),
        CancelOutcome::AlreadyBlocked(reason) => Err(ApiError::JobAlreadyBlocked {
            reason: reason.as_str().to_owned(),
        }),
    }
}

async fn health(State(state): State<ApiState>) -> Result<Json<HealthBody>, ApiError> {
    let health = read_health(&state.database).await.map_err(store_failure)?;
    let now = UnixSeconds::new(Utc::now().timestamp());
    Ok(Json(render_health(&health, now)))
}

// Offset paging over a ranking that moves as blocks land: a pool can shift across a page
// boundary between two requests. Accepted for a few hundred pools; swaps page by keyset.
async fn pools(
    State(state): State<ApiState>,
    query: Result<Query<PoolPageQuery>, QueryRejection>,
) -> Result<Json<PoolListBody>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::InvalidQuery)?;
    let limit = parse_limit(query.limit.as_deref())?;
    let offset = parse_offset(query.offset.as_deref())?;
    let page = live_rows(
        read_pools(&state.database, limit, offset)
            .await
            .map_err(store_failure)?,
    )?;
    Ok(Json(render_pool_page(&page, limit, offset)))
}

// Existence first, as for volume, so an unknown pool is a 404 even while a projection rebuilds.
async fn pool_summary(
    State(state): State<ApiState>,
    Path(pool_text): Path<String>,
) -> Result<Json<PoolBody>, ApiError> {
    let pool = existing_pool(&state, parse_pool(&pool_text)?).await?;
    let summary = live_rows(
        read_pool_summary(&state.database, pool.address)
            .await
            .map_err(store_failure)?,
    )?
    // Pools are never deleted, so a pool that just existed is still there.
    .ok_or(ApiError::PoolNotFound)?;
    Ok(Json(render_pool(&summary)))
}

async fn swaps(
    State(state): State<ApiState>,
    Path(pool_text): Path<String>,
    query: Result<Query<SwapPageQuery>, QueryRejection>,
) -> Result<Json<SwapListBody>, ApiError> {
    let pool = parse_pool(&pool_text)?;
    // Swaps come from the log, not a projection, so a rebuild does not hold them back; it only
    // takes away the first and last swaps that bound the scan.
    let metadata = existing_pool(&state, pool).await?;
    let Query(query) = query.map_err(|_| ApiError::InvalidQuery)?;
    let limit = parse_limit(query.limit.as_deref())?;
    let before = parse_cursor(query.before.as_deref())?;
    let page = read_swaps(
        &state.database,
        pool,
        metadata.first_swap_at,
        metadata.last_swap_at,
        limit,
        before,
    )
    .await
    .map_err(store_failure)?;
    Ok(Json(render_swap_page(pool, &page, limit)))
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
