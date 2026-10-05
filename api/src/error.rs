use std::fmt;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use dlmm_core::domain::error::StoreError;
use serde::Serialize;

use crate::range::RangeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeParameter {
    From,
    To,
}

impl fmt::Display for TimeParameter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::From => "from",
            Self::To => "to",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApiError {
    #[error("pool must be a base58 32-byte address")]
    InvalidPool,
    #[error("pool is not indexed")]
    PoolNotFound,
    #[error("query string is malformed")]
    InvalidQuery,
    #[error("bucket must be hour or day")]
    InvalidBucket,
    #[error("{parameter} must be an RFC 3339 timestamp")]
    InvalidTime { parameter: TimeParameter },
    #[error("from must be before to once aligned to the bucket")]
    InvalidRange,
    #[error("range spans {bucket_count} buckets; at most {bucket_count_max} are allowed")]
    RangeTooLarge {
        bucket_count: i64,
        bucket_count_max: i64,
    },
    #[error("limit must be an integer from 1 to {limit_max}")]
    InvalidLimit { limit_max: u32 },
    #[error("no such route")]
    NotFound,
    #[error("database is unavailable")]
    DatabaseUnavailable,
    #[error("{projection} is being rebuilt; retry once the rebuild finishes")]
    ProjectionRebuilding { projection: &'static str },
    #[error("body must be JSON with from, an RFC 3339 timestamp")]
    InvalidBackfillBody,
    #[error("from is in the future; a backfill fills history")]
    BackfillFromInFuture,
    #[error("from is at or after the oldest indexed block: nothing to fill")]
    BackfillFromAfterCoverage,
    #[error("nothing is indexed yet, so there is no range for a backfill to end below")]
    NothingIndexedYet,
    #[error("another job already owns part of that range")]
    BackfillOverlapsJob,
    #[error("backfill needs RPC_URL, which the API was started without")]
    BackfillUnavailable,
    #[error("the RPC endpoint failed while resolving from to a slot")]
    RpcUnavailable,
    #[error("internal error")]
    Internal,
}

impl ApiError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidPool => "invalid_pool",
            Self::PoolNotFound => "pool_not_found",
            Self::InvalidQuery => "invalid_query",
            Self::InvalidBucket => "invalid_bucket",
            Self::InvalidTime { .. } => "invalid_time",
            Self::InvalidRange => "invalid_range",
            Self::RangeTooLarge { .. } => "range_too_large",
            Self::InvalidLimit { .. } => "invalid_limit",
            Self::NotFound => "not_found",
            Self::DatabaseUnavailable => "database_unavailable",
            Self::ProjectionRebuilding { .. } => "projection_rebuilding",
            Self::InvalidBackfillBody => "invalid_backfill_body",
            Self::BackfillFromInFuture => "backfill_from_in_future",
            Self::BackfillFromAfterCoverage => "backfill_from_after_coverage",
            Self::NothingIndexedYet => "nothing_indexed_yet",
            Self::BackfillOverlapsJob => "backfill_overlaps_job",
            Self::BackfillUnavailable => "backfill_unavailable",
            Self::RpcUnavailable => "rpc_unavailable",
            Self::Internal => "internal",
        }
    }

    pub const fn status(&self) -> StatusCode {
        match self {
            Self::PoolNotFound | Self::NotFound => StatusCode::NOT_FOUND,
            Self::DatabaseUnavailable
            | Self::ProjectionRebuilding { .. }
            | Self::BackfillUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::NothingIndexedYet | Self::BackfillOverlapsJob => StatusCode::CONFLICT,
            Self::RpcUnavailable => StatusCode::BAD_GATEWAY,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidPool
            | Self::InvalidQuery
            | Self::InvalidBucket
            | Self::InvalidTime { .. }
            | Self::InvalidRange
            | Self::RangeTooLarge { .. }
            | Self::InvalidLimit { .. }
            | Self::InvalidBackfillBody
            | Self::BackfillFromInFuture
            | Self::BackfillFromAfterCoverage => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<RangeError> for ApiError {
    fn from(error: RangeError) -> Self {
        match error {
            RangeError::Empty => Self::InvalidRange,
            RangeError::TooLarge {
                bucket_count,
                bucket_count_max,
            } => Self::RangeTooLarge {
                bucket_count,
                bucket_count_max,
            },
        }
    }
}

// The cause is logged here because the envelope deliberately does not carry it to clients.
pub fn store_failure(error: StoreError) -> ApiError {
    tracing::error!(%error, "store read failed");
    match error {
        StoreError::Database(_) => ApiError::DatabaseUnavailable,
        StoreError::Migration(_)
        | StoreError::ValueOutOfRange { .. }
        | StoreError::Address { .. } => ApiError::Internal,
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let envelope = ErrorEnvelope {
            error: ErrorBody {
                code: self.code(),
                message: self.to_string(),
            },
        };
        (self.status(), Json(envelope)).into_response()
    }
}
