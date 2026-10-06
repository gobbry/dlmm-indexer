use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::Row;
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, PgPool};

use super::convert::{
    address_from_database, amount_from_database, count_from_database, decimals_from_database,
    direction_from_database, fee_rate_from_database, fee_side_from_database,
    fee_token_from_database, optional_amount_from_database, projection_name_from_database,
    projection_state_from_database, quote_asset_from_database, slot_from_database,
    slot_to_database, swap_ordinal_from_database, swap_ordinal_to_database,
    swap_source_from_database, timestamp_from_unix, transaction_index_from_database,
    transaction_index_to_database, unix_from_timestamp,
};
use super::write::inserted_swap_from_row;
use crate::domain::error::StoreError;
use crate::domain::ids::{JobId, PoolAddress, UnixSeconds};
use crate::domain::projection::{InsertedSwap, LogPosition, ProjectionName, ProjectionState};
use crate::domain::query::{
    AlignedRange, Bucket, IndexerHealth, PageLimit, PageOffset, PoolMetadata, PoolPage,
    PoolSummary, ProjectionRead, RowCountMax, SwapCursor, SwapPage, SwapRow, VolumeBucket,
};

const HOUR_SECONDS: i64 = 3_600;
const DAY_SECONDS: i64 = 86_400;

// The generated series makes empty buckets explicit; each width reads its own projection.
// The projection's state comes from the same statement, so a rebuild that commits between a
// separate check and this read cannot hand out a truncated table; while it is not live the
// buckets read as empty and the caller only looks at the state.
macro_rules! pool_volume_sql {
    ($table:literal, $step:literal) => {
        concat!(
            "SELECT p.state::text                         AS projection_state,
                    g.bucket                              AS start,
                    coalesce(v.swap_count, 0)             AS swap_count,
                    coalesce(v.volume_x, 0)               AS volume_x,
                    coalesce(v.volume_y, 0)               AS volume_y,
                    coalesce(v.volume_usd, 0)             AS volume_usd,
                    coalesce(v.unpriced_swap_count, 0)    AS unpriced_swap_count
             FROM projection p
             CROSS JOIN generate_series($2::timestamptz, $3::timestamptz - INTERVAL '",
            $step,
            "',
                                  INTERVAL '",
            $step,
            "') AS g(bucket)
             LEFT JOIN ",
            $table,
            " v ON p.state = $4::projection_state AND v.bucket = g.bucket AND v.pool = $1
             WHERE p.name = '",
            $table,
            "'
             ORDER BY g.bucket"
        )
    };
}

const POOL_VOLUME_1H_SQL: &str = pool_volume_sql!("pool_volume_1h", "1 hour");
const POOL_VOLUME_1D_SQL: &str = pool_volume_sql!("pool_volume_1d", "1 day");

// Log order, for replaying the event log into a projection page by page. The block_time
// bound lets the hypertable skip whole chunks behind the replay. block_time never decreases
// along the chain (Agave clamps each block's clock to at least its parent's), so any
// non-negative slack is safe; the hour is defensive against a clock skew never observed.
const SWAP_LOG_PAGE_SQL: &str = r#"
SELECT slot, transaction_index, swap_ordinal, pool, block_time, direction::text AS direction,
       amount_in, amount_out, volume_usd
FROM swap
WHERE (slot, transaction_index, swap_ordinal) > ($1, $2, $3)
  AND block_time >= $5::timestamptz - INTERVAL '1 hour'
ORDER BY slot, transaction_index, swap_ordinal
LIMIT $4
"#;

// The 24-hour window is the current hour and the 23 before it.
macro_rules! pool_summary_sql {
    () => {
        r#"
SELECT p.address, p.mint_x, p.mint_y,
       token_x.decimals AS decimals_x, token_y.decimals AS decimals_y,
       coalesce(recent.swap_count, 0)::bigint          AS swap_count_24h,
       coalesce(recent.unpriced_swap_count, 0)::bigint AS unpriced_swap_count_24h,
       coalesce(recent.volume_usd, 0)                  AS volume_usd_24h,
       stats.first_swap_at, stats.last_swap_at
FROM pool p
LEFT JOIN token token_x ON token_x.mint = p.mint_x
LEFT JOIN token token_y ON token_y.mint = p.mint_y
LEFT JOIN pool_stats stats ON stats.pool = p.address
LEFT JOIN (
    SELECT pool, sum(swap_count) AS swap_count,
           sum(unpriced_swap_count) AS unpriced_swap_count, sum(volume_usd) AS volume_usd
    FROM pool_volume_1h
    WHERE bucket >= date_trunc('hour', now(), 'UTC') - INTERVAL '23 hours'
    GROUP BY pool
) recent ON recent.pool = p.address
"#
    };
}

// Both projections a summary reads report their state from the same statement, for the reason
// given at pool_volume_sql. The summaries hang off a one-row base by LEFT JOIN, so the states
// (and the page's extra columns) come back even when the page or the pool is empty.
macro_rules! pool_summary_states_sql {
    ($extra_columns:literal) => {
        concat!(
            r#"
SELECT (SELECT state::text FROM projection WHERE name = 'pool_volume_1h') AS volume_1h_state,
       (SELECT state::text FROM projection WHERE name = 'pool_stats')     AS stats_state,
       "#,
            $extra_columns,
            r#"
       summary.*
FROM (SELECT 1) AS one
LEFT JOIN summary ON true
"#
        )
    };
}

// The swap count breaks ties among unpriced pools, which all rank at zero USD.
const POOL_PAGE_SQL: &str = concat!(
    "WITH summary AS (",
    pool_summary_sql!(),
    "ORDER BY volume_usd_24h DESC, swap_count_24h DESC, p.address LIMIT $1 OFFSET $2)",
    pool_summary_states_sql!("(SELECT count(*) FROM pool) AS pool_count,"),
    "ORDER BY summary.volume_usd_24h DESC, summary.swap_count_24h DESC, summary.address"
);

const POOL_SUMMARY_SQL: &str = concat!(
    "WITH summary AS (",
    pool_summary_sql!(),
    "WHERE p.address = $1)",
    pool_summary_states_sql!("")
);

// Newest first by the full log key, so rows sharing a block_time (a slot, or two slots in one
// second) have one order and a page boundary between them neither repeats nor skips a row.
// The bare block_time bound duplicates the row comparison's leading column so the planner can
// use it to bound the swap_pool_time scan and skip chunks. `$3` and `$4`, the pool's first and
// last swaps, bound it on both sides, so a quiet pool or the last page does not probe every
// older chunk, and a pool whose swaps are all old does not probe every newer one.
macro_rules! swap_page_sql {
    ($after:literal) => {
        concat!(
            "SELECT signature, swap_ordinal, slot, transaction_index, block_time, user_address,
                    direction::text AS direction, mint_in, mint_out, amount_in, amount_out, fee,
                    protocol_fee, host_fee, fee_rate_1e9, mm_fee, limit_order_fee, amount_left,
                    fee_side::text AS fee_side, fee_token::text AS fee_token,
                    quote_asset_symbol, volume_usd, source::text AS source, fill_job_id
             FROM swap
             WHERE pool = $1 AND block_time >= $3
               AND block_time <= coalesce($4::timestamptz, 'infinity') ",
            $after,
            "
             ORDER BY block_time DESC, slot DESC, transaction_index DESC, swap_ordinal DESC
             LIMIT $2"
        )
    };
}

const SWAP_FIRST_PAGE_SQL: &str = swap_page_sql!("");
const SWAP_PAGE_BEFORE_SQL: &str = swap_page_sql!(
    "AND block_time <= $5
               AND (block_time, slot, transaction_index, swap_ordinal) < ($5, $6, $7, $8)"
);

// pool_stats joins only while live: a rebuild may not have reached the pool's row yet, and an
// unknown first swap is honest where a missing one would not be.
const POOL_METADATA_SQL: &str = r#"
SELECT p.address, p.mint_x, p.mint_y,
       token_x.decimals AS decimals_x, token_y.decimals AS decimals_y,
       stats.first_swap_at, stats.last_swap_at
FROM pool p
LEFT JOIN token token_x ON token_x.mint = p.mint_x
LEFT JOIN token token_y ON token_y.mint = p.mint_y
LEFT JOIN pool_stats stats
       ON stats.pool = p.address
      AND (SELECT state FROM projection WHERE name = 'pool_stats') = $2::projection_state
WHERE p.address = $1
"#;

// A zero is never reported for an unknown: no priced swap means no USD figure.
fn volume_usd_known(
    swap_count: u64,
    unpriced_swap_count: u64,
    volume_usd: Decimal,
) -> Option<Decimal> {
    (swap_count > unpriced_swap_count).then_some(volume_usd)
}

pub async fn read_pool_volume(
    database: &PgPool,
    pool: PoolAddress,
    range: AlignedRange,
) -> Result<ProjectionRead<Vec<VolumeBucket>>, StoreError> {
    debug_assert!(range.from < range.to_exclusive);
    debug_assert_eq!(range.from.get().rem_euclid(HOUR_SECONDS), 0);
    debug_assert_eq!(range.to_exclusive.get().rem_euclid(HOUR_SECONDS), 0);
    let (statement, projection) = match range.bucket {
        Bucket::Hour => (POOL_VOLUME_1H_SQL, ProjectionName::PoolVolume1h),
        Bucket::Day => {
            debug_assert_eq!(range.from.get().rem_euclid(DAY_SECONDS), 0);
            debug_assert_eq!(range.to_exclusive.get().rem_euclid(DAY_SECONDS), 0);
            (POOL_VOLUME_1D_SQL, ProjectionName::PoolVolume1d)
        }
    };
    let rows = sqlx::query(statement)
        .bind(pool.to_string())
        .bind(timestamp_from_unix(range.from, "from")?)
        .bind(timestamp_from_unix(range.to_exclusive, "to")?)
        .bind(ProjectionState::Live.as_str())
        .fetch_all(database)
        .await?;
    // The range holds at least one bucket, so a missing first row means the migration's
    // projection row is gone, which reads as not live rather than as zero volume.
    let state = match rows.first() {
        Some(row) => {
            projection_state_from_database(&row.try_get::<String, _>("projection_state")?)?
        }
        None => ProjectionState::Building,
    };
    if state != ProjectionState::Live {
        return Ok(ProjectionRead::Rebuilding(projection));
    }
    let buckets = rows
        .iter()
        .map(volume_bucket_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ProjectionRead::Live(buckets))
}

fn volume_bucket_from_row(row: &PgRow) -> Result<VolumeBucket, StoreError> {
    let start: DateTime<Utc> = row.try_get("start")?;
    let swap_count: u64 = count_from_database(row.try_get("swap_count")?, "swap_count")?;
    let unpriced_swap_count: u64 =
        count_from_database(row.try_get("unpriced_swap_count")?, "unpriced_swap_count")?;
    debug_assert!(unpriced_swap_count <= swap_count);
    Ok(VolumeBucket {
        start: unix_from_timestamp(start),
        swap_count,
        volume_x: row.try_get("volume_x")?,
        volume_y: row.try_get("volume_y")?,
        volume_usd: volume_usd_known(swap_count, unpriced_swap_count, row.try_get("volume_usd")?),
        unpriced_swap_count,
    })
}

// Rows strictly after `after`, in log order; the last row's position is the next `after`.
// Any executor, so a rebuild reads each page inside the transaction that applies it.
// `after_block_time` is the block_time of the row at `after` (the previous page's last row),
// or the epoch when it is unknown.
pub async fn read_swap_log_page(
    database: impl PgExecutor<'_>,
    after: LogPosition,
    after_block_time: UnixSeconds,
    limit: RowCountMax,
) -> Result<Vec<InsertedSwap>, StoreError> {
    debug_assert!(limit.get() > 0);
    let rows = sqlx::query(SWAP_LOG_PAGE_SQL)
        .bind(slot_to_database(after.slot, "slot")?)
        .bind(transaction_index_to_database(after.transaction_index)?)
        .bind(swap_ordinal_to_database(after.swap_ordinal)?)
        .bind(i64::from(limit.get()))
        .bind(timestamp_from_unix(after_block_time, "after_block_time")?)
        .fetch_all(database)
        .await?;
    let page = rows
        .iter()
        .map(inserted_swap_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    debug_assert!(page.first().is_none_or(|swap| swap.position > after));
    debug_assert!(
        page.windows(2)
            .all(|pair| pair[0].position < pair[1].position)
    );
    Ok(page)
}

// One row past the limit is read to learn whether another page exists without a count.
// `first_swap_at` and `last_swap_at` are the pool's from pool_stats, None while it rebuilds:
// the epoch and infinity then bound nothing, and the read is only slower.
pub async fn read_swaps(
    database: &PgPool,
    pool: PoolAddress,
    first_swap_at: Option<UnixSeconds>,
    last_swap_at: Option<UnixSeconds>,
    limit: PageLimit,
    before: Option<SwapCursor>,
) -> Result<SwapPage, StoreError> {
    let fetch_count = i64::from(limit.get()) + 1;
    let since = timestamp_from_unix(
        first_swap_at.unwrap_or(UnixSeconds::new(0)),
        "first_swap_at",
    )?;
    let until = last_swap_at
        .map(|last| timestamp_from_unix(last, "last_swap_at"))
        .transpose()?;
    debug_assert!(first_swap_at.is_none_or(|first| last_swap_at.is_some_and(|last| first <= last)));
    let rows = match before {
        None => {
            sqlx::query(SWAP_FIRST_PAGE_SQL)
                // Unprepared: a generic plan may pick the time index on quiet pools.
                .persistent(false)
                .bind(pool.to_string())
                .bind(fetch_count)
                .bind(since)
                .bind(until)
                .fetch_all(database)
                .await?
        }
        Some(cursor) => {
            sqlx::query(SWAP_PAGE_BEFORE_SQL)
                // Unprepared: a generic plan may pick the time index on quiet pools.
                .persistent(false)
                .bind(pool.to_string())
                .bind(fetch_count)
                .bind(since)
                .bind(until)
                .bind(timestamp_from_unix(cursor.block_time, "before")?)
                .bind(slot_to_database(cursor.position.slot, "slot")?)
                .bind(transaction_index_to_database(
                    cursor.position.transaction_index,
                )?)
                .bind(swap_ordinal_to_database(cursor.position.swap_ordinal)?)
                .fetch_all(database)
                .await?
        }
    };
    let mut swaps = rows
        .iter()
        .map(swap_row_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let more = swaps.len() > limit.get() as usize;
    swaps.truncate(limit.get() as usize);
    let next_cursor = if more {
        swaps.last().map(SwapCursor::of)
    } else {
        None
    };
    debug_assert!(swaps.len() <= limit.get() as usize);
    debug_assert!(next_cursor.is_none() || swaps.len() == limit.get() as usize);
    debug_assert!(before.is_none_or(|cursor| {
        swaps.first().map(SwapCursor::of).is_none_or(|first| {
            (first.block_time, first.position) < (cursor.block_time, cursor.position)
        })
    }));
    Ok(SwapPage { swaps, next_cursor })
}

fn swap_row_from_row(row: &PgRow) -> Result<SwapRow, StoreError> {
    let signature: String = row.try_get("signature")?;
    let user: String = row.try_get("user_address")?;
    let direction: String = row.try_get("direction")?;
    let mint_in: String = row.try_get("mint_in")?;
    let mint_out: String = row.try_get("mint_out")?;
    let block_time: DateTime<Utc> = row.try_get("block_time")?;
    let quote_asset_symbol: Option<String> = row.try_get("quote_asset_symbol")?;
    let fill_job_id: Option<i64> = row.try_get("fill_job_id")?;
    let fee_side: Option<String> = row.try_get("fee_side")?;
    let fee_token: Option<String> = row.try_get("fee_token")?;
    let source: String = row.try_get("source")?;
    Ok(SwapRow {
        signature: address_from_database(&signature, "signature")?,
        swap_ordinal: swap_ordinal_from_database(row.try_get("swap_ordinal")?)?,
        slot: slot_from_database(row.try_get("slot")?, "slot")?,
        transaction_index: transaction_index_from_database(row.try_get("transaction_index")?)?,
        block_time: unix_from_timestamp(block_time),
        user: address_from_database(&user, "user_address")?,
        direction: direction_from_database(&direction)?,
        mint_in: address_from_database(&mint_in, "mint_in")?,
        mint_out: address_from_database(&mint_out, "mint_out")?,
        amount_in: amount_from_database(row.try_get("amount_in")?, "amount_in")?,
        amount_out: amount_from_database(row.try_get("amount_out")?, "amount_out")?,
        fee: amount_from_database(row.try_get("fee")?, "fee")?,
        protocol_fee: amount_from_database(row.try_get("protocol_fee")?, "protocol_fee")?,
        host_fee: amount_from_database(row.try_get("host_fee")?, "host_fee")?,
        fee_rate_1e9: fee_rate_from_database(row.try_get("fee_rate_1e9")?)?,
        mm_fee: optional_amount_from_database(row.try_get("mm_fee")?, "mm_fee")?,
        limit_order_fee: optional_amount_from_database(
            row.try_get("limit_order_fee")?,
            "limit_order_fee",
        )?,
        amount_left: optional_amount_from_database(row.try_get("amount_left")?, "amount_left")?,
        fee_side: fee_side
            .as_deref()
            .map(fee_side_from_database)
            .transpose()?,
        fee_token: fee_token
            .as_deref()
            .map(fee_token_from_database)
            .transpose()?,
        quote_asset: quote_asset_symbol
            .as_deref()
            .map(quote_asset_from_database)
            .transpose()?,
        volume_usd: row.try_get("volume_usd")?,
        source: swap_source_from_database(&source)?,
        fill_job_id: fill_job_id.map(JobId::new),
    })
}

// None when the pool is unknown, which the API turns into a 404. No projection rebuild holds
// this read back, so a request answers 503 only for the table it actually serves from.
pub async fn read_pool_metadata(
    database: &PgPool,
    pool: PoolAddress,
) -> Result<Option<PoolMetadata>, StoreError> {
    let row = sqlx::query(POOL_METADATA_SQL)
        .bind(pool.to_string())
        .bind(ProjectionState::Live.as_str())
        .fetch_optional(database)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let address: String = row.try_get("address")?;
    let mint_x: String = row.try_get("mint_x")?;
    let mint_y: String = row.try_get("mint_y")?;
    let first_swap_at: Option<DateTime<Utc>> = row.try_get("first_swap_at")?;
    let last_swap_at: Option<DateTime<Utc>> = row.try_get("last_swap_at")?;
    debug_assert_eq!(first_swap_at.is_some(), last_swap_at.is_some());
    Ok(Some(PoolMetadata {
        address: address_from_database(&address, "address")?,
        mint_x: address_from_database(&mint_x, "mint_x")?,
        mint_y: address_from_database(&mint_y, "mint_y")?,
        decimals_x: decimals_from_database(row.try_get("decimals_x")?, "decimals_x")?,
        decimals_y: decimals_from_database(row.try_get("decimals_y")?, "decimals_y")?,
        first_swap_at: first_swap_at.map(unix_from_timestamp),
        last_swap_at: last_swap_at.map(unix_from_timestamp),
    }))
}

pub async fn read_pools(
    database: &PgPool,
    limit: PageLimit,
    offset: PageOffset,
) -> Result<ProjectionRead<PoolPage>, StoreError> {
    let rows = sqlx::query(POOL_PAGE_SQL)
        .bind(i64::from(limit.get()))
        .bind(i64::from(offset.get()))
        .fetch_all(database)
        .await?;
    // The one-row base guarantees a first row: it carries the states and the count.
    let Some(first) = rows.first() else {
        return Err(StoreError::ValueOutOfRange {
            column: "pool_count",
        });
    };
    if let Some(projection) = pool_summary_rebuilding(first)? {
        return Ok(ProjectionRead::Rebuilding(projection));
    }
    let pool_count: u64 = count_from_database(first.try_get("pool_count")?, "pool_count")?;
    let pools = pool_summaries_from_rows(&rows)?;
    debug_assert!(pools.len() <= limit.get() as usize);
    debug_assert!(pools.len() as u64 <= pool_count);
    Ok(ProjectionRead::Live(PoolPage { pools, pool_count }))
}

// None when the pool is unknown. The caller checks existence first with read_pool_metadata,
// which no rebuild holds back, so an unknown pool is a 404 even mid-rebuild.
pub async fn read_pool_summary(
    database: &PgPool,
    pool: PoolAddress,
) -> Result<ProjectionRead<Option<PoolSummary>>, StoreError> {
    let rows = sqlx::query(POOL_SUMMARY_SQL)
        .bind(pool.to_string())
        .fetch_all(database)
        .await?;
    debug_assert!(rows.len() <= 1);
    let Some(first) = rows.first() else {
        return Err(StoreError::ValueOutOfRange {
            column: "volume_1h_state",
        });
    };
    if let Some(projection) = pool_summary_rebuilding(first)? {
        return Ok(ProjectionRead::Rebuilding(projection));
    }
    let summary = pool_summaries_from_rows(&rows)?.pop();
    debug_assert!(summary.is_none_or(|summary| summary.address == pool));
    Ok(ProjectionRead::Live(summary))
}

// A row whose address is null is the empty LEFT JOIN of the one-row base, not a pool.
fn pool_summaries_from_rows(rows: &[PgRow]) -> Result<Vec<PoolSummary>, StoreError> {
    let mut pools = Vec::with_capacity(rows.len());
    for row in rows {
        let address: Option<String> = row.try_get("address")?;
        if address.is_some() {
            pools.push(pool_summary_from_row(row)?);
        }
    }
    Ok(pools)
}

fn pool_summary_rebuilding(row: &PgRow) -> Result<Option<ProjectionName>, StoreError> {
    let columns = [
        ("volume_1h_state", ProjectionName::PoolVolume1h),
        ("stats_state", ProjectionName::PoolStats),
    ];
    for (column, projection) in columns {
        let state: Option<String> = row.try_get(column)?;
        let live = match state {
            Some(text) => projection_state_from_database(&text)? == ProjectionState::Live,
            None => false,
        };
        if !live {
            return Ok(Some(projection));
        }
    }
    Ok(None)
}

fn pool_summary_from_row(row: &PgRow) -> Result<PoolSummary, StoreError> {
    let address: String = row.try_get("address")?;
    let mint_x: String = row.try_get("mint_x")?;
    let mint_y: String = row.try_get("mint_y")?;
    let swap_count_24h: u64 = count_from_database(row.try_get("swap_count_24h")?, "swap_count")?;
    let unpriced_swap_count_24h: u64 = count_from_database(
        row.try_get("unpriced_swap_count_24h")?,
        "unpriced_swap_count",
    )?;
    let first_swap_at: Option<DateTime<Utc>> = row.try_get("first_swap_at")?;
    let last_swap_at: Option<DateTime<Utc>> = row.try_get("last_swap_at")?;
    Ok(PoolSummary {
        address: address_from_database(&address, "address")?,
        mint_x: address_from_database(&mint_x, "mint_x")?,
        mint_y: address_from_database(&mint_y, "mint_y")?,
        decimals_x: decimals_from_database(row.try_get("decimals_x")?, "decimals_x")?,
        decimals_y: decimals_from_database(row.try_get("decimals_y")?, "decimals_y")?,
        swap_count_24h,
        volume_usd_24h: volume_usd_known(
            swap_count_24h,
            unpriced_swap_count_24h,
            row.try_get("volume_usd_24h")?,
        ),
        first_swap_at: first_swap_at.map(unix_from_timestamp),
        last_swap_at: last_swap_at.map(unix_from_timestamp),
    })
}

// The cursor and its block time are the top coverage range's end. Open excludes blocked jobs:
// blocked ones wait on an operator, not on the filler. The rebuilding list is a scalar
// subquery, so health stays one statement.
pub async fn read_health(database: &PgPool) -> Result<IndexerHealth, StoreError> {
    let row = sqlx::query(
        "SELECT top.end_slot AS cursor_slot,
                top.end_block_time AS last_block_time,
                (SELECT count(*) FROM slot_range_job
                 WHERE completed_at IS NULL AND blocked_reason IS NULL
                   AND next_slot <= end_slot) AS open_job_count,
                (SELECT count(*) FROM slot_range_job
                 WHERE completed_at IS NULL AND blocked_reason IS NOT NULL) AS blocked_job_count,
                (SELECT coalesce(array_agg(name ORDER BY name), '{}') FROM projection
                 WHERE state <> $1::projection_state) AS rebuilding_projections
         FROM (SELECT 1) AS one
         LEFT JOIN (SELECT end_slot, end_block_time FROM slot_coverage
                    ORDER BY end_slot DESC LIMIT 1) AS top ON true",
    )
    .bind(ProjectionState::Live.as_str())
    .fetch_one(database)
    .await?;
    let cursor_slot: Option<i64> = row.try_get("cursor_slot")?;
    let last_block_time: Option<DateTime<Utc>> = row.try_get("last_block_time")?;
    let rebuilding: Vec<String> = row.try_get("rebuilding_projections")?;
    Ok(IndexerHealth {
        cursor_slot: cursor_slot
            .map(|value| slot_from_database(value, "slot_coverage.end_slot"))
            .transpose()?,
        last_block_time: last_block_time.map(unix_from_timestamp),
        open_job_count: count_from_database(row.try_get("open_job_count")?, "open_job_count")?,
        blocked_job_count: count_from_database(
            row.try_get("blocked_job_count")?,
            "blocked_job_count",
        )?,
        rebuilding_projections: rebuilding
            .iter()
            .map(|name| projection_name_from_database(name))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

// In name order; `indexer run` refuses to start while any of them is building.
pub async fn read_projection_states(
    database: &PgPool,
) -> Result<Vec<(ProjectionName, ProjectionState)>, StoreError> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT name, state::text FROM projection ORDER BY name")
            .fetch_all(database)
            .await?;
    rows.iter()
        .map(|(name, state)| {
            Ok((
                projection_name_from_database(name)?,
                projection_state_from_database(state)?,
            ))
        })
        .collect()
}
