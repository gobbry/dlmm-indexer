use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgConnection;

use super::convert::{sum_to_database, timestamp_from_unix};
use crate::domain::error::StoreError;
use crate::domain::ids::{PoolAddress, UnixSeconds};
use crate::domain::projection::{PoolStatsDelta, ProjectionDeltas, ProjectionName, VolumeDelta};

// Every projection column is a commutative monoid, so deltas add in any order. A filter
// applies one projection only, which is what a rebuild of that projection needs.
pub(crate) async fn apply_deltas(
    connection: &mut PgConnection,
    deltas: &ProjectionDeltas,
    filter: Option<ProjectionName>,
) -> Result<(), StoreError> {
    let applies = |name: ProjectionName| filter.is_none_or(|only| only == name);
    if applies(ProjectionName::PoolVolume1h) {
        let rows = deltas
            .pool_volume_1h
            .iter()
            .map(|((pool, bucket), delta)| (*pool, bucket.start(), delta));
        apply_pool_volume(&mut *connection, POOL_VOLUME_1H_UPSERT_SQL, rows).await?;
    }
    if applies(ProjectionName::PoolVolume1d) {
        let rows = deltas
            .pool_volume_1d
            .iter()
            .map(|((pool, bucket), delta)| (*pool, bucket.start(), delta));
        apply_pool_volume(&mut *connection, POOL_VOLUME_1D_UPSERT_SQL, rows).await?;
    }
    if applies(ProjectionName::PoolStats) {
        apply_pool_stats(connection, deltas).await?;
    }
    Ok(())
}

#[derive(Default)]
struct VolumeColumns {
    swap_counts: Vec<i64>,
    volumes_x: Vec<Decimal>,
    volumes_y: Vec<Decimal>,
    volumes_usd: Vec<Decimal>,
    unpriced_swap_counts: Vec<i64>,
}

impl VolumeColumns {
    fn push(&mut self, delta: &VolumeDelta) -> Result<(), StoreError> {
        self.swap_counts.push(delta.swap_count);
        self.volumes_x
            .push(sum_to_database(delta.volume_x, "volume_x")?);
        self.volumes_y
            .push(sum_to_database(delta.volume_y, "volume_y")?);
        self.volumes_usd.push(delta.volume_usd);
        self.unpriced_swap_counts.push(delta.unpriced_swap_count);
        Ok(())
    }
}

// One statement per table, built at compile time: a table name cannot be a bind parameter.
macro_rules! pool_volume_upsert_sql {
    ($table:literal) => {
        concat!(
            "INSERT INTO ",
            $table,
            " (bucket, pool, swap_count, volume_x, volume_y, volume_usd, unpriced_swap_count)
             SELECT * FROM unnest($1::timestamptz[], $2::text[], $3::bigint[], $4::numeric[],
                                  $5::numeric[], $6::numeric[], $7::bigint[])
             ON CONFLICT (pool, bucket) DO UPDATE SET
                 swap_count = ",
            $table,
            ".swap_count + EXCLUDED.swap_count,
                 volume_x = ",
            $table,
            ".volume_x + EXCLUDED.volume_x,
                 volume_y = ",
            $table,
            ".volume_y + EXCLUDED.volume_y,
                 volume_usd = ",
            $table,
            ".volume_usd + EXCLUDED.volume_usd,
                 unpriced_swap_count = ",
            $table,
            ".unpriced_swap_count
                                       + EXCLUDED.unpriced_swap_count"
        )
    };
}

const POOL_VOLUME_1H_UPSERT_SQL: &str = pool_volume_upsert_sql!("pool_volume_1h");
const POOL_VOLUME_1D_UPSERT_SQL: &str = pool_volume_upsert_sql!("pool_volume_1d");

// The hourly and daily tables share one shape and differ only in bucket width.
async fn apply_pool_volume<'delta>(
    connection: &mut PgConnection,
    statement: &'static str,
    rows: impl Iterator<Item = (PoolAddress, UnixSeconds, &'delta VolumeDelta)>,
) -> Result<(), StoreError> {
    let mut buckets: Vec<DateTime<Utc>> = Vec::new();
    let mut pools = Vec::new();
    let mut volumes = VolumeColumns::default();
    for (pool, bucket_start, delta) in rows {
        buckets.push(timestamp_from_unix(bucket_start, "bucket")?);
        pools.push(pool.to_string());
        volumes.push(delta)?;
    }
    if buckets.is_empty() {
        return Ok(());
    }
    debug_assert_eq!(buckets.len(), pools.len());
    sqlx::query(statement)
        .bind(buckets)
        .bind(pools)
        .bind(volumes.swap_counts)
        .bind(volumes.volumes_x)
        .bind(volumes.volumes_y)
        .bind(volumes.volumes_usd)
        .bind(volumes.unpriced_swap_counts)
        .execute(connection)
        .await?;
    Ok(())
}

async fn apply_pool_stats(
    connection: &mut PgConnection,
    deltas: &ProjectionDeltas,
) -> Result<(), StoreError> {
    if deltas.pool_stats.is_empty() {
        return Ok(());
    }
    let mut pools = Vec::with_capacity(deltas.pool_stats.len());
    let mut volumes = VolumeColumns::default();
    let mut first_swaps_at: Vec<DateTime<Utc>> = Vec::with_capacity(deltas.pool_stats.len());
    let mut last_swaps_at: Vec<DateTime<Utc>> = Vec::with_capacity(deltas.pool_stats.len());
    for (
        pool,
        PoolStatsDelta {
            volume,
            first_swap_at,
            last_swap_at,
        },
    ) in &deltas.pool_stats
    {
        debug_assert!(first_swap_at <= last_swap_at);
        pools.push(pool.to_string());
        volumes.push(volume)?;
        first_swaps_at.push(timestamp_from_unix(*first_swap_at, "first_swap_at")?);
        last_swaps_at.push(timestamp_from_unix(*last_swap_at, "last_swap_at")?);
    }
    sqlx::query(
        "INSERT INTO pool_stats (pool, swap_count, volume_x, volume_y, volume_usd,
                                 unpriced_swap_count, first_swap_at, last_swap_at)
         SELECT * FROM unnest($1::text[], $2::bigint[], $3::numeric[], $4::numeric[],
                              $5::numeric[], $6::bigint[], $7::timestamptz[], $8::timestamptz[])
         ON CONFLICT (pool) DO UPDATE SET
             swap_count = pool_stats.swap_count + EXCLUDED.swap_count,
             volume_x = pool_stats.volume_x + EXCLUDED.volume_x,
             volume_y = pool_stats.volume_y + EXCLUDED.volume_y,
             volume_usd = pool_stats.volume_usd + EXCLUDED.volume_usd,
             unpriced_swap_count = pool_stats.unpriced_swap_count + EXCLUDED.unpriced_swap_count,
             first_swap_at = LEAST(pool_stats.first_swap_at, EXCLUDED.first_swap_at),
             last_swap_at = GREATEST(pool_stats.last_swap_at, EXCLUDED.last_swap_at)",
    )
    .bind(pools)
    .bind(volumes.swap_counts)
    .bind(volumes.volumes_x)
    .bind(volumes.volumes_y)
    .bind(volumes.volumes_usd)
    .bind(volumes.unpriced_swap_counts)
    .bind(first_swaps_at)
    .bind(last_swaps_at)
    .execute(connection)
    .await?;
    Ok(())
}
