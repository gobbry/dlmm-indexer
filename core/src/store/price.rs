use chrono::{DateTime, TimeDelta, Utc};
use sqlx::postgres::PgRow;
use sqlx::{Connection, PgConnection, PgPool, Row};

use super::convert::{
    address_from_database, decimals_to_database, slot_from_database, swap_ordinal_from_database,
    timestamp_from_unix, transaction_index_from_database, unix_from_timestamp,
};
use super::deltas::apply_deltas;
use crate::domain::amounts::QuoteAsset;
use crate::domain::error::StoreError;
use crate::domain::ids::MinuteRange;
use crate::domain::price::{PRICE_AGE_MAX_SECONDS, PricePoint, PriceSource, UnpriceableMinute};
use crate::domain::projection::{LogPosition, RepricedSwap};
use crate::domain::registry::TokenRecord;
use crate::projection::project_repriced;

const MINUTE_SECONDS: i64 = 60;

// The one price rule, shared by the swap insert and the reprice sweep so the two can never
// disagree: the latest eligible row observed in the half-open window (t - age, t], so never a
// price from the swap's future. A tie on ts goes to the market source ($1[1]) by name, not by
// enum order, so a market added to the enum later still wins it. Inlined SQL rather than a
// database function: the plan is the same index scan on price's primary key, and the rule
// lives next to the Rust that binds it. A macro because concat! takes only literals, so both
// statements are still built at compile time. The caller names the swap row `s` (with
// quote_asset_symbol and block_time) and binds $1 = eligible sources, market first, and
// $2 = PRICE_AGE_MAX_SECONDS.
macro_rules! price_lookup_sql {
    () => {
        r#"LATERAL (
    SELECT p.ts, p.close_usd
    FROM price p
    WHERE p.asset_symbol = s.quote_asset_symbol
      AND p.source = ANY($1::text[]::price_source[])
      AND p.ts <= s.block_time
      AND p.ts > s.block_time - $2::bigint * INTERVAL '1 second'
    ORDER BY p.ts DESC, (p.source = ($1::text[]::price_source[])[1]) DESC, p.source
    LIMIT 1
)"#
    };
}
pub(super) use price_lookup_sql;

// A swap's USD value from the row price_lookup_sql found, in exact NUMERIC: the quote leg in
// base units times the close, over the quote decimals. Plain SQL, not a database function, for
// the same reason as the lookup; the stored volume_usd column fixes the scale. The caller names
// the swap row `s` (with quote_amount and quote_decimals) and the price row `quote_price`.
macro_rules! usd_value_sql {
    () => {
        "s.quote_amount * quote_price.close_usd / power(10::numeric, s.quote_decimals)"
    };
}
pub(super) use usd_value_sql;

// Quote decimals are not stored per swap; they come from QuoteAsset (as arrays built from
// QuoteAsset::ALL) so Rust stays the one source of them. The target of an UPDATE cannot be
// referenced from a LATERAL in its own FROM, hence the CTE.
const REPRICE_SQL: &str = concat!(
    r#"
WITH priced AS (
    SELECT s.signature, s.swap_ordinal, s.block_time, quote_price.ts AS price_ts,
           "#,
    usd_value_sql!(),
    r#" AS volume_usd
    FROM (
        SELECT w.signature, w.swap_ordinal, w.block_time, w.quote_asset_symbol, w.quote_amount,
               q.quote_decimals
        FROM swap w
        JOIN unnest($5::text[], $6::smallint[]) AS q(asset_symbol, quote_decimals)
          ON w.quote_asset_symbol = q.asset_symbol
        WHERE w.volume_usd IS NULL
          AND w.quote_asset_symbol IS NOT NULL
          AND w.block_time >= $3
          AND w.block_time < $4
    ) AS s
    CROSS JOIN "#,
    price_lookup_sql!(),
    r#" AS quote_price
)
UPDATE swap s
SET volume_usd = priced.volume_usd,
    price_ts = priced.price_ts
FROM priced
WHERE s.signature = priced.signature
  AND s.swap_ordinal = priced.swap_ordinal
  AND s.block_time = priced.block_time
  AND s.block_time >= $3
  AND s.block_time < $4
  AND s.volume_usd IS NULL
RETURNING s.slot, s.transaction_index, s.swap_ordinal, s.pool, s.block_time, s.volume_usd
"#
);

// The market first: price_lookup_sql breaks a tie on ts in favour of the first source.
pub(super) fn eligible_price_sources(market: PriceSource) -> Vec<&'static str> {
    PriceSource::eligible_with(market)
        .iter()
        .map(|source| source.as_str())
        .collect()
}

// `range` holds swap minutes (block_time floored). The reprice and its projection deltas
// commit together here, so no caller can price swaps and forget the projections; the
// returned rows are for logging.
pub(crate) async fn reprice_unpriced(
    connection: &mut PgConnection,
    range: MinuteRange,
    market: PriceSource,
) -> Result<Vec<RepricedSwap>, StoreError> {
    debug_assert!(range.start <= range.end_inclusive);
    debug_assert_eq!(range.start.get().rem_euclid(MINUTE_SECONDS), 0);
    let start = timestamp_from_unix(range.start, "block_time")?;
    let end_exclusive =
        timestamp_from_unix(range.end_inclusive, "block_time")? + TimeDelta::minutes(1);
    let asset_symbols: Vec<&str> = QuoteAsset::ALL.iter().map(|asset| asset.as_str()).collect();
    let decimals: Vec<i16> = QuoteAsset::ALL
        .iter()
        .map(|asset| decimals_to_database(asset.decimals()))
        .collect();
    let mut transaction = connection.begin().await?;
    let rows = sqlx::query(REPRICE_SQL)
        .bind(eligible_price_sources(market))
        .bind(PRICE_AGE_MAX_SECONDS)
        .bind(start)
        .bind(end_exclusive)
        .bind(asset_symbols)
        .bind(decimals)
        .fetch_all(&mut *transaction)
        .await?;
    let repriced = rows
        .iter()
        .map(repriced_swap_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    apply_deltas(&mut transaction, &project_repriced(&repriced), None).await?;
    transaction.commit().await?;
    Ok(repriced)
}

fn repriced_swap_from_row(row: &PgRow) -> Result<RepricedSwap, StoreError> {
    let block_time: DateTime<Utc> = row.try_get("block_time")?;
    let pool: String = row.try_get("pool")?;
    Ok(RepricedSwap {
        position: LogPosition {
            slot: slot_from_database(row.try_get("slot")?, "slot")?,
            transaction_index: transaction_index_from_database(row.try_get("transaction_index")?)?,
            swap_ordinal: swap_ordinal_from_database(row.try_get("swap_ordinal")?)?,
        },
        pool: address_from_database(&pool, "pool")?,
        block_time: unix_from_timestamp(block_time),
        volume_usd: row.try_get("volume_usd")?,
    })
}

// Each end of the span is an ordered scan of the partial unpriced index that stops at its first
// row, so a sweep reads a handful of rows rather than every unpriced swap (an archive backfill
// leaves millions); only rows in minutes listed unpriceable are stepped over, and the list is
// a hashed NOT IN, one probe per row.
const UNPRICED_SPAN_SQL: &str = r#"
SELECT (SELECT date_trunc('minute', s.block_time, 'UTC') FROM swap s
        WHERE s.volume_usd IS NULL AND s.quote_asset_symbol IS NOT NULL
          AND (s.quote_asset_symbol, date_trunc('minute', s.block_time, 'UTC')) NOT IN (
              SELECT u.asset_symbol, u.minute
              FROM unnest($1::text[], $2::timestamptz[]) AS u(asset_symbol, minute))
        ORDER BY s.block_time LIMIT 1) AS start,
       (SELECT date_trunc('minute', s.block_time, 'UTC') FROM swap s
        WHERE s.volume_usd IS NULL AND s.quote_asset_symbol IS NOT NULL
          AND (s.quote_asset_symbol, date_trunc('minute', s.block_time, 'UTC')) NOT IN (
              SELECT u.asset_symbol, u.minute
              FROM unnest($1::text[], $2::timestamptz[]) AS u(asset_symbol, minute))
        ORDER BY s.block_time DESC LIMIT 1) AS end_inclusive
"#;

// The span of swap minutes still unpriced, leaving out the minutes a sweep already found
// unpriceable so they stop widening every later window. A minute left out but inside the span
// is still repriced with it, so a price that turns up later (a seed from an older window) still
// lands.
pub(crate) async fn read_unpriced_minute_range(
    database: &PgPool,
    unpriceable: &[UnpriceableMinute],
) -> Result<Option<MinuteRange>, StoreError> {
    let asset_symbols: Vec<&str> = unpriceable
        .iter()
        .map(|minute| minute.asset.as_str())
        .collect();
    let minutes = unpriceable
        .iter()
        .map(|minute| timestamp_from_unix(minute.minute, "minute"))
        .collect::<Result<Vec<_>, _>>()?;
    let row = sqlx::query(UNPRICED_SPAN_SQL)
        .bind(asset_symbols)
        .bind(minutes)
        .fetch_one(database)
        .await?;
    let start: Option<DateTime<Utc>> = row.try_get("start")?;
    let end_inclusive: Option<DateTime<Utc>> = row.try_get("end_inclusive")?;
    let (Some(start), Some(end_inclusive)) = (start, end_inclusive) else {
        return Ok(None);
    };
    debug_assert!(start <= end_inclusive);
    Ok(Some(MinuteRange {
        start: unix_from_timestamp(start),
        end_inclusive: unix_from_timestamp(end_inclusive),
    }))
}

// Rows are immutable: a stored price is never overwritten, so a swap's USD never changes.
// Any ts is accepted; the lookup, not the writer, decides which row prices a swap.
pub(crate) async fn write_prices(
    database: &PgPool,
    points: &[PricePoint],
) -> Result<(), StoreError> {
    if points.is_empty() {
        return Ok(());
    }
    let mut asset_symbols = Vec::with_capacity(points.len());
    let mut observed_at: Vec<DateTime<Utc>> = Vec::with_capacity(points.len());
    let mut sources = Vec::with_capacity(points.len());
    let mut closes = Vec::with_capacity(points.len());
    for point in points {
        debug_assert!(point.close.get() > rust_decimal::Decimal::ZERO);
        asset_symbols.push(point.asset.as_str());
        observed_at.push(timestamp_from_unix(point.ts, "ts")?);
        sources.push(point.source.as_str());
        closes.push(point.close.get());
    }
    debug_assert_eq!(asset_symbols.len(), closes.len());
    sqlx::query(
        "INSERT INTO price (asset_symbol, ts, source, close_usd)
         SELECT asset_symbol, ts, source::price_source, close_usd
         FROM unnest($1::text[], $2::timestamptz[], $3::text[], $4::numeric[])
              AS i(asset_symbol, ts, source, close_usd)
         ON CONFLICT (asset_symbol, ts, source) DO NOTHING",
    )
    .bind(asset_symbols)
    .bind(observed_at)
    .bind(sources)
    .bind(closes)
    .execute(database)
    .await?;
    Ok(())
}

// A fetch that found no decimals still records fetched_at, and never erases known decimals.
pub(crate) async fn update_token_decimals(
    database: &PgPool,
    tokens: &[TokenRecord],
) -> Result<(), StoreError> {
    if tokens.is_empty() {
        return Ok(());
    }
    let mints: Vec<String> = tokens.iter().map(|token| token.mint.to_string()).collect();
    let decimals: Vec<Option<i16>> = tokens
        .iter()
        .map(|token| token.decimals.map(decimals_to_database))
        .collect();
    sqlx::query(
        "INSERT INTO token (mint, decimals, fetched_at)
         SELECT DISTINCT ON (mint) mint, decimals, now()
         FROM unnest($1::text[], $2::smallint[]) AS i(mint, decimals)
         ON CONFLICT (mint) DO UPDATE SET
             decimals = coalesce(EXCLUDED.decimals, token.decimals),
             fetched_at = now()",
    )
    .bind(mints)
    .bind(decimals)
    .execute(database)
    .await?;
    Ok(())
}
