use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::postgres::PgRow;
use sqlx::{Connection, PgConnection, Row};

use super::convert::{
    address_from_database, amount_from_database, amount_to_database, decimals_to_database,
    direction_from_database, fee_rate_to_database, slot_from_database, slot_to_database,
    swap_ordinal_from_database, swap_ordinal_to_database, timestamp_from_unix,
    transaction_index_from_database, transaction_index_to_database, unix_from_timestamp,
};
use super::coverage::cover;
use super::deltas::apply_deltas;
use super::price::{eligible_price_sources, price_lookup_sql, usd_value_sql};
use crate::domain::block::{BlockOrigin, EnrichedBlock, LiveSource, StoredBlock};
use crate::domain::error::StoreError;
use crate::domain::ids::{JobId, Slot};
use crate::domain::price::{PRICE_AGE_MAX_SECONDS, PriceSource};
use crate::domain::projection::{InsertedSwap, LogPosition};
use crate::domain::swap::{EnrichedSwap, SwapSource};
use crate::projection::project;

// Priced by price_lookup_sql and valued by usd_value_sql, the rules the reprice sweep shares,
// with the same bind positions:
// $1 the eligible sources (market first), $2 the age bound. The input rows are wrapped as `s`
// with a block_time column because the fragment reads the swap from there.
const INSERT_SWAPS_SQL: &str = concat!(
    r#"
WITH input AS (
    SELECT s.block_time, $4::bigint AS slot, s.transaction_index,
           s.signature, s.swap_ordinal, s.pool, s.user_address,
           s.direction::swap_direction AS direction, s.mint_in, s.mint_out,
           s.amount_in, s.amount_out, s.fee, s.protocol_fee, s.host_fee, s.fee_rate_1e9,
           s.start_bin_id, s.end_bin_id, s.mm_fee, s.limit_order_fee, s.amount_left,
           s.fee_side::fee_side AS fee_side, s.fee_token::fee_token AS fee_token,
           s.swap2_event_payload, s.quote_asset_symbol, s.quote_amount,
           quote_price.ts AS price_ts,
           "#,
    usd_value_sql!(),
    r#" AS volume_usd,
           $26::swap_source AS source, $5::bigint AS fill_job_id
    FROM (
        SELECT $3::timestamptz AS block_time, i.*
        FROM unnest($6::smallint[], $7::text[], $8::smallint[], $9::text[], $10::text[],
                    $11::text[], $12::text[], $13::text[], $14::numeric[], $15::numeric[],
                    $16::numeric[], $17::numeric[], $18::numeric[], $19::integer[],
                    $20::integer[], $21::bytea[], $22::text[], $23::numeric[], $24::smallint[],
                    $25::numeric[], $27::numeric[], $28::numeric[], $29::numeric[],
                    $30::text[], $31::text[])
             AS i(transaction_index, signature, swap_ordinal, pool, user_address, direction,
                  mint_in, mint_out, amount_in, amount_out, fee, protocol_fee, host_fee,
                  start_bin_id, end_bin_id, swap2_event_payload, quote_asset_symbol,
                  quote_amount, quote_decimals, fee_rate_1e9, mm_fee, limit_order_fee,
                  amount_left, fee_side, fee_token)
    ) AS s
    LEFT JOIN "#,
    price_lookup_sql!(),
    r#" AS quote_price ON true
)
INSERT INTO swap (block_time, slot, transaction_index, signature, swap_ordinal, pool,
                  user_address, direction, mint_in, mint_out, amount_in, amount_out, fee,
                  protocol_fee, host_fee, fee_rate_1e9, start_bin_id, end_bin_id, mm_fee,
                  limit_order_fee, amount_left, fee_side, fee_token, swap2_event_payload,
                  quote_asset_symbol, quote_amount, price_ts, volume_usd, source, fill_job_id)
SELECT * FROM input
ON CONFLICT (signature, swap_ordinal, block_time) DO NOTHING
RETURNING slot, transaction_index, swap_ordinal, pool, block_time, direction::text AS direction,
          amount_in, amount_out, volume_usd
"#
);

// Commits everything the block implies or nothing: swaps, projections, failures, coverage, job
// progress. Coverage is written for every origin, so completeness never depends on which
// source delivered a block.
pub(crate) async fn write_block(
    connection: &mut PgConnection,
    block: EnrichedBlock,
    origin: BlockOrigin,
    market: PriceSource,
) -> Result<StoredBlock, StoreError> {
    let mut transaction = connection.begin().await?;
    upsert_pools(&mut transaction, &block).await?;
    upsert_tokens(&mut transaction, &block).await?;
    let inserted = insert_swaps(&mut transaction, &block, origin, market).await?;
    apply_deltas(&mut transaction, &project(&inserted), None).await?;
    insert_decode_failures(&mut transaction, &block).await?;
    cover(
        &mut transaction,
        block.parent_slot,
        block.slot,
        block.block_time,
    )
    .await?;
    if let BlockOrigin::Fill {
        job_id,
        next_slot_after,
    } = origin
    {
        advance_job(&mut transaction, job_id, next_slot_after).await?;
    }
    transaction.commit().await?;

    debug_assert!(inserted.len() <= block.swaps.len());
    let inserted_swap_count = count_u32(inserted.len())?;
    let duplicate_swap_count = count_u32(block.swaps.len() - inserted.len())?;
    Ok(StoredBlock {
        slot: block.slot,
        block_time: block.block_time,
        origin,
        inserted_swap_count,
        duplicate_swap_count,
    })
}

fn count_u32(count: usize) -> Result<u32, StoreError> {
    u32::try_from(count).map_err(|_| StoreError::ValueOutOfRange {
        column: "swap_count",
    })
}

async fn upsert_pools(
    connection: &mut PgConnection,
    block: &EnrichedBlock,
) -> Result<(), StoreError> {
    if block.new_pools.is_empty() {
        return Ok(());
    }
    let mut addresses = Vec::with_capacity(block.new_pools.len());
    let mut mints_x = Vec::with_capacity(block.new_pools.len());
    let mut mints_y = Vec::with_capacity(block.new_pools.len());
    let mut first_seen_slots = Vec::with_capacity(block.new_pools.len());
    for pool in &block.new_pools {
        addresses.push(pool.address.to_string());
        mints_x.push(pool.mint_x.to_string());
        mints_y.push(pool.mint_y.to_string());
        first_seen_slots.push(slot_to_database(pool.first_seen_slot, "first_seen_slot")?);
    }
    sqlx::query(
        "INSERT INTO pool (address, mint_x, mint_y, first_seen_slot)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[])
         ON CONFLICT (address) DO NOTHING",
    )
    .bind(addresses)
    .bind(mints_x)
    .bind(mints_y)
    .bind(first_seen_slots)
    .execute(connection)
    .await?;
    Ok(())
}

async fn upsert_tokens(
    connection: &mut PgConnection,
    block: &EnrichedBlock,
) -> Result<(), StoreError> {
    if block.unknown_mints.is_empty() {
        return Ok(());
    }
    let mints: Vec<String> = block
        .unknown_mints
        .iter()
        .map(ToString::to_string)
        .collect();
    sqlx::query("INSERT INTO token (mint) SELECT unnest($1::text[]) ON CONFLICT (mint) DO NOTHING")
        .bind(mints)
        .execute(connection)
        .await?;
    Ok(())
}

async fn insert_decode_failures(
    connection: &mut PgConnection,
    block: &EnrichedBlock,
) -> Result<(), StoreError> {
    if block.failures.is_empty() {
        return Ok(());
    }
    let signatures: Vec<String> = block
        .failures
        .iter()
        .map(|failure| failure.signature.to_string())
        .collect();
    let reasons: Vec<String> = block
        .failures
        .iter()
        .map(|failure| failure.reason.to_string())
        .collect();
    sqlx::query(
        "INSERT INTO decode_failure (block_time, slot, signature, reason)
         SELECT $1, $2, signature, reason FROM unnest($3::text[], $4::text[]) AS i(signature, reason)
         ON CONFLICT (signature, reason) DO NOTHING",
    )
    .bind(timestamp_from_unix(block.block_time, "block_time")?)
    .bind(slot_to_database(block.slot, "slot")?)
    .bind(signatures)
    .bind(reasons)
    .execute(connection)
    .await?;
    Ok(())
}

// Column-major copy of the swaps, one Vec per unnest array.
#[derive(Default)]
struct SwapColumns {
    transaction_indexes: Vec<i16>,
    signatures: Vec<String>,
    swap_ordinals: Vec<i16>,
    pools: Vec<String>,
    users: Vec<String>,
    directions: Vec<&'static str>,
    mints_in: Vec<String>,
    mints_out: Vec<String>,
    amounts_in: Vec<Decimal>,
    amounts_out: Vec<Decimal>,
    fees: Vec<Decimal>,
    protocol_fees: Vec<Decimal>,
    host_fees: Vec<Decimal>,
    start_bin_ids: Vec<i32>,
    end_bin_ids: Vec<i32>,
    mm_fees: Vec<Option<Decimal>>,
    limit_order_fees: Vec<Option<Decimal>>,
    amounts_left: Vec<Option<Decimal>>,
    fee_sides: Vec<Option<&'static str>>,
    fee_tokens: Vec<Option<&'static str>>,
    swap2_event_payloads: Vec<Option<Vec<u8>>>,
    quote_asset_symbols: Vec<Option<&'static str>>,
    quote_amounts: Vec<Option<Decimal>>,
    quote_decimals: Vec<Option<i16>>,
    fee_rates_1e9: Vec<Decimal>,
}

impl SwapColumns {
    fn from_swaps(swaps: &[EnrichedSwap]) -> Result<Self, StoreError> {
        let mut columns = Self::default();
        for swap in swaps {
            columns.push(swap)?;
        }
        debug_assert_eq!(columns.signatures.len(), swaps.len());
        debug_assert_eq!(columns.quote_decimals.len(), swaps.len());
        Ok(columns)
    }

    fn push(&mut self, swap: &EnrichedSwap) -> Result<(), StoreError> {
        let decoded = &swap.decoded;
        let event = &decoded.event;
        debug_assert_eq!(event.lb_pair, decoded.pool);
        self.transaction_indexes
            .push(transaction_index_to_database(decoded.transaction_index)?);
        self.signatures.push(decoded.signature.to_string());
        self.swap_ordinals
            .push(swap_ordinal_to_database(decoded.ordinal)?);
        self.pools.push(decoded.pool.to_string());
        self.users.push(decoded.user.to_string());
        self.directions.push(event.direction.as_str());
        self.mints_in.push(swap.mint_in.to_string());
        self.mints_out.push(swap.mint_out.to_string());
        self.amounts_in.push(amount_to_database(event.amount_in));
        self.amounts_out.push(amount_to_database(event.amount_out));
        self.fees.push(amount_to_database(event.fee));
        self.protocol_fees
            .push(amount_to_database(event.protocol_fee));
        self.host_fees.push(amount_to_database(event.host_fee));
        self.start_bin_ids.push(event.start_bin_id.get());
        self.end_bin_ids.push(event.end_bin_id.get());
        let event2 = decoded.event2;
        self.mm_fees
            .push(event2.map(|event2| amount_to_database(event2.mm_fee)));
        self.limit_order_fees
            .push(event2.map(|event2| amount_to_database(event2.limit_order_fee)));
        self.amounts_left
            .push(event2.map(|event2| amount_to_database(event2.amount_left)));
        self.fee_sides
            .push(event2.map(|event2| event2.fee_side.as_str()));
        self.fee_tokens
            .push(event2.map(|event2| event2.fee_token.as_str()));
        self.swap2_event_payloads
            .push(decoded.swap2_event_payload.clone());
        if let Some(leg) = swap.quote_leg {
            debug_assert_eq!(leg.decimals, leg.asset.decimals());
        }
        self.quote_asset_symbols
            .push(swap.quote_leg.map(|leg| leg.asset.as_str()));
        self.quote_amounts
            .push(swap.quote_leg.map(|leg| amount_to_database(leg.amount)));
        self.quote_decimals
            .push(swap.quote_leg.map(|leg| decimals_to_database(leg.decimals)));
        self.fee_rates_1e9
            .push(fee_rate_to_database(event.fee_rate_1e9)?);
        Ok(())
    }
}

async fn insert_swaps(
    connection: &mut PgConnection,
    block: &EnrichedBlock,
    origin: BlockOrigin,
    market: PriceSource,
) -> Result<Vec<InsertedSwap>, StoreError> {
    if block.swaps.is_empty() {
        return Ok(Vec::new());
    }
    let columns = SwapColumns::from_swaps(&block.swaps)?;
    let (source, fill_job_id) = match origin {
        BlockOrigin::Live(LiveSource::Geyser) => (SwapSource::LiveGeyser, None),
        BlockOrigin::Live(LiveSource::RpcTail) => (SwapSource::LiveRpc, None),
        BlockOrigin::Fill { job_id, .. } => (SwapSource::Fill, Some(job_id.get())),
    };
    let rows = sqlx::query(INSERT_SWAPS_SQL)
        .bind(eligible_price_sources(market))
        .bind(PRICE_AGE_MAX_SECONDS)
        .bind(timestamp_from_unix(block.block_time, "block_time")?)
        .bind(slot_to_database(block.slot, "slot")?)
        .bind(fill_job_id)
        .bind(columns.transaction_indexes)
        .bind(columns.signatures)
        .bind(columns.swap_ordinals)
        .bind(columns.pools)
        .bind(columns.users)
        .bind(columns.directions)
        .bind(columns.mints_in)
        .bind(columns.mints_out)
        .bind(columns.amounts_in)
        .bind(columns.amounts_out)
        .bind(columns.fees)
        .bind(columns.protocol_fees)
        .bind(columns.host_fees)
        .bind(columns.start_bin_ids)
        .bind(columns.end_bin_ids)
        .bind(columns.swap2_event_payloads)
        .bind(columns.quote_asset_symbols)
        .bind(columns.quote_amounts)
        .bind(columns.quote_decimals)
        .bind(columns.fee_rates_1e9)
        .bind(source.as_str())
        .bind(columns.mm_fees)
        .bind(columns.limit_order_fees)
        .bind(columns.amounts_left)
        .bind(columns.fee_sides)
        .bind(columns.fee_tokens)
        .fetch_all(connection)
        .await?;
    rows.iter().map(inserted_swap_from_row).collect()
}

pub(super) fn inserted_swap_from_row(row: &PgRow) -> Result<InsertedSwap, StoreError> {
    let block_time: DateTime<Utc> = row.try_get("block_time")?;
    let direction: String = row.try_get("direction")?;
    let pool: String = row.try_get("pool")?;
    Ok(InsertedSwap {
        position: LogPosition {
            slot: slot_from_database(row.try_get("slot")?, "slot")?,
            transaction_index: transaction_index_from_database(row.try_get("transaction_index")?)?,
            swap_ordinal: swap_ordinal_from_database(row.try_get("swap_ordinal")?)?,
        },
        pool: address_from_database(&pool, "pool")?,
        block_time: unix_from_timestamp(block_time),
        direction: direction_from_database(&direction)?,
        amount_in: amount_from_database(row.try_get("amount_in")?, "amount_in")?,
        amount_out: amount_from_database(row.try_get("amount_out")?, "amount_out")?,
        volume_usd: row.try_get("volume_usd")?,
    })
}

// GREATEST so a replayed fill block can never move a job backwards.
async fn advance_job(
    connection: &mut PgConnection,
    job_id: JobId,
    next_slot_after: Slot,
) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE slot_range_job SET next_slot = GREATEST(next_slot, $2), updated_at = now()
         WHERE id = $1",
    )
    .bind(job_id.get())
    .bind(slot_to_database(next_slot_after, "next_slot")?)
    .execute(connection)
    .await?;
    Ok(())
}
