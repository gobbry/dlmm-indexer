use rust_decimal::Decimal;
use sqlx::{PgConnection, PgPool};

use super::{
    RebuildStart, read_health, read_pool_metadata, read_pool_volume, read_pools,
    read_projection_states, read_recent_swaps, read_unpriced_minute_range, rebuild_projection,
    reprice_unpriced, write_block, write_prices,
};
use crate::domain::amounts::{
    FeeRate1e9, PriceUsd, QuoteAllowlist, QuoteAsset, QuoteLeg, TokenAmountRaw,
};
use crate::domain::block::{BlockOrigin, EnrichedBlock, LiveSource};
use crate::domain::error::DecodeError;
use crate::domain::ids::{
    BinId, JobId, MintAddress, MinuteRange, PoolAddress, Signature, Slot, SwapOrdinal,
    TransactionIndex, UnixSeconds, UserAddress,
};
use crate::domain::price::{PricePoint, PriceSource, UnpriceableMinute};
use crate::domain::projection::{ProjectionName, ProjectionState};
use crate::domain::query::{AlignedRange, Bucket, ProjectionRead, RowCountMax};
use crate::domain::registry::PoolRecord;
use crate::domain::swap::{
    DecodeFailure, DecodedSwap, EnrichedSwap, FeeSide, FeeToken, Swap2Event, SwapDirection,
    SwapEvent,
};

// 2026-10-01T12:00:30Z; its price is the close of the 11:59 candle, observed at 12:00:00.
const BLOCK_TIME: i64 = 1_790_856_030;
const PRICE_TS: i64 = BLOCK_TIME - 30;
const SWAP_MINUTE: i64 = BLOCK_TIME - 30;
const HOUR_START: i64 = BLOCK_TIME - 30;
const DAY_START: i64 = BLOCK_TIME - 43_230;
const BLOCK_SLOT: u64 = 1_000;
const LIVE: BlockOrigin = BlockOrigin::Live(LiveSource::RpcTail);

fn sol_mint() -> MintAddress {
    QuoteAllowlist::mainnet().sol()
}

fn priced_pool() -> PoolRecord {
    PoolRecord {
        address: PoolAddress::new([11; 32]),
        mint_x: sol_mint(),
        mint_y: MintAddress::new([21; 32]),
        first_seen_slot: Slot::new(BLOCK_SLOT),
    }
}

fn unpriceable_pool() -> PoolRecord {
    PoolRecord {
        address: PoolAddress::new([12; 32]),
        mint_x: MintAddress::new([22; 32]),
        mint_y: MintAddress::new([23; 32]),
        first_seen_slot: Slot::new(BLOCK_SLOT),
    }
}

fn swap(
    pool: PoolRecord,
    signature_byte: u8,
    direction: SwapDirection,
    amount_in: u64,
    amount_out: u64,
) -> EnrichedSwap {
    let (mint_in, mint_out) = match direction {
        SwapDirection::XToY => (pool.mint_x, pool.mint_y),
        SwapDirection::YToX => (pool.mint_y, pool.mint_x),
    };
    let quote_amount = if mint_in == sol_mint() {
        Some(amount_in)
    } else if mint_out == sol_mint() {
        Some(amount_out)
    } else {
        None
    };
    let quote_leg = quote_amount.map(|amount| QuoteLeg {
        asset: QuoteAsset::Sol,
        amount: TokenAmountRaw::new(amount),
        decimals: QuoteAsset::Sol.decimals(),
    });
    let event = SwapEvent {
        lb_pair: pool.address,
        from: UserAddress::new([31; 32]),
        start_bin_id: BinId::new(-5),
        end_bin_id: BinId::new(-3),
        amount_in: TokenAmountRaw::new(amount_in),
        amount_out: TokenAmountRaw::new(amount_out),
        direction,
        fee: TokenAmountRaw::new(17),
        protocol_fee: TokenAmountRaw::new(3),
        fee_rate_1e9: FeeRate1e9::new(25),
        host_fee: TokenAmountRaw::new(0),
    };
    let decoded = DecodedSwap {
        signature: Signature::new([signature_byte; 64]),
        transaction_index: TransactionIndex::new(u16::from(signature_byte)),
        ordinal: SwapOrdinal::new(0),
        pool: pool.address,
        mint_x: pool.mint_x,
        mint_y: pool.mint_y,
        user: UserAddress::new([31; 32]),
        event,
        // fee 17 = mm_fee 14 + protocol_fee 3, as on chain.
        event2: Some(Swap2Event {
            amount_left: TokenAmountRaw::new(0),
            mm_fee: TokenAmountRaw::new(14),
            limit_order_fee: TokenAmountRaw::new(0),
            fee_side: FeeSide::Input,
            fee_token: FeeToken::X,
        }),
        swap2_event_payload: Some(vec![1, 2, 3]),
    };
    EnrichedSwap {
        decoded,
        mint_in,
        mint_out,
        quote_leg,
    }
}

fn block(slot: u64, parent_slot: u64, block_time: i64, swaps: Vec<EnrichedSwap>) -> EnrichedBlock {
    EnrichedBlock {
        slot: Slot::new(slot),
        parent_slot: Slot::new(parent_slot),
        block_time: UnixSeconds::new(block_time),
        swaps,
        failures: Vec::new(),
        new_pools: vec![priced_pool(), unpriceable_pool()],
        unknown_mints: vec![MintAddress::new([21; 32]), MintAddress::new([22; 32])],
    }
}

// Two SOL in at 150.5 USD is 301 USD; the second swap has no quote asset.
fn two_swap_block() -> EnrichedBlock {
    let mut block = block(
        BLOCK_SLOT,
        BLOCK_SLOT - 1,
        BLOCK_TIME,
        vec![
            swap(
                priced_pool(),
                1,
                SwapDirection::XToY,
                2_000_000_000,
                300_000_000,
            ),
            swap(unpriceable_pool(), 2, SwapDirection::YToX, 5_000, 7_000),
        ],
    );
    block.failures = vec![DecodeFailure {
        signature: Signature::new([3; 64]),
        reason: DecodeError::OrphanSwapEvent,
    }];
    block
}

fn sol_price(ts: i64) -> PricePoint {
    sol_price_from(ts, "150.5", PriceSource::Binance)
}

fn sol_price_from(ts: i64, close: &str, source: PriceSource) -> PricePoint {
    PricePoint {
        asset: QuoteAsset::Sol,
        ts: UnixSeconds::new(ts),
        close: PriceUsd::new(close.parse().expect("close")),
        source,
    }
}

async fn seed_job(database: &PgPool, start: i64, end: i64, blocked: Option<&str>) -> JobId {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, blocked_reason)
         VALUES ($1, $2, $1, $3) RETURNING id",
    )
    .bind(start)
    .bind(end)
    .bind(blocked)
    .fetch_one(database)
    .await
    .expect("seed job");
    JobId::new(id)
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct Snapshot {
    swap_count: i64,
    amount_in_sum: Option<Decimal>,
    volume_usd_sum: Option<Decimal>,
    price_ts: String,
    pool_volume_1h: String,
    pool_volume_1d: String,
    pool_stats: String,
    // Every swap column, so a replay that rewrote fill_job_id or a price would show.
    swap_rows: String,
    pool_rows: String,
    token_rows: String,
    decode_failure_count: i64,
    coverage_rows: String,
}

async fn snapshot(database: &PgPool) -> Snapshot {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM swap) AS swap_count,
                (SELECT sum(amount_in) FROM swap) AS amount_in_sum,
                (SELECT sum(volume_usd) FROM swap) AS volume_usd_sum,
                (SELECT coalesce(json_agg(price_ts ORDER BY transaction_index)::text, '')
                 FROM swap) AS price_ts,
                (SELECT coalesce(json_agg(v ORDER BY v.pool, v.bucket)::text, '')
                 FROM pool_volume_1h v) AS pool_volume_1h,
                (SELECT coalesce(json_agg(v ORDER BY v.pool, v.bucket)::text, '')
                 FROM pool_volume_1d v) AS pool_volume_1d,
                (SELECT coalesce(json_agg(s ORDER BY s.pool)::text, '')
                 FROM pool_stats s) AS pool_stats,
                (SELECT coalesce(json_agg(w ORDER BY w.transaction_index)::text, '')
                 FROM swap w) AS swap_rows,
                (SELECT coalesce(json_agg(json_build_array(address, mint_x, mint_y,
                                                           first_seen_slot)
                                          ORDER BY address)::text, '')
                 FROM pool) AS pool_rows,
                (SELECT coalesce(json_agg(k ORDER BY k.mint)::text, '')
                 FROM token k) AS token_rows,
                (SELECT count(*) FROM decode_failure) AS decode_failure_count,
                (SELECT coalesce(json_agg(c ORDER BY c.start_slot)::text, '')
                 FROM slot_coverage c) AS coverage_rows",
    )
    .fetch_one(database)
    .await
    .expect("snapshot")
}

async fn job_next_slot(database: &PgPool, job_id: JobId) -> i64 {
    sqlx::query_scalar("SELECT next_slot FROM slot_range_job WHERE id = $1")
        .bind(job_id.get())
        .fetch_one(database)
        .await
        .expect("job next_slot")
}

async fn write(
    connection: &mut PgConnection,
    block: EnrichedBlock,
    origin: BlockOrigin,
) -> (u32, u32) {
    let stored = write_block(connection, block, origin, PriceSource::Binance)
        .await
        .expect("write_block");
    (stored.inserted_swap_count, stored.duplicate_swap_count)
}

// Ingesting the same block live twice and once more through a fill changes nothing.
#[sqlx::test(migrations = "../migrations")]
async fn write_block_is_idempotent_across_live_and_fill(database: PgPool) {
    write_prices(&database, &[sol_price(PRICE_TS)])
        .await
        .expect("price");
    let job_id = seed_job(&database, 990, 1_000, None).await;
    let mut connection = database.acquire().await.expect("connection");

    let first = write(&mut connection, two_swap_block(), LIVE).await;
    assert_eq!(first, (2, 0));
    let expected = snapshot(&database).await;
    assert_eq!(expected.swap_count, 2);
    assert_eq!(
        expected.amount_in_sum,
        Some(Decimal::from(2_000_005_000_u64))
    );
    assert_eq!(expected.volume_usd_sum, Some(Decimal::from(301)));
    assert_eq!(expected.price_ts, r#"["2026-10-01T12:00:00+00:00", null]"#);
    assert_eq!(expected.decode_failure_count, 1);
    assert_eq!(
        expected.coverage_rows,
        r#"[{"start_slot":1000,"end_slot":1000,"end_block_time":"2026-10-01T12:00:30+00:00"}]"#
    );
    // The snapshot only proves "unchanged"; these pin it to one count per swap.
    let stats: (Decimal, Decimal, Decimal) = sqlx::query_as(
        "SELECT sum(swap_count)::numeric, sum(volume_usd), sum(unpriced_swap_count)::numeric
         FROM pool_stats",
    )
    .fetch_one(&database)
    .await
    .expect("pool_stats totals");
    assert_eq!(
        stats,
        (Decimal::from(2), Decimal::from(301), Decimal::from(1))
    );
    let daily: (Decimal, Decimal, Decimal) = sqlx::query_as(
        "SELECT sum(swap_count)::numeric, sum(volume_usd), sum(unpriced_swap_count)::numeric
         FROM pool_volume_1d",
    )
    .fetch_one(&database)
    .await
    .expect("pool_volume_1d totals");
    assert_eq!(daily, stats);
    let fee_split: Vec<(String, Decimal, String, String)> = sqlx::query_as(
        "SELECT source::text, mm_fee, fee_side::text, fee_token::text FROM swap
         ORDER BY transaction_index",
    )
    .fetch_all(&database)
    .await
    .expect("fee split");
    let live_row = (
        "live_rpc".to_owned(),
        Decimal::from(14),
        "input".to_owned(),
        "x".to_owned(),
    );
    assert_eq!(fee_split, vec![live_row.clone(), live_row]);

    let second = write(&mut connection, two_swap_block(), LIVE).await;
    assert_eq!(second, (0, 2));
    assert_eq!(snapshot(&database).await, expected);

    let fill = BlockOrigin::Fill {
        job_id,
        next_slot_after: Slot::new(BLOCK_SLOT + 1),
    };
    let third = write(&mut connection, two_swap_block(), fill).await;
    assert_eq!(third, (0, 2));
    assert_eq!(snapshot(&database).await, expected);
    assert_eq!(job_next_slot(&database, job_id).await, 1_001);
}

// A price arriving after the swap prices it exactly once and moves the projections with it.
#[sqlx::test(migrations = "../migrations")]
async fn reprice_prices_late_swaps_exactly_once(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let unpriced = snapshot(&database).await;
    assert_eq!(unpriced.volume_usd_sum, None);
    let range = read_unpriced_minute_range(&database, &[])
        .await
        .expect("range")
        .expect("unpriced swaps");
    assert_eq!(
        range,
        MinuteRange {
            start: UnixSeconds::new(SWAP_MINUTE),
            end_inclusive: UnixSeconds::new(SWAP_MINUTE),
        }
    );

    write_prices(&database, &[sol_price(PRICE_TS)])
        .await
        .expect("price");
    let repriced = reprice_unpriced(&mut connection, range, PriceSource::Binance)
        .await
        .expect("reprice");
    assert_eq!(repriced.len(), 1);
    assert_eq!(repriced[0].pool, priced_pool().address);
    assert_eq!(repriced[0].volume_usd, Decimal::from(301));

    let hourly: (i64, Decimal, i64) = sqlx::query_as(
        "SELECT swap_count, volume_usd, unpriced_swap_count FROM pool_volume_1h WHERE pool = $1",
    )
    .bind(priced_pool().address.to_string())
    .fetch_one(&database)
    .await
    .expect("hourly row");
    assert_eq!(hourly, (1, Decimal::from(301), 0));
    let daily: (i64, Decimal, i64) = sqlx::query_as(
        "SELECT swap_count, volume_usd, unpriced_swap_count FROM pool_volume_1d WHERE pool = $1",
    )
    .bind(priced_pool().address.to_string())
    .fetch_one(&database)
    .await
    .expect("daily row");
    assert_eq!(daily, hourly);
    let repriced_snapshot = snapshot(&database).await;
    assert_eq!(repriced_snapshot.volume_usd_sum, Some(Decimal::from(301)));
    assert_eq!(
        read_unpriced_minute_range(&database, &[])
            .await
            .expect("range"),
        None
    );

    let again = reprice_unpriced(&mut connection, range, PriceSource::Binance)
        .await
        .expect("reprice");
    assert!(again.is_empty());
    assert_eq!(snapshot(&database).await, repriced_snapshot);
}

async fn priced_swap_row(database: &PgPool) -> (Option<Decimal>, Option<String>) {
    sqlx::query_as(
        "SELECT volume_usd, to_char(price_ts AT TIME ZONE 'UTC', 'HH24:MI:SS') FROM swap
         WHERE quote_asset_symbol IS NOT NULL",
    )
    .fetch_one(database)
    .await
    .expect("priced swap")
}

// A swap takes the latest price at or before its block time: a newer row still in its past
// wins, while a row 61 s old and a row 1 s in its future are never used.
#[sqlx::test(migrations = "../migrations")]
async fn swap_price_is_latest_at_or_before_within_sixty_seconds(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    let stale_only = [sol_price_from(BLOCK_TIME - 61, "100", PriceSource::Binance)];
    let future_only = [sol_price_from(BLOCK_TIME + 1, "200", PriceSource::Binance)];
    write_prices(&database, &stale_only).await.expect("stale");
    write_prices(&database, &future_only).await.expect("future");
    write(&mut connection, two_swap_block(), LIVE).await;
    assert_eq!(priced_swap_row(&database).await, (None, None));

    let candidates = [
        sol_price_from(BLOCK_TIME - 59, "140", PriceSource::Binance),
        sol_price_from(BLOCK_TIME, "150.5", PriceSource::CarriedForward),
    ];
    write_prices(&database, &candidates)
        .await
        .expect("candidates");
    let range = read_unpriced_minute_range(&database, &[])
        .await
        .expect("range")
        .expect("unpriced swap");
    let repriced = reprice_unpriced(&mut connection, range, PriceSource::Binance)
        .await
        .expect("reprice");
    assert_eq!(repriced.len(), 1);
    assert_eq!(
        priced_swap_row(&database).await,
        (Some(Decimal::from(301)), Some("12:00:30".to_owned()))
    );

    // The insert path applies the same rule: a fresh block at the same instant agrees.
    let mut later = two_swap_block();
    later.slot = Slot::new(BLOCK_SLOT + 1);
    later.parent_slot = Slot::new(BLOCK_SLOT);
    later.swaps.truncate(1);
    later.swaps[0].decoded.signature = Signature::new([9; 64]);
    write(&mut connection, later, LIVE).await;
    let inserted: Decimal = sqlx::query_scalar("SELECT volume_usd FROM swap WHERE slot = $1")
        .bind(BLOCK_SLOT as i64 + 1)
        .fetch_one(&database)
        .await
        .expect("inserted swap");
    assert_eq!(inserted, Decimal::from(301));
}

// Only eligible sources price a swap: with peg as the market, a binance row is ignored.
#[sqlx::test(migrations = "../migrations")]
async fn swap_price_ignores_a_source_that_is_not_eligible(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    write_prices(&database, &[sol_price(PRICE_TS)])
        .await
        .expect("price");
    let range = read_unpriced_minute_range(&database, &[])
        .await
        .expect("range")
        .expect("unpriced swap");

    let ineligible = reprice_unpriced(&mut connection, range, PriceSource::Peg)
        .await
        .expect("reprice");
    assert!(ineligible.is_empty());
    assert_eq!(priced_swap_row(&database).await, (None, None));

    let eligible = reprice_unpriced(&mut connection, range, PriceSource::Binance)
        .await
        .expect("reprice");
    assert_eq!(eligible.len(), 1);
    assert_eq!(
        priced_swap_row(&database).await,
        (Some(Decimal::from(301)), Some("12:00:00".to_owned()))
    );
}

// A minute a sweep found unpriceable no longer opens the unpriced span, for its own asset
// only; the swap itself stays unpriced.
#[sqlx::test(migrations = "../migrations")]
async fn unpriced_span_leaves_out_minutes_found_unpriceable(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let unpriceable = |asset| UnpriceableMinute {
        minute: UnixSeconds::new(SWAP_MINUTE),
        asset,
    };
    let swap_minute = Some(MinuteRange {
        start: UnixSeconds::new(SWAP_MINUTE),
        end_inclusive: UnixSeconds::new(SWAP_MINUTE),
    });
    let other_asset = [unpriceable(QuoteAsset::Usdc)];
    assert_eq!(
        read_unpriced_minute_range(&database, &other_asset)
            .await
            .expect("range"),
        swap_minute
    );
    let own_asset = [unpriceable(QuoteAsset::Sol)];
    assert_eq!(
        read_unpriced_minute_range(&database, &own_asset)
            .await
            .expect("range"),
        None
    );
}

// The asset_symbol domain is the schema's only check on a ticker, so every variant of the Rust
// allowlist must satisfy it and be stored as its upper-case ticker; a variant that did not
// would fail every price write for the whole batch.
#[sqlx::test(migrations = "../migrations")]
async fn every_quote_asset_is_stored_as_its_ticker(database: PgPool) {
    let points: Vec<PricePoint> = QuoteAsset::ALL
        .iter()
        .map(|asset| PricePoint {
            asset: *asset,
            ts: UnixSeconds::new(PRICE_TS),
            close: PriceUsd::new(Decimal::ONE),
            source: PriceSource::Binance,
        })
        .collect();
    write_prices(&database, &points).await.expect("prices");

    let stored: Vec<String> =
        sqlx::query_scalar("SELECT asset_symbol FROM price ORDER BY asset_symbol")
            .fetch_all(&database)
            .await
            .expect("stored symbols");
    assert_eq!(stored, ["SOL", "USDC", "USDT"]);
}

// A tie on ts goes to the configured market by name, not by the enum's order: with
// carried_forward standing in as a market declared after peg, its row still wins the tie.
#[sqlx::test(migrations = "../migrations")]
async fn swap_price_tie_on_ts_goes_to_the_market_source(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let tied = [
        sol_price_from(PRICE_TS, "100", PriceSource::Peg),
        sol_price_from(PRICE_TS, "150.5", PriceSource::CarriedForward),
    ];
    write_prices(&database, &tied).await.expect("prices");
    let range = read_unpriced_minute_range(&database, &[])
        .await
        .expect("range")
        .expect("unpriced swap");
    reprice_unpriced(&mut connection, range, PriceSource::CarriedForward)
        .await
        .expect("reprice");
    assert_eq!(
        priced_swap_row(&database).await,
        (Some(Decimal::from(301)), Some("12:00:00".to_owned()))
    );
}

fn live<T>(read: ProjectionRead<T>) -> T {
    match read {
        ProjectionRead::Live(rows) => rows,
        ProjectionRead::Rebuilding(name) => panic!("{name} is rebuilding"),
    }
}

async fn set_projection_state(database: &PgPool, name: ProjectionName, state: ProjectionState) {
    sqlx::query("UPDATE projection SET state = $2::projection_state WHERE name = $1")
        .bind(name.table_name())
        .bind(state.as_str())
        .execute(database)
        .await
        .expect("projection state");
}

// Every projection read reports a table a rebuild owns instead of its partial rows, and only
// for the tables it reads: the hourly series is still served while the daily one rebuilds, and
// the pool's metadata reads a rebuilding pool_stats as an unknown first swap.
#[sqlx::test(migrations = "../migrations")]
async fn projection_reads_report_rebuilding_tables(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let pool = priced_pool().address;
    set_projection_state(
        &database,
        ProjectionName::PoolVolume1d,
        ProjectionState::Building,
    )
    .await;
    set_projection_state(
        &database,
        ProjectionName::PoolStats,
        ProjectionState::Building,
    )
    .await;
    let range = |bucket, to_exclusive| AlignedRange {
        from: UnixSeconds::new(DAY_START),
        to_exclusive: UnixSeconds::new(to_exclusive),
        bucket,
    };

    let daily = read_pool_volume(&database, pool, range(Bucket::Day, DAY_START + 86_400))
        .await
        .expect("daily");
    assert_eq!(
        daily,
        ProjectionRead::Rebuilding(ProjectionName::PoolVolume1d)
    );
    let hourly = read_pool_volume(&database, pool, range(Bucket::Hour, DAY_START + 3_600))
        .await
        .expect("hourly");
    assert!(matches!(hourly, ProjectionRead::Live(_)), "{hourly:?}");
    let metadata = read_pool_metadata(&database, pool)
        .await
        .expect("pool")
        .expect("known pool");
    assert_eq!(metadata.first_swap_at, None);
    let pools = read_pools(&database, RowCountMax::new(10))
        .await
        .expect("pools");
    assert_eq!(pools, ProjectionRead::Rebuilding(ProjectionName::PoolStats));
    let health = read_health(&database).await.expect("health");
    assert_eq!(
        health.rebuilding_projections,
        vec![ProjectionName::PoolStats, ProjectionName::PoolVolume1d]
    );
}

// Empty hours come back as explicit zero buckets, and a day sums its hours.
#[sqlx::test(migrations = "../migrations")]
async fn read_pool_volume_fills_empty_buckets_and_sums_days(database: PgPool) {
    write_prices(&database, &[sol_price(PRICE_TS)])
        .await
        .expect("price");
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let later_swap = swap(
        priced_pool(),
        4,
        SwapDirection::YToX,
        600_000_000,
        1_000_000_000,
    );
    let later = block(
        BLOCK_SLOT + 10,
        BLOCK_SLOT,
        BLOCK_TIME + 3_600,
        vec![later_swap],
    );
    write(&mut connection, later, LIVE).await;
    let pool = priced_pool().address;

    let hourly = read_pool_volume(
        &database,
        pool,
        AlignedRange {
            from: UnixSeconds::new(HOUR_START - 3_600),
            to_exclusive: UnixSeconds::new(HOUR_START + 7_200),
            bucket: Bucket::Hour,
        },
    )
    .await
    .map(live)
    .expect("hourly");
    assert_eq!(hourly.len(), 3);
    assert_eq!(hourly[0].start, UnixSeconds::new(HOUR_START - 3_600));
    assert_eq!(
        (
            hourly[0].swap_count,
            hourly[0].volume_x,
            hourly[0].volume_usd
        ),
        (0, Decimal::ZERO, None)
    );
    assert_eq!(hourly[1].volume_usd, Some(Decimal::from(301)));
    assert_eq!(
        (hourly[2].swap_count, hourly[2].unpriced_swap_count),
        (1, 1)
    );
    assert_eq!(hourly[2].volume_usd, None);

    let daily = read_pool_volume(
        &database,
        pool,
        AlignedRange {
            from: UnixSeconds::new(DAY_START),
            to_exclusive: UnixSeconds::new(DAY_START + 86_400),
            bucket: Bucket::Day,
        },
    )
    .await
    .map(live)
    .expect("daily");
    assert_eq!(daily.len(), 1);
    assert_eq!(daily[0].start, UnixSeconds::new(DAY_START));
    assert_eq!((daily[0].swap_count, daily[0].unpriced_swap_count), (2, 1));
    assert_eq!(daily[0].volume_x, Decimal::from(3_000_000_000_u64));
    assert_eq!(daily[0].volume_y, Decimal::from(900_000_000_u64));
    assert_eq!(daily[0].volume_usd, Some(Decimal::from(301)));
}

// Recent swaps come newest first; the pool's metadata carries its first swap, and its
// summary in the pool list carries the last.
#[sqlx::test(migrations = "../migrations")]
async fn read_recent_swaps_and_pool_summary(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let later_swap = swap(priced_pool(), 4, SwapDirection::YToX, 600, 1_000);
    let later = block(
        BLOCK_SLOT + 10,
        BLOCK_SLOT,
        BLOCK_TIME + 60,
        vec![later_swap],
    );
    write(&mut connection, later, LIVE).await;
    let pool = priced_pool().address;

    let swaps = read_recent_swaps(&database, pool, RowCountMax::new(10))
        .await
        .expect("swaps");
    let signatures: Vec<Signature> = swaps.iter().map(|row| row.signature).collect();
    assert_eq!(
        signatures,
        vec![Signature::new([4; 64]), Signature::new([1; 64])]
    );
    assert_eq!(swaps[1].amount_in, TokenAmountRaw::new(2_000_000_000));
    assert_eq!(swaps[1].quote_asset, Some(QuoteAsset::Sol));

    let metadata = read_pool_metadata(&database, pool)
        .await
        .expect("pool")
        .expect("known pool");
    assert_eq!(metadata.mint_x, sol_mint());
    assert_eq!(metadata.first_swap_at, Some(UnixSeconds::new(BLOCK_TIME)));
    let pools = read_pools(&database, RowCountMax::new(10))
        .await
        .map(live)
        .expect("pools");
    let summary = pools
        .iter()
        .find(|summary| summary.address == pool)
        .expect("listed pool");
    assert_eq!(
        summary.last_swap_at,
        Some(UnixSeconds::new(BLOCK_TIME + 60))
    );
    let unknown = read_pool_metadata(&database, PoolAddress::new([99; 32]))
        .await
        .expect("pool");
    assert_eq!(unknown, None);
}

// Health reports the top coverage range's end and open versus blocked jobs; a job whose walk
// is done or that the reconciler completed counts as neither.
#[sqlx::test(migrations = "../migrations")]
async fn read_health_reflects_coverage_and_jobs(database: PgPool) {
    let empty = read_health(&database).await.expect("health");
    assert_eq!((empty.cursor_slot, empty.last_block_time), (None, None));

    seed_job(&database, 10, 20, None).await;
    seed_job(&database, 30, 40, Some("missing_in_storage:31")).await;
    let walked = seed_job(&database, 50, 60, None).await;
    sqlx::query("UPDATE slot_range_job SET next_slot = 61 WHERE id = $1")
        .bind(walked.get())
        .execute(&database)
        .await
        .expect("walk job");
    let completed = seed_job(&database, 70, 80, Some("unmappable:75")).await;
    sqlx::query("UPDATE slot_range_job SET completed_at = now() WHERE id = $1")
        .bind(completed.get())
        .execute(&database)
        .await
        .expect("complete job");
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let older = block(BLOCK_SLOT - 10, BLOCK_SLOT - 11, BLOCK_TIME - 4, Vec::new());
    write(&mut connection, older, LIVE).await;

    let health = read_health(&database).await.expect("health");
    assert_eq!(health.cursor_slot, Some(Slot::new(BLOCK_SLOT)));
    assert_eq!(health.last_block_time, Some(UnixSeconds::new(BLOCK_TIME)));
    assert_eq!((health.open_job_count, health.blocked_job_count), (1, 1));
}

// A row claiming the fill source must name its job, and a fill row must claim the fill source.
#[sqlx::test(migrations = "../migrations")]
async fn swap_source_and_fill_job_must_agree(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let fill_without_job = sqlx::query("UPDATE swap SET source = 'fill'")
        .execute(&database)
        .await;
    let job_id = seed_job(&database, 990, 1_000, None).await;
    let live_with_job = sqlx::query("UPDATE swap SET fill_job_id = $1")
        .bind(job_id.get())
        .execute(&database)
        .await;
    for result in [fill_without_job, live_with_job] {
        let error = result.expect_err("the CHECK on source rejects it");
        let code = error.as_database_error().and_then(|error| error.code());
        assert_eq!(code.as_deref(), Some("23514"));
    }
}

async fn table_rows(database: &PgPool, statement: &'static str) -> String {
    sqlx::query_scalar(statement)
        .fetch_one(database)
        .await
        .expect("table rows")
}

const POOL_VOLUME_1D_ROWS_SQL: &str =
    "SELECT coalesce(json_agg(v ORDER BY v.pool, v.bucket)::text, '') FROM pool_volume_1d v";
const POOL_VOLUME_1H_ROWS_SQL: &str =
    "SELECT coalesce(json_agg(v ORDER BY v.pool, v.bucket)::text, '') FROM pool_volume_1h v";
const POOL_STATS_ROWS_SQL: &str =
    "SELECT coalesce(json_agg(s ORDER BY s.pool)::text, '') FROM pool_stats s";

// Two blocks a day apart, so the daily projection has a row per pool per day.
async fn write_two_days(database: &PgPool) {
    write_prices(database, &[sol_price(PRICE_TS)])
        .await
        .expect("price");
    let mut connection = database.acquire().await.expect("connection");
    write(&mut connection, two_swap_block(), LIVE).await;
    let next_day = block(
        BLOCK_SLOT + 1,
        BLOCK_SLOT,
        BLOCK_TIME + 86_400,
        vec![
            swap(priced_pool(), 4, SwapDirection::YToX, 600, 1_000),
            swap(unpriceable_pool(), 5, SwapDirection::XToY, 70, 90),
        ],
    );
    write(&mut connection, next_day, LIVE).await;
}

// Replaying the log into pool_volume_1d reproduces the live rows exactly, leaves the other
// projections alone, and marks the projection live again.
#[sqlx::test(migrations = "../migrations")]
async fn rebuild_projection_reproduces_live_rows(database: PgPool) {
    write_two_days(&database).await;
    let live_daily = table_rows(&database, POOL_VOLUME_1D_ROWS_SQL).await;
    let live_hourly = table_rows(&database, POOL_VOLUME_1H_ROWS_SQL).await;
    let live_stats = table_rows(&database, POOL_STATS_ROWS_SQL).await;
    assert_eq!(live_daily.matches("\"bucket\"").count(), 4);
    // Drifted rows, so a rebuild that left the table alone would not reproduce the live rows.
    sqlx::query("UPDATE pool_volume_1d SET swap_count = swap_count + 100")
        .execute(&database)
        .await
        .expect("drift the daily rows");

    let summary = rebuild_projection(&database, ProjectionName::PoolVolume1d)
        .await
        .expect("rebuild");
    assert_eq!(
        (summary.start, summary.row_count, summary.page_count),
        (RebuildStart::Fresh, 4, 1)
    );
    assert_eq!(
        table_rows(&database, POOL_VOLUME_1D_ROWS_SQL).await,
        live_daily
    );
    assert_eq!(
        table_rows(&database, POOL_VOLUME_1H_ROWS_SQL).await,
        live_hourly
    );
    assert_eq!(table_rows(&database, POOL_STATS_ROWS_SQL).await, live_stats);
    let states = read_projection_states(&database).await.expect("states");
    assert!(
        states
            .iter()
            .all(|(_, state)| *state == ProjectionState::Live)
    );
}

// A rebuild killed after its first day's page resumes from the stored cursor instead of
// truncating, and adds only what follows the cursor.
#[sqlx::test(migrations = "../migrations")]
async fn rebuild_projection_resumes_from_cursor(database: PgPool) {
    write_two_days(&database).await;
    let live_daily = table_rows(&database, POOL_VOLUME_1D_ROWS_SQL).await;
    // The state a kill leaves: building, cursor on the first block's last swap, its rows only.
    sqlx::query(
        "UPDATE projection SET state = 'building', cursor_slot = $1,
                cursor_transaction_index = 2, cursor_swap_ordinal = 0
         WHERE name = 'pool_volume_1d'",
    )
    .bind(BLOCK_SLOT as i64)
    .execute(&database)
    .await
    .expect("interrupted state");
    sqlx::query("DELETE FROM pool_volume_1d WHERE bucket > to_timestamp($1)")
        .bind(DAY_START as f64)
        .execute(&database)
        .await
        .expect("drop the second day");
    let states = read_projection_states(&database).await.expect("states");
    assert!(states.contains(&(ProjectionName::PoolVolume1d, ProjectionState::Building)));

    let summary = rebuild_projection(&database, ProjectionName::PoolVolume1d)
        .await
        .expect("rebuild");
    assert_eq!(
        (summary.start, summary.row_count),
        (RebuildStart::Resumed, 2)
    );
    assert_eq!(
        table_rows(&database, POOL_VOLUME_1D_ROWS_SQL).await,
        live_daily
    );
}
