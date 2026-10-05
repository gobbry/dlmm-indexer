use rust_decimal::Decimal;

use super::{project, project_repriced};
use crate::domain::amounts::TokenAmountRaw;
use crate::domain::ids::{PoolAddress, Slot, SwapOrdinal, TransactionIndex, UnixSeconds};
use crate::domain::projection::{
    DayBucket, HourBucket, InsertedSwap, LogPosition, PoolStatsDelta, RepricedSwap, VolumeDelta,
};
use crate::domain::swap::SwapDirection;

const HOUR_START: i64 = 1_790_000_000 - 1_790_000_000 % 3_600;

fn pool(byte: u8) -> PoolAddress {
    PoolAddress::new([byte; 32])
}

fn position(ordinal: u16) -> LogPosition {
    LogPosition {
        slot: Slot::new(100),
        transaction_index: TransactionIndex::new(0),
        swap_ordinal: SwapOrdinal::new(ordinal),
    }
}

fn inserted(
    pool_byte: u8,
    seconds: i64,
    direction: SwapDirection,
    volume_usd: Option<Decimal>,
) -> InsertedSwap {
    InsertedSwap {
        position: position(0),
        pool: pool(pool_byte),
        block_time: UnixSeconds::new(seconds),
        direction,
        amount_in: TokenAmountRaw::new(1_000),
        amount_out: TokenAmountRaw::new(30),
        volume_usd,
    }
}

// The X and Y legs follow direction, and USD sums only priced swaps while counting the rest.
#[test]
fn project_merges_one_hour_of_one_pool() {
    let swaps = [
        inserted(
            1,
            HOUR_START + 10,
            SwapDirection::XToY,
            Some(Decimal::new(25, 1)),
        ),
        inserted(1, HOUR_START + 3_599, SwapDirection::YToX, None),
    ];

    let deltas = project(&swaps);

    let expected_volume = VolumeDelta {
        swap_count: 2,
        volume_x: 1_030,
        volume_y: 1_030,
        volume_usd: Decimal::new(25, 1),
        unpriced_swap_count: 1,
    };
    let hour = HourBucket::containing(UnixSeconds::new(HOUR_START));
    assert_eq!(
        deltas.pool_volume_1h,
        vec![((pool(1), hour), expected_volume)]
    );
    assert_eq!(
        deltas.pool_stats,
        vec![(
            pool(1),
            PoolStatsDelta {
                volume: expected_volume,
                first_swap_at: UnixSeconds::new(HOUR_START + 10),
                last_swap_at: UnixSeconds::new(HOUR_START + 3_599),
            }
        )]
    );
}

// Different hours and pools stay separate keys; stats span all hours of a pool.
#[test]
fn project_keys_by_pool_and_hour() {
    let swaps = [
        inserted(1, HOUR_START + 3_600, SwapDirection::XToY, None),
        inserted(1, HOUR_START, SwapDirection::XToY, None),
        inserted(2, HOUR_START, SwapDirection::XToY, None),
    ];

    let deltas = project(&swaps);

    let keys: Vec<_> = deltas.pool_volume_1h.iter().map(|(key, _)| *key).collect();
    let hour_first = HourBucket::containing(UnixSeconds::new(HOUR_START));
    let hour_second = HourBucket::containing(UnixSeconds::new(HOUR_START + 3_600));
    assert_eq!(
        keys,
        vec![
            (pool(1), hour_first),
            (pool(1), hour_second),
            (pool(2), hour_first)
        ]
    );
    let (_, stats_first_pool) = deltas.pool_stats[0];
    assert_eq!(stats_first_pool.volume.swap_count, 2);
    assert_eq!(stats_first_pool.first_swap_at, UnixSeconds::new(HOUR_START));
    assert_eq!(
        stats_first_pool.last_swap_at,
        UnixSeconds::new(HOUR_START + 3_600)
    );
}

// A reprice adds USD and moves swaps out of unpriced without counting them again.
#[test]
fn project_repriced_moves_usd_only() {
    let repriced = [
        RepricedSwap {
            position: position(0),
            pool: pool(1),
            block_time: UnixSeconds::new(HOUR_START + 5),
            volume_usd: Decimal::new(150, 2),
        },
        RepricedSwap {
            position: position(1),
            pool: pool(1),
            block_time: UnixSeconds::new(HOUR_START + 6),
            volume_usd: Decimal::new(50, 2),
        },
    ];

    let deltas = project_repriced(&repriced);

    let expected_volume = VolumeDelta {
        swap_count: 0,
        volume_x: 0,
        volume_y: 0,
        volume_usd: Decimal::new(200, 2),
        unpriced_swap_count: -2,
    };
    let hour = HourBucket::containing(UnixSeconds::new(HOUR_START));
    assert_eq!(
        deltas.pool_volume_1h,
        vec![((pool(1), hour), expected_volume)]
    );
    let (_, stats) = deltas.pool_stats[0];
    assert_eq!(stats.volume, expected_volume);
    let day = DayBucket::containing(UnixSeconds::new(HOUR_START));
    assert_eq!(
        deltas.pool_volume_1d,
        vec![((pool(1), day), expected_volume)]
    );
}

// Swaps on either side of UTC midnight land in two days; the hours of one day merge.
#[test]
fn project_keys_daily_volume_by_utc_day() {
    let midnight = HOUR_START - HOUR_START.rem_euclid(86_400) + 86_400;
    let swaps = [
        inserted(1, midnight - 7_200, SwapDirection::XToY, None),
        inserted(1, midnight - 1, SwapDirection::XToY, None),
        inserted(1, midnight, SwapDirection::XToY, None),
    ];

    let deltas = project(&swaps);

    let days: Vec<_> = deltas
        .pool_volume_1d
        .iter()
        .map(|((_, day), delta)| (day.start().get(), delta.swap_count))
        .collect();
    assert_eq!(days, vec![(midnight - 86_400, 2), (midnight, 1)]);
    assert_eq!(deltas.pool_volume_1h.len(), 3);
}
