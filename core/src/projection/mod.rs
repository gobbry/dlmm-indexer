use std::collections::BTreeMap;

use crate::domain::amounts::TokenAmountRaw;
use crate::domain::ids::{PoolAddress, UnixSeconds};
use crate::domain::projection::{
    DayBucket, HourBucket, InsertedSwap, PoolStatsDelta, ProjectionDeltas, RepricedSwap,
    VolumeDelta,
};
use crate::domain::swap::SwapDirection;

#[cfg(test)]
mod tests;

// BTreeMap keeps the emitted deltas in key order, so the upserts lock rows in a stable order.
#[derive(Default)]
struct DeltaFold {
    pool_volume_1h: BTreeMap<(PoolAddress, HourBucket), VolumeDelta>,
    pool_volume_1d: BTreeMap<(PoolAddress, DayBucket), VolumeDelta>,
    pool_stats: BTreeMap<PoolAddress, PoolStatsDelta>,
}

impl DeltaFold {
    fn add(&mut self, pool: PoolAddress, block_time: UnixSeconds, delta: VolumeDelta) {
        let hour_key = (pool, HourBucket::containing(block_time));
        let hourly = self.pool_volume_1h.entry(hour_key).or_default();
        *hourly = volume_delta_merge(*hourly, delta);

        let day_key = (pool, DayBucket::containing(block_time));
        let daily = self.pool_volume_1d.entry(day_key).or_default();
        *daily = volume_delta_merge(*daily, delta);

        let stats = self.pool_stats.entry(pool).or_insert(PoolStatsDelta {
            volume: VolumeDelta::default(),
            first_swap_at: block_time,
            last_swap_at: block_time,
        });
        stats.volume = volume_delta_merge(stats.volume, delta);
        stats.first_swap_at = stats.first_swap_at.min(block_time);
        stats.last_swap_at = stats.last_swap_at.max(block_time);
        debug_assert!(stats.first_swap_at <= stats.last_swap_at);
    }

    fn finish(self) -> ProjectionDeltas {
        ProjectionDeltas {
            pool_volume_1h: self.pool_volume_1h.into_iter().collect(),
            pool_volume_1d: self.pool_volume_1d.into_iter().collect(),
            pool_stats: self.pool_stats.into_iter().collect(),
        }
    }
}

pub fn project(inserted: &[InsertedSwap]) -> ProjectionDeltas {
    let mut fold = DeltaFold::default();
    for swap in inserted {
        fold.add(swap.pool, swap.block_time, inserted_swap_delta(swap));
    }
    let deltas = fold.finish();
    let swap_count: i64 = deltas
        .pool_volume_1h
        .iter()
        .map(|(_, delta)| delta.swap_count)
        .sum();
    debug_assert_eq!(Ok(swap_count), i64::try_from(inserted.len()));
    deltas
}

// A reprice only moves USD: the swap was already counted, just as unpriced.
pub fn project_repriced(repriced: &[RepricedSwap]) -> ProjectionDeltas {
    let mut fold = DeltaFold::default();
    for swap in repriced {
        let delta = VolumeDelta {
            volume_usd: swap.volume_usd,
            unpriced_swap_count: -1,
            ..VolumeDelta::default()
        };
        fold.add(swap.pool, swap.block_time, delta);
    }
    let deltas = fold.finish();
    debug_assert!(
        deltas
            .pool_volume_1h
            .iter()
            .all(|(_, delta)| delta.swap_count == 0)
    );
    deltas
}

fn inserted_swap_delta(swap: &InsertedSwap) -> VolumeDelta {
    let (volume_x, volume_y) = legs_x_y(swap.direction, swap.amount_in, swap.amount_out);
    match swap.volume_usd {
        Some(volume_usd) => VolumeDelta {
            swap_count: 1,
            volume_x,
            volume_y,
            volume_usd,
            unpriced_swap_count: 0,
        },
        None => VolumeDelta {
            swap_count: 1,
            volume_x,
            volume_y,
            volume_usd: Default::default(),
            unpriced_swap_count: 1,
        },
    }
}

fn legs_x_y(
    direction: SwapDirection,
    amount_in: TokenAmountRaw,
    amount_out: TokenAmountRaw,
) -> (u128, u128) {
    let amount_in = u128::from(amount_in.get());
    let amount_out = u128::from(amount_out.get());
    match direction {
        SwapDirection::XToY => (amount_in, amount_out),
        SwapDirection::YToX => (amount_out, amount_in),
    }
}

fn volume_delta_merge(left: VolumeDelta, right: VolumeDelta) -> VolumeDelta {
    VolumeDelta {
        swap_count: left.swap_count + right.swap_count,
        volume_x: left.volume_x + right.volume_x,
        volume_y: left.volume_y + right.volume_y,
        volume_usd: left.volume_usd + right.volume_usd,
        unpriced_swap_count: left.unpriced_swap_count + right.unpriced_swap_count,
    }
}
