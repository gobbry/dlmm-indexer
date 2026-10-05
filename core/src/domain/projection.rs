use std::fmt;
use std::str::FromStr;

use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::amounts::TokenAmountRaw;
use crate::domain::ids::{PoolAddress, Slot, SwapOrdinal, TransactionIndex, UnixSeconds};
use crate::domain::swap::SwapDirection;

// Total order of the event log; field order makes the derived Ord the log order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogPosition {
    pub slot: Slot,
    pub transaction_index: TransactionIndex,
    pub swap_ordinal: SwapOrdinal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HourBucket(UnixSeconds);

impl HourBucket {
    const HOUR_SECONDS: i64 = 3_600;

    pub const fn containing(time: UnixSeconds) -> Self {
        let seconds = time.get();
        Self(UnixSeconds::new(
            seconds - seconds.rem_euclid(Self::HOUR_SECONDS),
        ))
    }

    pub const fn start(self) -> UnixSeconds {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DayBucket(UnixSeconds);

impl DayBucket {
    const DAY_SECONDS: i64 = 86_400;

    // UTC day start: Unix time has no leap seconds, so every day is 86 400 seconds long.
    pub const fn containing(time: UnixSeconds) -> Self {
        let seconds = time.get();
        Self(UnixSeconds::new(
            seconds - seconds.rem_euclid(Self::DAY_SECONDS),
        ))
    }

    pub const fn start(self) -> UnixSeconds {
        self.0
    }
}

// A swap row the insert actually returned; replayed rows never become one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertedSwap {
    pub position: LogPosition,
    pub pool: PoolAddress,
    pub block_time: UnixSeconds,
    pub direction: SwapDirection,
    pub amount_in: TokenAmountRaw,
    pub amount_out: TokenAmountRaw,
    pub volume_usd: Option<Decimal>,
}

// A swap row whose volume_usd the reprice UPDATE set from null.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepricedSwap {
    pub position: LogPosition,
    pub pool: PoolAddress,
    pub block_time: UnixSeconds,
    pub volume_usd: Decimal,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VolumeDelta {
    pub swap_count: i64,
    pub volume_x: u128,
    pub volume_y: u128,
    pub volume_usd: Decimal,
    pub unpriced_swap_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStatsDelta {
    pub volume: VolumeDelta,
    pub first_swap_at: UnixSeconds,
    pub last_swap_at: UnixSeconds,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionDeltas {
    pub pool_volume_1h: Vec<((PoolAddress, HourBucket), VolumeDelta)>,
    pub pool_volume_1d: Vec<((PoolAddress, DayBucket), VolumeDelta)>,
    pub pool_stats: Vec<(PoolAddress, PoolStatsDelta)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProjectionName {
    PoolVolume1h,
    PoolVolume1d,
    PoolStats,
}

impl ProjectionName {
    pub const ALL: [Self; 3] = [Self::PoolVolume1h, Self::PoolVolume1d, Self::PoolStats];

    // Matches the `projection.name` rows seeded by the migration.
    pub const fn table_name(self) -> &'static str {
        match self {
            Self::PoolVolume1h => "pool_volume_1h",
            Self::PoolVolume1d => "pool_volume_1d",
            Self::PoolStats => "pool_stats",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown projection {0}; expected pool_volume_1h, pool_volume_1d or pool_stats")]
pub struct UnknownProjection(pub String);

impl FromStr for ProjectionName {
    type Err = UnknownProjection;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|name| name.table_name() == text)
            .ok_or_else(|| UnknownProjection(text.to_owned()))
    }
}

impl fmt::Display for ProjectionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.table_name())
    }
}

// Building means a rebuild owns the table and its rows are incomplete; readers must not serve it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProjectionState {
    Building,
    Live,
}

impl ProjectionState {
    pub const ALL: [Self; 2] = [Self::Building, Self::Live];

    // The text form of the database enum projection_state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::Live => "live",
        }
    }
}
