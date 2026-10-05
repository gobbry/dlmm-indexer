use rust_decimal::Decimal;

use crate::domain::amounts::{Decimals, FeeRate1e9, QuoteAsset, TokenAmountRaw};
use crate::domain::ids::{
    JobId, MintAddress, PoolAddress, Signature, Slot, SwapOrdinal, UnixSeconds, UserAddress,
};
use crate::domain::projection::ProjectionName;
use crate::domain::swap::{FeeSide, FeeToken, SwapDirection, SwapSource};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Bucket {
    Hour,
    Day,
}

// Buckets are [start, end) aligned to the top of the hour or UTC midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlignedRange {
    pub from: UnixSeconds,
    pub to_exclusive: UnixSeconds,
    pub bucket: Bucket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowCountMax(u32);

impl RowCountMax {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

// volume_x and volume_y are raw-unit sums, which exceed u64, hence Decimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeBucket {
    pub start: UnixSeconds,
    pub swap_count: u64,
    pub volume_x: Decimal,
    pub volume_y: Decimal,
    pub volume_usd: Option<Decimal>,
    pub unpriced_swap_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapRow {
    pub signature: Signature,
    pub swap_ordinal: SwapOrdinal,
    pub slot: Slot,
    pub block_time: UnixSeconds,
    pub user: UserAddress,
    pub direction: SwapDirection,
    pub mint_in: MintAddress,
    pub mint_out: MintAddress,
    pub amount_in: TokenAmountRaw,
    pub amount_out: TokenAmountRaw,
    pub fee: TokenAmountRaw,
    pub protocol_fee: TokenAmountRaw,
    pub host_fee: TokenAmountRaw,
    pub fee_rate_1e9: FeeRate1e9,
    // The Swap2Evt fee split; null for a swap without a decoded 0.12.0 Swap2Evt.
    pub mm_fee: Option<TokenAmountRaw>,
    pub limit_order_fee: Option<TokenAmountRaw>,
    pub amount_left: Option<TokenAmountRaw>,
    pub fee_side: Option<FeeSide>,
    pub fee_token: Option<FeeToken>,
    pub quote_asset: Option<QuoteAsset>,
    pub volume_usd: Option<Decimal>,
    pub source: SwapSource,
    pub fill_job_id: Option<JobId>,
}

// Decimals are null until fetched; first and last swap are null before the first swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSummary {
    pub address: PoolAddress,
    pub mint_x: MintAddress,
    pub mint_y: MintAddress,
    pub decimals_x: Option<Decimals>,
    pub decimals_y: Option<Decimals>,
    pub swap_count_24h: u64,
    pub volume_usd_24h: Option<Decimal>,
    pub first_swap_at: Option<UnixSeconds>,
    pub last_swap_at: Option<UnixSeconds>,
}

// What the volume endpoint needs to label a pool's buckets, read without the projections that
// may be rebuilding: first_swap_at is null while pool_stats rebuilds, as before the first swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolMetadata {
    pub address: PoolAddress,
    pub mint_x: MintAddress,
    pub mint_y: MintAddress,
    pub decimals_x: Option<Decimals>,
    pub decimals_y: Option<Decimals>,
    pub first_swap_at: Option<UnixSeconds>,
}

// The cursor is absent until the first block commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexerHealth {
    pub cursor_slot: Option<Slot>,
    pub last_block_time: Option<UnixSeconds>,
    pub open_job_count: u32,
    pub blocked_job_count: u32,
    // In name order; a rebuild owns these tables and they hold only a prefix of the log.
    pub rebuilding_projections: Vec<ProjectionName>,
}

// A read of projection tables, checked against their state in the same statement: a table
// mid-rebuild holds a prefix of the log, and serving it would understate volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionRead<T> {
    Live(T),
    Rebuilding(ProjectionName),
}
