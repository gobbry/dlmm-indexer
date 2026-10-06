use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL;
use rust_decimal::Decimal;

use crate::domain::amounts::{Decimals, FeeRate1e9, QuoteAsset, TokenAmountRaw};
use crate::domain::ids::{
    JobId, MintAddress, PoolAddress, Signature, Slot, SwapOrdinal, TransactionIndex, UnixSeconds,
    UserAddress,
};
use crate::domain::projection::{LogPosition, ProjectionName};
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

// The page size a paginated route serves; the bound keeps one response a few hundred KB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimit(u32);

impl PageLimit {
    pub const DEFAULT: Self = Self(20);
    pub const MAX: u32 = 100;

    pub const fn new(value: u32) -> Option<Self> {
        if value >= 1 && value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PageOffset(u32);

impl PageOffset {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

// Where a newest-first page of one pool's swaps stopped: the last row's sort key. Keyset, not
// offset, so swaps landing at the head while a client pages never shift or repeat a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapCursor {
    pub block_time: UnixSeconds,
    pub position: LogPosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cursor is not one this API issued")]
pub struct SwapCursorParseError;

// The bounds of the columns the cursor is compared against; a value past them could never have
// come from a row, and binding it would fail in the database instead of at the edge.
const CURSOR_SLOT_MAX: u64 = i64::MAX as u64;
const CURSOR_SMALLINT_MAX: u16 = i16::MAX as u16;
const CURSOR_FIELD_COUNT: usize = 4;

impl SwapCursor {
    pub fn of(swap: &SwapRow) -> Self {
        Self {
            block_time: swap.block_time,
            position: LogPosition {
                slot: swap.slot,
                transaction_index: swap.transaction_index,
                swap_ordinal: swap.swap_ordinal,
            },
        }
    }

    pub fn encode(self) -> String {
        let text = format!(
            "{}|{}|{}|{}",
            self.block_time.get(),
            self.position.slot.get(),
            self.position.transaction_index.get(),
            self.position.swap_ordinal.get()
        );
        BASE64_URL.encode(text)
    }
}

fn cursor_field<T: FromStr>(text: Option<&str>) -> Result<T, SwapCursorParseError> {
    text.ok_or(SwapCursorParseError)?
        .parse()
        .map_err(|_| SwapCursorParseError)
}

impl FromStr for SwapCursor {
    type Err = SwapCursorParseError;

    // Only the exact text encode() produces is accepted, so a cursor has one spelling and a
    // hand-edited one fails here rather than paging from somewhere no row ever was.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let bytes = BASE64_URL.decode(text).map_err(|_| SwapCursorParseError)?;
        let decoded = String::from_utf8(bytes).map_err(|_| SwapCursorParseError)?;
        let mut fields = decoded.splitn(CURSOR_FIELD_COUNT + 1, '|');
        let block_time: i64 = cursor_field(fields.next())?;
        let slot: u64 = cursor_field(fields.next())?;
        let transaction_index: u16 = cursor_field(fields.next())?;
        let swap_ordinal: u16 = cursor_field(fields.next())?;
        if fields.next().is_some()
            || block_time < 0
            || slot > CURSOR_SLOT_MAX
            || transaction_index > CURSOR_SMALLINT_MAX
            || swap_ordinal > CURSOR_SMALLINT_MAX
        {
            return Err(SwapCursorParseError);
        }
        let cursor = Self {
            block_time: UnixSeconds::new(block_time),
            position: LogPosition {
                slot: Slot::new(slot),
                transaction_index: TransactionIndex::new(transaction_index),
                swap_ordinal: SwapOrdinal::new(swap_ordinal),
            },
        };
        if cursor.encode() != text {
            return Err(SwapCursorParseError);
        }
        Ok(cursor)
    }
}

// next_cursor is None on the last page: the read asked for one row more than the limit and
// did not get it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapPage {
    pub swaps: Vec<SwapRow>,
    pub next_cursor: Option<SwapCursor>,
}

// pool_count is every indexed pool, the population the ranking orders, read in the same
// statement as the page so the two agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolPage {
    pub pools: Vec<PoolSummary>,
    pub pool_count: u64,
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
    pub transaction_index: TransactionIndex,
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
// may be rebuilding: first_swap_at and last_swap_at are null while pool_stats rebuilds, as
// before the first swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolMetadata {
    pub address: PoolAddress,
    pub mint_x: MintAddress,
    pub mint_y: MintAddress,
    pub decimals_x: Option<Decimals>,
    pub decimals_y: Option<Decimals>,
    pub first_swap_at: Option<UnixSeconds>,
    pub last_swap_at: Option<UnixSeconds>,
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

#[cfg(test)]
mod tests {
    use super::*;

    // A cursor decodes back to the key it was issued for. The literal pins today's form (URL-safe
    // unpadded base64 of block_time|slot|transaction_index|ordinal) so a change to it is a
    // deliberate one; clients treat the cursor as opaque.
    #[test]
    fn swap_cursor_round_trips_through_its_wire_form() {
        let cursor = SwapCursor {
            block_time: UnixSeconds::new(1_790_856_030),
            position: LogPosition {
                slot: Slot::new(1_000),
                transaction_index: TransactionIndex::new(3),
                swap_ordinal: SwapOrdinal::new(1),
            },
        };
        assert_eq!(cursor.encode(), "MTc5MDg1NjAzMHwxMDAwfDN8MQ");
        assert_eq!("MTc5MDg1NjAzMHwxMDAwfDN8MQ".parse(), Ok(cursor));
    }

    // Anything encode() could not have produced is refused: not base64, padded, standard
    // alphabet, wrong field count, a negative time, values past the columns, a non-canonical
    // number, a non-numeric field.
    #[test]
    fn swap_cursor_refuses_text_it_did_not_issue() {
        let refused = [
            ("", "empty"),
            ("not base64!", "not base64"),
            ("MHwwfDB8MA==", "padded"),
            ("MXwyfDM", "three fields"),
            ("MXwyfDN8NHw1", "five fields"),
            ("LTF8MHwwfDA", "negative block_time"),
            ("MXw5MjIzMzcyMDM2ODU0Nzc1ODA4fDB8MA", "slot past bigint"),
            ("MXwwfDMyNzY4fDA", "transaction_index past smallint"),
            ("MDF8MHwwfDA", "leading zero"),
            ("YXwwfDB8MA", "non-numeric"),
        ];
        for (text, case) in refused {
            assert_eq!(
                text.parse::<SwapCursor>(),
                Err(SwapCursorParseError),
                "{case}"
            );
        }
    }
}
