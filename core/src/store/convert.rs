// Newtypes stop at this boundary: the database sees i64, i16, NUMERIC and base58 TEXT.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::domain::amounts::{Decimals, FeeRate1e9, QuoteAsset, TokenAmountRaw};
use crate::domain::error::{AddressParseError, StoreError};
use crate::domain::ids::{Slot, SwapOrdinal, TransactionIndex, UnixSeconds};
use crate::domain::job::JobEndKind;
use crate::domain::projection::{ProjectionName, ProjectionState};
use crate::domain::swap::{FeeSide, FeeToken, SwapDirection, SwapSource};

pub(super) fn timestamp_from_unix(
    seconds: UnixSeconds,
    column: &'static str,
) -> Result<DateTime<Utc>, StoreError> {
    DateTime::from_timestamp(seconds.get(), 0).ok_or(StoreError::ValueOutOfRange { column })
}

pub(super) fn unix_from_timestamp(time: DateTime<Utc>) -> UnixSeconds {
    UnixSeconds::new(time.timestamp())
}

pub(super) fn slot_to_database(slot: Slot, column: &'static str) -> Result<i64, StoreError> {
    i64::try_from(slot.get()).map_err(|_| StoreError::ValueOutOfRange { column })
}

pub(super) fn slot_from_database(value: i64, column: &'static str) -> Result<Slot, StoreError> {
    u64::try_from(value)
        .map(Slot::new)
        .map_err(|_| StoreError::ValueOutOfRange { column })
}

pub(super) fn transaction_index_to_database(index: TransactionIndex) -> Result<i16, StoreError> {
    i16::try_from(index.get()).map_err(|_| StoreError::ValueOutOfRange {
        column: "transaction_index",
    })
}

pub(super) fn transaction_index_from_database(value: i16) -> Result<TransactionIndex, StoreError> {
    u16::try_from(value)
        .map(TransactionIndex::new)
        .map_err(|_| StoreError::ValueOutOfRange {
            column: "transaction_index",
        })
}

pub(super) fn swap_ordinal_to_database(ordinal: SwapOrdinal) -> Result<i16, StoreError> {
    i16::try_from(ordinal.get()).map_err(|_| StoreError::ValueOutOfRange {
        column: "swap_ordinal",
    })
}

pub(super) fn swap_ordinal_from_database(value: i16) -> Result<SwapOrdinal, StoreError> {
    u16::try_from(value)
        .map(SwapOrdinal::new)
        .map_err(|_| StoreError::ValueOutOfRange {
            column: "swap_ordinal",
        })
}

pub(super) fn amount_to_database(amount: TokenAmountRaw) -> Decimal {
    Decimal::from(amount.get())
}

pub(super) fn amount_from_database(
    value: Decimal,
    column: &'static str,
) -> Result<TokenAmountRaw, StoreError> {
    if !value.fract().is_zero() {
        return Err(StoreError::ValueOutOfRange { column });
    }
    value
        .to_u64()
        .map(TokenAmountRaw::new)
        .ok_or(StoreError::ValueOutOfRange { column })
}

pub(super) fn optional_amount_from_database(
    value: Option<Decimal>,
    column: &'static str,
) -> Result<Option<TokenAmountRaw>, StoreError> {
    value
        .map(|value| amount_from_database(value, column))
        .transpose()
}

// The event carries a u128 rate but the column is the u64 domain; a rate past u64 would be a
// misdecode, so it is refused here instead of by the database's CHECK.
pub(super) fn fee_rate_to_database(rate: FeeRate1e9) -> Result<Decimal, StoreError> {
    u64::try_from(rate.get())
        .map(Decimal::from)
        .map_err(|_| StoreError::ValueOutOfRange {
            column: "fee_rate_1e9",
        })
}

pub(super) fn fee_rate_from_database(value: Decimal) -> Result<FeeRate1e9, StoreError> {
    if !value.fract().is_zero() {
        return Err(StoreError::ValueOutOfRange {
            column: "fee_rate_1e9",
        });
    }
    value
        .to_u64()
        .map(|rate| FeeRate1e9::new(u128::from(rate)))
        .ok_or(StoreError::ValueOutOfRange {
            column: "fee_rate_1e9",
        })
}

pub(super) fn sum_to_database(value: u128, column: &'static str) -> Result<Decimal, StoreError> {
    i128::try_from(value)
        .ok()
        .and_then(|value| Decimal::try_from_i128_with_scale(value, 0).ok())
        .ok_or(StoreError::ValueOutOfRange { column })
}

pub(super) fn count_from_database<T: TryFrom<i64>>(
    value: i64,
    column: &'static str,
) -> Result<T, StoreError> {
    T::try_from(value).map_err(|_| StoreError::ValueOutOfRange { column })
}

pub(super) fn decimals_to_database(decimals: Decimals) -> i16 {
    i16::from(decimals.get())
}

pub(super) fn decimals_from_database(
    value: Option<i16>,
    column: &'static str,
) -> Result<Option<Decimals>, StoreError> {
    value
        .map(|value| {
            u8::try_from(value)
                .map(Decimals::new)
                .map_err(|_| StoreError::ValueOutOfRange { column })
        })
        .transpose()
}

pub(super) fn address_from_database<Address>(
    text: &str,
    column: &'static str,
) -> Result<Address, StoreError>
where
    Address: FromStr<Err = AddressParseError>,
{
    Address::from_str(text).map_err(|source| StoreError::Address { column, source })
}

// Each database enum's text comes from the Rust enum's as_str, so the reverse lookup is
// derived from it and the two can never disagree.
fn variant_from_database<Variant: Copy>(
    text: &str,
    variants: &[Variant],
    as_str: fn(Variant) -> &'static str,
    column: &'static str,
) -> Result<Variant, StoreError> {
    variants
        .iter()
        .copied()
        .find(|variant| as_str(*variant) == text)
        .ok_or(StoreError::ValueOutOfRange { column })
}

pub(super) fn direction_from_database(text: &str) -> Result<SwapDirection, StoreError> {
    variant_from_database(
        text,
        &SwapDirection::ALL,
        SwapDirection::as_str,
        "direction",
    )
}

pub(super) fn quote_asset_from_database(text: &str) -> Result<QuoteAsset, StoreError> {
    variant_from_database(
        text,
        &QuoteAsset::ALL,
        QuoteAsset::as_str,
        "quote_asset_symbol",
    )
}

pub(super) fn fee_side_from_database(text: &str) -> Result<FeeSide, StoreError> {
    variant_from_database(text, &FeeSide::ALL, FeeSide::as_str, "fee_side")
}

pub(super) fn fee_token_from_database(text: &str) -> Result<FeeToken, StoreError> {
    variant_from_database(text, &FeeToken::ALL, FeeToken::as_str, "fee_token")
}

pub(super) fn swap_source_from_database(text: &str) -> Result<SwapSource, StoreError> {
    variant_from_database(text, &SwapSource::ALL, SwapSource::as_str, "source")
}

pub(super) fn projection_name_from_database(text: &str) -> Result<ProjectionName, StoreError> {
    variant_from_database(
        text,
        &ProjectionName::ALL,
        ProjectionName::table_name,
        "projection.name",
    )
}

pub(super) fn projection_state_from_database(text: &str) -> Result<ProjectionState, StoreError> {
    variant_from_database(
        text,
        &ProjectionState::ALL,
        ProjectionState::as_str,
        "projection.state",
    )
}

pub(super) fn job_end_kind_from_database(text: &str) -> Result<JobEndKind, StoreError> {
    variant_from_database(text, &JobEndKind::ALL, JobEndKind::as_str, "end_kind")
}

#[cfg(test)]
mod tests {
    use super::*;

    // A fee rate round-trips through the u64 column, and one past u64 is refused, not stored.
    #[test]
    fn fee_rate_fits_the_u64_column_or_is_out_of_range() {
        let largest = FeeRate1e9::new(u128::from(u64::MAX));
        let stored = fee_rate_to_database(largest).expect("u64::MAX fits");
        assert_eq!(stored, Decimal::from(u64::MAX));
        assert_eq!(fee_rate_from_database(stored).expect("read back"), largest);
        let too_large = FeeRate1e9::new(u128::from(u64::MAX) + 1);
        assert!(matches!(
            fee_rate_to_database(too_large),
            Err(StoreError::ValueOutOfRange {
                column: "fee_rate_1e9"
            })
        ));
        assert!(matches!(
            fee_rate_from_database(Decimal::new(-1, 0)),
            Err(StoreError::ValueOutOfRange {
                column: "fee_rate_1e9"
            })
        ));
    }
}
