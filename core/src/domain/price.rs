use crate::domain::amounts::{PriceUsd, QuoteAsset};
use crate::domain::ids::UnixSeconds;

// A swap at t is priced by the latest eligible row observed in the half-open window
// (t - 60 s, t]: never one from its future, and never one a full minute old. On the one-minute
// grid that window holds exactly one ts, the last candle closed at or before t.
pub const PRICE_AGE_MAX_SECONDS: i64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PriceSource {
    Binance,
    Peg,
    CarriedForward,
}

impl PriceSource {
    pub const ALL: [Self; 3] = [Self::Binance, Self::Peg, Self::CarriedForward];

    // The text form of the database enum price_source.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Peg => "peg",
            Self::CarriedForward => "carried_forward",
        }
    }

    // Peg and carried_forward rows stay eligible whatever the market: the feed writes them
    // itself, the peg for USDT and the carry for an exchange gap, so they are part of the
    // market's own series rather than a rival source.
    pub const fn eligible_with(market: Self) -> [Self; 3] {
        [market, Self::Peg, Self::CarriedForward]
    }
}

// A swap minute of one quote asset that a finished sweep could not price: no candle in it and
// none in the seed lookback before it. Exchange history is immutable, so sweeping it again
// repeats the same fetches for the same answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnpriceableMinute {
    pub minute: UnixSeconds,
    pub asset: QuoteAsset,
}

// `ts` is the instant the price was observed: for a one-minute candle, its close time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PricePoint {
    pub asset: QuoteAsset,
    pub ts: UnixSeconds,
    pub close: PriceUsd,
    pub source: PriceSource,
}
