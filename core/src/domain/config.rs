// Operational limits read from the environment or fixed in code, typed so they never mix.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelCapacity(usize);

impl ChannelCapacity {
    pub const LIVE_BLOCKS: Self = Self(64);
    pub const FILL_BLOCKS: Self = Self(16);
    pub const NUDGES: Self = Self(16);
    // Control messages to the price feed; its price notifications ride the fill channel.
    pub const PRICE_FEED_MESSAGES: Self = Self(16);

    pub fn new(value: usize) -> Self {
        debug_assert!(value > 0);
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpsMax(u32);

impl RpsMax {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionVersionMax(u8);

impl TransactionVersionMax {
    pub const SUPPORTED: Self = Self(1);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

// Mainnet slots measured 0.27 s in epoch 1045 (2026-10), well under the protocol's nominal
// 0.4 s, and every rate the indexer must keep pace with (live getBlock calls, slot estimates)
// derives from this one number rather than from the nominal pace.
pub const SLOT_MILLISECONDS_OBSERVED: u64 = 270;

// Old Faithful publishes an epoch only after it ends, and near the tip a provider still serves
// blocks cheaply, so only history at least this old goes to the archive. Defined in time, not
// slots, because the slot rate drifts; it is resolved to a block on the archive itself.
// A margin, not a boundary: correctness never depends on it.
pub const ARCHIVE_SAFE_LAG_SECONDS: i64 = 604_800;

// The archive assembles each ~6 MB block from range requests against remote CAR files, so a
// block takes tens of seconds and latency, not a rate limit, is the pacer: twelve in flight
// measured 0.5 to 0.7 blocks/s against faithful-cli v0.7.28.
pub const ARCHIVE_IN_FLIGHT_MAX: usize = 12;
