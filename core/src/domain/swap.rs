use crate::domain::amounts::{FeeRate1e9, QuoteLeg, TokenAmountRaw};
use crate::domain::error::DecodeError;
use crate::domain::ids::{
    BinId, MintAddress, PoolAddress, Signature, SwapOrdinal, TransactionIndex, UserAddress,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SwapDirection {
    XToY,
    YToX,
}

impl SwapDirection {
    pub const ALL: [Self; 2] = [Self::XToY, Self::YToX];

    // The text form shared by the database enum and the logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::XToY => "x_to_y",
            Self::YToX => "y_to_x",
        }
    }
}

// Swap2Evt.fees_on_input: whether the fee was charged on the input or the output token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeeSide {
    Input,
    Output,
}

impl FeeSide {
    pub const ALL: [Self; 2] = [Self::Input, Self::Output];

    // The text form of the database enum fee_side.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

// Swap2Evt.fees_on_token_x: which of the pool's two tokens the fee is denominated in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeeToken {
    X,
    Y,
}

impl FeeToken {
    pub const ALL: [Self; 2] = [Self::X, Self::Y];

    // The text form of the database enum fee_token.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::X => "x",
            Self::Y => "y",
        }
    }
}

// The fee split of the 147-byte (0.12.0) `Swap2Evt`; its protocol_fee and host_fee duplicate
// Swap's, so they are checked against it and not kept twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Swap2Event {
    pub amount_left: TokenAmountRaw,
    pub mm_fee: TokenAmountRaw,
    pub limit_order_fee: TokenAmountRaw,
    pub fee_side: FeeSide,
    pub fee_token: FeeToken,
}

// Which path stored a swap row first, so reconciliation between sources is auditable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SwapSource {
    LiveGeyser,
    LiveRpc,
    Fill,
}

impl SwapSource {
    pub const ALL: [Self; 3] = [Self::LiveGeyser, Self::LiveRpc, Self::Fill];

    // The text form of the database enum swap_source.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LiveGeyser => "live_geyser",
            Self::LiveRpc => "live_rpc",
            Self::Fill => "fill",
        }
    }
}

// Borsh layout of the `Swap` event, 129 bytes. fee_rate_1e9 is the fee rate scaled by 1e9
// (the IDL calls it fee_bps, but fixtures show 108039 and 100000000).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapEvent {
    pub lb_pair: PoolAddress,
    pub from: UserAddress,
    pub start_bin_id: BinId,
    pub end_bin_id: BinId,
    pub amount_in: TokenAmountRaw,
    pub amount_out: TokenAmountRaw,
    pub direction: SwapDirection,
    pub fee: TokenAmountRaw,
    pub protocol_fee: TokenAmountRaw,
    pub fee_rate_1e9: FeeRate1e9,
    pub host_fee: TokenAmountRaw,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSwap {
    pub signature: Signature,
    pub transaction_index: TransactionIndex,
    pub ordinal: SwapOrdinal,
    pub pool: PoolAddress,
    pub mint_x: MintAddress,
    pub mint_y: MintAddress,
    pub user: UserAddress,
    pub event: SwapEvent,
    // None when the swap emitted no Swap2Evt or one in a layout other than 0.12.0.
    pub event2: Option<Swap2Event>,
    // Raw `Swap2Evt` bytes, kept so a later layout can be replayed without a re-index.
    pub swap2_event_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichedSwap {
    pub decoded: DecodedSwap,
    pub mint_in: MintAddress,
    pub mint_out: MintAddress,
    pub quote_leg: Option<QuoteLeg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeFailure {
    pub signature: Signature,
    pub reason: DecodeError,
}
