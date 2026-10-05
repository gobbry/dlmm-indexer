// Anchor emit_cpi! events: a DLMM self-CPI whose data is a fixed tag, the event
// discriminator, then the Borsh payload.
use crate::domain::amounts::{FeeRate1e9, TokenAmountRaw};
use crate::domain::error::DecodeError;
use crate::domain::ids::{BinId, Discriminator, PoolAddress, UserAddress};
use crate::domain::swap::{FeeSide, FeeToken, Swap2Event, SwapDirection, SwapEvent};

// sha256("anchor:event")[..8] byte-reversed: Anchor stores it as a little-endian u64.
pub const EVENT_CPI_TAG: Discriminator = Discriminator::hex("e445a52e51cb9a1d");
pub const SWAP_EVENT_DISCRIMINATOR: Discriminator = Discriminator::hex("516ce3becdd00ac4"); // event:Swap
pub const SWAP2_EVENT_DISCRIMINATOR: Discriminator = Discriminator::hex("2e7452d7941b544d"); // event:Swap2Evt
pub const SWAP_EVENT_LENGTH_BYTES: usize = swap_layout::LENGTH_BYTES;
pub const SWAP2_EVENT_LENGTH_BYTES: usize = swap2_layout::LENGTH_BYTES;

const PUBKEY_BYTES: usize = 32;
const U64_BYTES: usize = 8;
const U128_BYTES: usize = 16;
const I32_BYTES: usize = 4;
const BOOL_BYTES: usize = 1;

// Borsh has no padding, so each offset is the previous one plus its field's size, in IDL
// declaration order. Layout of `Swap` in the lb_clmm 0.12.0 IDL; a payload of any other
// length is another program version and is rejected rather than misread.
mod swap_layout {
    use super::{BOOL_BYTES, I32_BYTES, PUBKEY_BYTES, U64_BYTES, U128_BYTES};

    pub const LB_PAIR: usize = 0;
    pub const FROM: usize = LB_PAIR + PUBKEY_BYTES;
    pub const START_BIN_ID: usize = FROM + PUBKEY_BYTES;
    pub const END_BIN_ID: usize = START_BIN_ID + I32_BYTES;
    pub const AMOUNT_IN: usize = END_BIN_ID + I32_BYTES;
    pub const AMOUNT_OUT: usize = AMOUNT_IN + U64_BYTES;
    pub const SWAP_FOR_Y: usize = AMOUNT_OUT + U64_BYTES;
    pub const FEE: usize = SWAP_FOR_Y + BOOL_BYTES;
    pub const PROTOCOL_FEE: usize = FEE + U64_BYTES;
    // IDL name: fee_bps. It is a rate scaled by 1e9, not basis points.
    pub const FEE_RATE_1E9: usize = PROTOCOL_FEE + U64_BYTES;
    pub const HOST_FEE: usize = FEE_RATE_1E9 + U128_BYTES;
    pub const LENGTH_BYTES: usize = HOST_FEE + U64_BYTES;
}
const _: () = assert!(swap_layout::LENGTH_BYTES == 129);

// Layout of `Swap2Evt` in the lb_clmm 0.12.0 IDL. Earlier versions emitted a different
// Swap2Evt, and length is the only version signal in the payload, so a payload of any other
// length is kept raw instead of decoded.
mod swap2_layout {
    use super::{BOOL_BYTES, I32_BYTES, PUBKEY_BYTES, U64_BYTES, U128_BYTES};

    pub const LB_PAIR: usize = 0;
    pub const FROM: usize = LB_PAIR + PUBKEY_BYTES;
    pub const START_BIN_ID: usize = FROM + PUBKEY_BYTES;
    pub const END_BIN_ID: usize = START_BIN_ID + I32_BYTES;
    pub const SWAP_FOR_Y: usize = END_BIN_ID + I32_BYTES;
    // IDL name: fee_bps. Read by nothing: Swap carries the same rate.
    pub const FEE_RATE_1E9: usize = SWAP_FOR_Y + BOOL_BYTES;
    pub const AMOUNT_IN: usize = FEE_RATE_1E9 + U128_BYTES;
    pub const AMOUNT_LEFT: usize = AMOUNT_IN + U64_BYTES;
    pub const AMOUNT_OUT: usize = AMOUNT_LEFT + U64_BYTES;
    pub const MM_FEE: usize = AMOUNT_OUT + U64_BYTES;
    pub const PROTOCOL_FEE: usize = MM_FEE + U64_BYTES;
    pub const LIMIT_ORDER_FEE: usize = PROTOCOL_FEE + U64_BYTES;
    pub const HOST_FEE: usize = LIMIT_ORDER_FEE + U64_BYTES;
    pub const FEES_ON_INPUT: usize = HOST_FEE + U64_BYTES;
    pub const FEES_ON_TOKEN_X: usize = FEES_ON_INPUT + BOOL_BYTES;
    pub const LENGTH_BYTES: usize = FEES_ON_TOKEN_X + BOOL_BYTES;
}
const _: () = assert!(swap2_layout::LENGTH_BYTES == 147);

const EVENT_HEADER_LENGTH_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Swap,
    Swap2,
    Unknown(Discriminator),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmittedEvent<'data> {
    pub kind: EventKind,
    pub payload: &'data [u8],
}

pub fn is_event_cpi(data: &[u8]) -> bool {
    data.starts_with(EVENT_CPI_TAG.as_bytes())
}

// Only called on data that carries the event tag.
pub fn split_event(data: &[u8]) -> Result<EmittedEvent<'_>, DecodeError> {
    debug_assert!(is_event_cpi(data));
    let (header, payload) =
        data.split_at_checked(EVENT_HEADER_LENGTH_BYTES)
            .ok_or(DecodeError::PayloadLength {
                expected: EVENT_HEADER_LENGTH_BYTES,
                actual: data.len(),
            })?;
    let discriminator = Discriminator::new(header[8..].try_into().expect("header is 16 bytes"));
    let kind = match discriminator {
        SWAP_EVENT_DISCRIMINATOR => EventKind::Swap,
        SWAP2_EVENT_DISCRIMINATOR => EventKind::Swap2,
        other => EventKind::Unknown(other),
    };
    Ok(EmittedEvent { kind, payload })
}

pub fn decode_swap_event(payload: &[u8]) -> Result<SwapEvent, DecodeError> {
    let bytes: &[u8; SWAP_EVENT_LENGTH_BYTES] =
        payload.try_into().map_err(|_| DecodeError::PayloadLength {
            expected: SWAP_EVENT_LENGTH_BYTES,
            actual: payload.len(),
        })?;
    Ok(SwapEvent {
        lb_pair: PoolAddress::new(array_at(bytes, swap_layout::LB_PAIR)),
        from: UserAddress::new(array_at(bytes, swap_layout::FROM)),
        start_bin_id: BinId::new(i32::from_le_bytes(array_at(
            bytes,
            swap_layout::START_BIN_ID,
        ))),
        end_bin_id: BinId::new(i32::from_le_bytes(array_at(bytes, swap_layout::END_BIN_ID))),
        amount_in: amount_at(bytes, swap_layout::AMOUNT_IN),
        amount_out: amount_at(bytes, swap_layout::AMOUNT_OUT),
        direction: direction_at(bytes, swap_layout::SWAP_FOR_Y),
        fee: amount_at(bytes, swap_layout::FEE),
        protocol_fee: amount_at(bytes, swap_layout::PROTOCOL_FEE),
        fee_rate_1e9: FeeRate1e9::new(u128::from_le_bytes(array_at(
            bytes,
            swap_layout::FEE_RATE_1E9,
        ))),
        host_fee: amount_at(bytes, swap_layout::HOST_FEE),
    })
}

// Ok(None) for any layout but 0.12.0. The fields Swap2Evt shares with Swap must agree with
// it and its fee split must add up to Swap's fee, or the pair is not one fill and the split
// cannot be trusted.
pub fn decode_swap2_event(
    payload: &[u8],
    swap: &SwapEvent,
) -> Result<Option<Swap2Event>, DecodeError> {
    let Ok(bytes) = <&[u8; SWAP2_EVENT_LENGTH_BYTES]>::try_from(payload) else {
        return Ok(None);
    };
    let fee_split_total = [
        swap2_layout::MM_FEE,
        swap2_layout::PROTOCOL_FEE,
        swap2_layout::LIMIT_ORDER_FEE,
        swap2_layout::HOST_FEE,
    ]
    .into_iter()
    .try_fold(0_u64, |total, offset| {
        total.checked_add(amount_at(bytes, offset).get())
    });
    let agrees = PoolAddress::new(array_at(bytes, swap2_layout::LB_PAIR)) == swap.lb_pair
        && direction_at(bytes, swap2_layout::SWAP_FOR_Y) == swap.direction
        && amount_at(bytes, swap2_layout::AMOUNT_IN) == swap.amount_in
        && amount_at(bytes, swap2_layout::AMOUNT_OUT) == swap.amount_out
        && amount_at(bytes, swap2_layout::PROTOCOL_FEE) == swap.protocol_fee
        && amount_at(bytes, swap2_layout::HOST_FEE) == swap.host_fee
        && fee_split_total == Some(swap.fee.get());
    if !agrees {
        return Err(DecodeError::EventMismatch);
    }
    let fee_side = match flag_at(bytes, swap2_layout::FEES_ON_INPUT) {
        true => FeeSide::Input,
        false => FeeSide::Output,
    };
    let fee_token = match flag_at(bytes, swap2_layout::FEES_ON_TOKEN_X) {
        true => FeeToken::X,
        false => FeeToken::Y,
    };
    Ok(Some(Swap2Event {
        amount_left: amount_at(bytes, swap2_layout::AMOUNT_LEFT),
        mm_fee: amount_at(bytes, swap2_layout::MM_FEE),
        limit_order_fee: amount_at(bytes, swap2_layout::LIMIT_ORDER_FEE),
        fee_side,
        fee_token,
    }))
}

fn direction_at<const TOTAL: usize>(bytes: &[u8; TOTAL], offset: usize) -> SwapDirection {
    match flag_at(bytes, offset) {
        true => SwapDirection::XToY,
        false => SwapDirection::YToX,
    }
}

// Borsh writes a bool as one byte; any non-zero value is read as true, as the decoder always has.
fn flag_at<const TOTAL: usize>(bytes: &[u8; TOTAL], offset: usize) -> bool {
    bytes[offset] != 0
}

fn amount_at<const TOTAL: usize>(bytes: &[u8; TOTAL], offset: usize) -> TokenAmountRaw {
    TokenAmountRaw::new(u64::from_le_bytes(array_at(bytes, offset)))
}

// Offsets are constants within a fixed layout, so the slice always fits.
fn array_at<const LENGTH: usize, const TOTAL: usize>(
    bytes: &[u8; TOTAL],
    offset: usize,
) -> [u8; LENGTH] {
    debug_assert!(offset + LENGTH <= TOTAL);
    bytes[offset..offset + LENGTH]
        .try_into()
        .expect("offset within the fixed layout")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Distinct non-zero values per field, so a decoder reading any field from a neighbour's
    // offset fails; the two flags differ so swapping them fails too.
    const AMOUNT_IN: u64 = 1_001;
    const AMOUNT_LEFT: u64 = 2_002;
    const AMOUNT_OUT: u64 = 3_003;
    const MM_FEE: u64 = 4_004;
    const PROTOCOL_FEE: u64 = 5_005;
    const LIMIT_ORDER_FEE: u64 = 6_006;
    const HOST_FEE: u64 = 7_007;

    fn swap2_payload() -> [u8; SWAP2_EVENT_LENGTH_BYTES] {
        let mut payload = [0xAB_u8; SWAP2_EVENT_LENGTH_BYTES];
        payload[swap2_layout::LB_PAIR..swap2_layout::FROM].copy_from_slice(&[1; PUBKEY_BYTES]);
        let fields = [
            (swap2_layout::AMOUNT_IN, AMOUNT_IN),
            (swap2_layout::AMOUNT_LEFT, AMOUNT_LEFT),
            (swap2_layout::AMOUNT_OUT, AMOUNT_OUT),
            (swap2_layout::MM_FEE, MM_FEE),
            (swap2_layout::PROTOCOL_FEE, PROTOCOL_FEE),
            (swap2_layout::LIMIT_ORDER_FEE, LIMIT_ORDER_FEE),
            (swap2_layout::HOST_FEE, HOST_FEE),
        ];
        for (offset, value) in fields {
            payload[offset..offset + U64_BYTES].copy_from_slice(&u64::to_le_bytes(value));
        }
        payload[swap2_layout::SWAP_FOR_Y] = 1;
        payload[swap2_layout::FEES_ON_INPUT] = 0;
        payload[swap2_layout::FEES_ON_TOKEN_X] = 1;
        payload
    }

    fn matching_swap() -> SwapEvent {
        SwapEvent {
            lb_pair: PoolAddress::new([1; 32]),
            from: UserAddress::new([2; 32]),
            start_bin_id: BinId::new(-7),
            end_bin_id: BinId::new(-6),
            amount_in: TokenAmountRaw::new(AMOUNT_IN),
            amount_out: TokenAmountRaw::new(AMOUNT_OUT),
            direction: SwapDirection::XToY,
            fee: TokenAmountRaw::new(MM_FEE + PROTOCOL_FEE + LIMIT_ORDER_FEE + HOST_FEE),
            protocol_fee: TokenAmountRaw::new(PROTOCOL_FEE),
            fee_rate_1e9: FeeRate1e9::new(100_000_000),
            host_fee: TokenAmountRaw::new(HOST_FEE),
        }
    }

    // Each Swap2Evt field is read from its own 0.12.0 offset.
    #[test]
    fn swap2_event_reads_every_field_at_its_offset() {
        let decoded = decode_swap2_event(&swap2_payload(), &matching_swap());
        assert_eq!(
            decoded,
            Ok(Some(Swap2Event {
                amount_left: TokenAmountRaw::new(AMOUNT_LEFT),
                mm_fee: TokenAmountRaw::new(MM_FEE),
                limit_order_fee: TokenAmountRaw::new(LIMIT_ORDER_FEE),
                fee_side: FeeSide::Output,
                fee_token: FeeToken::X,
            }))
        );
    }

    // A Swap2Evt that disagrees with its Swap on any shared field, or whose fee split does not
    // add up to Swap's fee, is a mismatch.
    #[test]
    fn swap2_event_disagreeing_with_swap_is_a_mismatch() {
        let swap = matching_swap();
        let disagreeing = [
            SwapEvent {
                amount_in: TokenAmountRaw::new(AMOUNT_IN + 1),
                ..swap
            },
            SwapEvent {
                amount_out: TokenAmountRaw::new(AMOUNT_OUT + 1),
                ..swap
            },
            SwapEvent {
                direction: SwapDirection::YToX,
                ..swap
            },
            SwapEvent {
                protocol_fee: TokenAmountRaw::new(PROTOCOL_FEE + 1),
                ..swap
            },
            SwapEvent {
                host_fee: TokenAmountRaw::new(HOST_FEE + 1),
                ..swap
            },
            SwapEvent {
                lb_pair: PoolAddress::new([9; 32]),
                ..swap
            },
            SwapEvent {
                fee: TokenAmountRaw::new(swap.fee.get() - 1),
                ..swap
            },
        ];
        for (index, other) in disagreeing.iter().enumerate() {
            let decoded = decode_swap2_event(&swap2_payload(), other);
            assert_eq!(decoded, Err(DecodeError::EventMismatch), "case {index}");
        }
    }

    // Any other layout is left undecoded rather than misread.
    #[test]
    fn swap2_event_of_another_length_is_not_decoded() {
        let longer = [0_u8; SWAP2_EVENT_LENGTH_BYTES + 8];
        assert_eq!(decode_swap2_event(&longer, &matching_swap()), Ok(None));
    }

    // A payload one byte short is a typed error, never a panic.
    #[test]
    fn swap_event_rejects_wrong_length() {
        let payload = [0_u8; SWAP_EVENT_LENGTH_BYTES - 1];
        assert_eq!(
            decode_swap_event(&payload),
            Err(DecodeError::PayloadLength {
                expected: SWAP_EVENT_LENGTH_BYTES,
                actual: SWAP_EVENT_LENGTH_BYTES - 1,
            })
        );
    }

    // An event tag with no room for a discriminator is a typed error.
    #[test]
    fn split_event_rejects_truncated_header() {
        assert_eq!(
            split_event(EVENT_CPI_TAG.as_bytes()),
            Err(DecodeError::PayloadLength {
                expected: EVENT_HEADER_LENGTH_BYTES,
                actual: Discriminator::LENGTH_BYTES,
            })
        );
    }
}
