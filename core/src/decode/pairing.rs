// Which instructions are swaps, and which emitted events belong to each of them.
use crate::decode::DLMM_PROGRAM_ID;
use crate::decode::event::{
    EventKind, SWAP2_EVENT_LENGTH_BYTES, decode_swap_event, decode_swap2_event, is_event_cpi,
    split_event,
};
use crate::domain::block::{FinalizedTransaction, FlatInstruction};
use crate::domain::error::DecodeError;
use crate::domain::ids::{Discriminator, StackHeight};
use crate::domain::swap::{Swap2Event, SwapEvent};

// sha256("global:<instruction>")[..8]; the names are proven by a test, not trusted.
pub const SWAP_INSTRUCTION_DISCRIMINATORS: [Discriminator; 6] = [
    Discriminator::hex("f8c69e91e17587c8"), // swap
    Discriminator::hex("414b3f4ceb5b5b88"), // swap2
    Discriminator::hex("fa49652126cf4bb8"), // swap_exact_out
    Discriminator::hex("2bd7f784893cf351"), // swap_exact_out2
    Discriminator::hex("38ade6d0ade49ccd"), // swap_with_price_impact
    Discriminator::hex("4a62c0d6b1334b33"), // swap_with_price_impact2
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairedEvents {
    pub swap_event: SwapEvent,
    pub swap2_event: Option<Swap2Event>,
    pub swap2_event_payload: Option<Vec<u8>>,
    pub swap_event_position: usize,
    // Problems with the events around a decoded Swap: the swap stands on its Swap event, so
    // these are recorded for a redecode without dropping it.
    pub event_failures: Vec<DecodeError>,
}

fn is_dlmm_instruction(transaction: &FinalizedTransaction, instruction: &FlatInstruction) -> bool {
    transaction
        .account_keys
        .get(usize::from(instruction.program_index))
        .is_some_and(|program| *program == DLMM_PROGRAM_ID)
}

fn is_swap_instruction(transaction: &FinalizedTransaction, instruction: &FlatInstruction) -> bool {
    is_dlmm_instruction(transaction, instruction)
        && instruction
            .data
            .first_chunk::<8>()
            .is_some_and(|discriminator| {
                SWAP_INSTRUCTION_DISCRIMINATORS
                    .iter()
                    .any(|known| known == discriminator)
            })
}

fn is_dlmm_event(transaction: &FinalizedTransaction, instruction: &FlatInstruction) -> bool {
    is_dlmm_instruction(transaction, instruction) && is_event_cpi(&instruction.data)
}

pub fn find_swap_instructions(transaction: &FinalizedTransaction) -> Vec<usize> {
    let positions: Vec<usize> = transaction
        .instructions
        .iter()
        .enumerate()
        .filter(|(_, instruction)| is_swap_instruction(transaction, instruction))
        .map(|(position, _)| position)
        .collect();
    debug_assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
    positions
}

// The events of a swap at height h are the DLMM self-CPIs at h + 1 after it, up to the next
// instruction at height <= h; token transfers interleave, so adjacency is not assumed.
pub fn pair_events(
    transaction: &FinalizedTransaction,
    swap_position: usize,
) -> Result<PairedEvents, DecodeError> {
    let swap_height = transaction.instructions[swap_position].stack_height;
    let event_height = StackHeight::new(swap_height.get().saturating_add(1));
    let window = transaction
        .instructions
        .iter()
        .enumerate()
        .skip(swap_position + 1)
        .take_while(|(_, instruction)| instruction.stack_height > swap_height)
        .filter(|(_, instruction)| is_dlmm_event(transaction, instruction));
    let mut swap_found: Option<(usize, SwapEvent)> = None;
    let mut swap2_event_payload: Option<Vec<u8>> = None;
    let mut event_failures = Vec::new();
    for (position, instruction) in window {
        if instruction.stack_height != event_height {
            return Err(DecodeError::EventStackHeight {
                expected: event_height,
                actual: instruction.stack_height,
            });
        }
        let event = split_event(&instruction.data)?;
        match event.kind {
            // A second event of a kind has no swap left to pair with.
            EventKind::Swap if swap_found.is_some() => return Err(DecodeError::OrphanSwapEvent),
            EventKind::Swap => swap_found = Some((position, decode_swap_event(event.payload)?)),
            EventKind::Swap2 if swap2_event_payload.is_some() => {
                return Err(DecodeError::OrphanSwapEvent);
            }
            EventKind::Swap2 if event.payload.is_empty() => {
                return Err(DecodeError::PayloadLength {
                    expected: SWAP2_EVENT_LENGTH_BYTES,
                    actual: 0,
                });
            }
            EventKind::Swap2 => swap2_event_payload = Some(event.payload.to_vec()),
            EventKind::Unknown(discriminator) => {
                event_failures.push(DecodeError::UnknownEventDiscriminator(discriminator.get()));
            }
        }
    }
    // Without a Swap there is nothing to keep, and an unknown event is the likelier cause.
    let Some((swap_event_position, swap_event)) = swap_found else {
        return Err(event_failures
            .into_iter()
            .next()
            .unwrap_or(DecodeError::SwapWithoutEvent));
    };
    debug_assert!(swap_event_position > swap_position);
    let swap2_event = match swap2_event_payload.as_deref() {
        Some(payload) => decode_swap2_event(payload, &swap_event).unwrap_or_else(|error| {
            event_failures.push(error);
            None
        }),
        None => None,
    };
    debug_assert!(swap2_event.is_none() || swap2_event_payload.is_some());
    Ok(PairedEvents {
        swap_event,
        swap2_event,
        swap2_event_payload,
        swap_event_position,
        event_failures,
    })
}

// A Swap event no swap instruction claimed is how a future swap3 would surface.
pub fn check_orphan_swap_events(
    transaction: &FinalizedTransaction,
    claimed_positions: &[usize],
) -> Result<(), DecodeError> {
    let orphan = transaction
        .instructions
        .iter()
        .enumerate()
        .filter(|(_, instruction)| is_dlmm_event(transaction, instruction))
        .filter(|(position, _)| !claimed_positions.contains(position))
        .any(|(_, instruction)| {
            split_event(&instruction.data).is_ok_and(|event| event.kind == EventKind::Swap)
        });
    if orphan {
        return Err(DecodeError::OrphanSwapEvent);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::SWAP_INSTRUCTION_DISCRIMINATORS;
    use crate::decode::event::{
        EVENT_CPI_TAG, SWAP_EVENT_DISCRIMINATOR, SWAP2_EVENT_DISCRIMINATOR,
    };
    use crate::domain::ids::Discriminator;

    fn anchor_discriminator(preimage: &str) -> Discriminator {
        let digest = Sha256::digest(preimage.as_bytes());
        Discriminator::new(digest[..Discriminator::LENGTH_BYTES].try_into().unwrap())
    }

    // Each hex constant is the Anchor discriminator its comment names, so a swapped or
    // mistyped entry fails here instead of silently dropping a swap kind.
    #[test]
    fn discriminators_derive_from_their_anchor_names() {
        let instruction_names = [
            "swap",
            "swap2",
            "swap_exact_out",
            "swap_exact_out2",
            "swap_with_price_impact",
            "swap_with_price_impact2",
        ];
        for (name, constant) in instruction_names
            .iter()
            .zip(SWAP_INSTRUCTION_DISCRIMINATORS)
        {
            assert_eq!(
                anchor_discriminator(&format!("global:{name}")),
                constant,
                "{name}"
            );
        }
        assert_eq!(anchor_discriminator("event:Swap"), SWAP_EVENT_DISCRIMINATOR);
        assert_eq!(
            anchor_discriminator("event:Swap2Evt"),
            SWAP2_EVENT_DISCRIMINATOR
        );
        let mut event_tag = anchor_discriminator("anchor:event").get();
        event_tag.reverse();
        assert_eq!(Discriminator::new(event_tag), EVENT_CPI_TAG);
    }
}
