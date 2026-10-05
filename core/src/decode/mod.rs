pub mod event;
mod map;
mod pairing;
pub mod rpc_json;

pub(crate) use map::{
    ACCOUNT_KEY_COUNT_MAX, RawInnerGroup, RawInstruction, check_version, flatten_instructions,
};
pub use map::{TransactionVersion, map_rpc_block, map_rpc_transaction};

use crate::domain::block::{DecodedBlock, FinalizedBlock, FinalizedTransaction};
use crate::domain::error::DecodeError;
use crate::domain::ids::{AccountAddress, MintAddress, PoolAddress, SwapOrdinal, UserAddress};
use crate::domain::swap::{DecodeFailure, DecodedSwap};

pub const DLMM_PROGRAM_ID: AccountAddress =
    AccountAddress::from_base58("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo");

// Stable across all six swap instructions: optional accounts always occupy their slot.
const ACCOUNT_POSITION_LB_PAIR: usize = 0;
const ACCOUNT_POSITION_TOKEN_X_MINT: usize = 6;
const ACCOUNT_POSITION_TOKEN_Y_MINT: usize = 7;
const ACCOUNT_POSITION_USER: usize = 10;

// A transaction's swaps with the event problems that did not cost a swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionSwaps {
    pub swaps: Vec<DecodedSwap>,
    pub event_failures: Vec<DecodeError>,
}

pub fn decode_block(block: &FinalizedBlock) -> DecodedBlock {
    let mut swaps = Vec::new();
    let mut failures = block.mapping_failures.clone();
    for transaction in &block.transactions {
        let signature = transaction.signature;
        match decode_transaction(transaction) {
            Ok(decoded) => {
                swaps.extend(decoded.swaps);
                let kept = decoded.event_failures.into_iter();
                failures.extend(kept.map(|reason| DecodeFailure { signature, reason }));
            }
            Err(reason) => failures.push(DecodeFailure { signature, reason }),
        }
    }
    debug_assert!(failures.iter().all(|failure| {
        block
            .transactions
            .iter()
            .any(|transaction| transaction.signature == failure.signature)
            || block.mapping_failures.contains(failure)
    }));
    debug_assert!(swaps.windows(2).all(|pair| {
        (pair[0].transaction_index, pair[0].ordinal) < (pair[1].transaction_index, pair[1].ordinal)
    }));
    DecodedBlock {
        slot: block.slot,
        parent_slot: block.parent_slot,
        block_time: block.block_time,
        swaps,
        failures,
    }
}

// A swap whose own Swap event fails keeps every swap of the transaction out, so a redecode by
// signature replays the whole transaction. A swap that yielded its Swap is kept even when the
// events beside it do not decode (a Swap2Evt that disagrees, an unknown event): Swap alone
// carries identity and amounts, and the problem is still recorded under the signature.
pub fn decode_transaction(
    transaction: &FinalizedTransaction,
) -> Result<TransactionSwaps, DecodeError> {
    let swap_positions = pairing::find_swap_instructions(transaction);
    let mut swaps = Vec::with_capacity(swap_positions.len());
    let mut claimed_positions = Vec::with_capacity(swap_positions.len());
    let mut event_failures: Vec<DecodeError> = Vec::new();
    // SwapOrdinal is u16; a transaction cannot hold that many instructions within its limits.
    for (ordinal, &swap_position) in swap_positions
        .iter()
        .enumerate()
        .take(usize::from(u16::MAX))
    {
        let paired = pairing::pair_events(transaction, swap_position)?;
        let accounts = swap_accounts(transaction, swap_position)?;
        claimed_positions.push(paired.swap_event_position);
        // decode_failure is keyed by (signature, reason), so each reason is recorded once.
        for failure in paired.event_failures {
            if !event_failures.contains(&failure) {
                event_failures.push(failure);
            }
        }
        swaps.push(DecodedSwap {
            signature: transaction.signature,
            transaction_index: transaction.transaction_index,
            ordinal: SwapOrdinal::new(ordinal as u16),
            pool: accounts.pool,
            mint_x: accounts.mint_x,
            mint_y: accounts.mint_y,
            user: accounts.user,
            event: paired.swap_event,
            event2: paired.swap2_event,
            swap2_event_payload: paired.swap2_event_payload,
        });
    }
    pairing::check_orphan_swap_events(transaction, &claimed_positions)?;
    debug_assert_eq!(swaps.len(), claimed_positions.len());
    debug_assert!(
        swaps
            .windows(2)
            .all(|pair| pair[0].ordinal < pair[1].ordinal)
    );
    Ok(TransactionSwaps {
        swaps,
        event_failures,
    })
}

struct SwapAccounts {
    pool: PoolAddress,
    mint_x: MintAddress,
    mint_y: MintAddress,
    user: UserAddress,
}

fn swap_accounts(
    transaction: &FinalizedTransaction,
    swap_position: usize,
) -> Result<SwapAccounts, DecodeError> {
    let account_at = |account_position: usize| -> Result<[u8; 32], DecodeError> {
        let account_indexes = &transaction.instructions[swap_position].account_indexes;
        let key_index = *account_indexes
            .get(account_position)
            .ok_or(DecodeError::AccountIndexOutOfRange(account_position as u8))?;
        let key = transaction
            .account_keys
            .get(usize::from(key_index))
            .ok_or(DecodeError::AccountIndexOutOfRange(key_index))?;
        Ok(key.get())
    };
    Ok(SwapAccounts {
        pool: PoolAddress::new(account_at(ACCOUNT_POSITION_LB_PAIR)?),
        mint_x: MintAddress::new(account_at(ACCOUNT_POSITION_TOKEN_X_MINT)?),
        mint_y: MintAddress::new(account_at(ACCOUNT_POSITION_TOKEN_Y_MINT)?),
        user: UserAddress::new(account_at(ACCOUNT_POSITION_USER)?),
    })
}
