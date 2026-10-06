use crate::decode::DLMM_PROGRAM_ID;
use crate::decode::rpc_json::{
    RpcBlock, RpcInstruction, RpcTransactionVersion, RpcTransactionWithMeta,
};
use crate::domain::block::{FinalizedBlock, FinalizedTransaction, FlatInstruction};
use crate::domain::config::TransactionVersionMax;
use crate::domain::error::{DecodeError, MapError};
use crate::domain::ids::{
    AccountAddress, Signature, Slot, StackHeight, TransactionIndex, UnixSeconds,
};
use crate::domain::swap::DecodeFailure;

const STACK_HEIGHT_TOP: StackHeight = StackHeight::new(1);
// Instruction indexes are u8, so a larger table cannot be addressed consistently.
pub(crate) const ACCOUNT_KEY_COUNT_MAX: usize = u8::MAX as usize + 1;

// None when the transaction failed (meta.err set): failed routes can carry executed events.
pub fn map_rpc_transaction(
    transaction: &RpcTransactionWithMeta,
    transaction_index: TransactionIndex,
) -> Result<Option<FinalizedTransaction>, MapError> {
    if transaction.meta.err.is_some() {
        return Ok(None);
    }
    let signature_text = transaction
        .transaction
        .signatures
        .first()
        .ok_or(MapError::SignatureMissing)?;
    let signature: Signature = signature_text.parse().map_err(MapError::Signature)?;
    let account_keys = account_key_table(transaction)
        .map(|key| key.parse::<AccountAddress>().map_err(MapError::AccountKey))
        .collect::<Result<Vec<_>, _>>()?;
    if account_keys.len() > ACCOUNT_KEY_COUNT_MAX {
        return Err(MapError::AccountIndex(account_keys.len() as u64));
    }
    let instructions = flatten_rpc_instructions(transaction, account_keys.len())?;
    debug_assert!(!account_keys.is_empty());
    debug_assert!(instructions.len() >= transaction.transaction.message.instructions.len());
    Ok(Some(FinalizedTransaction {
        signature,
        transaction_index,
        account_keys,
        instructions,
    }))
}

// getBlock does not echo its own slot, so the caller passes the slot it asked for.
// Block-level problems (no block_time, a version above the ceiling) reject the block. A DLMM
// transaction that cannot be mapped becomes a mapping failure on the block, stored in
// decode_failure for a later redecode, so one malformed entry never stalls the job or the
// tail on its block.
pub fn map_rpc_block(
    slot: Slot,
    block: &RpcBlock,
    transaction_version_max: TransactionVersionMax,
) -> Result<FinalizedBlock, MapError> {
    let block_time = block
        .block_time
        .map(UnixSeconds::new)
        .ok_or(MapError::BlockTimeMissing { slot })?;
    let program_text = DLMM_PROGRAM_ID.to_string();
    let mut transactions = Vec::new();
    let mut mapping_failures = Vec::new();
    for (position, entry) in block.transactions.iter().enumerate() {
        check_version(entry.version.into(), transaction_version_max)?;
        // Filtering on the base58 text skips parsing transactions that never touch DLMM.
        if !account_key_table(entry).any(|key| *key == program_text) {
            continue;
        }
        let index = u16::try_from(position).map_err(|_| MapError::TransactionIndex(position))?;
        match map_rpc_transaction(entry, TransactionIndex::new(index)) {
            Ok(Some(mapped)) => transactions.push(mapped),
            Ok(None) => {}
            Err(error) => match unmapped_failure(entry, error) {
                Ok(failure) => mapping_failures.push(failure),
                Err(error) => log_unmapped_transaction(slot, entry, &error),
            },
        }
    }
    debug_assert!(transactions.len() <= block.transactions.len());
    debug_assert!(
        transactions
            .windows(2)
            .all(|pair| pair[0].transaction_index < pair[1].transaction_index)
    );
    Ok(FinalizedBlock {
        slot,
        parent_slot: Slot::new(block.parent_slot),
        block_time,
        transactions,
        mapping_failures,
    })
}

// A failure is stored by signature, so a transaction without a readable one can only be logged.
fn unmapped_failure(
    entry: &RpcTransactionWithMeta,
    error: MapError,
) -> Result<DecodeFailure, MapError> {
    if matches!(error, MapError::SignatureMissing | MapError::Signature(_)) {
        return Err(error);
    }
    let signature_text = entry.transaction.signatures.first();
    let Some(signature) = signature_text.and_then(|text| text.parse::<Signature>().ok()) else {
        return Err(error);
    };
    Ok(DecodeFailure {
        signature,
        reason: DecodeError::Unmappable(error.to_string()),
    })
}

fn log_unmapped_transaction(slot: Slot, entry: &RpcTransactionWithMeta, error: &MapError) {
    let signature = entry.transaction.signatures.first().map(String::as_str);
    tracing::warn!(
        slot = slot.get(),
        signature,
        %error,
        "transaction_unmappable"
    );
}

// Legacy messages carry no version number and pass any ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionVersion {
    Legacy,
    Number(u8),
}

impl From<Option<RpcTransactionVersion>> for TransactionVersion {
    fn from(version: Option<RpcTransactionVersion>) -> Self {
        match version {
            None | Some(RpcTransactionVersion::Legacy(_)) => Self::Legacy,
            Some(RpcTransactionVersion::Number(number)) => Self::Number(number),
        }
    }
}

pub(crate) fn check_version(
    version: TransactionVersion,
    transaction_version_max: TransactionVersionMax,
) -> Result<(), MapError> {
    match version {
        TransactionVersion::Legacy => Ok(()),
        TransactionVersion::Number(number) if number <= transaction_version_max.get() => Ok(()),
        TransactionVersion::Number(number) => Err(MapError::TransactionVersion(number)),
    }
}

fn account_key_table(transaction: &RpcTransactionWithMeta) -> impl Iterator<Item = &String> {
    let loaded = transaction.meta.loaded_addresses.as_ref();
    let writable = loaded.map(|addresses| addresses.writable.iter());
    let readonly = loaded.map(|addresses| addresses.readonly.iter());
    transaction
        .transaction
        .message
        .account_keys
        .iter()
        .chain(writable.into_iter().flatten())
        .chain(readonly.into_iter().flatten())
}

// The RPC JSON side of the shared flattening: indexes narrow and data decodes here, so the
// structural rules stay in one place.
fn flatten_rpc_instructions(
    transaction: &RpcTransactionWithMeta,
    account_key_count: usize,
) -> Result<Vec<FlatInstruction>, MapError> {
    let tops = transaction
        .transaction
        .message
        .instructions
        .iter()
        .map(raw_rpc_instruction)
        .collect::<Result<Vec<_>, _>>()?;
    let empty = Vec::new();
    let inner_groups = transaction
        .meta
        .inner_instructions
        .as_ref()
        .unwrap_or(&empty)
        .iter()
        .map(|group| {
            let instructions = group
                .instructions
                .iter()
                .map(raw_rpc_instruction)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(RawInnerGroup {
                top_position: group.index,
                instructions,
            })
        })
        .collect::<Result<Vec<_>, MapError>>()?;
    flatten_instructions(tops, inner_groups, account_key_count)
}

fn raw_rpc_instruction(instruction: &RpcInstruction) -> Result<RawInstruction, MapError> {
    let account_indexes = instruction
        .accounts
        .iter()
        .map(|&index| u8::try_from(index).map_err(|_| MapError::AccountIndex(index)))
        .collect::<Result<Vec<_>, _>>()?;
    let data = bs58::decode(&instruction.data)
        .into_vec()
        .map_err(|_| MapError::InstructionData)?;
    Ok(RawInstruction {
        program_id_index: instruction.program_id_index,
        account_indexes,
        data,
        stack_height: instruction.stack_height,
    })
}

// One instruction as a source mapper extracted it, before validation against the key table.
// Owned rather than borrowed so the Geyser mapper can move its vectors in without a copy.
pub(crate) struct RawInstruction {
    pub(crate) program_id_index: u64,
    pub(crate) account_indexes: Vec<u8>,
    pub(crate) data: Vec<u8>,
    // Read for inner instructions only; a top-level one is at the top by definition.
    pub(crate) stack_height: Option<u64>,
}

pub(crate) struct RawInnerGroup {
    pub(crate) top_position: u64,
    pub(crate) instructions: Vec<RawInstruction>,
}

// Execution order: each top-level instruction, then the inner ones it invoked.
pub(crate) fn flatten_instructions(
    tops: Vec<RawInstruction>,
    mut inner_groups: Vec<RawInnerGroup>,
    account_key_count: usize,
) -> Result<Vec<FlatInstruction>, MapError> {
    let top_count = tops.len();
    let mut flat = Vec::with_capacity(top_count);
    for (top_position, top) in tops.into_iter().enumerate() {
        flat.push(flat_instruction(top, STACK_HEIGHT_TOP, account_key_count)?);
        let inner_of_top = inner_groups
            .iter_mut()
            .filter(|group| group.top_position == top_position as u64)
            .flat_map(|group| std::mem::take(&mut group.instructions));
        for inner in inner_of_top {
            let stack_height = inner_stack_height(inner.stack_height)?;
            flat.push(flat_instruction(inner, stack_height, account_key_count)?);
        }
    }
    debug_assert!(flat.len() >= top_count);
    debug_assert!(
        flat.first()
            .is_none_or(|first| first.stack_height == STACK_HEIGHT_TOP)
    );
    Ok(flat)
}

// Without a stack height the parent of an event cannot be found; 0 marks it as absent.
fn inner_stack_height(stack_height: Option<u64>) -> Result<StackHeight, MapError> {
    let value = stack_height.ok_or(MapError::StackHeight(0))?;
    let height = u8::try_from(value).map_err(|_| MapError::StackHeight(value))?;
    if height <= STACK_HEIGHT_TOP.get() {
        return Err(MapError::StackHeight(value));
    }
    Ok(StackHeight::new(height))
}

fn flat_instruction(
    instruction: RawInstruction,
    stack_height: StackHeight,
    account_key_count: usize,
) -> Result<FlatInstruction, MapError> {
    let program_index = u8::try_from(instruction.program_id_index)
        .ok()
        .filter(|&index| usize::from(index) < account_key_count)
        .ok_or(MapError::ProgramIndex(instruction.program_id_index))?;
    if let Some(&index) = instruction
        .account_indexes
        .iter()
        .find(|&&index| usize::from(index) >= account_key_count)
    {
        return Err(MapError::AccountIndex(u64::from(index)));
    }
    Ok(FlatInstruction {
        stack_height,
        program_index,
        account_indexes: instruction.account_indexes,
        data: instruction.data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_json(name: &str) -> serde_json::Value {
        let path = format!("{}/tests/fixtures/{name}.json", env!("CARGO_MANIFEST_DIR"));
        let text = std::fs::read_to_string(&path).expect("fixture reads");
        let fixture: serde_json::Value = serde_json::from_str(&text).expect("fixture parses");
        serde_json::json!({
            "transaction": fixture["transaction"],
            "meta": fixture["meta"],
            "version": fixture["version"],
        })
    }

    // Without a readable signature there is nothing to store the failure under.
    #[test]
    fn transaction_without_signature_is_not_stored() {
        let mut entry = entry_json("direct_swap2");
        entry["transaction"]["signatures"] = serde_json::json!([]);
        let block: RpcBlock = serde_json::from_value(serde_json::json!({
            "blockTime": 1_790_818_031_i64,
            "parentSlot": 9,
            "transactions": [entry],
        }))
        .expect("block parses");
        let mapped =
            map_rpc_block(Slot::new(10), &block, TransactionVersionMax::SUPPORTED).expect("maps");
        assert!(mapped.transactions.is_empty());
        assert!(mapped.mapping_failures.is_empty());
    }
}
