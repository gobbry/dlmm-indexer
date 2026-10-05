use crate::domain::ids::{
    AccountAddress, JobId, MintAddress, Signature, Slot, StackHeight, TransactionIndex, UnixSeconds,
};
use crate::domain::registry::PoolRecord;
use crate::domain::swap::{DecodeFailure, DecodedSwap, EnrichedSwap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveSource {
    Geyser,
    RpcTail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockOrigin {
    Live(LiveSource),
    Fill {
        job_id: JobId,
        next_slot_after: Slot,
    },
}

// Failed and vote transactions are already dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: UnixSeconds,
    pub transactions: Vec<FinalizedTransaction>,
    // One Unmappable failure per transaction that would not map; the block still maps.
    pub mapping_failures: Vec<DecodeFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedTransaction {
    pub signature: Signature,
    // Position in the block: the log order between transactions of one slot.
    pub transaction_index: TransactionIndex,
    // Static keys, then loaded writable, then loaded readonly.
    pub account_keys: Vec<AccountAddress>,
    // Execution order, inner instructions flattened after their parent.
    pub instructions: Vec<FlatInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatInstruction {
    pub stack_height: StackHeight,
    pub program_index: u8,
    pub account_indexes: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedBlock {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: UnixSeconds,
    pub swaps: Vec<DecodedSwap>,
    pub failures: Vec<DecodeFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichedBlock {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: UnixSeconds,
    pub swaps: Vec<EnrichedSwap>,
    pub failures: Vec<DecodeFailure>,
    pub new_pools: Vec<PoolRecord>,
    pub unknown_mints: Vec<MintAddress>,
}

// The post-commit receipt: nothing downstream of a block runs before its commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBlock {
    pub slot: Slot,
    pub block_time: UnixSeconds,
    pub origin: BlockOrigin,
    pub inserted_swap_count: u32,
    pub duplicate_swap_count: u32,
}

// The end of the top coverage range: the newest block every live source resumes after.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub slot: Slot,
    pub block_time: UnixSeconds,
}
