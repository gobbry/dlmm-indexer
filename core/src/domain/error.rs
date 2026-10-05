use thiserror::Error;

use crate::domain::ids::{Slot, StackHeight};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AddressParseError {
    #[error("not valid base58")]
    InvalidBase58,
    #[error("expected {expected} bytes, got {actual}")]
    Length { expected: usize, actual: usize },
}

// Chain input that cannot become a FinalizedBlock; the block is rejected, never stored.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MapError {
    #[error("block {slot:?} has no block_time")]
    BlockTimeMissing { slot: Slot },
    #[error("transaction has no signature")]
    SignatureMissing,
    #[error("signature: {0}")]
    Signature(AddressParseError),
    #[error("account key: {0}")]
    AccountKey(AddressParseError),
    #[error("instruction data is not valid base58")]
    InstructionData,
    #[error("instruction stack height {0} out of range")]
    StackHeight(u64),
    #[error("program index {0} out of range")]
    ProgramIndex(u64),
    #[error("account index {0} out of range")]
    AccountIndex(u64),
    #[error("transaction index {0} exceeds u16")]
    TransactionIndex(usize),
    #[error("transaction version {0} above the configured maximum")]
    TransactionVersion(u8),
    #[error("transaction has no message")]
    MessageMissing,
    #[error("transaction has no status meta")]
    MetaMissing,
}

// Persisted per transaction in decode_failure; decode errors are data, not failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("unmappable: {0}")]
    Unmappable(String),
    #[error("event_mismatch")]
    EventMismatch,
    #[error("payload_length: expected {expected}, actual {actual}")]
    PayloadLength { expected: usize, actual: usize },
    #[error("unknown_event_discriminator: {0:02x?}")]
    UnknownEventDiscriminator([u8; 8]),
    #[error("event_stack_height: expected {expected:?}, actual {actual:?}")]
    EventStackHeight {
        expected: StackHeight,
        actual: StackHeight,
    },
    #[error("orphan_swap_event")]
    OrphanSwapEvent,
    #[error("swap_without_event")]
    SwapWithoutEvent,
    #[error("account_index_out_of_range: {0}")]
    AccountIndexOutOfRange(u8),
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("column {column} holds a value outside its domain type")]
    ValueOutOfRange { column: &'static str },
    #[error("column {column}: {source}")]
    Address {
        column: &'static str,
        source: AddressParseError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcErrorClass {
    Retry,
    SkippedSlot,
    MissingInStorage,
    ConfigurationBug,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("http status {status}")]
    Status {
        status: u16,
        retry_after_ms: Option<u64>,
    },
    #[error("json-rpc error {code}: {message}")]
    JsonRpc { code: i64, message: String },
    #[error("response body: {0}")]
    Body(#[from] serde_json::Error),
    // A 200 with no bytes is a connection cut mid-answer (seen from faithful-cli when its
    // remote read fails), never an answer, so it is retried rather than read as a bad block.
    #[error("empty response body")]
    EmptyBody,
    #[error("block mapping: {0}")]
    Map(#[from] MapError),
}

#[derive(Debug, Error)]
pub enum PriceError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("http status {status}")]
    Status {
        status: u16,
        retry_after_ms: Option<u64>,
    },
    #[error("response body: {0}")]
    Body(#[from] serde_json::Error),
    #[error("malformed kline: {reason}")]
    MalformedKline { reason: &'static str },
    #[error("price decimal: {0}")]
    Decimal(#[from] rust_decimal::Error),
}
