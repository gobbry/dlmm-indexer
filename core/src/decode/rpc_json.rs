// Our own serde shapes for getBlock and getTransaction (encoding json,
// maxSupportedTransactionVersion 1). Fields we never read are left out; serde ignores them.
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlock {
    pub block_time: Option<i64>,
    pub parent_slot: u64,
    pub transactions: Vec<RpcTransactionWithMeta>,
}

// getTransaction wraps the same entry a getBlock carries with its slot and time.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionResult {
    pub slot: u64,
    pub block_time: Option<i64>,
    #[serde(flatten)]
    pub transaction_with_meta: RpcTransactionWithMeta,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcTransactionWithMeta {
    pub transaction: RpcTransaction,
    pub meta: RpcTransactionMeta,
    // Absent only when the request omits maxSupportedTransactionVersion, which means legacy.
    #[serde(default)]
    pub version: Option<RpcTransactionVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum RpcTransactionVersion {
    Legacy(RpcLegacyTag),
    Number(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum RpcLegacyTag {
    #[serde(rename = "legacy")]
    Legacy,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcTransaction {
    pub signatures: Vec<String>,
    pub message: RpcMessage,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcMessage {
    pub account_keys: Vec<String>,
    pub instructions: Vec<RpcInstruction>,
    #[serde(default)]
    pub address_table_lookups: Option<Vec<RpcAddressTableLookup>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcAddressTableLookup {
    pub account_key: String,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

// Indexes stay u64 here so an out-of-range value is a MapError, not a serde failure.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcInstruction {
    pub program_id_index: u64,
    pub accounts: Vec<u64>,
    pub data: String,
    #[serde(default)]
    pub stack_height: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionMeta {
    pub err: Option<serde_json::Value>,
    #[serde(default)]
    pub inner_instructions: Option<Vec<RpcInnerInstructions>>,
    #[serde(default)]
    pub loaded_addresses: Option<RpcLoadedAddresses>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcInnerInstructions {
    pub index: u64,
    pub instructions: Vec<RpcInstruction>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RpcLoadedAddresses {
    pub writable: Vec<String>,
    pub readonly: Vec<String>,
}
