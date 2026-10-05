use dlmm_core::decode::event::{EVENT_CPI_TAG, SWAP2_EVENT_DISCRIMINATOR};
use dlmm_core::decode::rpc_json::RpcBlock;
use dlmm_core::domain::block::FinalizedTransaction;

pub const FIXTURE_NAMES: [&str; 5] = [
    "direct_swap2",
    "jupiter_route_one_swap",
    "aggregator_two_swaps",
    "deep_nesting_two_swap2",
    "failed_with_swap_event",
];
pub const BLOCK_SLOT: u64 = 452_139_025;
pub const BLOCK_PARENT_SLOT: u64 = 452_139_024;
pub const BLOCK_TIME: i64 = 1_790_818_031;

pub fn fixture_json(name: &str) -> serde_json::Value {
    let path = format!("{}/tests/fixtures/{name}.json", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    serde_json::from_str(&text).expect("fixture parses")
}

// The five fixtures as getBlock entries, with a non-DLMM copy of the legacy fixture at
// position 1. Envelope order: direct, unrelated, jupiter, aggregator, deep, failed.
pub fn synthetic_block_json(block_time: serde_json::Value) -> serde_json::Value {
    let mut entries: Vec<serde_json::Value> = FIXTURE_NAMES
        .iter()
        .map(|name| {
            let fixture = fixture_json(name);
            serde_json::json!({
                "transaction": fixture["transaction"],
                "meta": fixture["meta"],
                "version": fixture["version"],
            })
        })
        .collect();
    let mut unrelated = entries[0].clone();
    let keys = unrelated["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .expect("legacy fixture has static account keys");
    for key in keys.iter_mut() {
        if key == "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo" {
            *key = serde_json::json!("Vote111111111111111111111111111111111111111");
        }
    }
    entries.insert(1, unrelated);
    serde_json::json!({
        "blockhash": "11111111111111111111111111111111",
        "blockHeight": 1,
        "blockTime": block_time,
        "parentSlot": BLOCK_PARENT_SLOT,
        "previousBlockhash": "11111111111111111111111111111111",
        "transactions": entries,
    })
}

pub fn synthetic_block(block_time: serde_json::Value) -> RpcBlock {
    serde_json::from_value(synthetic_block_json(block_time)).expect("block parses")
}

// Where the payload starts in an event self-CPI's data: the tag, then the discriminator.
pub const EVENT_PAYLOAD_OFFSET: usize = 16;
// amount_in in the 0.12.0 Swap2Evt layout; Swap's amount_in must equal it.
pub const SWAP2_AMOUNT_IN_OFFSET: usize = 89;

// The data of the transaction's first Swap2Evt self-CPI, for tests that corrupt it.
pub fn swap2_event_data(transaction: &mut FinalizedTransaction) -> &mut Vec<u8> {
    let mut header = EVENT_CPI_TAG.as_bytes().to_vec();
    header.extend_from_slice(SWAP2_EVENT_DISCRIMINATOR.as_bytes());
    let instruction = transaction
        .instructions
        .iter_mut()
        .find(|instruction| instruction.data.starts_with(&header))
        .expect("the fixture emits a Swap2Evt");
    &mut instruction.data
}
