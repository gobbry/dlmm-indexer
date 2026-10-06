// Expected values were read independently of the decoder under test, on 2026-10-03:
// - amount_in, amount_out, mints, user and direction: mainnet-beta getTransaction with
//   encoding jsonParsed, from the spl-token transferChecked instructions inside each swap
//   (user to reserve is amount_in, reserve to user is amount_out; the authority is the user;
//   the swap's accounts 0, 6, 7 and 10 give pool, mint_x, mint_y, user). Solscan returned 403
//   and Solana FM renders client-side, so neither page could be read.
// - fee and protocol_fee: no explorer was readable, so these come from a separate 20-line
//   Python reader of the Swap event bytes (offsets 89 and 97 of the 129-byte payload, from the
//   IDL), not from this decoder. The same reader's amount_in, amount_out and swap_for_y (offsets
//   72, 80, 88) agreed with the token transfers above, which pins the neighbouring layout. As a
//   cross-check independent of those bytes' interpretation, the four single-rate swaps (direct,
//   both aggregator swaps, the first deep-nesting swap) satisfy
//   fee == ceil(amount_in * fee_rate / 1e9) with amount_in from the transfers; the two
//   multi-bin swaps change rate mid-swap and cannot be checked that way. Re-fetched on
//   2026-10-03, the live jsonParsed Swap event bytes equal the fixtures' for all four files.
// - Swap2Evt fee split: all six mm_fee values, limit_order_fee and host_fee 0, fees_on_input
//   true everywhere and fees_on_token_x true for deep-nesting swap 1, direct and Jupiter only,
//   read on 2026-10-04 by a separate Python reader of the 147-byte Swap2Evt payloads (offsets
//   113, 129, 137, 145, 146 from the 0.12.0 layout), not by this decoder. That reader's
//   amount_in (offset 89) and protocol_fee (121) equal the Swap event's, which pins the layout.
//   The identity fee == mm_fee + protocol_fee + limit_order_fee + host_fee then checks the
//   decoder across the two events, not the expected values.
mod common;

use common::{
    BLOCK_PARENT_SLOT, BLOCK_SLOT, BLOCK_TIME, EVENT_PAYLOAD_OFFSET, SWAP2_AMOUNT_IN_OFFSET,
    fixture_json, swap2_event_data, synthetic_block, synthetic_block_json,
};
use dlmm_core::decode::rpc_json::{RpcBlock, RpcTransactionResult};
use dlmm_core::decode::{decode_block, map_rpc_block, map_rpc_transaction};
use dlmm_core::domain::block::{DecodedBlock, FinalizedBlock, FinalizedTransaction};
use dlmm_core::domain::config::TransactionVersionMax;
use dlmm_core::domain::error::{DecodeError, MapError};
use dlmm_core::domain::ids::{Discriminator, Slot, TransactionIndex, UnixSeconds};
use dlmm_core::domain::swap::{DecodeFailure, DecodedSwap, FeeSide, FeeToken, SwapDirection};

const SOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const SWAP2_EVENT_LENGTH_BYTES: usize = 147;

struct Expected {
    ordinal: u16,
    pool: &'static str,
    mint_x: &'static str,
    mint_y: &'static str,
    user: &'static str,
    direction: SwapDirection,
    amount_in: u64,
    amount_out: u64,
    fee: u64,
    protocol_fee: u64,
    mm_fee: u64,
    fee_token: FeeToken,
}

fn load(name: &str) -> RpcTransactionResult {
    serde_json::from_value(fixture_json(name)).expect("fixture parses")
}

fn map_fixture(name: &str) -> Option<FinalizedTransaction> {
    let result = load(name);
    map_rpc_transaction(&result.transaction_with_meta, TransactionIndex::new(0)).expect("maps")
}

fn decode_transactions(name: &str, transactions: Vec<FinalizedTransaction>) -> DecodedBlock {
    let result = load(name);
    decode_block(&FinalizedBlock {
        slot: Slot::new(result.slot),
        parent_slot: Slot::new(result.slot - 1),
        block_time: UnixSeconds::new(result.block_time.expect("fixture has a block time")),
        transactions,
        mapping_failures: Vec::new(),
    })
}

fn decode_fixture(name: &str) -> DecodedBlock {
    decode_transactions(name, map_fixture(name).into_iter().collect())
}

fn assert_swap(swap: &DecodedSwap, expected: &Expected) {
    assert_eq!(swap.ordinal.get(), expected.ordinal);
    assert_eq!(swap.pool.to_string(), expected.pool);
    assert_eq!(swap.event.lb_pair.to_string(), expected.pool);
    assert_eq!(swap.mint_x.to_string(), expected.mint_x);
    assert_eq!(swap.mint_y.to_string(), expected.mint_y);
    assert_eq!(swap.user.to_string(), expected.user);
    assert_eq!(swap.event.from.to_string(), expected.user);
    assert_eq!(swap.event.direction, expected.direction);
    assert_eq!(swap.event.amount_in.get(), expected.amount_in);
    assert_eq!(swap.event.amount_out.get(), expected.amount_out);
    assert_eq!(swap.event.fee.get(), expected.fee);
    assert_eq!(swap.event.protocol_fee.get(), expected.protocol_fee);
    assert_eq!(
        swap.swap2_event_payload.as_ref().map(Vec::len),
        Some(SWAP2_EVENT_LENGTH_BYTES)
    );
    let event2 = swap.event2.expect("a 0.12.0 Swap2Evt");
    assert_eq!(event2.mm_fee.get(), expected.mm_fee);
    assert_eq!(event2.limit_order_fee.get(), 0);
    assert_eq!(swap.event.host_fee.get(), 0);
    assert_eq!(event2.fee_side, FeeSide::Input);
    assert_eq!(event2.fee_token, expected.fee_token);
    assert_eq!(
        swap.event.fee.get(),
        event2.mm_fee.get()
            + swap.event.protocol_fee.get()
            + event2.limit_order_fee.get()
            + swap.event.host_fee.get()
    );
}

#[test]
fn direct_swap2_decodes_one_swap() {
    let decoded = decode_fixture("direct_swap2");
    assert!(decoded.failures.is_empty());
    assert_eq!(decoded.swaps.len(), 1);
    assert_swap(
        &decoded.swaps[0],
        &Expected {
            ordinal: 0,
            pool: "HTvjzsfX3yU6BUodCjZ5vZkUrAxMDTrBs3CJaq43ashR",
            mint_x: SOL,
            mint_y: USDC,
            user: "CxPowg2whtNKU55THf2yxve5kTSWAyRpWDcoxzjFSjDs",
            direction: SwapDirection::XToY,
            amount_in: 338_794_033,
            amount_out: 40_032_478,
            fee: 36_603,
            protocol_fee: 3_660,
            mm_fee: 32_943,
            fee_token: FeeToken::X,
        },
    );
}

// A DLMM swap inside a Jupiter route is found at stack height 2.
#[test]
fn jupiter_route_decodes_one_nested_swap() {
    let decoded = decode_fixture("jupiter_route_one_swap");
    assert!(decoded.failures.is_empty());
    assert_eq!(decoded.swaps.len(), 1);
    assert_swap(
        &decoded.swaps[0],
        &Expected {
            ordinal: 0,
            pool: "2cZvajs8srNoK4ikyWPxExmecaiR22uESKHRMGTEkzXT",
            mint_x: "BPxxfRCXkUVhig4HS1Lh7kZqV6SPJhzfEk4x6fVBjPCy",
            mint_y: SOL,
            user: "6U91aKa8pmMxkJwBCfPTmUEfZi6dHe7DcFq2ALvB2tbB",
            direction: SwapDirection::XToY,
            amount_in: 4_516_656_892,
            amount_out: 50_582_121,
            fee: 11_348_389,
            protocol_fee: 1_134_838,
            mm_fee: 10_213_551,
            fee_token: FeeToken::X,
        },
    );
}

// Two swaps in one route get ordinals 0 and 1 in execution order.
#[test]
fn aggregator_decodes_two_swaps_in_order() {
    let decoded = decode_fixture("aggregator_two_swaps");
    assert!(decoded.failures.is_empty());
    assert_eq!(decoded.swaps.len(), 2);
    let user = "4M3iNdbvuuRBQgBWJ44KSetqxx7j6UCfFr4Ju4Pp3RT4";
    assert_swap(
        &decoded.swaps[0],
        &Expected {
            ordinal: 0,
            pool: "HTvjzsfX3yU6BUodCjZ5vZkUrAxMDTrBs3CJaq43ashR",
            mint_x: SOL,
            mint_y: USDC,
            user,
            direction: SwapDirection::YToX,
            amount_in: 6_370_003,
            amount_out: 53_798_310,
            fee: 963,
            protocol_fee: 96,
            mm_fee: 867,
            fee_token: FeeToken::Y,
        },
    );
    assert_swap(
        &decoded.swaps[1],
        &Expected {
            ordinal: 1,
            pool: "GaiM9oX6w1NvBWPC4XuoyTu1yohFYzrPgpcGmLi27RjE",
            mint_x: "G6autZruJLEzYKRh84qTsbVhrTvLEbjBYdHnBQH1zitp",
            mint_y: SOL,
            user,
            direction: SwapDirection::YToX,
            amount_in: 53_798_310,
            amount_out: 4_884_849_221,
            fee: 5_379_831,
            protocol_fee: 1_075_966,
            mm_fee: 4_303_865,
            fee_token: FeeToken::Y,
        },
    );
}

// Swaps nested at stack height 3 each decode with their own events.
#[test]
fn deep_nesting_decodes_two_swaps() {
    let decoded = decode_fixture("deep_nesting_two_swap2");
    assert!(decoded.failures.is_empty());
    assert_eq!(decoded.swaps.len(), 2);
    let user = "HgJ5zad5N4pwKpAM8HQDA3g2r2H7EMLVN6S5HvHdiNyR";
    assert_swap(
        &decoded.swaps[0],
        &Expected {
            ordinal: 0,
            pool: "CrgrtGnVtv7L4DsXHXrxKQDJqgsoaz1WnMkTHW1iwjqY",
            mint_x: "CbcyNo7m1amFWqEQm2m4PLv1UNvpcL3C1Ujm6AkzpKoU",
            mint_y: USDC,
            user,
            direction: SwapDirection::XToY,
            amount_in: 677_960_436,
            amount_out: 12_539_523,
            fee: 10_182_227,
            protocol_fee: 1_018_222,
            mm_fee: 9_164_005,
            fee_token: FeeToken::X,
        },
    );
    assert_swap(
        &decoded.swaps[1],
        &Expected {
            ordinal: 1,
            pool: "CLM92hJx6CGNBqTifR6Lvcvs3BuFbGWw1U4zLHELzQFL",
            mint_x: SOL,
            mint_y: USDC,
            user,
            direction: SwapDirection::YToX,
            amount_in: 12_539_523,
            amount_out: 105_890_655,
            fee: 5_150,
            protocol_fee: 514,
            mm_fee: 4_636,
            fee_token: FeeToken::Y,
        },
    );
}

// A failed transaction is dropped at mapping even though it carries an executed Swap event.
#[test]
fn failed_transaction_yields_nothing() {
    assert_eq!(map_fixture("failed_with_swap_event"), None);
    let decoded = decode_fixture("failed_with_swap_event");
    assert!(decoded.swaps.is_empty());
    assert!(decoded.failures.is_empty());
}

// The block mapper drops the failed and the non-DLMM entry and keeps block positions.
#[test]
fn map_rpc_block_drops_failed_and_non_dlmm_transactions() {
    let block = synthetic_block(serde_json::json!(BLOCK_TIME));
    let slot = Slot::new(BLOCK_SLOT);
    let mapped = map_rpc_block(slot, &block, TransactionVersionMax::SUPPORTED).expect("maps");
    assert_eq!(mapped.slot, slot);
    assert_eq!(mapped.parent_slot, Slot::new(BLOCK_PARENT_SLOT));
    assert_eq!(mapped.block_time, UnixSeconds::new(BLOCK_TIME));
    let positions: Vec<u16> = mapped
        .transactions
        .iter()
        .map(|transaction| transaction.transaction_index.get())
        .collect();
    // Envelope order: direct, unrelated, jupiter, aggregator, deep, failed.
    assert_eq!(positions, vec![0, 2, 3, 4]);
    let decoded = decode_block(&mapped);
    assert_eq!(decoded.swaps.len(), 6);
    assert!(decoded.failures.is_empty());
}

// A block without a time cannot be bucketed and is rejected.
#[test]
fn map_rpc_block_rejects_missing_block_time() {
    let block = synthetic_block(serde_json::Value::Null);
    let slot = Slot::new(BLOCK_SLOT);
    let result = map_rpc_block(slot, &block, TransactionVersionMax::SUPPORTED);
    assert_eq!(result, Err(MapError::BlockTimeMissing { slot }));
}

// A transaction version above the ceiling is rejected rather than misread.
#[test]
fn map_rpc_block_rejects_version_above_ceiling() {
    let block = synthetic_block(serde_json::json!(BLOCK_TIME));
    let result = map_rpc_block(Slot::new(1), &block, TransactionVersionMax::new(0));
    assert!(result.is_ok(), "version 0 is within a ceiling of 0");
    let mut json = synthetic_block_json(serde_json::json!(BLOCK_TIME));
    json["transactions"][2]["version"] = serde_json::json!(1);
    let block: RpcBlock = serde_json::from_value(json).unwrap();
    let result = map_rpc_block(Slot::new(1), &block, TransactionVersionMax::new(0));
    assert_eq!(result, Err(MapError::TransactionVersion(1)));
}

// A Swap event under a DLMM instruction the decoder does not know (a future swap3) is recorded
// as a decode failure for the whole transaction, never dropped as zero volume.
#[test]
fn swap_event_without_known_swap_instruction_is_a_failure() {
    // The direct fixture's top-level call.
    const SWAP2_DISCRIMINATOR: Discriminator = Discriminator::hex("414b3f4ceb5b5b88");
    let mut transaction = map_fixture("direct_swap2").expect("not failed");
    let swap2 = transaction
        .instructions
        .iter_mut()
        .find(|instruction| instruction.data.starts_with(SWAP2_DISCRIMINATOR.as_bytes()))
        .expect("the fixture calls swap2");
    swap2.data[..8].copy_from_slice(&[0xff; 8]);
    let signature = transaction.signature;

    let decoded = decode_transactions("direct_swap2", vec![transaction]);
    assert!(decoded.swaps.is_empty());
    assert_eq!(
        decoded.failures,
        vec![DecodeFailure {
            signature,
            reason: DecodeError::OrphanSwapEvent,
        }]
    );
}

// A Swap2Evt that disagrees with its Swap, or an event the decoder does not know, costs only
// the fee split: the swap is kept from its Swap event (with the raw Swap2Evt bytes when there
// are any) and the problem is recorded under the transaction's signature.
#[test]
fn bad_event_beside_a_swap_keeps_the_swap() {
    let mut mismatched = map_fixture("direct_swap2").expect("not failed");
    swap2_event_data(&mut mismatched)[EVENT_PAYLOAD_OFFSET + SWAP2_AMOUNT_IN_OFFSET] ^= 1;
    let mut unknown = map_fixture("direct_swap2").expect("not failed");
    swap2_event_data(&mut unknown)[8..EVENT_PAYLOAD_OFFSET].copy_from_slice(&[0xff; 8]);
    let cases = [
        (
            mismatched,
            DecodeError::EventMismatch,
            Some(SWAP2_EVENT_LENGTH_BYTES),
        ),
        (
            unknown,
            DecodeError::UnknownEventDiscriminator([0xff; 8]),
            None,
        ),
    ];
    for (transaction, reason, payload_length) in cases {
        let signature = transaction.signature;
        let decoded = decode_transactions("direct_swap2", vec![transaction]);
        assert_eq!(decoded.swaps.len(), 1, "{reason}");
        let swap = &decoded.swaps[0];
        assert_eq!(swap.event.amount_in.get(), 338_794_033, "{reason}");
        assert_eq!(swap.event2, None, "{reason}");
        assert_eq!(
            swap.swap2_event_payload.as_ref().map(Vec::len),
            payload_length
        );
        assert_eq!(decoded.failures, vec![DecodeFailure { signature, reason }]);
    }
}
