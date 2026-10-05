// The archive lane's proof: a block fetched from an Old Faithful `faithful-cli rpc` server
// maps to the same transactions, and decodes to the same swaps, as the mainnet getTransaction
// fixtures saved from api.mainnet-beta.solana.com. Ignored by default so a run without the
// archive reports it as ignored, never as passed: start `archive/run.sh` (epoch 1046), set
// ARCHIVE_RPC_URL, and run `cargo test --test archive -- --ignored`. The fetch takes tens of
// seconds.

// Each test crate uses its own part of the shared helpers.
#[allow(dead_code)]
mod common;

use common::{BLOCK_PARENT_SLOT, BLOCK_SLOT, BLOCK_TIME, fixture_json};
use dlmm_core::decode::rpc_json::RpcTransactionResult;
use dlmm_core::decode::{decode_block, map_rpc_transaction};
use dlmm_core::domain::block::{FinalizedBlock, FinalizedTransaction};
use dlmm_core::domain::config::{RpsMax, TransactionVersionMax};
use dlmm_core::domain::ids::{Signature, Slot, TransactionIndex, UnixSeconds};
use dlmm_core::domain::swap::DecodedSwap;
use dlmm_core::gateway::rpc::{Endpoint, RequestLane, RpcGateway};

// Both fixtures sit in the proof block: a Jupiter-routed swap and two deeply nested ones.
const FIXTURES_IN_BLOCK: [&str; 2] = ["jupiter_route_one_swap", "deep_nesting_two_swap2"];

fn archive_url() -> String {
    std::env::var("ARCHIVE_RPC_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
        .expect("ARCHIVE_RPC_URL points at ./archive/run.sh (with epoch 1046)")
}

// The fixture mapped at the position the mainnet getTransaction reported for it.
fn mainnet_transaction(name: &str) -> FinalizedTransaction {
    let json = fixture_json(name);
    let position = json["transactionIndex"]
        .as_u64()
        .and_then(|index| u16::try_from(index).ok())
        .expect("the fixture records its transaction index");
    let result: RpcTransactionResult = serde_json::from_value(json).expect("fixture parses");
    assert_eq!(result.slot, BLOCK_SLOT, "{name} is in the proof block");
    map_rpc_transaction(
        &result.transaction_with_meta,
        TransactionIndex::new(position),
    )
    .expect("maps")
    .expect("a successful DLMM transaction")
}

fn swaps_of(block: &FinalizedBlock, signature: &Signature) -> Vec<DecodedSwap> {
    decode_block(block)
        .swaps
        .into_iter()
        .filter(|swap| swap.signature == *signature)
        .collect()
}

// The archive assembles a block from CDN range reads and now and then answers a real block
// with -32009 or an internal error; the filler retries those, and so does this proof, since
// it checks the mapping, not the server's availability.
async fn proof_block(gateway: &RpcGateway) -> FinalizedBlock {
    const ATTEMPT_COUNT_MAX: u32 = 5;
    let mut attempt = 1;
    loop {
        match gateway
            .block(Slot::new(BLOCK_SLOT), RequestLane::Fill)
            .await
        {
            Ok(block) => return block,
            Err(error) if attempt < ATTEMPT_COUNT_MAX => {
                eprintln!("archive attempt {attempt} failed: {error}; retrying");
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(error) => panic!("the archive serves the proof block: {error}"),
        }
    }
}

#[tokio::test]
#[ignore = "needs ./archive/run.sh with epoch 1046 and ARCHIVE_RPC_URL; run with --ignored"]
async fn archive_block_maps_and_decodes_like_the_mainnet_fixtures() {
    let gateway = RpcGateway::new(
        archive_url().parse().expect("ARCHIVE_RPC_URL is a URL"),
        Endpoint::Archive,
        RpsMax::new(20),
        TransactionVersionMax::SUPPORTED,
    )
    .expect("gateway");
    let archived = proof_block(&gateway).await;
    assert_eq!(archived.parent_slot, Slot::new(BLOCK_PARENT_SLOT));
    assert_eq!(archived.block_time, UnixSeconds::new(BLOCK_TIME));
    assert!(archived.mapping_failures.is_empty());

    for name in FIXTURES_IN_BLOCK {
        let expected = mainnet_transaction(name);
        let found: Vec<&FinalizedTransaction> = archived
            .transactions
            .iter()
            .filter(|transaction| transaction.signature == expected.signature)
            .collect();
        assert_eq!(found, vec![&expected], "{name}");

        let alone = FinalizedBlock {
            transactions: vec![expected.clone()],
            mapping_failures: Vec::new(),
            ..archived.clone()
        };
        let expected_swaps = swaps_of(&alone, &expected.signature);
        assert!(!expected_swaps.is_empty(), "{name} decodes to swaps");
        assert_eq!(
            swaps_of(&archived, &expected.signature),
            expected_swaps,
            "{name}"
        );
    }
}
