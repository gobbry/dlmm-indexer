mod common;

use common::{
    BLOCK_SLOT, BLOCK_TIME, EVENT_PAYLOAD_OFFSET, SWAP2_AMOUNT_IN_OFFSET, swap2_event_data,
    synthetic_block, synthetic_block_json,
};
use dlmm_core::actor::block_processor::{Caches, PricingSettings, process_block};
use dlmm_core::actor::range_filler::route;
use dlmm_core::decode::map_rpc_block;
use dlmm_core::decode::rpc_json::RpcBlock;
use dlmm_core::domain::amounts::QuoteAllowlist;
use dlmm_core::domain::block::{BlockOrigin, FinalizedBlock, LiveSource};
use dlmm_core::domain::config::TransactionVersionMax;
use dlmm_core::domain::ids::{JobId, Slot, UnixSeconds};
use dlmm_core::domain::job::{ArchiveWindow, BackfillInsert, JobEndKind, ReconcileSummary};
use dlmm_core::domain::price::PriceSource;
use dlmm_core::gateway::rpc::Endpoint;
use dlmm_core::store::{insert_backfill_job, reconcile};
use sqlx::{PgConnection, PgPool};

// Five pools (the SOL-USDC pool appears twice) over five mints, per the fixture README.
const POOL_COUNT: usize = 5;
const MINT_COUNT: usize = 5;

fn pricing() -> PricingSettings {
    PricingSettings {
        allowlist: QuoteAllowlist::mainnet(),
        market: PriceSource::Binance,
    }
}

async fn count(database: &PgPool, query: &'static str) -> i64 {
    sqlx::query_scalar(query)
        .fetch_one(database)
        .await
        .expect("count")
}

async fn stored_identities(database: &PgPool) -> Vec<(i16, String, i16)> {
    sqlx::query_as(
        "SELECT transaction_index, left(signature, 8), swap_ordinal FROM swap
         ORDER BY transaction_index, swap_ordinal",
    )
    .fetch_all(database)
    .await
    .expect("identities")
}

// Each fixture swap is stored once; a replay inserts nothing and queues no second decimals fetch.
#[sqlx::test(migrations = "../migrations")]
async fn process_block_stores_fixture_swaps_once(database: PgPool) {
    let block = map_rpc_block(
        Slot::new(BLOCK_SLOT),
        &synthetic_block(serde_json::json!(BLOCK_TIME)),
        TransactionVersionMax::SUPPORTED,
    )
    .expect("maps");
    let mut caches = Caches::new();
    let mut connection = database.acquire().await.expect("connection");

    let first = process_block(
        &mut connection,
        &block,
        BlockOrigin::Live(LiveSource::RpcTail),
        &pricing(),
        &mut caches,
    )
    .await
    .expect("first pass");
    assert_eq!(first.inserted_swap_count, 6);
    assert_eq!(first.duplicate_swap_count, 0);
    // Position 1 is the non-DLMM entry and position 5 the failed transaction.
    let expected_identities = vec![
        (0, "i5A32BcC".to_owned(), 0),
        (2, "3Pn3nn4p".to_owned(), 0),
        (3, "yiaAsmFh".to_owned(), 0),
        (3, "yiaAsmFh".to_owned(), 1),
        (4, "3kytARpR".to_owned(), 0),
        (4, "3kytARpR".to_owned(), 1),
    ];
    assert_eq!(stored_identities(&database).await, expected_identities);
    assert_eq!(
        count(&database, "SELECT count(*) FROM decode_failure").await,
        0
    );
    assert_eq!(
        count(&database, "SELECT count(*) FROM pool").await,
        POOL_COUNT as i64
    );
    assert_eq!(
        count(
            &database,
            "SELECT count(*) FROM token WHERE decimals IS NULL"
        )
        .await,
        MINT_COUNT as i64
    );
    let coverage: Vec<(i64, i64)> =
        sqlx::query_as("SELECT start_slot, end_slot FROM slot_coverage")
            .fetch_all(&database)
            .await
            .expect("coverage");
    let parent_slot = block.parent_slot.get() as i64;
    assert_eq!(coverage, vec![(parent_slot + 1, BLOCK_SLOT as i64)]);
    assert_eq!(caches.pools.len(), POOL_COUNT);
    assert_eq!(caches.mints_awaiting_decimals.len(), MINT_COUNT);

    let second = process_block(
        &mut connection,
        &block,
        BlockOrigin::Live(LiveSource::RpcTail),
        &pricing(),
        &mut caches,
    )
    .await
    .expect("second pass");
    assert_eq!(second.inserted_swap_count, 0);
    assert_eq!(second.duplicate_swap_count, 6);
    assert_eq!(stored_identities(&database).await, expected_identities);
    assert_eq!(caches.mints_awaiting_decimals.len(), MINT_COUNT);
    assert_eq!(
        count(&database, "SELECT count(*) FROM slot_range_job").await,
        0
    );
}

// A transaction that cannot be mapped is stored as a decode failure by signature, and the
// rest of its block still commits.
#[sqlx::test(migrations = "../migrations")]
async fn process_block_stores_mapping_failure(database: PgPool) {
    // Position 2 is the Jupiter route, whose one DLMM swap is an inner instruction.
    let mut json = synthetic_block_json(serde_json::json!(BLOCK_TIME));
    let jupiter = &mut json["transactions"][2];
    let signature = jupiter["transaction"]["signatures"][0]
        .as_str()
        .expect("signature")
        .to_owned();
    jupiter["meta"]["innerInstructions"][0]["instructions"][0]["stackHeight"] =
        serde_json::Value::Null;
    let rpc_block: RpcBlock = serde_json::from_value(json).expect("block parses");
    let block = map_rpc_block(
        Slot::new(BLOCK_SLOT),
        &rpc_block,
        TransactionVersionMax::SUPPORTED,
    )
    .expect("the block still maps");
    let mut connection = database.acquire().await.expect("connection");

    let stored = process_block(
        &mut connection,
        &block,
        BlockOrigin::Live(LiveSource::RpcTail),
        &pricing(),
        &mut Caches::new(),
    )
    .await
    .expect("process_block");

    assert_eq!(stored.inserted_swap_count, 5);
    let failures: Vec<(String, String, i64)> =
        sqlx::query_as("SELECT signature, reason, slot FROM decode_failure")
            .fetch_all(&database)
            .await
            .expect("failures");
    assert_eq!(
        failures,
        vec![(
            signature,
            "unmappable: instruction stack height 0 out of range".to_owned(),
            BLOCK_SLOT as i64
        )]
    );
}

// A Swap2Evt that disagrees with its Swap is stored as an event_mismatch failure beside the
// swap, which is kept with its raw Swap2Evt bytes and no fee split.
#[sqlx::test(migrations = "../migrations")]
async fn process_block_keeps_swap_beside_event_mismatch(database: PgPool) {
    let mut block = map_rpc_block(
        Slot::new(BLOCK_SLOT),
        &synthetic_block(serde_json::json!(BLOCK_TIME)),
        TransactionVersionMax::SUPPORTED,
    )
    .expect("maps");
    // Position 0 is the direct swap2.
    let direct = &mut block.transactions[0];
    swap2_event_data(direct)[EVENT_PAYLOAD_OFFSET + SWAP2_AMOUNT_IN_OFFSET] ^= 1;
    let signature = direct.signature.to_string();
    let mut connection = database.acquire().await.expect("connection");

    let stored = process_block(
        &mut connection,
        &block,
        BlockOrigin::Live(LiveSource::RpcTail),
        &pricing(),
        &mut Caches::new(),
    )
    .await
    .expect("process_block");

    assert_eq!(stored.inserted_swap_count, 6);
    let kept: (Option<String>, bool) = sqlx::query_as(
        "SELECT mm_fee::text, swap2_event_payload IS NOT NULL FROM swap WHERE signature = $1",
    )
    .bind(&signature)
    .fetch_one(&database)
    .await
    .expect("the swap is stored");
    assert_eq!(kept, (None, true));
    let failures: Vec<(String, String)> =
        sqlx::query_as("SELECT signature, reason FROM decode_failure")
            .fetch_all(&database)
            .await
            .expect("failures");
    assert_eq!(failures, vec![(signature, "event_mismatch".to_owned())]);
}

const LIVE: BlockOrigin = BlockOrigin::Live(LiveSource::RpcTail);

fn empty_block(slot: u64, parent_slot: u64) -> FinalizedBlock {
    FinalizedBlock {
        slot: Slot::new(slot),
        parent_slot: Slot::new(parent_slot),
        block_time: UnixSeconds::new(BLOCK_TIME + i64::try_from(slot % 1_000).expect("fits")),
        transactions: Vec::new(),
        mapping_failures: Vec::new(),
    }
}

async fn process(connection: &mut PgConnection, block: &FinalizedBlock, origin: BlockOrigin) {
    process_block(connection, block, origin, &pricing(), &mut Caches::new())
        .await
        .expect("process_block");
}

async fn coverage(database: &PgPool) -> Vec<(i64, i64)> {
    sqlx::query_as("SELECT start_slot, end_slot FROM slot_coverage ORDER BY start_slot")
        .fetch_all(database)
        .await
        .expect("coverage")
}

// A live block that skips past the cursor leaves a hole; one reconcile turns it into exactly
// one job, and filling that job's blocks closes the hole and completes it.
#[sqlx::test(migrations = "../migrations")]
async fn hole_left_by_live_jump_is_filled_and_completed(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    process(&mut connection, &empty_block(100, 99), LIVE).await;
    process(&mut connection, &empty_block(110, 108), LIVE).await;
    assert_eq!(coverage(&database).await, vec![(100, 100), (109, 110)]);

    let opened = reconcile(&mut connection, None).await.expect("reconciles");
    assert_eq!(
        opened,
        ReconcileSummary {
            opened_job_count: 1,
            completed_job_count: 0,
            splits: Vec::new(),
        }
    );
    let (job_id, start_slot, end_slot): (i64, i64, i64) =
        sqlx::query_as("SELECT id, start_slot, end_slot FROM slot_range_job")
            .fetch_one(&database)
            .await
            .expect("one job");
    assert_eq!((start_slot, end_slot), (101, 108));

    // Slots 101 to 103 and 105 to 107 were skipped.
    for (slot, parent_slot, next_slot_after) in [(104, 100, 108), (108, 104, 109)] {
        let origin = BlockOrigin::Fill {
            job_id: JobId::new(job_id),
            next_slot_after: Slot::new(next_slot_after),
        };
        process(&mut connection, &empty_block(slot, parent_slot), origin).await;
    }
    assert_eq!(coverage(&database).await, vec![(100, 110)]);
    let completed = reconcile(&mut connection, None).await.expect("reconciles");
    assert_eq!(
        completed,
        ReconcileSummary {
            opened_job_count: 0,
            completed_job_count: 1,
            splits: Vec::new(),
        }
    );
    let completed_at_set: bool =
        sqlx::query_scalar("SELECT completed_at IS NOT NULL FROM slot_range_job WHERE id = $1")
            .bind(job_id)
            .fetch_one(&database)
            .await
            .expect("job");
    assert!(completed_at_set);
}

// A backfill job the API inserted below the lowest range is walked and completed like any
// reconciler hole: its fill blocks join coverage to the range above, and the next reconcile
// completes it and opens nothing new.
#[sqlx::test(migrations = "../migrations")]
async fn backfill_job_below_the_lowest_range_is_filled_and_completed(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    process(&mut connection, &empty_block(100, 99), LIVE).await;
    process(&mut connection, &empty_block(101, 100), LIVE).await;
    let BackfillInsert::Inserted { id, range } = insert_backfill_job(&database, Slot::new(60))
        .await
        .expect("inserts")
    else {
        panic!("expected a job");
    };
    assert_eq!((range.start.get(), range.end_inclusive.get()), (60, 99));

    // Slots 60 and 81 to 98 were skipped.
    for (slot, parent_slot, next_slot_after) in [(80, 59, 99), (99, 80, 100)] {
        let origin = BlockOrigin::Fill {
            job_id: id,
            next_slot_after: Slot::new(next_slot_after),
        };
        process(&mut connection, &empty_block(slot, parent_slot), origin).await;
    }
    assert_eq!(coverage(&database).await, vec![(60, 101)]);
    let completed = reconcile(&mut connection, None).await.expect("reconciles");
    assert_eq!(
        completed,
        ReconcileSummary {
            opened_job_count: 0,
            completed_job_count: 1,
            splits: Vec::new(),
        }
    );
    let completed_at_set: bool =
        sqlx::query_scalar("SELECT completed_at IS NOT NULL FROM slot_range_job WHERE id = $1")
            .bind(id.get())
            .fetch_one(&database)
            .await
            .expect("job");
    assert!(completed_at_set);
}

// A backfill reaching from below the archive's bottom to the live range would go wholly to the
// provider; one reconcile tick replaces it with pieces cut at the window's edges, so the
// archive serves its share and the provider only what lies outside the window.
#[sqlx::test(migrations = "../migrations")]
async fn backfill_job_straddling_the_archive_window_is_split_by_lane(database: PgPool) {
    let mut connection = database.acquire().await.expect("connection");
    process(&mut connection, &empty_block(1_000, 999), LIVE).await;
    let BackfillInsert::Inserted { id, range } = insert_backfill_job(&database, Slot::new(100))
        .await
        .expect("inserts")
    else {
        panic!("expected a job");
    };
    let window = ArchiveWindow::new(Slot::new(300), Slot::new(600));
    assert_eq!(route(range, window), Endpoint::Provider);

    let summary = reconcile(&mut connection, window)
        .await
        .expect("reconciles");
    assert_eq!(summary.splits.len(), 1);
    let split = &summary.splits[0];
    assert_eq!(split.job_id, id);
    let pieces: Vec<(u64, u64, JobEndKind, Endpoint)> = split
        .pieces
        .iter()
        .map(|piece| {
            (
                piece.range.start.get(),
                piece.range.end_inclusive.get(),
                piece.end_kind,
                route(piece.range, window),
            )
        })
        .collect();
    assert_eq!(
        pieces,
        vec![
            (100, 299, JobEndKind::ArchiveLowerCut, Endpoint::Provider),
            (300, 600, JobEndKind::Block, Endpoint::Archive),
            (601, 999, JobEndKind::Block, Endpoint::Provider),
        ]
    );
    let stored: Vec<(i64, i64, i64)> =
        sqlx::query_as("SELECT id, start_slot, next_slot FROM slot_range_job ORDER BY start_slot")
            .fetch_all(&database)
            .await
            .expect("jobs");
    let piece_ids: Vec<(i64, i64, i64)> = split
        .pieces
        .iter()
        .map(|piece| {
            let start = i64::try_from(piece.range.start.get()).expect("fits");
            (piece.id.get(), start, start)
        })
        .collect();
    assert_eq!(stored, piece_ids, "only the pieces remain, none walked yet");
}
