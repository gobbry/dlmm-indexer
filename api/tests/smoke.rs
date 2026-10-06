use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use dlmm_core::domain::config::{RpsMax, TransactionVersionMax};
use dlmm_core::domain::ids::{MintAddress, PoolAddress, Signature, Slot, UserAddress};
use dlmm_core::domain::job::ArchiveWindow;
use dlmm_core::gateway::rpc::{Endpoint, RpcGateway, Url};
use dlmm_core::store::reconcile;
use serde_json::{Value, json};
use sqlx::PgPool;

struct Response {
    status: u16,
    body: Value,
}

async fn serve(database: PgPool) -> SocketAddr {
    serve_with_gateway(database, None).await
}

async fn serve_with_gateway(database: PgPool, gateway: Option<Arc<RpcGateway>>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let address = listener.local_addr().expect("listener has an address");
    tokio::spawn(async move { axum::serve(listener, dlmm_api::router(database, gateway)).await });
    address
}

// A bare HTTP/1.1 client keeps the test on the real router and socket without a client crate.
async fn get(address: SocketAddr, path: &str) -> Response {
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    send(address, request).await
}

// No content type, as `curl -d` sends it: the endpoint must not depend on the header.
async fn post(address: SocketAddr, path: &str, body: &str) -> Response {
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    send(address, request).await
}

async fn delete(address: SocketAddr, path: &str) -> Response {
    let request = format!("DELETE {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    send(address, request).await
}

async fn send(address: SocketAddr, request: String) -> Response {
    let raw = tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(address).expect("connect to the router");
        stream.write_all(request.as_bytes()).expect("send request");
        let mut raw = String::new();
        stream.read_to_string(&mut raw).expect("read response");
        raw
    })
    .await
    .expect("client task completes");
    let (head, body) = raw.split_once("\r\n\r\n").expect("response has a body");
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line has a code");
    let body = serde_json::from_str(body).expect("body is JSON");
    Response { status, body }
}

async fn seed(database: &PgPool, pool: &str, mint_x: &str, mint_y: &str) {
    sqlx::query(
        "INSERT INTO pool (address, mint_x, mint_y, first_seen_slot) VALUES ($1, $2, $3, 1)",
    )
    .bind(pool)
    .bind(mint_x)
    .bind(mint_y)
    .execute(database)
    .await
    .expect("seed pool");
    sqlx::query("INSERT INTO token (mint, decimals) VALUES ($1, 9), ($2, 6)")
        .bind(mint_x)
        .bind(mint_y)
        .execute(database)
        .await
        .expect("seed tokens");
    // 01:00 is partly priced; 05:00 is wholly unpriced and falls outside the hourly range.
    sqlx::query(
        "INSERT INTO pool_volume_1h
         (bucket, pool, swap_count, volume_x, volume_y, volume_usd, unpriced_swap_count) VALUES
         ('2026-10-01T01:00:00Z', $1, 3, 1532123456789, 181204561234, 2.5, 1),
         ('2026-10-01T05:00:00Z', $1, 2, 1000000000, 4000000, 0, 2)",
    )
    .bind(pool)
    .execute(database)
    .await
    .expect("seed hourly volume");
    // The daily projection is its own fold. Seeded deliberately off the sum of the two hours
    // (6 swaps, 3.5 USD instead of 5 and 2.5), so a day read that summed hourly rows would show.
    sqlx::query(
        "INSERT INTO pool_volume_1d
         (bucket, pool, swap_count, volume_x, volume_y, volume_usd, unpriced_swap_count) VALUES
         ('2026-10-01T00:00:00Z', $1, 6, 1533123456789, 181208561234, 3.5, 3)",
    )
    .bind(pool)
    .execute(database)
    .await
    .expect("seed daily volume");
    sqlx::query(
        "INSERT INTO pool_stats (pool, swap_count, volume_x, volume_y, volume_usd,
         unpriced_swap_count, first_swap_at, last_swap_at) VALUES
         ($1, 5, 1533123456789, 181208561234, 2.5, 3,
          '2026-10-01T01:00:00Z', '2026-10-01T05:30:00Z')",
    )
    .bind(pool)
    .execute(database)
    .await
    .expect("seed pool stats");
}

fn empty_bucket(start: &str) -> Value {
    json!({
        "start": start, "swap_count": 0,
        "volume_x": "0", "volume_x_raw": "0", "volume_y": "0", "volume_y_raw": "0",
        "volume_usd": null, "unpriced_swap_count": 0
    })
}

struct ServedPool {
    address: SocketAddr,
    pool: String,
    mint_x: String,
    mint_y: String,
}

async fn serve_seeded_pool(database: PgPool) -> ServedPool {
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed(&database, &pool, &mint_x, &mint_y).await;
    let address = serve(database).await;
    ServedPool {
        address,
        pool,
        mint_x,
        mint_y,
    }
}

// A partly priced hour still reports the USD of its priced swaps.
#[sqlx::test(migrations = "../migrations")]
async fn hourly_volume_aligns_the_range_and_fills_empty_buckets(database: PgPool) {
    let ServedPool {
        address,
        pool,
        mint_x,
        mint_y,
    } = serve_seeded_pool(database).await;
    let hourly = get(
        address,
        &format!(
            "/v1/pools/{pool}/volume?bucket=hour\
             &from=2026-10-01T02:30:00%2B02:00&to=2026-10-01T02:59:59Z"
        ),
    )
    .await;
    assert_eq!(hourly.status, 200);
    let filled = json!({
        "start": "2026-10-01T01:00:00Z", "swap_count": 3,
        "volume_x": "1532.123456789", "volume_x_raw": "1532123456789",
        "volume_y": "181204.561234", "volume_y_raw": "181204561234",
        "volume_usd": "2.5", "unpriced_swap_count": 1
    });
    assert_eq!(
        hourly.body,
        json!({
            "pool": pool, "mint_x": mint_x, "mint_y": mint_y, "decimals_x": 9, "decimals_y": 6,
            "first_swap_at": "2026-10-01T01:00:00Z",
            "bucket": "hour", "from": "2026-10-01T00:00:00Z", "to": "2026-10-01T03:00:00Z",
            "buckets": [
                empty_bucket("2026-10-01T00:00:00Z"),
                filled,
                empty_bucket("2026-10-01T02:00:00Z"),
            ]
        })
    );
}

#[sqlx::test(migrations = "../migrations")]
async fn daily_volume_reads_the_daily_projection(database: PgPool) {
    let ServedPool { address, pool, .. } = serve_seeded_pool(database).await;
    let daily = get(
        address,
        &format!(
            "/v1/pools/{pool}/volume?bucket=day\
             &from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
        ),
    )
    .await;
    assert_eq!(daily.status, 200);
    assert_eq!(
        daily.body["buckets"],
        json!([{
            "start": "2026-10-01T00:00:00Z", "swap_count": 6,
            "volume_x": "1533.123456789", "volume_x_raw": "1533123456789",
            "volume_y": "181208.561234", "volume_y_raw": "181208561234",
            "volume_usd": "3.5", "unpriced_swap_count": 3
        }])
    );
}

#[sqlx::test(migrations = "../migrations")]
async fn volume_rejects_an_unknown_pool_and_bucket(database: PgPool) {
    let ServedPool { address, pool, .. } = serve_seeded_pool(database).await;
    let unknown_pool = PoolAddress::new([9; 32]);
    let missing = get(
        address,
        &format!("/v1/pools/{unknown_pool}/volume?bucket=hour&from=2026-10-01T00:00:00Z&to=2026-10-01T01:00:00Z"),
    )
    .await;
    assert_eq!(missing.status, 404);
    assert_eq!(missing.body["error"]["code"], "pool_not_found");

    let bad_bucket = get(
        address,
        &format!(
            "/v1/pools/{pool}/volume?bucket=week&from=2026-10-01T00:00:00Z&to=2026-10-01T01:00:00Z"
        ),
    )
    .await;
    assert_eq!(bad_bucket.status, 400);
    assert_eq!(bad_bucket.body["error"]["code"], "invalid_bucket");
}

#[sqlx::test(migrations = "../migrations")]
async fn health_reports_starting_on_empty_database(database: PgPool) {
    let address = serve(database).await;
    let health = get(address, "/v1/health").await;
    assert_eq!(health.status, 200);
    assert_eq!(
        health.body,
        json!({
            "status": "starting", "cursor_slot": null, "last_block_time": null,
            "lag_seconds": null, "open_job_count": 0, "blocked_job_count": 0
        })
    );
}

// While an offline rebuild holds pool_volume_1d in the building state, day requests answer 503
// and health names it; hour requests, served from another table, still answer.
#[sqlx::test(migrations = "../migrations")]
async fn volume_answers_503_while_projection_rebuilds(database: PgPool) {
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed(&database, &pool, &mint_x, &mint_y).await;
    sqlx::query("UPDATE projection SET state = 'building' WHERE name = 'pool_volume_1d'")
        .execute(&database)
        .await
        .expect("mark building");
    let address = serve(database).await;

    let daily = get(
        address,
        &format!(
            "/v1/pools/{pool}/volume?bucket=day\
             &from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
        ),
    )
    .await;
    assert_eq!(daily.status, 503);
    assert_eq!(daily.body["error"]["code"], "projection_rebuilding");

    let hourly = get(
        address,
        &format!(
            "/v1/pools/{pool}/volume?bucket=hour\
             &from=2026-10-01T00:00:00Z&to=2026-10-01T01:00:00Z"
        ),
    )
    .await;
    assert_eq!(hourly.status, 200);

    let health = get(address, "/v1/health").await;
    assert_eq!(health.status, 200);
    assert_eq!(health.body["rebuilding"], json!(["pool_volume_1d"]));
}

// A rebuild holds back only the requests served from its table: while pool_stats rebuilds the
// pool list answers 503 but volume still answers, its first swap unknown; while
// pool_volume_1h rebuilds the list and hour buckets answer 503 and day buckets still answer.
// Swaps, served from the log, answer throughout.
#[sqlx::test(migrations = "../migrations")]
async fn pool_reads_answer_503_only_for_rebuilding_tables(database: PgPool) {
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed(&database, &pool, &mint_x, &mint_y).await;
    let mark_building = |name: &'static str| {
        let database = database.clone();
        async move {
            sqlx::query("UPDATE projection SET state = 'building' WHERE name = $1")
                .bind(name)
                .execute(&database)
                .await
                .expect("mark building");
        }
    };
    let address = serve(database.clone()).await;
    let hourly_path = format!(
        "/v1/pools/{pool}/volume?bucket=hour&from=2026-10-01T01:00:00Z&to=2026-10-01T02:00:00Z"
    );
    let daily_path = format!(
        "/v1/pools/{pool}/volume?bucket=day&from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    );

    mark_building("pool_stats").await;
    let pools = get(address, "/v1/pools").await;
    assert_eq!(pools.status, 503);
    assert_eq!(pools.body["error"]["code"], "projection_rebuilding");
    let summary = get(address, &format!("/v1/pools/{pool}")).await;
    assert_eq!(error_code(&summary), (503, "projection_rebuilding"));
    let unknown = get(address, &format!("/v1/pools/{}", PoolAddress::new([9; 32]))).await;
    assert_eq!(error_code(&unknown), (404, "pool_not_found"));
    let hourly = get(address, &hourly_path).await;
    assert_eq!(hourly.status, 200);
    assert_eq!(hourly.body["first_swap_at"], Value::Null);
    assert_eq!(hourly.body["buckets"][0]["swap_count"], 3);
    let daily = get(address, &daily_path).await;
    assert_eq!(daily.status, 200);

    mark_building("pool_volume_1h").await;
    let pools = get(address, "/v1/pools").await;
    assert_eq!(pools.status, 503);
    let summary = get(address, &format!("/v1/pools/{pool}")).await;
    assert_eq!(error_code(&summary), (503, "projection_rebuilding"));
    let hourly = get(address, &hourly_path).await;
    assert_eq!(hourly.status, 503);
    assert_eq!(hourly.body["error"]["code"], "projection_rebuilding");
    let daily = get(address, &daily_path).await;
    assert_eq!(daily.status, 200);
    assert_eq!(daily.body["buckets"][0]["swap_count"], 6);
    let swaps = get(address, &format!("/v1/pools/{pool}/swaps")).await;
    assert_eq!(swaps.status, 200);
}

fn error_code(response: &Response) -> (u16, &str) {
    let code = response.body["error"]["code"]
        .as_str()
        .expect("an error envelope");
    (response.status, code)
}

// A backfill is refused before any RPC call when its body or instant is wrong, or when the API
// was started without RPC_URL; nothing is inserted.
#[sqlx::test(migrations = "../migrations")]
async fn backfill_rejects_bad_requests_before_resolving(database: PgPool) {
    let address = serve(database.clone()).await;
    for body in [
        "",
        "{}",
        r#"{"from": "yesterday"}"#,
        r#"{"from": "2026-10-04"}"#,
    ] {
        let response = post(address, "/v1/backfills", body).await;
        assert_eq!(
            error_code(&response),
            (400, "invalid_backfill_body"),
            "{body:?}"
        );
    }
    let future = post(
        address,
        "/v1/backfills",
        r#"{"from": "2999-01-01T00:00:00Z"}"#,
    )
    .await;
    assert_eq!(error_code(&future), (400, "backfill_from_in_future"));
    let past = r#"{"from": "2026-10-04T12:00:00Z"}"#;
    let unconfigured = post(address, "/v1/backfills", past).await;
    assert_eq!(error_code(&unconfigured), (503, "backfill_unavailable"));
    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM slot_range_job")
        .fetch_one(&database)
        .await
        .expect("count");
    assert_eq!(job_count, 0);
}

// With nothing indexed there is no range for the job to end below, so the request is refused
// before the bisection: the gateway points at a closed port and is never called.
#[sqlx::test(migrations = "../migrations")]
async fn backfill_conflicts_while_nothing_is_indexed(database: PgPool) {
    let gateway = RpcGateway::new(
        Url::parse("http://127.0.0.1:9").expect("url"),
        Endpoint::Provider,
        RpsMax::new(2),
        TransactionVersionMax::SUPPORTED,
    )
    .expect("gateway");
    let address = serve_with_gateway(database, Some(Arc::new(gateway))).await;
    let response = post(
        address,
        "/v1/backfills",
        r#"{"from": "2026-10-04T12:00:00Z"}"#,
    )
    .await;
    assert_eq!(error_code(&response), (409, "nothing_indexed_yet"));
}

#[derive(Clone, Copy)]
enum SeededJob {
    Open,
    Completed,
    Blocked(&'static str),
}

async fn seed_job(database: &PgPool, (start, end, next): (i64, i64, i64), job: SeededJob) -> i64 {
    let (completed, blocked_reason) = match job {
        SeededJob::Open => (false, None),
        SeededJob::Completed => (true, None),
        SeededJob::Blocked(reason) => (false, Some(reason)),
    };
    sqlx::query_scalar(
        "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, blocked_reason, completed_at)
         VALUES ($1, $2, $3, $4, CASE WHEN $5 THEN now() END)
         RETURNING id",
    )
    .bind(start)
    .bind(end)
    .bind(next)
    .bind(blocked_reason)
    .bind(completed)
    .fetch_one(database)
    .await
    .expect("seed job")
}

#[sqlx::test(migrations = "../migrations")]
async fn cancel_blocks_an_open_job_and_refuses_the_rest(database: PgPool) {
    let open = seed_job(&database, (100, 199, 150), SeededJob::Open).await;
    let completed = seed_job(&database, (300, 399, 400), SeededJob::Completed).await;
    let blocked = seed_job(
        &database,
        (500, 599, 500),
        SeededJob::Blocked("missing_in_storage:510"),
    )
    .await;
    let address = serve(database).await;

    let cancelled = delete(address, &format!("/v1/backfills/{open}")).await;
    assert_eq!(cancelled.status, 200);
    assert_eq!(
        cancelled.body,
        json!({
            "job_id": open, "start_slot": 100, "end_slot": 199, "next_slot": 150,
            "state": "cancelled"
        })
    );
    let again = delete(address, &format!("/v1/backfills/{open}")).await;
    assert_eq!(error_code(&again), (409, "job_already_blocked"));
    assert!(
        again.body["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("(cancelled)"))
    );
    let finished = delete(address, &format!("/v1/backfills/{completed}")).await;
    assert_eq!(error_code(&finished), (409, "job_already_completed"));
    let stuck = delete(address, &format!("/v1/backfills/{blocked}")).await;
    assert_eq!(error_code(&stuck), (409, "job_already_blocked"));
    assert!(
        stuck.body["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("missing_in_storage:510"))
    );
    let unknown = delete(address, "/v1/backfills/999999").await;
    assert_eq!(error_code(&unknown), (404, "job_not_found"));
    let malformed = delete(address, "/v1/backfills/seven").await;
    assert_eq!(error_code(&malformed), (400, "invalid_job_id"));

    let read = get(address, &format!("/v1/backfills/{open}")).await;
    assert_eq!(read.status, 200);
    assert_eq!(read.body["state"], "cancelled");
    assert_eq!(read.body["blocked_reason"], "cancelled");
    let unknown_read = get(address, "/v1/backfills/999999").await;
    assert_eq!(error_code(&unknown_read), (404, "job_not_found"));
}

// The reconciler cuts an unstarted backfill at the archive window, and the id the request was
// given still cancels it: the job kept that id on its piece nearest the tip.
#[sqlx::test(migrations = "../migrations")]
async fn cancel_by_the_requested_id_after_a_split(database: PgPool) {
    sqlx::query(
        "INSERT INTO slot_coverage (start_slot, end_slot, end_block_time)
         VALUES (1000, 1099, to_timestamp(1099))",
    )
    .execute(&database)
    .await
    .expect("seed coverage");
    let requested = seed_job(&database, (100, 999, 100), SeededJob::Open).await;
    let window = ArchiveWindow::new(Slot::new(50), Slot::new(600)).expect("window");
    let mut connection = database.acquire().await.expect("connection");
    let summary = reconcile(&mut connection, Some(window))
        .await
        .expect("reconciles");
    assert_eq!(summary.splits.len(), 1);
    drop(connection);
    let address = serve(database).await;

    let cancelled = delete(address, &format!("/v1/backfills/{requested}")).await;
    assert_eq!(cancelled.status, 200);
    assert_eq!(
        cancelled.body,
        json!({
            "job_id": requested, "start_slot": 601, "end_slot": 999, "next_slot": 601,
            "state": "cancelled"
        })
    );
}

#[sqlx::test(migrations = "../migrations")]
async fn backfill_list_is_newest_first_and_capped_at_fifty(database: PgPool) {
    sqlx::query(
        "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, created_at)
         SELECT i * 1000, i * 1000 + 999, i * 1000,
                '2026-10-01T00:00:00Z'::timestamptz + i * interval '1 minute'
         FROM generate_series(0, 50) AS i",
    )
    .execute(&database)
    .await
    .expect("seed jobs");
    let address = serve(database).await;

    let list = get(address, "/v1/backfills").await;
    assert_eq!(list.status, 200);
    let jobs = list.body["jobs"].as_array().expect("a jobs list");
    assert_eq!(jobs.len(), 50);
    let newest = &jobs[0];
    assert_eq!(
        (
            &newest["start_slot"],
            &newest["end_slot"],
            &newest["next_slot"],
            &newest["end_kind"],
            &newest["state"],
            &newest["blocked_reason"],
            &newest["completed_at"],
            &newest["created_at"],
        ),
        (
            &json!(50_000),
            &json!(50_999),
            &json!(50_000),
            &json!("block"),
            &json!("open"),
            &Value::Null,
            &Value::Null,
            &json!("2026-10-01T00:50:00Z"),
        )
    );
    let starts: Vec<u64> = jobs
        .iter()
        .map(|job| job["start_slot"].as_u64().expect("start_slot"))
        .collect();
    let expected: Vec<u64> = (1..=50).rev().map(|i| i * 1000).collect();
    assert_eq!(starts, expected);
}

// The log key of seeded swap `i`: two slots share each block_time, five transactions share each
// slot and two swaps share each transaction, and the key rises with `i`. Swap 0 is at 01:00,
// the first swap `seed` gives the pool's stats; its last swap, 05:30, is above every seeded one.
async fn seed_swap(database: &PgPool, pool: &str, mint_x: &str, mint_y: &str, i: u8) {
    sqlx::query(
        "INSERT INTO swap (block_time, slot, transaction_index, signature, swap_ordinal, pool,
         user_address, direction, mint_in, mint_out, amount_in, amount_out, fee, protocol_fee,
         host_fee, fee_rate_1e9, start_bin_id, end_bin_id, source)
         VALUES ('2026-10-01T01:00:00Z'::timestamptz + ($1 / 20) * INTERVAL '1 second',
                 100 + $1 / 10, ($1 % 10) / 2, $2, $1 % 2, $3, $4, 'x_to_y', $5, $6,
                 1000, 900, 3, 1, 0, 1000000, 0, 0, 'live_geyser')",
    )
    .bind(i32::from(i))
    .bind(Signature::new([i + 1; 64]).to_string())
    .bind(pool)
    .bind(UserAddress::new([7; 32]).to_string())
    .bind(mint_x)
    .bind(mint_y)
    .execute(database)
    .await
    .expect("seed swap");
}

fn swap_signatures(response: &Response) -> Vec<String> {
    response.body["swaps"]
        .as_array()
        .expect("a swaps list")
        .iter()
        .map(|swap| swap["signature"].as_str().expect("signature").to_owned())
        .collect()
}

fn next_cursor(response: &Response) -> Option<String> {
    response.body["page"]["next_cursor"]
        .as_str()
        .map(str::to_owned)
}

async fn serve_seeded_swaps(database: &PgPool, count: u8) -> (SocketAddr, String) {
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed(database, &pool, &mint_x, &mint_y).await;
    for i in 0..count {
        seed_swap(database, &pool, &mint_x, &mint_y, i).await;
    }
    let address = serve(database.clone()).await;
    (address, format!("/v1/pools/{pool}/swaps"))
}

fn seeded_signatures_newest_first(count: u8) -> Vec<String> {
    (0..count)
        .rev()
        .map(|i| Signature::new([i + 1; 64]).to_string())
        .collect()
}

// Keyset pages of 17 over 50 swaps that tie on block_time and on slot meet exactly: together
// they are the 50 swaps newest first, each once, though the boundaries split a transaction's
// swaps and a newer swap lands between two requests.
#[sqlx::test(migrations = "../migrations")]
async fn swaps_pages_meet_exactly_under_a_head_insert(database: PgPool) {
    let (address, swaps_path) = serve_seeded_swaps(&database, 50).await;

    let first = get(address, &format!("{swaps_path}?limit=17")).await;
    assert_eq!(first.status, 200);
    assert_eq!(first.body["page"]["limit"], 17);
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed_swap(&database, &pool, &mint_x, &mint_y, 50).await;
    let first_cursor = next_cursor(&first).expect("a second page");
    let second = get(
        address,
        &format!("{swaps_path}?limit=17&before={first_cursor}"),
    )
    .await;
    let second_cursor = next_cursor(&second).expect("a third page");
    let third = get(
        address,
        &format!("{swaps_path}?limit=17&before={second_cursor}"),
    )
    .await;
    assert_eq!(third.status, 200);
    assert_eq!(next_cursor(&third), None);
    let paged: Vec<String> = [&first, &second, &third]
        .into_iter()
        .flat_map(swap_signatures)
        .collect();
    assert_eq!(paged, seeded_signatures_newest_first(50));
}

// The last page has a null cursor: one with a single swap left (25 + 25 + 1), one that ends
// exactly on the oldest swap, and one past it.
#[sqlx::test(migrations = "../migrations")]
async fn swaps_last_page_has_a_null_cursor(database: PgPool) {
    let (address, swaps_path) = serve_seeded_swaps(&database, 51).await;
    let expected = seeded_signatures_newest_first(51);

    let half = get(address, &format!("{swaps_path}?limit=25")).await;
    let half_cursor = next_cursor(&half).expect("a second page");
    let rest = get(
        address,
        &format!("{swaps_path}?limit=25&before={half_cursor}"),
    )
    .await;
    let rest_cursor = next_cursor(&rest).expect("a third page");
    let last = get(
        address,
        &format!("{swaps_path}?limit=25&before={rest_cursor}"),
    )
    .await;
    assert_eq!(swap_signatures(&last), vec![expected[50].clone()]);
    assert_eq!(next_cursor(&last), None);
    let exact = get(
        address,
        &format!("{swaps_path}?limit=26&before={half_cursor}"),
    )
    .await;
    assert_eq!(swap_signatures(&exact), expected[25..].to_vec());
    assert_eq!(next_cursor(&exact), None);

    // 0|0|0|0, older than every swap.
    let beyond = get(address, &format!("{swaps_path}?before=MHwwfDB8MA")).await;
    assert_eq!(beyond.status, 200);
    assert_eq!(
        beyond.body["page"],
        json!({"limit": 20, "next_cursor": null})
    );
    assert_eq!(beyond.body["swaps"], json!([]));
}

// A cursor the API did not issue, and a limit outside 1..=100 or not in canonical decimal, are
// refused with the envelope.
#[sqlx::test(migrations = "../migrations")]
async fn swaps_refuse_bad_cursor_and_limit(database: PgPool) {
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed(&database, &pool, &mint_x, &mint_y).await;
    let address = serve(database).await;
    for cursor in ["garbage!", "MHwwfDB8MA%3D%3D", "MXwyfDM"] {
        let response = get(address, &format!("/v1/pools/{pool}/swaps?before={cursor}")).await;
        assert_eq!(error_code(&response), (400, "invalid_cursor"), "{cursor}");
    }
    for limit in ["0", "101", "ten", "%2B5", "05", "1e2", "%205"] {
        let response = get(address, &format!("/v1/pools/{pool}/swaps?limit={limit}")).await;
        assert_eq!(error_code(&response), (400, "invalid_limit"), "{limit}");
    }
    let hundred = get(address, &format!("/v1/pools/{pool}/swaps?limit=100")).await;
    assert_eq!(hundred.status, 200);
}

async fn seed_recent_volume(database: &PgPool, pool: &str, swap_count: i64, volume_usd: &str) {
    sqlx::query(
        "INSERT INTO pool_volume_1h
         (bucket, pool, swap_count, volume_x, volume_y, volume_usd, unpriced_swap_count)
         VALUES (date_trunc('hour', now(), 'UTC'), $1, $2, 1, 1, $3::numeric, 1)",
    )
    .bind(pool)
    .bind(swap_count)
    .bind(volume_usd)
    .execute(database)
    .await
    .expect("seed recent volume");
}

fn pool_addresses(response: &Response) -> Vec<String> {
    response.body["pools"]
        .as_array()
        .expect("a pools list")
        .iter()
        .map(|pool| pool["address"].as_str().expect("address").to_owned())
        .collect()
}

// Offset pages walk the 24-hour USD ranking, a pool with no recent volume last, with the pool
// count as total on every page; an offset past the end is an empty page with the same total;
// no parameters is the first page of 20; a bad limit or offset is refused.
#[sqlx::test(migrations = "../migrations")]
async fn pools_page_by_offset_with_total(database: PgPool) {
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    let pools: Vec<String> = (10..15_u8)
        .map(|byte| PoolAddress::new([byte; 32]).to_string())
        .collect();
    for pool in &pools {
        sqlx::query(
            "INSERT INTO pool (address, mint_x, mint_y, first_seen_slot) VALUES ($1, $2, $3, 1)",
        )
        .bind(pool)
        .bind(&mint_x)
        .bind(&mint_y)
        .execute(&database)
        .await
        .expect("seed pool");
    }
    // Ranked 13, 11, 14, 10, then 12 with no recent volume.
    for (pool, volume_usd) in [(3, "40"), (1, "30"), (4, "20"), (0, "10")] {
        seed_recent_volume(&database, &pools[pool], 2, volume_usd).await;
    }
    let ranked = [3, 1, 4, 0, 2].map(|index| pools[index].clone());
    let address = serve(database).await;

    let mut walked = Vec::new();
    for offset in [0, 2, 4] {
        let page = get(address, &format!("/v1/pools?limit=2&offset={offset}")).await;
        assert_eq!(page.status, 200);
        assert_eq!(
            page.body["page"],
            json!({"limit": 2, "offset": offset, "total": 5})
        );
        walked.extend(pool_addresses(&page));
    }
    assert_eq!(walked, ranked);

    let beyond = get(address, "/v1/pools?limit=2&offset=10").await;
    assert_eq!(beyond.status, 200);
    assert_eq!(beyond.body["pools"], json!([]));
    assert_eq!(
        beyond.body["page"],
        json!({"limit": 2, "offset": 10, "total": 5})
    );
    let default = get(address, "/v1/pools").await;
    assert_eq!(
        default.body["page"],
        json!({"limit": 20, "offset": 0, "total": 5})
    );
    assert_eq!(pool_addresses(&default), ranked);

    for limit in ["0", "101", "-1", "%2B5", "05"] {
        let response = get(address, &format!("/v1/pools?limit={limit}")).await;
        assert_eq!(error_code(&response), (400, "invalid_limit"), "{limit}");
    }
    for offset in ["-1", "1.5", "two", "%2B5", "05", "1e2", "%205", "-0"] {
        let response = get(address, &format!("/v1/pools?offset={offset}")).await;
        assert_eq!(error_code(&response), (400, "invalid_offset"), "{offset}");
    }
}

// The pool route answers one pool's summary, the same object as a list entry, with its 24-hour
// volume from the current hours only; an unknown pool is 404 and a malformed one 400.
#[sqlx::test(migrations = "../migrations")]
async fn pool_route_answers_one_summary(database: PgPool) {
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed(&database, &pool, &mint_x, &mint_y).await;
    seed_recent_volume(&database, &pool, 4, "7.25").await;
    let address = serve(database).await;

    let summary = get(address, &format!("/v1/pools/{pool}")).await;
    assert_eq!(summary.status, 200);
    assert_eq!(
        summary.body,
        json!({
            "address": pool, "mint_x": mint_x, "mint_y": mint_y,
            "decimals_x": 9, "decimals_y": 6,
            "swap_count_24h": 4, "volume_usd_24h": "7.25",
            "first_swap_at": "2026-10-01T01:00:00Z", "last_swap_at": "2026-10-01T05:30:00Z"
        })
    );
    let unknown = get(address, &format!("/v1/pools/{}", PoolAddress::new([9; 32]))).await;
    assert_eq!(error_code(&unknown), (404, "pool_not_found"));
    let malformed = get(address, "/v1/pools/not-a-pool").await;
    assert_eq!(error_code(&malformed), (400, "invalid_pool"));
}
