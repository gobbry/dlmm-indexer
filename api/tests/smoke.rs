use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use dlmm_core::domain::config::{RpsMax, TransactionVersionMax};
use dlmm_core::domain::ids::{MintAddress, PoolAddress};
use dlmm_core::gateway::rpc::{Endpoint, RpcGateway, Url};
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

// The volume endpoint aligns the range, fills empty buckets, applies the null USD rule,
// reads days from the daily projection, and answers unknown pools and bad buckets with the
// envelope.
#[sqlx::test(migrations = "../migrations")]
async fn volume_endpoint_serves_aligned_buckets(database: PgPool) {
    let pool = PoolAddress::new([1; 32]).to_string();
    let mint_x = MintAddress::new([2; 32]).to_string();
    let mint_y = MintAddress::new([3; 32]).to_string();
    seed(&database, &pool, &mint_x, &mint_y).await;
    let address = serve(database).await;

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

// Health answers 200 with "starting" before the indexer has committed any block.
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
    let hourly = get(address, &hourly_path).await;
    assert_eq!(hourly.status, 200);
    assert_eq!(hourly.body["first_swap_at"], Value::Null);
    assert_eq!(hourly.body["buckets"][0]["swap_count"], 3);
    let daily = get(address, &daily_path).await;
    assert_eq!(daily.status, 200);

    mark_building("pool_volume_1h").await;
    let pools = get(address, "/v1/pools").await;
    assert_eq!(pools.status, 503);
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
