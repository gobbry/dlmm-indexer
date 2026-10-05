use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use governor::{DefaultDirectRateLimiter, Jitter, Quota, RateLimiter};
// Re-exported so the API builds a gateway without a reqwest dependency of its own.
pub use reqwest::Url;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::decode::map_rpc_block;
use crate::decode::rpc_json::RpcBlock;
use crate::domain::amounts::Decimals;
use crate::domain::block::FinalizedBlock;
use crate::domain::config::{RpsMax, SLOT_MILLISECONDS_OBSERVED, TransactionVersionMax};
use crate::domain::error::{MapError, RpcError, RpcErrorClass};
use crate::domain::ids::{MintAddress, Slot, SlotRange, UnixSeconds};
use crate::domain::registry::TokenRecord;

// Governor's default burst equals the rate and would spike a provider's window counter.
const BURST_MAX: NonZeroU32 = NonZeroU32::new(2).unwrap();
const LIMITER_JITTER_MAX: Duration = Duration::from_millis(50);
const RETRY_DELAY_MIN: Duration = Duration::from_secs(1);
const RETRY_DELAY_MAX: Duration = Duration::from_secs(30);
// Six attempts: the first plus five retries.
const RETRY_COUNT_MAX: usize = 5;
// A full 8 MB getBlock finishes well inside this; a node that stops answering mid-body must
// not hold a fetch window slot (or the tail) forever.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
// Each pause is capped at RETRY_DELAY_MAX, so this bounds one wait to a few minutes even if
// fresh 429s keep extending it.
const PAUSE_WAIT_COUNT_MAX: usize = 8;
// getMultipleAccounts accepts at most 100 keys per call.
pub const MINTS_PER_REQUEST_MAX: usize = 100;
// SPL Token and Token-2022 mints both keep decimals at byte 44 of the base layout.
const MINT_DECIMALS_OFFSET: usize = 44;
// A null getBlock result means "not available yet" on the nodes we target, so it is
// reported under the code for block-not-available and retried like it.
const CODE_BLOCK_NOT_AVAILABLE: i64 = -32004;
const CODE_BLOCK_CLEANED_UP: i64 = -32001;
const CODE_NODE_UNHEALTHY: i64 = -32005;
const CODE_LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32011;
const CODE_UNSUPPORTED_TRANSACTION_VERSION: i64 = -32015;
const CODE_BLOCK_STATUS_NOT_AVAILABLE_YET: i64 = -32019;
const CODE_INTERNAL_ERROR: i64 = -32603;
// getBlockTime answers these for a slot without a block (skipped, or skipped-or-missing);
// asking again returns the same, so they are an answer, not a failure.
const CODE_SLOT_SKIPPED: i64 = -32007;
const CODE_SLOT_SKIPPED_OR_MISSING: i64 = -32009;
// Bisection probes read getBlocks pages this wide; leader skips run far shorter than this.
const PROBE_PAGE_SLOT_COUNT: u64 = 32;
// Pages scanned for one probe before the search gives up on the node.
const PROBE_PAGE_COUNT_MAX: u32 = 16;
// Probes for one search: a bracket of a few widenings plus a bisection over at most 2^64 slots.
const SEARCH_PROBE_COUNT_MAX: u32 = 64;
// The first bracket reaches this many times the distance estimated at the observed slot pace,
// so a faster stretch of slots does not land the first guess after the target.
const BRACKET_MARGIN: u64 = 2;
const BRACKET_GROWTH: u64 = 4;
const BRACKET_DISTANCE_MIN_SLOTS: u64 = 64;

// Live share of RPC_RPS_MAX, in tenths, rounded up.
const LIVE_SHARE_TENTHS: u32 = 6;

// Which limiter a call pays: the live tail and the filler each own a share of the quota, so
// a long fill can never starve the tail of permits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestLane {
    Live,
    Fill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneShare {
    pub live: RpsMax,
    pub fill: RpsMax,
}

// Live gets ceil(60 percent) and fill the rest, each at least one; the two sum to the total
// for any total of two or more, which the config enforces.
pub fn lane_share(total: RpsMax) -> LaneShare {
    let total = total.get();
    let live = total
        .saturating_mul(LIVE_SHARE_TENTHS)
        .div_ceil(10)
        .min(total.saturating_sub(1))
        .max(1);
    let fill = total.saturating_sub(live).max(1);
    debug_assert!(live >= 1);
    debug_assert!(total < 2 || live + fill == total);
    LaneShare {
        live: RpsMax::new(live),
        fill: RpsMax::new(fill),
    }
}

// Which node a gateway talks to. The archive (an Old Faithful `faithful-cli rpc` server) serves
// the same getBlock shape as a provider but publishes no slot list, answers a skipped slot with
// -32009 and takes tens of seconds per block, so four behaviours differ: listing, the -32009
// class, the fetch window, and what a job does when its end block never arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Provider,
    Archive,
}

// The provider gateway is shared by the live tail, the filler's provider lane and the processor
// (mint decimals); an archive gateway belongs to the filler's archive lane alone.
pub struct RpcGateway {
    client: reqwest::Client,
    url: Url,
    endpoint: Endpoint,
    live_limiter: DefaultDirectRateLimiter,
    fill_limiter: DefaultDirectRateLimiter,
    share: LaneShare,
    // A 429 throttles the whole key, so every in-flight call waits it out, not just the one
    // that saw it.
    pause_until: Mutex<Option<Instant>>,
    transaction_version_max: TransactionVersionMax,
}

impl fmt::Debug for RpcGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcGateway")
            .field("host", &self.url.host_str())
            .field("endpoint", &self.endpoint)
            .field("share", &self.share)
            .field("transaction_version_max", &self.transaction_version_max)
            .finish()
    }
}

impl RpcGateway {
    pub fn new(
        url: Url,
        endpoint: Endpoint,
        rps_max: RpsMax,
        transaction_version_max: TransactionVersionMax,
    ) -> Result<Self, RpcError> {
        let share = lane_share(rps_max);
        let client = reqwest::Client::builder()
            .gzip(true)
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;
        Ok(Self {
            client,
            url,
            endpoint,
            live_limiter: lane_limiter(share.live),
            fill_limiter: lane_limiter(share.fill),
            share,
            pause_until: Mutex::new(None),
            transaction_version_max,
        })
    }

    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    pub fn rps_max(&self, lane: RequestLane) -> RpsMax {
        match lane {
            RequestLane::Live => self.share.live,
            RequestLane::Fill => self.share.fill,
        }
    }

    pub async fn slot_finalized(&self, lane: RequestLane) -> Result<Slot, RpcError> {
        let slot: u64 = self.call(request_slot_finalized(), lane).await?;
        Ok(Slot::new(slot))
    }

    // The oldest slot the node serves; on the archive, the first slot of its oldest epoch.
    pub async fn first_available_block(&self, lane: RequestLane) -> Result<Slot, RpcError> {
        let slot: u64 = self.call(request_first_available_block(), lane).await?;
        Ok(Slot::new(slot))
    }

    // Slots in the range that may hold blocks. A provider lists them (skipped slots are
    // absent); the archive's getBlocks answers null because published epochs carry no slot
    // list, so there every slot is a candidate, at no request, and getBlock sorts them out.
    pub async fn blocks_in_range(
        &self,
        range: SlotRange,
        lane: RequestLane,
    ) -> Result<Vec<Slot>, RpcError> {
        debug_assert!(range.start <= range.end_inclusive);
        if self.endpoint == Endpoint::Archive {
            return Ok(every_slot(range));
        }
        let mut slots: Vec<u64> = self.call(request_blocks_in_range(range), lane).await?;
        // Provider output: an unsorted or duplicated list would wrongly fail the parent-chain
        // check and block the job, so normalise it instead of asserting.
        slots.sort_unstable();
        slots.dedup();
        Ok(slots.into_iter().map(Slot::new).collect())
    }

    // None for a slot that holds no block: answered by the node, so never retried.
    pub async fn block_time(
        &self,
        slot: Slot,
        lane: RequestLane,
    ) -> Result<Option<UnixSeconds>, RpcError> {
        let seconds = self
            .call_parsed(request_block_time(slot), lane, parse_block_time_response)
            .await?;
        Ok(seconds.map(UnixSeconds::new))
    }

    // The first slot holding a block whose time is at or after `target`; None when the
    // finalized tip is still before it. Probes land only on listed blocks, never on a skip.
    pub async fn slot_at_or_after(
        &self,
        target: UnixSeconds,
        lane: RequestLane,
    ) -> Result<Option<Slot>, RpcError> {
        let tip = self.slot_finalized(lane).await?;
        let tip_block = self.block_near(tip, Slot::new(0), lane).await?;
        self.slot_at_or_after_from(tip_block, Slot::new(0), target, lane)
            .await
    }

    // The last block with a time in the probe page ending at `slot`, never below `lowest`.
    pub async fn block_near(
        &self,
        slot: Slot,
        lowest: Slot,
        lane: RequestLane,
    ) -> Result<TimedBlock, RpcError> {
        debug_assert!(lowest <= slot);
        let window = SlotRange {
            start: Slot::new(
                slot.get()
                    .saturating_sub(PROBE_PAGE_SLOT_COUNT - 1)
                    .max(lowest.get()),
            ),
            end_inclusive: slot,
        };
        self.last_timed_block(window, lane)
            .await?
            .ok_or_else(|| search_failed("no block with a time near the requested slot"))
    }

    // The first block with a time at or above `lowest`: how far back a node's history reaches.
    pub async fn first_block_from(
        &self,
        lowest: Slot,
        highest: Slot,
        lane: RequestLane,
    ) -> Result<TimedBlock, RpcError> {
        debug_assert!(lowest <= highest);
        let range = SlotRange {
            start: lowest,
            end_inclusive: highest,
        };
        self.first_timed_block(range, lane)
            .await?
            .ok_or_else(|| search_failed("no block with a time above the lowest slot"))
    }

    // slot_at_or_after from a known tip block, probing nothing below `lowest`: the archive
    // answers a slot outside its epochs with a retried error, not with a skip.
    pub async fn slot_at_or_after_from(
        &self,
        tip_block: TimedBlock,
        lowest: Slot,
        target: UnixSeconds,
        lane: RequestLane,
    ) -> Result<Option<Slot>, RpcError> {
        let Some(mut search) = SlotSearch::start(tip_block, target, lowest) else {
            return Ok(None);
        };
        for _ in 0..SEARCH_PROBE_COUNT_MAX {
            let Some(range) = search.next_probe(target) else {
                break;
            };
            let Some(probe) = self.first_timed_block(range, lane).await? else {
                return Err(search_failed("no block with a time in a probed range"));
            };
            search = search.observe(probe, target);
        }
        match search {
            SlotSearch::Resolved(slot) => Ok(Some(slot)),
            _ => Err(search_failed(
                "probe budget spent before the search resolved",
            )),
        }
    }

    async fn first_timed_block(
        &self,
        range: SlotRange,
        lane: RequestLane,
    ) -> Result<Option<TimedBlock>, RpcError> {
        let mut page_start = range.start;
        for _ in 0..PROBE_PAGE_COUNT_MAX {
            if page_start > range.end_inclusive {
                return Ok(None);
            }
            let page_end = page_start
                .get()
                .saturating_add(PROBE_PAGE_SLOT_COUNT - 1)
                .min(range.end_inclusive.get());
            let page = SlotRange {
                start: page_start,
                end_inclusive: Slot::new(page_end),
            };
            for slot in self.blocks_in_range(page, lane).await? {
                if let Some(block_time) = self.block_time(slot, lane).await? {
                    return Ok(Some(TimedBlock { slot, block_time }));
                }
            }
            page_start = Slot::new(page_end.saturating_add(1));
        }
        Ok(None)
    }

    async fn last_timed_block(
        &self,
        range: SlotRange,
        lane: RequestLane,
    ) -> Result<Option<TimedBlock>, RpcError> {
        for slot in self.blocks_in_range(range, lane).await?.into_iter().rev() {
            if let Some(block_time) = self.block_time(slot, lane).await? {
                return Ok(Some(TimedBlock { slot, block_time }));
            }
        }
        Ok(None)
    }

    pub async fn block(&self, slot: Slot, lane: RequestLane) -> Result<FinalizedBlock, RpcError> {
        let request = request_block(slot, self.transaction_version_max);
        let rpc_block: RpcBlock = self.call(request, lane).await?;
        let block = map_rpc_block(slot, &rpc_block, self.transaction_version_max)?;
        debug_assert_eq!(block.slot, slot);
        Ok(block)
    }

    pub fn classify(&self, error: &RpcError) -> RpcErrorClass {
        classify_rpc_error(self.endpoint, error)
    }

    // Decimals are off the hot path, so they pay the fill lane.
    pub async fn mint_decimals(&self, mints: &[MintAddress]) -> Result<Vec<TokenRecord>, RpcError> {
        let mut records = Vec::with_capacity(mints.len());
        for chunk in mints.chunks(MINTS_PER_REQUEST_MAX) {
            let response: MultipleAccounts = self
                .call(request_mint_decimals(chunk), RequestLane::Fill)
                .await?;
            if response.value.len() != chunk.len() {
                return Err(RpcError::JsonRpc {
                    code: 0,
                    message: format!(
                        "getMultipleAccounts returned {} accounts for {} keys",
                        response.value.len(),
                        chunk.len()
                    ),
                });
            }
            for (mint, account) in chunk.iter().zip(response.value) {
                let decimals = account.and_then(|account| decimals_from_account(&account));
                records.push(TokenRecord {
                    mint: *mint,
                    decimals,
                });
            }
        }
        debug_assert_eq!(records.len(), mints.len());
        Ok(records)
    }

    // The permit is taken inside the retried closure so every attempt pays for itself. The
    // pause is checked again after the limiter admits the call, because a 429 seen by another
    // call while this one queued for a permit throttles this one too.
    async fn call<T: DeserializeOwned>(
        &self,
        request: JsonRpcRequest,
        lane: RequestLane,
    ) -> Result<T, RpcError> {
        self.call_parsed(request, lane, parse_response).await
    }

    async fn call_parsed<T>(
        &self,
        request: JsonRpcRequest,
        lane: RequestLane,
        parse: fn(&[u8]) -> Result<T, RpcError>,
    ) -> Result<T, RpcError> {
        let limiter = match lane {
            RequestLane::Live => &self.live_limiter,
            RequestLane::Fill => &self.fill_limiter,
        };
        let attempt = || async {
            self.await_pause().await;
            limiter
                .until_ready_with_jitter(Jitter::up_to(LIMITER_JITTER_MAX))
                .await;
            self.await_pause().await;
            self.post(&request, parse).await
        };
        with_retry(attempt, request.method).await
    }

    async fn post<T>(
        &self,
        request: &JsonRpcRequest,
        parse: fn(&[u8]) -> Result<T, RpcError>,
    ) -> Result<T, RpcError> {
        let response = self
            .client
            .post(self.url.clone())
            .json(request)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let retry_after_ms = retry_after_ms(response.headers());
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                self.extend_pause(retry_after_ms.map_or(RETRY_DELAY_MIN, Duration::from_millis));
            }
            return Err(RpcError::Status {
                status: status.as_u16(),
                retry_after_ms,
            });
        }
        let body = response.bytes().await?;
        if body.is_empty() {
            return Err(RpcError::EmptyBody);
        }
        parse(&body)
    }

    // Re-read after each sleep: a 429 that lands while this call sleeps pushes the pause out.
    async fn await_pause(&self) {
        for _ in 0..PAUSE_WAIT_COUNT_MAX {
            let pause_until = *self.pause_until_guard();
            match pause_until {
                Some(pause_until) if pause_until > Instant::now() => {
                    tokio::time::sleep_until(pause_until).await;
                }
                _ => return,
            }
        }
    }

    fn extend_pause(&self, pause: Duration) {
        let candidate = Instant::now() + pause.min(RETRY_DELAY_MAX);
        let mut guard = self.pause_until_guard();
        *guard = Some(guard.map_or(candidate, |current| current.max(candidate)));
        tracing::warn!(pause_ms = pause.as_millis() as u64, "rpc_throttled_pause");
    }

    fn pause_until_guard(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        // A panic while holding this lock cannot leave the Option half-written, so a poisoned
        // lock still holds a valid instant.
        self.pause_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn lane_limiter(rate: RpsMax) -> DefaultDirectRateLimiter {
    let rate = NonZeroU32::new(rate.get()).unwrap_or(NonZeroU32::MIN);
    RateLimiter::direct(Quota::per_second(rate).allow_burst(BURST_MAX.min(rate)))
}

fn retry_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(RETRY_DELAY_MIN)
        .with_max_delay(RETRY_DELAY_MAX)
        .with_max_times(RETRY_COUNT_MAX)
        .with_jitter()
}

async fn with_retry<T, Attempt, AttemptFuture>(
    attempt: Attempt,
    method: &'static str,
) -> Result<T, RpcError>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<T, RpcError>>,
{
    attempt
        .retry(retry_policy())
        .when(is_retryable)
        .adjust(adjust_retry_delay)
        .notify(|error, delay| {
            tracing::warn!(method, %error, delay_ms = delay.as_millis() as u64, "rpc_retry");
        })
        .await
}

// A None delay means backon has spent its attempts; Retry-After may reshape a planned wait but
// never revive a finished one, or a provider that always answers 429 is retried forever.
fn adjust_retry_delay(error: &RpcError, delay: Option<Duration>) -> Option<Duration> {
    delay.map(|delay| retry_after(error).unwrap_or(delay))
}

fn retry_after(error: &RpcError) -> Option<Duration> {
    match error {
        RpcError::Status {
            retry_after_ms: Some(milliseconds),
            ..
        } => Some(Duration::from_millis(*milliseconds).min(RETRY_DELAY_MAX)),
        _ => None,
    }
}

// Only the delta-seconds form; an HTTP-date Retry-After falls back to the backoff.
fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let seconds: u64 = headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    seconds.checked_mul(1000)
}

fn is_retryable(error: &RpcError) -> bool {
    match error {
        RpcError::Transport(_) | RpcError::EmptyBody => true,
        RpcError::Status { status, .. } => *status == 429 || *status >= 500,
        RpcError::JsonRpc { code, .. } => matches!(
            *code,
            CODE_BLOCK_NOT_AVAILABLE
                | CODE_BLOCK_STATUS_NOT_AVAILABLE_YET
                | CODE_NODE_UNHEALTHY
                | CODE_INTERNAL_ERROR
        ),
        RpcError::Body(_) | RpcError::Map(_) => false,
    }
}

// For getBlock results only: a body that does not parse, or a block that cannot be mapped (no
// block_time), comes back the same on every retry, so callers treat it as terminal for that
// slot. A getBlocks or getSlot error never names one slot, so callers classify it instead. A
// version above the ceiling is excluded: it is a configuration bug and stops the process.
pub fn is_unmappable(error: &RpcError) -> bool {
    match error {
        RpcError::Body(_) => true,
        RpcError::Map(MapError::TransactionVersion(_)) => false,
        RpcError::Map(_) => true,
        RpcError::Transport(_)
        | RpcError::EmptyBody
        | RpcError::Status { .. }
        | RpcError::JsonRpc { .. } => false,
    }
}

// Whether a failed request still reached a node that answered about what was asked: a JSON-RPC
// error naming the request (an epoch not loaded, a status not ready), or an empty body from a
// read that failed behind it. A connection refused, a timeout, an HTTP status or an unhealthy
// node says only that the node is down, nothing about the slot.
pub fn is_answer_about_request(error: &RpcError) -> bool {
    match error {
        RpcError::JsonRpc { code, .. } => *code != CODE_NODE_UNHEALTHY,
        RpcError::EmptyBody => true,
        RpcError::Transport(_) | RpcError::Status { .. } | RpcError::Body(_) | RpcError::Map(_) => {
            false
        }
    }
}

fn every_slot(range: SlotRange) -> Vec<Slot> {
    (range.start.get()..=range.end_inclusive.get())
        .map(Slot::new)
        .collect()
}

// Agave custom_error.rs codes. -32009 is "skipped, or missing in long-term storage". A provider
// lists before it fetches, so a listed slot answering it reads as missing. The archive lists
// nothing and answers every skipped slot with it, so there it reads as skipped; the filler's
// parent-chain check is the guard against a block the archive truly lacks. -32004 ("epoch not
// available" on the archive) stays a retry on both; the filler bounds how many passes an
// archive job spends on it.
pub fn classify_rpc_error(endpoint: Endpoint, error: &RpcError) -> RpcErrorClass {
    match error {
        RpcError::JsonRpc { code, .. } => match (*code, endpoint) {
            (CODE_SLOT_SKIPPED, _) | (CODE_SLOT_SKIPPED_OR_MISSING, Endpoint::Archive) => {
                RpcErrorClass::SkippedSlot
            }
            (
                CODE_BLOCK_CLEANED_UP
                | CODE_SLOT_SKIPPED_OR_MISSING
                | CODE_LONG_TERM_STORAGE_SLOT_SKIPPED,
                _,
            ) => RpcErrorClass::MissingInStorage,
            (CODE_UNSUPPORTED_TRANSACTION_VERSION, _) => RpcErrorClass::ConfigurationBug,
            _ => RpcErrorClass::Retry,
        },
        RpcError::Map(MapError::TransactionVersion(_)) => RpcErrorClass::ConfigurationBug,
        _ => RpcErrorClass::Retry,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct JsonRpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: Value,
}

impl JsonRpcRequest {
    fn new(method: &'static str, params: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        }
    }
}

fn request_slot_finalized() -> JsonRpcRequest {
    JsonRpcRequest::new("getSlot", json!([{ "commitment": "finalized" }]))
}

// No commitment parameter: the archive rejects one here, and the answer is the same anyway.
fn request_first_available_block() -> JsonRpcRequest {
    JsonRpcRequest::new("getFirstAvailableBlock", json!([]))
}

fn request_blocks_in_range(range: SlotRange) -> JsonRpcRequest {
    JsonRpcRequest::new(
        "getBlocks",
        json!([
            range.start.get(),
            range.end_inclusive.get(),
            { "commitment": "finalized" }
        ]),
    )
}

fn request_block_time(slot: Slot) -> JsonRpcRequest {
    JsonRpcRequest::new("getBlockTime", json!([slot.get()]))
}

fn request_block(slot: Slot, transaction_version_max: TransactionVersionMax) -> JsonRpcRequest {
    JsonRpcRequest::new(
        "getBlock",
        json!([
            slot.get(),
            {
                "commitment": "finalized",
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": false,
                "maxSupportedTransactionVersion": transaction_version_max.get(),
            }
        ]),
    )
}

fn request_mint_decimals(mints: &[MintAddress]) -> JsonRpcRequest {
    debug_assert!(mints.len() <= MINTS_PER_REQUEST_MAX);
    let keys: Vec<String> = mints.iter().map(ToString::to_string).collect();
    JsonRpcRequest::new(
        "getMultipleAccounts",
        json!([
            keys,
            {
                "commitment": "finalized",
                "encoding": "base64",
                "dataSlice": { "offset": MINT_DECIMALS_OFFSET, "length": 1 },
            }
        ]),
    )
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcErrorBody>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcErrorBody {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct MultipleAccounts {
    value: Vec<Option<AccountSlice>>,
}

#[derive(Debug, Deserialize)]
struct AccountSlice {
    // [base64 text, "base64"]
    data: (String, String),
}

fn parse_response<T: DeserializeOwned>(body: &[u8]) -> Result<T, RpcError> {
    let response: JsonRpcResponse<T> = serde_json::from_slice(body)?;
    if let Some(error) = response.error {
        return Err(RpcError::JsonRpc {
            code: error.code,
            message: error.message,
        });
    }
    response.result.ok_or_else(|| RpcError::JsonRpc {
        code: CODE_BLOCK_NOT_AVAILABLE,
        message: "null result".to_owned(),
    })
}

// getBlockTime's null result and its skipped-slot codes all mean "no block here", unlike
// parse_response, which reads a null as not-available-yet and retries it.
fn parse_block_time_response(body: &[u8]) -> Result<Option<i64>, RpcError> {
    let response: JsonRpcResponse<i64> = serde_json::from_slice(body)?;
    match response.error {
        Some(error)
            if error.code == CODE_SLOT_SKIPPED || error.code == CODE_SLOT_SKIPPED_OR_MISSING =>
        {
            Ok(None)
        }
        Some(error) => Err(RpcError::JsonRpc {
            code: error.code,
            message: error.message,
        }),
        None => Ok(response.result),
    }
}

// A search that cannot finish is the node's fault (a hole in its storage, a clock that runs
// backwards), so it surfaces as an error that names no JSON-RPC code.
fn search_failed(reason: &str) -> RpcError {
    RpcError::JsonRpc {
        code: 0,
        message: format!("slot search: {reason}"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimedBlock {
    pub slot: Slot,
    pub block_time: UnixSeconds,
}

// The pure half of slot_at_or_after. `answer` is always a block whose time is at or after the
// target, and the first block at or after `high` is `answer`; the search narrows the bracket
// until no earlier block can qualify. Bracketing walks back from the tip until a probe lands
// before the target. Narrowing then guesses from the clock, because slots tick at a near-steady
// pace and a guess lands within seconds of the boundary where halving would need a probe per
// bit of the distance; a guess that stays on one side gallops toward the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotSearch {
    Bracketing {
        low: Slot,
        high: Slot,
        answer: TimedBlock,
        distance_slots: u64,
        // Nothing below this is probed; a bracket that reaches it and still qualifies resolves.
        lowest: Slot,
    },
    Narrowing {
        // The latest block known to be before the target: the bracket starts right after it.
        before: TimedBlock,
        high: Slot,
        answer: TimedBlock,
        step: NarrowStep,
    },
    Resolved(Slot),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BracketEnd {
    Before,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NarrowStep {
    // From the clock between the bracket's ends; halving when the clock gives no slope.
    Interpolate,
    // The last probe moved `from`; step in from it, doubling while probes keep landing there.
    Gallop {
        from: BracketEnd,
        distance_slots: u64,
    },
}

impl SlotSearch {
    fn start(tip: TimedBlock, target: UnixSeconds, lowest: Slot) -> Option<Self> {
        debug_assert!(lowest <= tip.slot);
        if tip.block_time < target {
            return None;
        }
        let seconds_before_tip =
            u64::try_from(tip.block_time.get().saturating_sub(target.get())).unwrap_or(0);
        let estimate_slots = seconds_before_tip.saturating_mul(1000) / SLOT_MILLISECONDS_OBSERVED;
        let distance_slots = estimate_slots
            .saturating_mul(BRACKET_MARGIN)
            .max(BRACKET_DISTANCE_MIN_SLOTS);
        Some(Self::Bracketing {
            low: bracket_low(tip.slot, distance_slots, lowest),
            high: tip.slot,
            answer: tip,
            distance_slots,
            lowest,
        })
    }

    // The range whose first timed block the shell must report next; None once resolved.
    fn next_probe(self, target: UnixSeconds) -> Option<SlotRange> {
        let (start, answer) = match self {
            Self::Bracketing { low, answer, .. } => (low, answer),
            Self::Narrowing {
                before,
                high,
                answer,
                step,
            } => (narrowing_probe(before, high, answer, step, target), answer),
            Self::Resolved(_) => return None,
        };
        debug_assert!(start <= answer.slot);
        Some(SlotRange {
            start,
            end_inclusive: answer.slot,
        })
    }

    // `probe` is the first block with a time at or after the start of `next_probe`.
    fn observe(self, probe: TimedBlock, target: UnixSeconds) -> Self {
        let Some(range) = self.next_probe(target) else {
            return self;
        };
        debug_assert!(probe.slot >= range.start);
        debug_assert!(probe.slot <= range.end_inclusive);
        let qualifies = probe.block_time >= target;
        match self {
            Self::Bracketing { high, answer, .. } if !qualifies => {
                Self::narrowing(probe, high, answer, NarrowStep::Interpolate)
            }
            Self::Bracketing { low, lowest, .. } if low <= lowest => Self::Resolved(probe.slot),
            Self::Bracketing {
                low,
                distance_slots,
                lowest,
                ..
            } => {
                let distance_slots = distance_slots.saturating_mul(BRACKET_GROWTH);
                Self::Bracketing {
                    low: bracket_low(low, distance_slots, lowest),
                    high: low,
                    answer: probe,
                    distance_slots,
                    lowest,
                }
            }
            Self::Narrowing {
                before,
                high,
                answer,
                step,
            } => {
                let moved = if qualifies {
                    BracketEnd::High
                } else {
                    BracketEnd::Before
                };
                let step = next_step(step, moved, slots_per_second(before, high, answer));
                match moved {
                    BracketEnd::High => Self::narrowing(before, range.start, probe, step),
                    BracketEnd::Before => Self::narrowing(probe, high, answer, step),
                }
            }
            Self::Resolved(_) => self,
        }
    }

    // A clock that runs backwards can push the bracket shut; the answer still qualifies.
    fn narrowing(before: TimedBlock, high: Slot, answer: TimedBlock, step: NarrowStep) -> Self {
        if before.slot.get() + 1 >= high.get() {
            return Self::Resolved(answer.slot);
        }
        Self::Narrowing {
            before,
            high,
            answer,
            step,
        }
    }
}

fn bracket_low(from: Slot, distance_slots: u64, lowest: Slot) -> Slot {
    Slot::new(from.get().saturating_sub(distance_slots).max(lowest.get()))
}

fn narrowing_probe(
    before: TimedBlock,
    high: Slot,
    answer: TimedBlock,
    step: NarrowStep,
    target: UnixSeconds,
) -> Slot {
    let low = before.slot.get() + 1;
    debug_assert!(low < high.get());
    let guess = match step {
        NarrowStep::Interpolate => interpolate(before, high, answer.block_time, target),
        NarrowStep::Gallop {
            from: BracketEnd::Before,
            distance_slots,
        } => Some(before.slot.get().saturating_add(distance_slots)),
        NarrowStep::Gallop {
            from: BracketEnd::High,
            distance_slots,
        } => Some(high.get().saturating_sub(distance_slots)),
    };
    let midpoint = low + (high.get() - low) / 2;
    Slot::new(guess.unwrap_or(midpoint).clamp(low, high.get() - 1))
}

// Interpolating again only pays once the probes have crossed the boundary from both sides.
fn next_step(step: NarrowStep, moved: BracketEnd, slots_per_second: u64) -> NarrowStep {
    match step {
        NarrowStep::Interpolate => NarrowStep::Gallop {
            from: moved,
            distance_slots: slots_per_second,
        },
        NarrowStep::Gallop {
            from,
            distance_slots,
        } if from == moved => NarrowStep::Gallop {
            from,
            distance_slots: distance_slots.saturating_mul(2),
        },
        NarrowStep::Gallop { .. } => NarrowStep::Interpolate,
    }
}

// The bracket's own pace, at least one slot; `high` stands in for the answer's slot because
// the slots between them hold no block.
fn slots_per_second(before: TimedBlock, high: Slot, answer: TimedBlock) -> u64 {
    let span_seconds = answer
        .block_time
        .get()
        .saturating_sub(before.block_time.get());
    let span_slots = high.get().saturating_sub(before.slot.get());
    match u64::try_from(span_seconds) {
        Ok(seconds) if seconds > 0 => span_slots.div_ceil(seconds).max(1),
        _ => 1,
    }
}

// Where the clock puts the target between the last block before it and `high`, aimed half a
// second early: block times are whole seconds, so the first qualifying block follows the last
// block of the second before. None when the clock gives no slope.
fn interpolate(
    before: TimedBlock,
    high: Slot,
    high_time: UnixSeconds,
    target: UnixSeconds,
) -> Option<u64> {
    let span_seconds = i128::from(high_time.get()) - i128::from(before.block_time.get());
    let span_slots = i128::from(high.get()) - i128::from(before.slot.get());
    if span_seconds <= 0 || span_slots <= 0 {
        return None;
    }
    let half_seconds_after_before =
        2 * (i128::from(target.get()) - i128::from(before.block_time.get())) - 1;
    let offset_slots = half_seconds_after_before.max(0) * span_slots / (2 * span_seconds);
    u64::try_from(i128::from(before.slot.get()) + offset_slots).ok()
}

// An account that is not a mint (wrong length, closed) has no decimals byte.
fn decimals_from_account(account: &AccountSlice) -> Option<Decimals> {
    let bytes = BASE64.decode(&account.data.0).ok()?;
    match bytes.as_slice() {
        [decimals] => Some(Decimals::new(*decimals)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_rpc_error(code: i64) -> RpcError {
        RpcError::JsonRpc {
            code,
            message: String::new(),
        }
    }

    // Each agave error code lands in the class the filler acts on, on either endpoint.
    #[test]
    fn classify_rpc_error_maps_agave_codes() {
        let cases = [
            (-32007, RpcErrorClass::SkippedSlot),
            (-32009, RpcErrorClass::MissingInStorage),
            (-32001, RpcErrorClass::MissingInStorage),
            (-32011, RpcErrorClass::MissingInStorage),
            (-32015, RpcErrorClass::ConfigurationBug),
            (-32004, RpcErrorClass::Retry),
            (-32019, RpcErrorClass::Retry),
            (-32005, RpcErrorClass::Retry),
            (-32603, RpcErrorClass::Retry),
        ];
        for (code, class) in cases {
            assert_eq!(
                classify_rpc_error(Endpoint::Provider, &json_rpc_error(code)),
                class,
                "code {code}"
            );
            if code != -32009 {
                assert_eq!(
                    classify_rpc_error(Endpoint::Archive, &json_rpc_error(code)),
                    class,
                    "archive code {code}"
                );
            }
        }
        let version = RpcError::Map(MapError::TransactionVersion(2));
        assert_eq!(
            classify_rpc_error(Endpoint::Provider, &version),
            RpcErrorClass::ConfigurationBug
        );
        let throttled = RpcError::Status {
            status: 429,
            retry_after_ms: Some(2000),
        };
        assert_eq!(
            classify_rpc_error(Endpoint::Provider, &throttled),
            RpcErrorClass::Retry
        );
    }

    // The archive answers every skipped slot with -32009, while a provider only returns it for
    // a slot it listed and then lost, so the one code means skipped on one and missing on the
    // other; an unloaded epoch (-32004 on the archive) is retried later on both.
    #[test]
    fn skipped_or_missing_code_is_skipped_on_archive_and_missing_on_provider() {
        let skipped_or_missing = json_rpc_error(-32009);
        assert_eq!(
            classify_rpc_error(Endpoint::Archive, &skipped_or_missing),
            RpcErrorClass::SkippedSlot
        );
        assert_eq!(
            classify_rpc_error(Endpoint::Provider, &skipped_or_missing),
            RpcErrorClass::MissingInStorage
        );
        let epoch_not_loaded = RpcError::JsonRpc {
            code: -32004,
            message: "Epoch 1048 is not available".to_owned(),
        };
        assert_eq!(
            classify_rpc_error(Endpoint::Archive, &epoch_not_loaded),
            RpcErrorClass::Retry
        );
    }

    fn gateway(url: &str, endpoint: Endpoint) -> RpcGateway {
        RpcGateway::new(
            url.parse().expect("url"),
            endpoint,
            RpsMax::new(100),
            TransactionVersionMax::new(1),
        )
        .expect("gateway")
    }

    // The archive publishes no slot list, so its page is every slot of the range and costs no
    // request; a provider's page is what its getBlocks lists.
    #[tokio::test]
    async fn archive_page_is_every_slot_without_a_request() {
        let request_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = std::sync::Arc::clone(&request_count);
        let url = crate::actor::fake_rpc_node::spawn_fake_rpc_node(move |method, _| {
            counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            assert_eq!(method, "getBlocks");
            Ok(json!([11, 13]))
        });
        let range = SlotRange {
            start: Slot::new(10),
            end_inclusive: Slot::new(14),
        };
        let archive_page = gateway(&url, Endpoint::Archive)
            .blocks_in_range(range, RequestLane::Fill)
            .await
            .expect("archive page");
        assert_eq!(archive_page, (10..=14).map(Slot::new).collect::<Vec<_>>());
        assert_eq!(request_count.load(std::sync::atomic::Ordering::Relaxed), 0);
        let provider_page = gateway(&url, Endpoint::Provider)
            .blocks_in_range(range, RequestLane::Fill)
            .await
            .expect("provider page");
        assert_eq!(provider_page, vec![Slot::new(11), Slot::new(13)]);
        assert_eq!(request_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    // Only transient failures are retried in place; terminal ones surface at once.
    #[test]
    fn retry_predicate_covers_transient_failures_only() {
        assert!(is_retryable(&RpcError::Status {
            status: 503,
            retry_after_ms: None
        }));
        assert!(!is_retryable(&RpcError::Status {
            status: 401,
            retry_after_ms: None
        }));
        assert!(is_retryable(&json_rpc_error(-32004)));
        assert!(!is_retryable(&json_rpc_error(-32007)));
        assert!(!is_retryable(&json_rpc_error(-32015)));
        let throttled = RpcError::Status {
            status: 429,
            retry_after_ms: Some(3000),
        };
        assert_eq!(retry_after(&throttled), Some(Duration::from_secs(3)));
    }

    // An endpoint that answers 429 forever is given up on after the configured retries.
    #[tokio::test]
    async fn throttled_stream_stops_after_retry_count_max() {
        let attempt_count = std::sync::atomic::AtomicUsize::new(0);
        let attempt = || async {
            attempt_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err::<(), _>(RpcError::Status {
                status: 429,
                retry_after_ms: Some(0),
            })
        };
        let result = with_retry(attempt, "getSlot").await;
        assert!(matches!(result, Err(RpcError::Status { status: 429, .. })));
        assert_eq!(
            attempt_count.load(std::sync::atomic::Ordering::Relaxed),
            RETRY_COUNT_MAX + 1
        );
        let throttled = RpcError::Status {
            status: 429,
            retry_after_ms: Some(2000),
        };
        assert_eq!(adjust_retry_delay(&throttled, None), None);
    }

    // Live always gets the larger share and the two shares add up to the configured total.
    #[test]
    fn lane_share_reserves_live_majority() {
        let cases = [(2, 1, 1), (3, 2, 1), (4, 3, 1), (10, 6, 4), (11, 7, 4)];
        for (total, live, fill) in cases {
            let share = lane_share(RpsMax::new(total));
            assert_eq!(share.live.get(), live, "total {total}");
            assert_eq!(share.fill.get(), fill, "total {total}");
        }
    }

    // Unparseable bodies and unmappable blocks are terminal; a version overflow is not theirs.
    #[test]
    fn unmappable_covers_body_and_block_mapping_only() {
        let slot = Slot::new(5);
        assert!(is_unmappable(&RpcError::Map(MapError::BlockTimeMissing {
            slot
        })));
        let body = serde_json::from_slice::<u64>(b"{").unwrap_err();
        assert!(is_unmappable(&RpcError::Body(body)));
        assert!(!is_unmappable(&RpcError::Map(
            MapError::TransactionVersion(2)
        )));
        assert!(!is_unmappable(&json_rpc_error(-32004)));
        assert!(!is_unmappable(&RpcError::EmptyBody));
        assert!(is_retryable(&RpcError::EmptyBody));
    }

    // getBlock asks for the full json shape with the configured version ceiling.
    #[test]
    fn block_request_carries_version_ceiling() {
        let request = request_block(Slot::new(42), TransactionVersionMax::new(1));
        let expected = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlock",
            "params": [42, {
                "commitment": "finalized",
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": false,
                "maxSupportedTransactionVersion": 1,
            }],
        });
        assert_eq!(serde_json::to_value(&request).unwrap(), expected);
    }

    // getBlocks and getSlot are pinned to finalized commitment.
    #[test]
    fn slot_requests_use_finalized_commitment() {
        let range = SlotRange {
            start: Slot::new(10),
            end_inclusive: Slot::new(20),
        };
        let blocks = serde_json::to_value(request_blocks_in_range(range)).unwrap();
        assert_eq!(
            blocks["params"],
            json!([10, 20, { "commitment": "finalized" }])
        );
        let slot = serde_json::to_value(request_slot_finalized()).unwrap();
        assert_eq!(slot["method"], "getSlot");
        assert_eq!(slot["params"], json!([{ "commitment": "finalized" }]));
    }

    // Decimals come from one sliced byte; a missing or non-mint account yields none.
    #[test]
    fn mint_decimals_reads_sliced_byte() {
        let mint: MintAddress = "So11111111111111111111111111111111111111112"
            .parse()
            .unwrap();
        let request = serde_json::to_value(request_mint_decimals(&[mint])).unwrap();
        assert_eq!(
            request["params"][1]["dataSlice"],
            json!({ "offset": 44, "length": 1 })
        );
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},
            "value":[{"data":["CQ==","base64"],"owner":"x"},null,{"data":["","base64"]}]}}"#;
        let accounts: MultipleAccounts = parse_response(body).unwrap();
        let decimals: Vec<Option<Decimals>> = accounts
            .value
            .iter()
            .map(|account| account.as_ref().and_then(decimals_from_account))
            .collect();
        assert_eq!(decimals, vec![Some(Decimals::new(9)), None, None]);
    }

    // getBlockTime's null and skip codes are "no block", answered once; other errors stay
    // errors so the retry policy sees them.
    #[test]
    fn block_time_reads_null_and_skip_codes_as_no_block() {
        let time = parse_block_time_response(br#"{"jsonrpc":"2.0","id":1,"result":1790000000}"#);
        assert_eq!(time.unwrap(), Some(1_790_000_000));
        let null = parse_block_time_response(br#"{"jsonrpc":"2.0","id":1,"result":null}"#);
        assert_eq!(null.unwrap(), None);
        for code in [-32007, -32009] {
            let body =
                format!(r#"{{"jsonrpc":"2.0","id":1,"error":{{"code":{code},"message":"x"}}}}"#);
            assert_eq!(parse_block_time_response(body.as_bytes()).unwrap(), None);
        }
        let body = br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"x"}}"#;
        let error = parse_block_time_response(body).unwrap_err();
        assert!(is_retryable(&error));
    }

    // A chain where `times[slot]` is the block time, None for a skipped slot.
    fn first_timed_block(times: &[Option<i64>], range: SlotRange) -> Option<TimedBlock> {
        (range.start.get()..=range.end_inclusive.get()).find_map(|slot| {
            let block_time = (*times.get(usize::try_from(slot).ok()?)?)?;
            Some(TimedBlock {
                slot: Slot::new(slot),
                block_time: UnixSeconds::new(block_time),
            })
        })
    }

    // The resolved slot and how many probes it took.
    fn search(times: &[Option<i64>], target: UnixSeconds) -> (Option<Slot>, u32) {
        let tip_slot = times.iter().rposition(Option::is_some).expect("a tip");
        let tip = TimedBlock {
            slot: Slot::new(tip_slot as u64),
            block_time: UnixSeconds::new(times[tip_slot].expect("a tip time")),
        };
        let Some(mut search) = SlotSearch::start(tip, target, Slot::new(0)) else {
            return (None, 0);
        };
        let mut probe_count = 0;
        for _ in 0..SEARCH_PROBE_COUNT_MAX {
            let Some(range) = search.next_probe(target) else {
                break;
            };
            let probe = first_timed_block(times, range).expect("a probe range ends on a block");
            search = search.observe(probe, target);
            probe_count += 1;
        }
        match search {
            SlotSearch::Resolved(slot) => (Some(slot), probe_count),
            other => panic!("search did not resolve: {other:?}"),
        }
    }

    // Every target resolves to the first block at or after it, skipping runs of empty slots,
    // and a target past the tip resolves to nothing. Bracketing plus bisection would spend
    // about 17 probes on this chain; the clock-guided search needs at most 11, and that only
    // for a target inside the 100-slot skipped run, where the clock says nothing.
    #[test]
    fn slot_search_lands_on_first_block_at_or_after_target() {
        // Eight slots a second, twice mainnet's pace, so the first bracket must widen; a long
        // skipped run and a skip every seventh slot.
        let times: Vec<Option<i64>> = (0..20_000_i64)
            .map(|slot| {
                let skipped = slot % 7 == 3 || (9_000..9_100).contains(&slot);
                (!skipped).then_some(1_000 + slot / 8)
            })
            .collect();
        for target in (990..3_600).step_by(7) {
            let target = UnixSeconds::new(target);
            let expected = times
                .iter()
                .position(|time| time.is_some_and(|time| time >= target.get()))
                .map(|slot| Slot::new(slot as u64));
            let (resolved, probe_count) = search(&times, target);
            assert_eq!(resolved, expected, "target {}", target.get());
            assert!(
                probe_count <= 11,
                "target {}: {probe_count} probes",
                target.get()
            );
        }
        let skipped_run_start = UnixSeconds::new(1_000 + 9_000 / 8);
        assert_eq!(search(&times, skipped_run_start).0, Some(Slot::new(9_100)));
        assert_eq!(search(&times, UnixSeconds::new(1_000 + 20_000 / 8)).0, None);
    }

    // A JSON-RPC error inside HTTP 200 and a null result are both errors, not data.
    #[test]
    fn parse_response_surfaces_errors_and_null() {
        let body = br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32007,"message":"skipped"}}"#;
        let error = parse_response::<u64>(body).unwrap_err();
        assert_eq!(
            classify_rpc_error(Endpoint::Provider, &error),
            RpcErrorClass::SkippedSlot
        );
        let null = parse_response::<u64>(br#"{"jsonrpc":"2.0","id":1,"result":null}"#);
        assert!(matches!(null, Err(RpcError::JsonRpc { code: -32004, .. })));
        let slot: u64 = parse_response(br#"{"jsonrpc":"2.0","id":1,"result":7}"#).unwrap();
        assert_eq!(slot, 7);
    }
}
