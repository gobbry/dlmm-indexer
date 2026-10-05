use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{Client, StatusCode, Url};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::time::Instant;
use tracing::warn;

use crate::domain::amounts::{PriceUsd, QuoteAsset};
use crate::domain::error::PriceError;
use crate::domain::ids::{MinuteRange, UnixSeconds};
use crate::domain::price::{PricePoint, PriceSource};

// Joined onto BINANCE_DATA_API_URL; main validates the join at config time.
pub const KLINES_PATH: &str = "api/v3/klines";
const MINUTE_SECONDS: i64 = 60;
const MINUTE_MS: i64 = 60_000;
const KLINES_PER_REQUEST_MAX: i64 = 1000;
const KLINE_FIELD_COUNT_MIN: usize = 7;
const WEIGHT_PER_MINUTE_MAX: NonZeroU32 = NonZeroU32::new(6000).unwrap();
// Burst 2 admits exactly one klines call at a time, so our own spend never spikes Binance's
// one-minute window counter.
const WEIGHT_BURST_MAX: NonZeroU32 = NonZeroU32::new(2).unwrap();
const KLINES_WEIGHT: NonZeroU32 = NonZeroU32::new(2).unwrap();
// Pause before Binance does it for us: at 80 percent of the window the next calls wait for
// the window to roll over instead of risking a 429 and then a 418 ban.
const WEIGHT_USED_1M_PAUSE_THRESHOLD: u32 = 4800;
const HEADER_WEIGHT_USED_1M: &str = "x-mbx-used-weight-1m";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY_MIN: Duration = Duration::from_secs(1);
const RETRY_DELAY_MAX: Duration = Duration::from_secs(30);
// Five attempts in total: the first plus four retries.
const RETRY_COUNT_MAX: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct UnixMillis(i64);

impl UnixMillis {
    fn now() -> Self {
        Self(chrono::Utc::now().timestamp_millis())
    }

    fn seconds(self) -> UnixSeconds {
        UnixSeconds::new(self.0.div_euclid(1000))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Kline {
    open_time: UnixMillis,
    close_time: UnixMillis,
    close: PriceUsd,
}

#[derive(Debug)]
pub struct BinanceGateway {
    client: Client,
    klines_url: Url,
    limiter: DefaultDirectRateLimiter,
    pause_until: Mutex<Option<Instant>>,
}

impl BinanceGateway {
    pub fn new(klines_url: Url) -> Result<Self, PriceError> {
        let client = Client::builder()
            .gzip(true)
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        let quota = Quota::per_minute(WEIGHT_PER_MINUTE_MAX).allow_burst(WEIGHT_BURST_MAX);
        Ok(Self {
            client,
            klines_url,
            limiter: RateLimiter::direct(quota),
            pause_until: Mutex::new(None),
        })
    }

    // Closed one-minute candles only; the still-open candle is never returned. `range` holds
    // candle open minutes; each point's ts is its candle's close, one minute later.
    pub async fn klines(
        &self,
        asset: QuoteAsset,
        range: MinuteRange,
    ) -> Result<Vec<PricePoint>, PriceError> {
        debug_assert!(range.start <= range.end_inclusive);
        debug_assert_eq!(range.start.get().rem_euclid(MINUTE_SECONDS), 0);
        let Some(symbol) = symbol_of(asset) else {
            return Ok(peg_points(range, UnixMillis::now()));
        };
        let mut points = Vec::new();
        for chunk in minute_chunks(range) {
            points.extend(self.klines_chunk_retried(asset, symbol, chunk).await?);
        }
        Ok(points)
    }

    async fn klines_chunk_retried(
        &self,
        asset: QuoteAsset,
        symbol: &'static str,
        chunk: MinuteRange,
    ) -> Result<Vec<PricePoint>, PriceError> {
        let policy = ExponentialBuilder::default()
            .with_min_delay(RETRY_DELAY_MIN)
            .with_max_delay(RETRY_DELAY_MAX)
            .with_jitter()
            .with_max_times(RETRY_COUNT_MAX);
        (|| self.klines_chunk(asset, symbol, chunk))
            .retry(policy)
            .when(is_retryable)
            .adjust(adjust_for_retry_after)
            .notify(|error, delay| {
                warn!(symbol, error = %error, delay_ms = delay.as_millis(), "binance_klines_retry");
            })
            .await
    }

    async fn klines_chunk(
        &self,
        asset: QuoteAsset,
        symbol: &'static str,
        chunk: MinuteRange,
    ) -> Result<Vec<PricePoint>, PriceError> {
        self.await_pause().await;
        let admission = self.limiter.until_n_ready(KLINES_WEIGHT).await;
        // Only fails when the weight exceeds the burst, both of which are constants above.
        debug_assert!(admission.is_ok());
        let response = self
            .client
            .get(klines_request_url(&self.klines_url, symbol, chunk))
            .send()
            .await?;
        let status = response.status();
        let retry_after_ms = retry_after_ms_from_headers(response.headers());
        let pause_ms = pause_ms_after_response(
            status,
            weight_used_1m_from_headers(response.headers()),
            retry_after_ms,
            UnixMillis::now(),
        );
        if let Some(pause_ms) = pause_ms {
            self.extend_pause(pause_ms);
        }
        if !status.is_success() {
            return Err(PriceError::Status {
                status: status.as_u16(),
                retry_after_ms,
            });
        }
        let body = response.bytes().await?;
        parse_klines(&body, asset, UnixMillis::now())
    }

    async fn await_pause(&self) {
        let pause_until = *self.pause_until_guard();
        if let Some(pause_until) = pause_until {
            tokio::time::sleep_until(pause_until).await;
        }
    }

    fn extend_pause(&self, pause_ms: u64) {
        let candidate = Instant::now() + Duration::from_millis(pause_ms);
        let mut guard = self.pause_until_guard();
        let extended = guard.map_or(candidate, |current| current.max(candidate));
        *guard = Some(extended);
        warn!(pause_ms, "binance_weight_pause");
    }

    fn pause_until_guard(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        // A panic while holding this lock cannot leave the Option half-written, so a poisoned
        // lock still holds a valid instant.
        self.pause_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

const fn symbol_of(asset: QuoteAsset) -> Option<&'static str> {
    match asset {
        QuoteAsset::Sol => Some("SOLUSDT"),
        QuoteAsset::Usdc => Some("USDCUSDT"),
        QuoteAsset::Usdt => None,
    }
}

fn klines_request_url(klines_url: &Url, symbol: &str, chunk: MinuteRange) -> Url {
    let mut url = klines_url.clone();
    url.query_pairs_mut()
        .append_pair("symbol", symbol)
        .append_pair("interval", "1m")
        .append_pair("startTime", &(chunk.start.get() * 1000).to_string())
        .append_pair("endTime", &(chunk.end_inclusive.get() * 1000).to_string())
        .append_pair("limit", &KLINES_PER_REQUEST_MAX.to_string());
    url
}

fn minute_count(range: MinuteRange) -> i64 {
    (range.end_inclusive.get() - range.start.get()) / MINUTE_SECONDS + 1
}

fn minute_chunks(range: MinuteRange) -> Vec<MinuteRange> {
    debug_assert!(range.start <= range.end_inclusive);
    let chunk_count = (minute_count(range) + KLINES_PER_REQUEST_MAX - 1) / KLINES_PER_REQUEST_MAX;
    let chunks: Vec<MinuteRange> = (0..chunk_count)
        .map(|chunk_index| {
            let start = range.start.get() + chunk_index * KLINES_PER_REQUEST_MAX * MINUTE_SECONDS;
            let end = start + (KLINES_PER_REQUEST_MAX - 1) * MINUTE_SECONDS;
            MinuteRange {
                start: UnixSeconds::new(start),
                end_inclusive: UnixSeconds::new(end.min(range.end_inclusive.get())),
            }
        })
        .collect();
    debug_assert_eq!(
        chunks.last().map(|chunk| chunk.end_inclusive),
        Some(range.end_inclusive)
    );
    chunks
}

// USDT is the unit of account, so its price is 1 by definition, for closed minutes only to
// keep the same contract as the candle path.
fn peg_points(range: MinuteRange, now: UnixMillis) -> Vec<PricePoint> {
    let now_seconds = now.seconds().get();
    (0..minute_count(range))
        .map(|minute_index| range.start.get() + minute_index * MINUTE_SECONDS)
        .take_while(|minute| minute + MINUTE_SECONDS <= now_seconds)
        .map(|minute| PricePoint {
            asset: QuoteAsset::Usdt,
            ts: UnixSeconds::new(minute + MINUTE_SECONDS),
            close: PriceUsd::new(Decimal::ONE),
            source: PriceSource::Peg,
        })
        .collect()
}

fn parse_klines(
    body: &[u8],
    asset: QuoteAsset,
    now: UnixMillis,
) -> Result<Vec<PricePoint>, PriceError> {
    let rows: Vec<Vec<Value>> = serde_json::from_slice(body)?;
    let mut points = Vec::with_capacity(rows.len());
    for row in &rows {
        let kline = parse_kline_row(row)?;
        if kline.close_time < now {
            // Binance's close time is the candle's last millisecond; the close is observed at
            // the minute boundary that ends it, which is the ts the lookup compares.
            points.push(PricePoint {
                asset,
                ts: UnixSeconds::new(kline.open_time.seconds().get() + MINUTE_SECONDS),
                close: kline.close,
                source: PriceSource::Binance,
            });
        }
    }
    debug_assert!(points.len() <= rows.len());
    Ok(points)
}

fn parse_kline_row(row: &[Value]) -> Result<Kline, PriceError> {
    if row.len() < KLINE_FIELD_COUNT_MIN {
        return Err(PriceError::MalformedKline {
            reason: "fewer than seven fields",
        });
    }
    let open_time = row[0].as_i64().ok_or(PriceError::MalformedKline {
        reason: "open time is not an integer",
    })?;
    let close_text = row[4].as_str().ok_or(PriceError::MalformedKline {
        reason: "close is not a string",
    })?;
    let close_time = row[6].as_i64().ok_or(PriceError::MalformedKline {
        reason: "close time is not an integer",
    })?;
    if open_time.rem_euclid(MINUTE_MS) != 0 {
        return Err(PriceError::MalformedKline {
            reason: "open time is not on a minute boundary",
        });
    }
    if close_time <= open_time {
        return Err(PriceError::MalformedKline {
            reason: "close time is not after open time",
        });
    }
    let close = Decimal::from_str_exact(close_text)?;
    if close <= Decimal::ZERO {
        return Err(PriceError::MalformedKline {
            reason: "close is not positive",
        });
    }
    Ok(Kline {
        open_time: UnixMillis(open_time),
        close_time: UnixMillis(close_time),
        close: PriceUsd::new(close),
    })
}

fn weight_used_1m_from_headers(headers: &HeaderMap) -> Option<u32> {
    headers
        .get(HEADER_WEIGHT_USED_1M)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

// Binance sends Retry-After in whole seconds.
fn retry_after_ms_from_headers(headers: &HeaderMap) -> Option<u64> {
    let seconds: u64 = headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(seconds.saturating_mul(1000))
}

fn pause_ms_after_response(
    status: StatusCode,
    weight_used_1m: Option<u32>,
    retry_after_ms: Option<u64>,
    now: UnixMillis,
) -> Option<u64> {
    let until_next_minute_ms = (MINUTE_MS - now.0.rem_euclid(MINUTE_MS)).unsigned_abs();
    debug_assert!(until_next_minute_ms > 0 && until_next_minute_ms <= 60_000);
    let rate_limited = status == StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 418;
    let rate_limit_pause_ms = rate_limited.then(|| retry_after_ms.unwrap_or(until_next_minute_ms));
    let weight_pause_ms = weight_used_1m
        .filter(|weight| *weight > WEIGHT_USED_1M_PAUSE_THRESHOLD)
        .map(|_| until_next_minute_ms);
    rate_limit_pause_ms.max(weight_pause_ms)
}

fn is_retryable(error: &PriceError) -> bool {
    match error {
        PriceError::Transport(_) => true,
        PriceError::Status { status, .. } => matches!(status, 418 | 429 | 500..=599),
        PriceError::Body(_) | PriceError::MalformedKline { .. } | PriceError::Decimal(_) => false,
    }
}

// A None delay means backon has given up; Retry-After only ever lengthens a planned wait.
fn adjust_for_retry_after(error: &PriceError, delay: Option<Duration>) -> Option<Duration> {
    let delay = delay?;
    match error {
        PriceError::Status {
            retry_after_ms: Some(retry_after_ms),
            ..
        } => Some(delay.max(Duration::from_millis(*retry_after_ms))),
        _ => Some(delay),
    }
}

#[cfg(test)]
#[path = "binance_tests.rs"]
mod tests;
