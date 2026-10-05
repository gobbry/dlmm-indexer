use std::str::FromStr;
use std::time::Duration;

use reqwest::StatusCode;
use rust_decimal::Decimal;

use super::*;

// Recorded from data-api.binance.vision at unix 1790982680 (the file name). The fifth candle,
// open 1790982660000 and close 1790982719999, was still open at that instant.
const RECORDED_KLINES: &[u8] = include_bytes!("binance_klines_solusdt_recorded_1790982680.json");
const RECORDED_AT: UnixMillis = UnixMillis(1_790_982_680_000);

fn decimal(text: &str) -> Decimal {
    Decimal::from_str(text).unwrap()
}

fn minutes(start: i64, end_inclusive: i64) -> MinuteRange {
    MinuteRange {
        start: UnixSeconds::new(start),
        end_inclusive: UnixSeconds::new(end_inclusive),
    }
}

// Closed candles become Binance price points stamped at their close, with exact decimal closes;
// the open one is dropped.
#[test]
fn recorded_klines_parse_to_closed_candles_only() {
    let points = parse_klines(RECORDED_KLINES, QuoteAsset::Sol, RECORDED_AT).unwrap();
    let expected = [
        (1_790_982_480, "118.44"),
        (1_790_982_540, "118.38"),
        (1_790_982_600, "118.38"),
        (1_790_982_660, "118.35"),
    ];
    let actual: Vec<(i64, Decimal)> = points
        .iter()
        .map(|point| (point.ts.get(), point.close.get()))
        .collect();
    let expected: Vec<(i64, Decimal)> = expected
        .iter()
        .map(|(ts, close)| (*ts, decimal(close)))
        .collect();
    assert_eq!(actual, expected);
    assert!(
        points
            .iter()
            .all(|point| point.source == PriceSource::Binance)
    );
    assert!(points.iter().all(|point| point.asset == QuoteAsset::Sol));
}

// Once the last candle's close time has passed it is returned too.
#[test]
fn recorded_klines_include_last_candle_after_it_closes() {
    let points = parse_klines(
        RECORDED_KLINES,
        QuoteAsset::Sol,
        UnixMillis(1_790_982_720_000),
    );
    assert_eq!(points.unwrap().len(), 5);
}

// A candle whose open time is not on a minute boundary is rejected, never stored.
#[test]
fn kline_off_minute_boundary_is_malformed() {
    let body = br#"[[1790982420001,"1","1","1","1","0",1790982479999,"0",0,"0","0","0"]]"#;
    let result = parse_klines(body, QuoteAsset::Sol, RECORDED_AT);
    assert!(matches!(result, Err(PriceError::MalformedKline { .. })));
}

// USDT is priced at exactly 1 for every closed minute, stamped at its close, with no network
// call.
#[test]
fn usdt_peg_covers_closed_minutes() {
    let points = peg_points(minutes(600, 780), UnixMillis(780_000));
    let actual: Vec<i64> = points.iter().map(|point| point.ts.get()).collect();
    assert_eq!(actual, vec![660, 720, 780]);
    assert!(points.iter().all(|point| point.close.get() == Decimal::ONE));
    assert!(points.iter().all(|point| point.source == PriceSource::Peg));
}

// A 2500-minute range splits into requests of at most 1000 candles that tile it exactly.
#[test]
fn minute_chunks_tile_range_in_thousands() {
    let chunks = minute_chunks(minutes(0, 2499 * 60));
    assert_eq!(
        chunks,
        vec![
            minutes(0, 999 * 60),
            minutes(1000 * 60, 1999 * 60),
            minutes(2000 * 60, 2499 * 60),
        ]
    );
}

// Above 80 percent of the minute's weight, calls wait for the next minute boundary.
#[test]
fn heavy_weight_pauses_until_next_minute() {
    let now = UnixMillis(1_790_982_680_000);
    let pause = pause_ms_after_response(StatusCode::OK, Some(4801), None, now);
    assert_eq!(pause, Some(40_000));
    let light = pause_ms_after_response(StatusCode::OK, Some(4800), None, now);
    assert_eq!(light, None);
}

// 429 and 418 pause for Retry-After, or to the next minute when the header is missing.
#[test]
fn rate_limit_status_pauses_for_retry_after() {
    let now = UnixMillis(1_790_982_680_000);
    let too_many = StatusCode::TOO_MANY_REQUESTS;
    let banned = StatusCode::from_u16(418).unwrap();
    assert_eq!(
        pause_ms_after_response(too_many, Some(10), Some(7_000), now),
        Some(7_000)
    );
    assert_eq!(
        pause_ms_after_response(banned, None, Some(120_000), now),
        Some(120_000)
    );
    assert_eq!(
        pause_ms_after_response(too_many, None, None, now),
        Some(40_000)
    );
}

// Retry-After lengthens the backoff delay but never revives a retry backon gave up on.
#[test]
fn retry_after_adjusts_backoff_delay() {
    let error = PriceError::Status {
        status: 429,
        retry_after_ms: Some(5_000),
    };
    let short = Some(Duration::from_millis(1_000));
    assert_eq!(
        adjust_for_retry_after(&error, short),
        Some(Duration::from_millis(5_000))
    );
    assert_eq!(adjust_for_retry_after(&error, None), None);
    assert!(is_retryable(&error));
    assert!(!is_retryable(&PriceError::Status {
        status: 400,
        retry_after_ms: None
    }));
}
