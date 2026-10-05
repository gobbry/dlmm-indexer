use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::Utc;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};

use crate::actor::messages::{PriceFeedMessage, ProcessorMessage};
use crate::domain::amounts::{PriceUsd, QuoteAsset};
use crate::domain::error::PriceError;
use crate::domain::ids::{MinuteRange, UnixSeconds};
use crate::domain::price::{PRICE_AGE_MAX_SECONDS, PricePoint, PriceSource, UnpriceableMinute};
use crate::gateway::binance::BinanceGateway;
use crate::store::{read_unpriced_minute_range, write_prices};

const MINUTE_SECONDS: i64 = 60;
const TICK_INTERVAL: Duration = Duration::from_secs(15);
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
// The tick re-reads two closed minutes so one missed tick never leaves a hole for the sweep.
const TICK_LOOKBACK_SECONDS: i64 = 2 * MINUTE_SECONDS;
// Older unpriced history catches up over successive sweeps instead of one long blocking call.
const SWEEP_MINUTES_MAX: i64 = 24 * 60;
// A missing candle counts as an exchange gap only once it is well past its close, so a
// candle Binance has not published yet is never frozen as a carried-forward row.
const CARRY_FORWARD_SETTLE_SECONDS: i64 = 5 * MINUTE_SECONDS;
// A leading exchange gap is seeded from the last candle in this lookback before the window;
// Binance history is immutable, so the seed is the same whichever sweep fills the gap.
const SEED_LOOKBACK_SECONDS: i64 = 60 * MINUTE_SECONDS;
// A week of minutes for each quote asset; past it that asset's oldest are forgotten and swept
// once more, so one asset's long gap never evicts another's.
const UNPRICEABLE_MINUTE_COUNT_PER_ASSET_MAX: usize = 7 * 24 * 60;

// Next swap minute a bounded sweep resumes from; None starts at the oldest unpriced minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SweepCursor(Option<UnixSeconds>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct CarryForwardFill {
    points: Vec<PricePoint>,
    // Settled grid ts with no candle and no seed: the swap minutes they price stay unpriced.
    unseeded: Vec<UnixSeconds>,
}

// Kept in memory: a restart sweeps these once more, which costs one sweep, not correctness.
#[derive(Debug, Default)]
struct UnpriceableMinutes(BTreeMap<QuoteAsset, BTreeSet<UnixSeconds>>);

impl UnpriceableMinutes {
    fn record(&mut self, asset: QuoteAsset, unseeded: &[UnixSeconds]) {
        let minutes = self.0.entry(asset).or_default();
        minutes.extend(unseeded.iter().copied());
        while minutes.len() > UNPRICEABLE_MINUTE_COUNT_PER_ASSET_MAX {
            minutes.pop_first();
        }
        debug_assert!(minutes.len() <= UNPRICEABLE_MINUTE_COUNT_PER_ASSET_MAX);
        debug_assert!(self.0.len() <= QuoteAsset::ALL.len());
    }

    fn to_vec(&self) -> Vec<UnpriceableMinute> {
        self.0
            .iter()
            .flat_map(|(asset, minutes)| {
                minutes.iter().map(|minute| UnpriceableMinute {
                    minute: *minute,
                    asset: *asset,
                })
            })
            .collect()
    }
}

enum SweepOutcome {
    Continue(SweepCursor),
    ProcessorClosed,
}

pub async fn run(
    database: PgPool,
    gateway: BinanceGateway,
    mut receiver: mpsc::Receiver<PriceFeedMessage>,
    processor_sender: mpsc::Sender<ProcessorMessage>,
) -> Result<(), PriceError> {
    let mut tick_interval = interval(TICK_INTERVAL);
    tick_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sweep_interval = interval(SWEEP_INTERVAL);
    sweep_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sweep_cursor = SweepCursor(None);
    let mut unpriceable = UnpriceableMinutes::default();
    loop {
        let message = tokio::select! {
            biased;
            message = receiver.recv() => message.unwrap_or(PriceFeedMessage::Shutdown),
            _ = tick_interval.tick() => PriceFeedMessage::Tick,
            _ = sweep_interval.tick() => PriceFeedMessage::SweepTick,
        };
        match message {
            PriceFeedMessage::Tick => tick(&database, &gateway).await,
            PriceFeedMessage::SweepTick => {
                let outcome = sweep(
                    &database,
                    &gateway,
                    &processor_sender,
                    sweep_cursor,
                    &mut unpriceable,
                )
                .await;
                match outcome {
                    SweepOutcome::Continue(next_cursor) => sweep_cursor = next_cursor,
                    SweepOutcome::ProcessorClosed => return Ok(()),
                }
            }
            PriceFeedMessage::Shutdown => return Ok(()),
        }
    }
}

async fn tick(database: &PgPool, gateway: &BinanceGateway) {
    let now_minute = floor_minute(UnixSeconds::new(Utc::now().timestamp()));
    let range = MinuteRange {
        start: UnixSeconds::new(now_minute.get() - TICK_LOOKBACK_SECONDS),
        end_inclusive: now_minute,
    };
    let mut points = Vec::new();
    for asset in QuoteAsset::ALL {
        match gateway.klines(asset, range).await {
            Ok(asset_points) => points.extend(asset_points),
            Err(error) => warn!(asset = asset.as_str(), error = %error, "price_tick_fetch_failed"),
        }
    }
    if let Err(error) = write_prices(database, &points).await {
        warn!(error = %error, "price_tick_write_failed");
    }
}

async fn sweep(
    database: &PgPool,
    gateway: &BinanceGateway,
    processor_sender: &mpsc::Sender<ProcessorMessage>,
    cursor: SweepCursor,
    unpriceable: &mut UnpriceableMinutes,
) -> SweepOutcome {
    let unpriced = match read_unpriced_minute_range(database, &unpriceable.to_vec()).await {
        Ok(Some(unpriced)) => unpriced,
        Ok(None) => return SweepOutcome::Continue(SweepCursor(None)),
        Err(error) => {
            warn!(error = %error, "unpriced_range_read_failed");
            return SweepOutcome::Continue(cursor);
        }
    };
    let (window, next_cursor) = sweep_window(unpriced, cursor);
    if window != unpriced {
        info!(?unpriced, ?window, "price_sweep_bounded_to_one_day");
    }
    let price_window = price_ts_window(window);
    let now = UnixSeconds::new(Utc::now().timestamp());
    let mut points = Vec::new();
    let mut unseeded = Vec::new();
    for asset in QuoteAsset::ALL {
        let fill = price_asset(gateway, asset, price_window, now).await;
        points.extend(fill.points);
        unseeded.push((asset, fill.unseeded));
    }
    if let Err(error) = write_prices(database, &points).await {
        warn!(error = %error, "price_sweep_write_failed");
        return SweepOutcome::Continue(cursor);
    }
    for (asset, minutes) in &unseeded {
        unpriceable.record(*asset, minutes);
    }
    // Never wait on the fill channel: a long fill keeps it full, and the repricing this
    // notification asks for is repeated by the next sweep anyway, from the same cursor.
    match processor_sender.try_send(ProcessorMessage::OnPricesFilled(window)) {
        Ok(()) => SweepOutcome::Continue(next_cursor),
        Err(TrySendError::Full(_)) => {
            warn!(?window, "price_notification_dropped_channel_full");
            SweepOutcome::Continue(cursor)
        }
        Err(TrySendError::Closed(_)) => SweepOutcome::ProcessorClosed,
    }
}

async fn price_asset(
    gateway: &BinanceGateway,
    asset: QuoteAsset,
    price_window: MinuteRange,
    now: UnixSeconds,
) -> CarryForwardFill {
    // The gateway takes candle open minutes; a candle's ts is its open plus one minute.
    let fetch_window = MinuteRange {
        start: UnixSeconds::new(price_window.start.get() - MINUTE_SECONDS - SEED_LOOKBACK_SECONDS),
        end_inclusive: UnixSeconds::new(price_window.end_inclusive.get() - MINUTE_SECONDS),
    };
    let fetched = match gateway.klines(asset, fetch_window).await {
        Ok(fetched) => fetched,
        Err(error) => {
            warn!(asset = asset.as_str(), error = %error, "price_sweep_fetch_failed");
            return CarryForwardFill {
                points: Vec::new(),
                unseeded: Vec::new(),
            };
        }
    };
    let seed = seed_before(&fetched, price_window.start);
    let fill = carry_forward(asset, price_window, &fetched, seed, now);
    if !fill.unseeded.is_empty() {
        // No candle in the lookback either, so the leading minutes stay unpriced, and later
        // sweeps leave them out of their window.
        warn!(
            asset = asset.as_str(),
            unseeded_minute_count = fill.unseeded.len(),
            "exchange_gap_unseeded"
        );
    }
    fill
}

// The close of the last fetched candle before the window, so a gap at the window start
// carries the price that was current then.
fn seed_before(fetched: &[PricePoint], window_start: UnixSeconds) -> Option<PriceUsd> {
    fetched
        .iter()
        .filter(|point| point.ts < window_start)
        .max_by_key(|point| point.ts)
        .map(|point| point.close)
}

fn floor_minute(time: UnixSeconds) -> UnixSeconds {
    UnixSeconds::new(time.get() - time.get().rem_euclid(MINUTE_SECONDS))
}

// A swap at t is priced by a row with ts in (t - PRICE_AGE_MAX_SECONDS, t]. On the minute grid
// the rows a window of swap minutes needs run from the first grid ts past its first minute
// minus the age bound to its last minute; a swap misses only when one of these is absent.
fn price_ts_window(swap_minutes: MinuteRange) -> MinuteRange {
    debug_assert!(swap_minutes.start <= swap_minutes.end_inclusive);
    debug_assert_eq!(PRICE_AGE_MAX_SECONDS.rem_euclid(MINUTE_SECONDS), 0);
    MinuteRange {
        start: UnixSeconds::new(swap_minutes.start.get() - PRICE_AGE_MAX_SECONDS + MINUTE_SECONDS),
        end_inclusive: swap_minutes.end_inclusive,
    }
}

// Bounds one sweep to a day of swap minutes; the cursor walks forward through older history
// so a minute that can never be priced does not pin every sweep to the same window.
fn sweep_window(unpriced: MinuteRange, cursor: SweepCursor) -> (MinuteRange, SweepCursor) {
    debug_assert!(unpriced.start <= unpriced.end_inclusive);
    let start = match cursor.0 {
        Some(resume) if resume > unpriced.start && resume <= unpriced.end_inclusive => resume,
        _ => unpriced.start,
    };
    let end_max = start.get() + (SWEEP_MINUTES_MAX - 1) * MINUTE_SECONDS;
    if end_max >= unpriced.end_inclusive.get() {
        let window = MinuteRange {
            start,
            end_inclusive: unpriced.end_inclusive,
        };
        return (window, SweepCursor(None));
    }
    let window = MinuteRange {
        start,
        end_inclusive: UnixSeconds::new(end_max),
    };
    debug_assert!(window.end_inclusive < unpriced.end_inclusive);
    (
        window,
        SweepCursor(Some(UnixSeconds::new(end_max + MINUTE_SECONDS))),
    )
}

// The age bound is one minute, so pricing needs a row at every grid ts; an exchange gap repeats
// the previous close, marked so it is never mistaken for a traded price. Only window ts are
// emitted; a fetched candle before the window serves only as the seed.
fn carry_forward(
    asset: QuoteAsset,
    price_window: MinuteRange,
    fetched: &[PricePoint],
    seed: Option<PriceUsd>,
    now: UnixSeconds,
) -> CarryForwardFill {
    debug_assert!(price_window.start <= price_window.end_inclusive);
    debug_assert!(fetched.iter().all(|point| point.asset == asset));
    let fetched_by_ts: BTreeMap<UnixSeconds, PricePoint> =
        fetched.iter().map(|point| (point.ts, *point)).collect();
    let ts_count =
        (price_window.end_inclusive.get() - price_window.start.get()) / MINUTE_SECONDS + 1;
    let settled_before = now.get() - CARRY_FORWARD_SETTLE_SECONDS;
    let mut previous_close = seed;
    let mut fill = CarryForwardFill {
        points: Vec::with_capacity(fetched.len()),
        unseeded: Vec::new(),
    };
    for ts_index in 0..ts_count {
        let ts = UnixSeconds::new(price_window.start.get() + ts_index * MINUTE_SECONDS);
        if let Some(point) = fetched_by_ts.get(&ts) {
            fill.points.push(*point);
            previous_close = Some(point.close);
        } else if ts.get() <= settled_before {
            match previous_close {
                Some(close) => fill.points.push(PricePoint {
                    asset,
                    ts,
                    close,
                    source: PriceSource::CarriedForward,
                }),
                None => fill.unseeded.push(ts),
            }
        }
    }
    fill
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::*;

    fn minutes(start: i64, end_inclusive: i64) -> MinuteRange {
        MinuteRange {
            start: UnixSeconds::new(start),
            end_inclusive: UnixSeconds::new(end_inclusive),
        }
    }

    fn binance_point(ts: i64, close: &str) -> PricePoint {
        PricePoint {
            asset: QuoteAsset::Sol,
            ts: UnixSeconds::new(ts),
            close: PriceUsd::new(Decimal::from_str(close).unwrap()),
            source: PriceSource::Binance,
        }
    }

    fn summary(points: &[PricePoint]) -> Vec<(i64, String, PriceSource)> {
        points
            .iter()
            .map(|point| (point.ts.get(), point.close.get().to_string(), point.source))
            .collect()
    }

    const SETTLED_NOW: UnixSeconds = UnixSeconds::new(1_000_000);

    // A hole between candles repeats the previous close as a carried-forward row.
    #[test]
    fn carry_forward_fills_hole_with_previous_close() {
        let fetched = [binance_point(60, "100.5"), binance_point(240, "101.25")];
        let fill = carry_forward(
            QuoteAsset::Sol,
            minutes(60, 240),
            &fetched,
            None,
            SETTLED_NOW,
        );
        let expected = vec![
            (60, "100.5".to_string(), PriceSource::Binance),
            (120, "100.5".to_string(), PriceSource::CarriedForward),
            (180, "100.5".to_string(), PriceSource::CarriedForward),
            (240, "101.25".to_string(), PriceSource::Binance),
        ];
        assert_eq!(summary(&fill.points), expected);
        assert_eq!(fill.unseeded, Vec::new());
    }

    // A hole at the window start uses the stored seed, or is skipped and counted without one.
    #[test]
    fn carry_forward_leading_hole_needs_seed() {
        let fetched = [binance_point(120, "99")];
        let seed = Some(PriceUsd::new(Decimal::from_str("98.75").unwrap()));
        let seeded = carry_forward(
            QuoteAsset::Sol,
            minutes(60, 120),
            &fetched,
            seed,
            SETTLED_NOW,
        );
        assert_eq!(
            summary(&seeded.points),
            vec![
                (60, "98.75".to_string(), PriceSource::CarriedForward),
                (120, "99".to_string(), PriceSource::Binance),
            ]
        );
        let unseeded = carry_forward(
            QuoteAsset::Sol,
            minutes(60, 120),
            &fetched,
            None,
            SETTLED_NOW,
        );
        assert_eq!(
            summary(&unseeded.points),
            vec![(120, "99".to_string(), PriceSource::Binance)]
        );
        assert_eq!(unseeded.unseeded, vec![UnixSeconds::new(60)]);
    }

    // The seed is the last candle before the window, never one inside it or a later one.
    #[test]
    fn seed_is_last_candle_before_window() {
        let fetched = [
            binance_point(0, "97"),
            binance_point(60, "98"),
            binance_point(180, "99"),
        ];
        let seed = seed_before(&fetched, UnixSeconds::new(180));
        assert_eq!(
            seed.map(|close| close.get().to_string()),
            Some("98".to_string())
        );
        assert_eq!(seed_before(&fetched, UnixSeconds::new(0)), None);
        let fill = carry_forward(
            QuoteAsset::Sol,
            minutes(120, 180),
            &fetched,
            seed,
            SETTLED_NOW,
        );
        assert_eq!(
            summary(&fill.points),
            vec![
                (120, "98".to_string(), PriceSource::CarriedForward),
                (180, "99".to_string(), PriceSource::Binance),
            ]
        );
    }

    // A recent minute with no candle yet is left for a later sweep, never frozen.
    #[test]
    fn carry_forward_skips_unsettled_minutes() {
        let fetched = [binance_point(600, "100")];
        let now = UnixSeconds::new(600 + 60 + 120);
        let fill = carry_forward(QuoteAsset::Sol, minutes(600, 720), &fetched, None, now);
        assert_eq!(
            summary(&fill.points),
            vec![(600, "100".to_string(), PriceSource::Binance)]
        );
        assert_eq!(fill.unseeded, Vec::new());
    }

    // Each swap minute is priced by the candle that closed at its start, so the price rows a
    // window needs are stamped at the window's own minutes.
    #[test]
    fn price_ts_window_matches_swap_minutes() {
        assert_eq!(price_ts_window(minutes(600, 1200)), minutes(600, 1200));
    }

    // More than a day of unpriced minutes is swept a day at a time, then wraps to the start.
    #[test]
    fn sweep_window_walks_long_history_a_day_at_a_time() {
        let day_seconds = SWEEP_MINUTES_MAX * MINUTE_SECONDS;
        let unpriced = minutes(0, 2 * day_seconds + 60);
        let (first, cursor) = sweep_window(unpriced, SweepCursor(None));
        assert_eq!(first, minutes(0, day_seconds - 60));
        let (second, cursor) = sweep_window(unpriced, cursor);
        assert_eq!(second, minutes(day_seconds, 2 * day_seconds - 60));
        let (third, cursor) = sweep_window(unpriced, cursor);
        assert_eq!(third, minutes(2 * day_seconds, 2 * day_seconds + 60));
        assert_eq!(cursor, SweepCursor(None));
    }

    // Each asset keeps its own week of unpriceable minutes: one asset's long gap evicts only
    // its own oldest minutes, never another asset's.
    #[test]
    fn unpriceable_minutes_are_capped_per_asset() {
        let minute = |index: usize| UnixSeconds::new(1_790_000_000 + 60 * index as i64);
        let mut unpriceable = UnpriceableMinutes::default();
        unpriceable.record(QuoteAsset::Usdc, &[minute(0)]);
        let long_gap: Vec<UnixSeconds> = (0..=UNPRICEABLE_MINUTE_COUNT_PER_ASSET_MAX)
            .map(|index| minute(index + 1))
            .collect();
        unpriceable.record(QuoteAsset::Sol, &long_gap);
        let recorded = unpriceable.to_vec();
        let sol: Vec<UnixSeconds> = recorded
            .iter()
            .filter(|entry| entry.asset == QuoteAsset::Sol)
            .map(|entry| entry.minute)
            .collect();
        assert_eq!(sol, long_gap[1..]);
        assert!(recorded.contains(&UnpriceableMinute {
            minute: minute(0),
            asset: QuoteAsset::Usdc,
        }));
    }
}
