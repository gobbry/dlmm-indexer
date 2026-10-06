use chrono::{DateTime, SecondsFormat, Utc};
use dlmm_core::domain::amounts::{Decimals, QuoteAsset, TokenAmountRaw};
use dlmm_core::domain::ids::{JobId, PoolAddress, Slot, SlotRange, UnixSeconds};
use dlmm_core::domain::job::JobListing;
use dlmm_core::domain::query::{
    AlignedRange, Bucket, IndexerHealth, PageLimit, PageOffset, PoolMetadata, PoolPage,
    PoolSummary, SwapPage, SwapRow, VolumeBucket,
};
use dlmm_core::domain::swap::{FeeSide, FeeToken};
use rust_decimal::Decimal;
use serde::Serialize;

const LAG_SECONDS_MAX: i64 = 120;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexerStatus {
    Ok,
    Backfilling,
    Blocked,
    Lagging,
    Starting,
}

#[derive(Debug, Serialize)]
pub struct BackfillBody {
    pub job_id: i64,
    pub start_slot: u64,
    pub end_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Open,
    Blocked,
    Cancelled,
    Completed,
}

#[derive(Debug, Serialize)]
pub struct JobBody {
    pub job_id: i64,
    pub state: JobState,
    pub start_slot: u64,
    pub end_slot: u64,
    pub next_slot: u64,
    pub end_kind: &'static str,
    pub blocked_reason: Option<String>,
    pub completed_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct JobListBody {
    pub jobs: Vec<JobBody>,
}

#[derive(Debug, Serialize)]
pub struct CancelBody {
    pub job_id: i64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub next_slot: u64,
    pub state: JobState,
}

#[derive(Debug, Serialize)]
pub struct HealthBody {
    pub status: IndexerStatus,
    pub cursor_slot: Option<u64>,
    pub last_block_time: Option<String>,
    pub lag_seconds: Option<i64>,
    pub open_job_count: u32,
    pub blocked_job_count: u32,
    // Projections an offline rebuild left in the building state; omitted when none.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rebuilding: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct VolumeBody {
    pub pool: String,
    pub mint_x: String,
    pub mint_y: String,
    pub decimals_x: Option<u8>,
    pub decimals_y: Option<u8>,
    pub first_swap_at: Option<String>,
    pub bucket: &'static str,
    pub from: String,
    pub to: String,
    pub buckets: Vec<VolumeBucketBody>,
}

#[derive(Debug, Serialize)]
pub struct VolumeBucketBody {
    pub start: String,
    pub swap_count: u64,
    pub volume_x: Option<String>,
    pub volume_x_raw: String,
    pub volume_y: Option<String>,
    pub volume_y_raw: String,
    pub volume_usd: Option<String>,
    pub unpriced_swap_count: u64,
}

#[derive(Debug, Serialize)]
pub struct PoolBody {
    pub address: String,
    pub mint_x: String,
    pub mint_y: String,
    pub decimals_x: Option<u8>,
    pub decimals_y: Option<u8>,
    pub swap_count_24h: u64,
    pub volume_usd_24h: Option<String>,
    pub first_swap_at: Option<String>,
    pub last_swap_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OffsetPageBody {
    pub limit: u32,
    pub offset: u32,
    pub total: u64,
}

#[derive(Debug, Serialize)]
pub struct PoolListBody {
    pub pools: Vec<PoolBody>,
    pub page: OffsetPageBody,
}

// next_cursor goes back as `before` for the next older page; null on the last one.
#[derive(Debug, Serialize)]
pub struct CursorPageBody {
    pub limit: u32,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SwapBody {
    pub signature: String,
    pub swap_ordinal: u16,
    pub slot: u64,
    pub block_time: String,
    pub user: String,
    pub direction: &'static str,
    pub mint_in: String,
    pub mint_out: String,
    pub amount_in: String,
    pub amount_out: String,
    pub fee: String,
    pub protocol_fee: String,
    pub host_fee: String,
    pub fee_rate_1e9: String,
    pub mm_fee: Option<String>,
    pub limit_order_fee: Option<String>,
    pub amount_left: Option<String>,
    pub fee_side: Option<&'static str>,
    pub fee_token: Option<&'static str>,
    pub quote_asset: Option<&'static str>,
    pub volume_usd: Option<String>,
    pub source: &'static str,
    pub fill_job_id: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SwapListBody {
    pub pool: String,
    pub swaps: Vec<SwapBody>,
    pub page: CursorPageBody,
}

pub fn timestamp_text(seconds: UnixSeconds) -> String {
    let time = DateTime::<Utc>::from_timestamp(seconds.get(), 0);
    // Every UnixSeconds the store hands out came from a database timestamp, so it is in range.
    debug_assert!(time.is_some());
    time.unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

// Decimal holds at most 28 fractional digits; no SPL mint has more, so beyond that is unknown.
pub fn human_amount(raw: Decimal, decimals: Option<Decimals>) -> Option<String> {
    let decimals = decimals?;
    let scale = raw.scale().checked_add(u32::from(decimals.get()))?;
    let mut human = raw;
    human.set_scale(scale).ok()?;
    Some(decimal_text(human))
}

const fn bucket_name(bucket: Bucket) -> &'static str {
    match bucket {
        Bucket::Hour => "hour",
        Bucket::Day => "day",
    }
}

fn raw_text(amount: TokenAmountRaw) -> String {
    amount.get().to_string()
}

// The live tail outranks the past: a lagging tail is the most urgent; then a job only an
// operator can unblock; then ranges the filler is still working through.
pub fn indexer_status(health: &IndexerHealth, lag_seconds: Option<i64>) -> IndexerStatus {
    if health.cursor_slot.is_none() {
        return IndexerStatus::Starting;
    }
    if lag_seconds.is_some_and(|lag| lag > LAG_SECONDS_MAX) {
        return IndexerStatus::Lagging;
    }
    if health.blocked_job_count > 0 {
        return IndexerStatus::Blocked;
    }
    if health.open_job_count > 0 {
        return IndexerStatus::Backfilling;
    }
    IndexerStatus::Ok
}

pub fn render_health(health: &IndexerHealth, now: UnixSeconds) -> HealthBody {
    // Clock skew between the database and this host must not report negative lag.
    let lag_seconds = health
        .last_block_time
        .map(|last| (now.get() - last.get()).max(0));
    HealthBody {
        status: indexer_status(health, lag_seconds),
        cursor_slot: health.cursor_slot.map(|slot| slot.get()),
        last_block_time: health.last_block_time.map(timestamp_text),
        lag_seconds,
        open_job_count: health.open_job_count,
        blocked_job_count: health.blocked_job_count,
        rebuilding: health
            .rebuilding_projections
            .iter()
            .map(|name| name.table_name())
            .collect(),
    }
}

pub fn job_state(job: &JobListing) -> JobState {
    if job.completed_at.is_some() {
        return JobState::Completed;
    }
    match &job.blocked_reason {
        None => JobState::Open,
        Some(reason) if reason.is_cancelled() => JobState::Cancelled,
        Some(_) => JobState::Blocked,
    }
}

pub fn render_job(job: &JobListing) -> JobBody {
    debug_assert!(job.range.start <= job.range.end_inclusive);
    debug_assert!(job.next_slot >= job.range.start);
    JobBody {
        job_id: job.id.get(),
        state: job_state(job),
        start_slot: job.range.start.get(),
        end_slot: job.range.end_inclusive.get(),
        next_slot: job.next_slot.get(),
        end_kind: job.end_kind.as_str(),
        blocked_reason: job
            .blocked_reason
            .as_ref()
            .map(|reason| reason.as_str().to_owned()),
        completed_at: job.completed_at.map(timestamp_text),
        created_at: timestamp_text(job.created_at),
    }
}

pub fn render_cancel(id: JobId, range: SlotRange, next_slot: Slot) -> CancelBody {
    debug_assert!(range.start <= range.end_inclusive);
    debug_assert!(next_slot >= range.start);
    CancelBody {
        job_id: id.get(),
        start_slot: range.start.get(),
        end_slot: range.end_inclusive.get(),
        next_slot: next_slot.get(),
        state: JobState::Cancelled,
    }
}

fn render_volume_bucket(pool: &PoolMetadata, bucket: &VolumeBucket) -> VolumeBucketBody {
    debug_assert!(bucket.unpriced_swap_count <= bucket.swap_count);
    VolumeBucketBody {
        start: timestamp_text(bucket.start),
        swap_count: bucket.swap_count,
        volume_x: human_amount(bucket.volume_x, pool.decimals_x),
        volume_x_raw: decimal_text(bucket.volume_x),
        volume_y: human_amount(bucket.volume_y, pool.decimals_y),
        volume_y_raw: decimal_text(bucket.volume_y),
        volume_usd: bucket.volume_usd.map(decimal_text),
        unpriced_swap_count: bucket.unpriced_swap_count,
    }
}

pub fn render_volume(
    pool: &PoolMetadata,
    range: AlignedRange,
    buckets: &[VolumeBucket],
) -> VolumeBody {
    debug_assert!(range.from < range.to_exclusive);
    debug_assert!(!buckets.is_empty());
    VolumeBody {
        pool: pool.address.to_string(),
        mint_x: pool.mint_x.to_string(),
        mint_y: pool.mint_y.to_string(),
        decimals_x: pool.decimals_x.map(Decimals::get),
        decimals_y: pool.decimals_y.map(Decimals::get),
        first_swap_at: pool.first_swap_at.map(timestamp_text),
        bucket: bucket_name(range.bucket),
        from: timestamp_text(range.from),
        to: timestamp_text(range.to_exclusive),
        buckets: buckets
            .iter()
            .map(|bucket| render_volume_bucket(pool, bucket))
            .collect(),
    }
}

pub fn render_pool(pool: &PoolSummary) -> PoolBody {
    PoolBody {
        address: pool.address.to_string(),
        mint_x: pool.mint_x.to_string(),
        mint_y: pool.mint_y.to_string(),
        decimals_x: pool.decimals_x.map(Decimals::get),
        decimals_y: pool.decimals_y.map(Decimals::get),
        swap_count_24h: pool.swap_count_24h,
        volume_usd_24h: pool.volume_usd_24h.map(decimal_text),
        first_swap_at: pool.first_swap_at.map(timestamp_text),
        last_swap_at: pool.last_swap_at.map(timestamp_text),
    }
}

pub fn render_pool_page(page: &PoolPage, limit: PageLimit, offset: PageOffset) -> PoolListBody {
    debug_assert!(page.pools.len() <= limit.get() as usize);
    debug_assert!(page.pools.len() as u64 <= page.pool_count);
    PoolListBody {
        pools: page.pools.iter().map(render_pool).collect(),
        page: OffsetPageBody {
            limit: limit.get(),
            offset: offset.get(),
            total: page.pool_count,
        },
    }
}

pub fn render_swap_page(pool: PoolAddress, page: &SwapPage, limit: PageLimit) -> SwapListBody {
    debug_assert!(page.swaps.len() <= limit.get() as usize);
    debug_assert!(page.next_cursor.is_none() || page.swaps.len() == limit.get() as usize);
    SwapListBody {
        pool: pool.to_string(),
        swaps: page.swaps.iter().map(render_swap).collect(),
        page: CursorPageBody {
            limit: limit.get(),
            next_cursor: page.next_cursor.map(|cursor| cursor.encode()),
        },
    }
}

fn render_swap(swap: &SwapRow) -> SwapBody {
    SwapBody {
        signature: swap.signature.to_string(),
        swap_ordinal: swap.swap_ordinal.get(),
        slot: swap.slot.get(),
        block_time: timestamp_text(swap.block_time),
        user: swap.user.to_string(),
        direction: swap.direction.as_str(),
        mint_in: swap.mint_in.to_string(),
        mint_out: swap.mint_out.to_string(),
        amount_in: raw_text(swap.amount_in),
        amount_out: raw_text(swap.amount_out),
        fee: raw_text(swap.fee),
        protocol_fee: raw_text(swap.protocol_fee),
        host_fee: raw_text(swap.host_fee),
        fee_rate_1e9: swap.fee_rate_1e9.get().to_string(),
        mm_fee: swap.mm_fee.map(raw_text),
        limit_order_fee: swap.limit_order_fee.map(raw_text),
        amount_left: swap.amount_left.map(raw_text),
        fee_side: swap.fee_side.map(FeeSide::as_str),
        fee_token: swap.fee_token.map(FeeToken::as_str),
        quote_asset: swap.quote_asset.map(QuoteAsset::as_str),
        volume_usd: swap.volume_usd.map(decimal_text),
        source: swap.source.as_str(),
        fill_job_id: swap.fill_job_id.map(|job| job.get()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dlmm_core::domain::ids::Slot;

    #[test]
    fn human_amount_scales_raw_by_decimals() {
        let raw = Decimal::from(1_532_123_456_789_u64);
        let human = human_amount(raw, Some(Decimals::new(9)));
        assert_eq!(human.as_deref(), Some("1532.123456789"));
        let whole = human_amount(Decimal::from(5_000_000_u64), Some(Decimals::new(6)));
        assert_eq!(whole.as_deref(), Some("5"));
        assert_eq!(human_amount(raw, None), None);
    }

    #[test]
    fn health_status_follows_cursor_lag_and_jobs() {
        let health = IndexerHealth {
            cursor_slot: Some(Slot::new(1)),
            last_block_time: Some(UnixSeconds::new(1_000)),
            open_job_count: 0,
            blocked_job_count: 0,
            rebuilding_projections: Vec::new(),
        };
        let status =
            |health: &IndexerHealth, now: i64| render_health(health, UnixSeconds::new(now)).status;
        let open = IndexerHealth {
            open_job_count: 2,
            ..health.clone()
        };
        let blocked = IndexerHealth {
            blocked_job_count: 1,
            ..open.clone()
        };
        let starting = IndexerHealth {
            cursor_slot: None,
            ..blocked.clone()
        };
        assert_eq!(status(&health, 1_120), IndexerStatus::Ok);
        assert_eq!(status(&health, 1_121), IndexerStatus::Lagging);
        assert_eq!(status(&open, 1_120), IndexerStatus::Backfilling);
        assert_eq!(status(&blocked, 1_120), IndexerStatus::Blocked);
        assert_eq!(status(&blocked, 1_121), IndexerStatus::Lagging);
        assert_eq!(status(&starting, 9_999), IndexerStatus::Starting);
    }
}
