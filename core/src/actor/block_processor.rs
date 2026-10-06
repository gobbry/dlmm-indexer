use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgConnection, PgPool};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};

use crate::actor::messages::{FillerMessage, ProcessorMessage};
use crate::decode::decode_block;
use crate::domain::amounts::QuoteAllowlist;
use crate::domain::block::{BlockOrigin, FinalizedBlock, LiveSource, StoredBlock};
use crate::domain::config::ChannelCapacity;
use crate::domain::error::StoreError;
use crate::domain::ids::{MintAddress, MinuteRange};
use crate::domain::job::{ArchiveRouting, JobSplit, ReconcileSummary};
use crate::domain::price::PriceSource;
use crate::domain::registry::{PoolCache, TokenCache, TokenRecord};
use crate::enrich::enrich;
use crate::gateway::rpc::{MINTS_PER_REQUEST_MAX, RpcGateway};
use crate::store::{reconcile, reprice_unpriced, update_token_decimals, write_block};

const HEALTH_FILE_PATH: &str = "/tmp/healthy";
// Holes are derived, not signalled, so a tick only bounds how late a hole gets its job.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
const RETRY_DELAY_MIN: Duration = Duration::from_secs(1);
const RETRY_DELAY_MAX: Duration = Duration::from_secs(30);
// Past this many consecutive failures the database is not coming back soon; exiting hands
// the block to the restart policy, which resumes from the committed cursor.
const FAILURE_COUNT_MAX: u32 = 10;
const DECIMALS_FETCHES_IN_FLIGHT_MAX: usize = 4;
// A mint whose fetch keeps failing must not keep spending the fill lane: it is retried a few
// times, spaced out, then left with null decimals (the API renders null human amounts).
const DECIMALS_ATTEMPT_COUNT_MAX: u32 = 5;
const DECIMALS_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Default)]
pub struct Caches {
    pub pools: PoolCache,
    pub tokens: TokenCache,
    // Seen for the first time and committed with null decimals; fetched off the hot path.
    pub mints_awaiting_decimals: Vec<MintAddress>,
}

impl Caches {
    pub fn new() -> Self {
        Self::default()
    }
}

// One database transaction per block. Caches change only after the commit, so a failed attempt
// leaves them as they were and a retry enriches the same way.
pub async fn process_block(
    connection: &mut PgConnection,
    block: &FinalizedBlock,
    origin: BlockOrigin,
    pricing: &PricingSettings,
    caches: &mut Caches,
) -> Result<StoredBlock, StoreError> {
    let slot = block.slot;
    let decoded = decode_block(block);
    let failures = decoded.failures.clone();
    let enriched = enrich(decoded, &caches.pools, &caches.tokens, &pricing.allowlist);
    let new_pools = enriched.new_pools.clone();
    let unknown_mints = enriched.unknown_mints.clone();
    let stored = write_block(connection, enriched, origin, pricing.market).await?;
    debug_assert_eq!(stored.slot, slot);
    for failure in &failures {
        tracing::warn!(
            slot = slot.get(),
            signature = %failure.signature,
            reason = %failure.reason,
            "decode_failure"
        );
    }
    for pool in new_pools {
        caches.pools.insert(pool);
    }
    for mint in unknown_mints {
        caches.tokens.insert(TokenRecord {
            mint,
            decimals: None,
        });
        caches.mints_awaiting_decimals.push(mint);
    }
    Ok(stored)
}

pub struct PricingSettings {
    pub allowlist: QuoteAllowlist,
    pub market: PriceSource,
}

// Fixed at boot from the environment; the processor only reads them.
pub struct ProcessorSettings {
    pub pricing: PricingSettings,
}

// `routing` is the archive window the filler publishes: holes are cut at its edges so each job
// has one route, and none is opened while it is still pending.
pub async fn run(
    database: PgPool,
    gateway: Arc<RpcGateway>,
    settings: ProcessorSettings,
    routing: watch::Receiver<ArchiveRouting>,
    mut live_receiver: mpsc::Receiver<ProcessorMessage>,
    mut fill_receiver: mpsc::Receiver<ProcessorMessage>,
    filler_sender: mpsc::Sender<FillerMessage>,
) -> Result<(), StoreError> {
    let mut processor = Processor {
        database,
        gateway,
        settings,
        routing,
        filler_sender,
        caches: Caches::new(),
        decimals_queue: DecimalsQueue::default(),
        decimals_fetches: JoinSet::new(),
    };
    // The reconciler runs here, between blocks, so the processor stays the only writer.
    let mut reconcile_tick = tokio::time::interval(RECONCILE_INTERVAL);
    reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        // The tick first, so a busy stream never starves it; then live, so a heavy fill never
        // delays the stream.
        let message = tokio::select! {
            biased;
            _ = reconcile_tick.tick() => {
                processor.reconcile().await;
                continue;
            }
            message = live_receiver.recv() => message,
            message = fill_receiver.recv() => message,
        };
        match message {
            Some(ProcessorMessage::Shutdown) | None => break,
            Some(message) => processor.handle_with_retry(message).await?,
        }
    }
    processor
        .drain(&mut live_receiver, &mut fill_receiver)
        .await?;
    tracing::info!("block_processor_stopped");
    Ok(())
}

struct Processor {
    database: PgPool,
    gateway: Arc<RpcGateway>,
    settings: ProcessorSettings,
    routing: watch::Receiver<ArchiveRouting>,
    filler_sender: mpsc::Sender<FillerMessage>,
    caches: Caches,
    decimals_queue: DecimalsQueue,
    // Each fetch hands back the entries it could not resolve, to be queued again.
    decimals_fetches: JoinSet<Vec<DecimalsEntry>>,
}

impl Processor {
    // Producers are stopped before Shutdown is sent, so this empties what they left queued.
    async fn drain(
        &mut self,
        live_receiver: &mut mpsc::Receiver<ProcessorMessage>,
        fill_receiver: &mut mpsc::Receiver<ProcessorMessage>,
    ) -> Result<(), StoreError> {
        // Price notifications ride the fill channel, so the two capacities bound the drain.
        let message_count_max =
            ChannelCapacity::LIVE_BLOCKS.get() + ChannelCapacity::FILL_BLOCKS.get();
        for _ in 0..message_count_max {
            let message = match live_receiver.try_recv() {
                Ok(message) => message,
                Err(_) => match fill_receiver.try_recv() {
                    Ok(message) => message,
                    Err(_) => return Ok(()),
                },
            };
            if message != ProcessorMessage::Shutdown {
                self.handle_with_retry(message).await?;
            }
        }
        Ok(())
    }

    // The popped message is kept and retried; nothing partial was committed by a failure.
    async fn handle_with_retry(&mut self, message: ProcessorMessage) -> Result<(), StoreError> {
        let mut delay = RETRY_DELAY_MIN;
        for failure_count in 1..=FAILURE_COUNT_MAX {
            let error = match self.handle(&message).await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if failure_count == FAILURE_COUNT_MAX {
                tracing::error!(%error, failure_count, "block_processor_giving_up");
                return Err(error);
            }
            tracing::warn!(%error, failure_count, delay_ms = delay.as_millis() as u64, "store_retry");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RETRY_DELAY_MAX);
        }
        unreachable!("the last iteration returns")
    }

    async fn handle(&mut self, message: &ProcessorMessage) -> Result<(), StoreError> {
        match message {
            ProcessorMessage::OnBlock(block, origin) => self.on_block(block, *origin).await,
            ProcessorMessage::OnPricesFilled(range) => self.on_prices_filled(*range).await,
            ProcessorMessage::Shutdown => Ok(()),
        }
    }

    async fn on_block(
        &mut self,
        block: &FinalizedBlock,
        origin: BlockOrigin,
    ) -> Result<(), StoreError> {
        let mut connection = self.database.acquire().await?;
        let stored = process_block(
            &mut connection,
            block,
            origin,
            &self.settings.pricing,
            &mut self.caches,
        )
        .await?;
        log_block_indexed(&stored);
        touch_health_file().await;
        self.schedule_decimals_fetch();
        Ok(())
    }

    // Everything it decides is re-derived from the tables next tick, so a failure is only
    // logged, never retried here.
    async fn reconcile(&mut self) {
        let summary = match self.reconcile_once().await {
            Ok(summary) => summary,
            Err(error) => {
                tracing::warn!(%error, "reconcile_failed");
                return;
            }
        };
        if summary == ReconcileSummary::default() {
            return;
        }
        for split in &summary.splits {
            log_job_split(split);
        }
        tracing::info!(
            opened_job_count = summary.opened_job_count,
            completed_job_count = summary.completed_job_count,
            split_job_count = summary.splits.len(),
            "coverage_reconciled"
        );
        if summary.opened_job_count > 0 || !summary.splits.is_empty() {
            // The filler rescans every 10 s, so a nudge lost to a full channel only delays
            // the fill; blocking here could deadlock against a filler waiting on us.
            let _ = self.filler_sender.try_send(FillerMessage::OnJobsOpened);
        }
    }

    async fn reconcile_once(&self) -> Result<ReconcileSummary, StoreError> {
        let routing = *self.routing.borrow();
        reconcile_routed(&self.database, routing).await
    }

    async fn on_prices_filled(&mut self, range: MinuteRange) -> Result<(), StoreError> {
        let mut connection = self.database.acquire().await?;
        let repriced =
            reprice_unpriced(&mut connection, range, self.settings.pricing.market).await?;
        tracing::info!(
            start = range.start.get(),
            end_inclusive = range.end_inclusive.get(),
            repriced_swap_count = repriced.len(),
            "swaps_repriced"
        );
        Ok(())
    }

    // Decimals only affect human-readable amounts, so they never hold up a block commit.
    fn schedule_decimals_fetch(&mut self) {
        let now = Instant::now();
        self.decimals_queue
            .push_new(self.caches.mints_awaiting_decimals.drain(..), now);
        while let Some(joined) = self.decimals_fetches.try_join_next() {
            // A panicked fetch loses its mints until restart; failures come back.
            if let Ok(failed) = joined {
                for mint in self.decimals_queue.on_failed(failed, now) {
                    tracing::error!(%mint, attempt_count = DECIMALS_ATTEMPT_COUNT_MAX, "decimals_given_up");
                }
            }
        }
        for _ in self.decimals_fetches.len()..DECIMALS_FETCHES_IN_FLIGHT_MAX {
            let entries = self.decimals_queue.take_ready(now, MINTS_PER_REQUEST_MAX);
            if entries.is_empty() {
                break;
            }
            let gateway = Arc::clone(&self.gateway);
            let database = self.database.clone();
            self.decimals_fetches
                .spawn(fetch_decimals(gateway, database, entries));
        }
    }
}

fn log_job_split(split: &JobSplit) {
    let pieces = split
        .pieces
        .iter()
        .map(|piece| {
            format!(
                "{}:[{},{}]:{}",
                piece.id.get(),
                piece.range.start.get(),
                piece.range.end_inclusive.get(),
                piece.end_kind.as_str()
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    tracing::info!(job_id = split.job_id.get(), %pieces, "backfill_job_split");
}

// A hole opened before the archive window is known would never be cut to it later, so
// nothing is opened (or completed) until it is.
async fn reconcile_routed(
    database: &PgPool,
    routing: ArchiveRouting,
) -> Result<ReconcileSummary, StoreError> {
    let ArchiveRouting::Ready(window) = routing else {
        return Ok(ReconcileSummary::default());
    };
    let mut connection = database.acquire().await?;
    reconcile(&mut connection, window).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecimalsEntry {
    mint: MintAddress,
    failure_count: u32,
    ready_at: Instant,
}

#[derive(Debug, Default)]
struct DecimalsQueue {
    entries: Vec<DecimalsEntry>,
}

impl DecimalsQueue {
    fn push_new(&mut self, mints: impl IntoIterator<Item = MintAddress>, now: Instant) {
        self.entries
            .extend(mints.into_iter().map(|mint| DecimalsEntry {
                mint,
                failure_count: 0,
                ready_at: now,
            }));
    }

    fn take_ready(&mut self, now: Instant, count_max: usize) -> Vec<DecimalsEntry> {
        debug_assert!(count_max > 0);
        let mut ready = Vec::new();
        self.entries.retain(|entry| {
            let take = entry.ready_at <= now && ready.len() < count_max;
            if take {
                ready.push(*entry);
            }
            !take
        });
        debug_assert!(ready.len() <= count_max);
        ready
    }

    fn on_failed(&mut self, failed: Vec<DecimalsEntry>, now: Instant) -> Vec<MintAddress> {
        let mut given_up = Vec::new();
        for entry in failed {
            let failure_count = entry.failure_count + 1;
            if failure_count >= DECIMALS_ATTEMPT_COUNT_MAX {
                given_up.push(entry.mint);
            } else {
                self.entries.push(DecimalsEntry {
                    mint: entry.mint,
                    failure_count,
                    ready_at: now + DECIMALS_RETRY_DELAY,
                });
            }
        }
        given_up
    }
}

// Returns the entries still owed decimals: all of them on failure, none on success. The retry
// rides a later block, after the gateway's own backoff has already been spent.
async fn fetch_decimals(
    gateway: Arc<RpcGateway>,
    database: PgPool,
    entries: Vec<DecimalsEntry>,
) -> Vec<DecimalsEntry> {
    let mints: Vec<MintAddress> = entries.iter().map(|entry| entry.mint).collect();
    let records = match gateway.mint_decimals(&mints).await {
        Ok(records) => records,
        Err(error) => {
            tracing::warn!(mint_count = mints.len(), %error, "decimals_fetch_failed");
            return entries;
        }
    };
    if let Err(error) = update_token_decimals(&database, &records).await {
        tracing::warn!(mint_count = mints.len(), %error, "decimals_write_failed");
        return entries;
    }
    Vec::new()
}

fn log_block_indexed(stored: &StoredBlock) {
    let (origin, job_id) = match stored.origin {
        BlockOrigin::Live(LiveSource::Geyser) => ("live_geyser", None),
        BlockOrigin::Live(LiveSource::RpcTail) => ("live_rpc", None),
        BlockOrigin::Fill { job_id, .. } => ("fill", Some(job_id.get())),
    };
    tracing::info!(
        slot = stored.slot.get(),
        origin,
        job_id,
        inserted_swap_count = stored.inserted_swap_count,
        duplicate_swap_count = stored.duplicate_swap_count,
        "block_indexed"
    );
}

// The container healthcheck reads this file's age; failing to touch it is not worth a retry.
async fn touch_health_file() {
    if let Err(error) = tokio::fs::write(HEALTH_FILE_PATH, b"").await {
        tracing::debug!(%error, "health_file_not_touched");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::Slot;
    use crate::domain::job::ArchiveWindow;

    async fn job_ranges(database: &PgPool) -> Vec<(i64, i64)> {
        sqlx::query_as("SELECT start_slot, end_slot FROM slot_range_job ORDER BY start_slot")
            .fetch_all(database)
            .await
            .expect("reads jobs")
    }

    // While the archive window is unknown a hole gets no job, since one opened then could
    // never be cut to the window and would send the archive's share to the provider; once the
    // window is known the same hole is opened, cut at it.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_opens_nothing_until_the_archive_window_is_known(database: PgPool) {
        sqlx::query(
            "INSERT INTO slot_coverage (start_slot, end_slot, end_block_time)
             VALUES (100, 199, now()), (500, 599, now())",
        )
        .execute(&database)
        .await
        .expect("seeds a hole [200, 499]");
        let pending = reconcile_routed(&database, ArchiveRouting::Pending)
            .await
            .expect("reconciles");
        assert_eq!(pending, ReconcileSummary::default());
        assert_eq!(job_ranges(&database).await, vec![]);

        let window = ArchiveWindow::new(Slot::new(300), Slot::new(400));
        reconcile_routed(&database, ArchiveRouting::Ready(window))
            .await
            .expect("reconciles");
        assert_eq!(
            job_ranges(&database).await,
            vec![(200, 299), (300, 400), (401, 499)]
        );
    }

    fn mint(byte: u8) -> MintAddress {
        MintAddress::new([byte; 32])
    }

    // A failing mint waits out the retry delay each time and is dropped after the attempt cap.
    #[test]
    fn decimals_queue_delays_retries_and_gives_up() {
        let start = Instant::now();
        let mut queue = DecimalsQueue::default();
        queue.push_new([mint(1), mint(2)], start);
        let first = queue.take_ready(start, 1);
        assert_eq!(first.len(), 1);
        assert_eq!(queue.take_ready(start, 10).len(), 1);
        let mut now = start;
        let mut failed = first;
        for attempt_count in 1..DECIMALS_ATTEMPT_COUNT_MAX {
            assert!(
                queue.on_failed(failed, now).is_empty(),
                "attempt {attempt_count}"
            );
            assert!(queue.take_ready(now, 10).is_empty());
            now += DECIMALS_RETRY_DELAY;
            failed = queue.take_ready(now, 10);
            assert_eq!(failed.len(), 1);
        }
        assert_eq!(queue.on_failed(failed, now), vec![mint(1)]);
        assert!(queue.take_ready(now + DECIMALS_RETRY_DELAY, 10).is_empty());
    }
}
