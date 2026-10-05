use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::actor::archive_window;
use crate::actor::messages::{FillerMessage, ProcessorMessage};
use crate::actor::ordered_fetch::{FetchResult, ListedBlockHandler, fetch_listed};
use crate::domain::block::{BlockOrigin, FinalizedBlock};
use crate::domain::config::ChannelCapacity;
use crate::domain::error::{RpcError, RpcErrorClass, StoreError};
use crate::domain::ids::{JobId, Slot, SlotRange};
use crate::domain::job::{ArchiveRouting, ArchiveWindow, JobEndKind, SlotRangeJob};
use crate::domain::query::RowCountMax;
use crate::gateway::rpc::{
    Endpoint, RequestLane, RpcGateway, is_answer_about_request, is_unmappable,
};
use crate::store::{
    JobBlock, JobSelection, block_job, read_block_above_stuck, read_coverage_start_above,
    read_open_jobs,
};

const SCAN_INTERVAL: Duration = Duration::from_secs(10);
const PAGE_SLOT_COUNT_MAX: u64 = 1000;
const JOB_SCAN_COUNT_MAX: RowCountMax = RowCountMax::new(100);
// Thirty idle scans, about five minutes: a node lagging that long deserves an operator's look.
const WAITING_PASS_COUNT_WARN: u32 = 30;
// Each pass already spent six attempts with backoff (about 40 s), so this is about twenty
// minutes of an archive answering one slot with a retryable error: an epoch it did not load,
// or a CAR read that keeps failing, rather than a hiccup. A block answered as skipped is not
// retried by the gateway, so the same budget there is thirty scans, about five minutes.
const ARCHIVE_RETRY_PASS_COUNT_MAX: u32 = 30;

#[derive(Debug, Error)]
pub enum FillerError {
    #[error("rpc: {0}")]
    Rpc(#[from] RpcError),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("block processor channel closed")]
    ProcessorGone,
}

// How the next fetched block must link to what came before it in its job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainLink {
    // The previously fetched block.
    Parent(Slot),
    // A fresh job, or one resumed after a restart: the first block must not name a parent at
    // or after next_slot, which would be a block the listing omitted. A parent below it is
    // the chain's own proof that the slots between are skipped: a hole above a coverage range
    // (parent = the range's end block), the floor (the block sits on next_slot), and a job cut
    // at an archive window edge, whose start may follow a skipped slot, all pass.
    Resumed { next_slot: Slot },
}

// Ok, or the slot that getBlocks omitted although the chain says it holds a block.
fn verify_parent_chain(link: ChainLink, slot: Slot, parent_slot: Slot) -> Result<(), Slot> {
    debug_assert!(parent_slot < slot);
    match link {
        ChainLink::Parent(expected) if parent_slot == expected => Ok(()),
        ChainLink::Parent(expected) if parent_slot > expected => Err(parent_slot),
        ChainLink::Parent(expected) => Err(expected),
        ChainLink::Resumed { next_slot } if parent_slot < next_slot => Ok(()),
        ChainLink::Resumed { .. } => Err(parent_slot),
    }
}

// Where a walk resumes and what its first block must link to. Memory wins while it is ahead of
// the table.
fn walk_start(job: &SlotRangeJob, memory: Option<JobMemory>) -> (Slot, ChainLink) {
    match memory {
        Some(memory) if memory.next_slot >= job.next_slot => {
            (memory.next_slot, ChainLink::Parent(memory.tip))
        }
        _ => (
            job.next_slot,
            ChainLink::Resumed {
                next_slot: job.next_slot,
            },
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobEnd {
    Reached,
    Missing(Slot),
}

// A job's end_slot is a block (the parent of the block that starts the coverage range above
// the hole, or the archive window's top), so a walk that ran out of pages without fetching
// end_slot last met a node that lacks it; the one exception is a cut below the archive window.
fn job_end(last_fetched: Option<Slot>, end_slot: Slot) -> JobEnd {
    debug_assert!(last_fetched.is_none_or(|slot| slot <= end_slot));
    match last_fetched {
        Some(slot) if slot == end_slot => JobEnd::Reached,
        _ => JobEnd::Missing(end_slot),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingEnd {
    AboveFinalized,
    Absent,
}

fn missing_end(end_slot: Slot, finalized: Slot) -> MissingEnd {
    if end_slot > finalized {
        MissingEnd::AboveFinalized
    } else {
        MissingEnd::Absent
    }
}

// The job stays open either way; only the log level says how long it has waited.
fn waited_long(waiting_pass_count: u32) -> bool {
    waiting_pass_count >= WAITING_PASS_COUNT_WARN
}

// What a walk that never fetched its job's end block does about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndRule {
    // The archive was asked for the end slot itself, and every archive job ends on a block.
    ArchiveLacksIt,
    // A job cut below the archive window may end on a skipped slot, and nothing in it can
    // prove that: the archive's first block above the cut names the real last block.
    BlockAboveProves,
    // A provider listing may lag its node's finalized root, so the end is listed again.
    Relist,
}

// Keyed on how the end was set when the job was cut, never on the current window: the
// archive's bottom moves when an operator loads or drops an epoch, and a cut job's end stays
// a slot only the block above can prove, on whichever lane it now routes to.
fn end_rule(endpoint: Endpoint, end_kind: JobEndKind) -> EndRule {
    match (end_kind, endpoint) {
        (JobEndKind::ArchiveLowerCut, _) => EndRule::BlockAboveProves,
        (JobEndKind::Block, Endpoint::Archive) => EndRule::ArchiveLacksIt,
        (JobEndKind::Block, Endpoint::Provider) => EndRule::Relist,
    }
}

// The first coverage range above a cut starts right after its first block's parent. A parent
// past what the walk claimed, at or below the job's end, is a block this lane never returned;
// a parent further up means the block above has not arrived yet.
fn block_missing_below(
    claimed_through: Slot,
    end_slot: Slot,
    coverage_start_above: Option<Slot>,
) -> Option<Slot> {
    debug_assert!(claimed_through <= end_slot);
    let parent = Slot::new(coverage_start_above?.get().checked_sub(1)?);
    (parent > claimed_through && parent <= end_slot).then_some(parent)
}

fn next_page(next_slot: Slot, end_slot: Slot) -> Option<SlotRange> {
    if next_slot > end_slot {
        return None;
    }
    let page_end = next_slot.get().saturating_add(PAGE_SLOT_COUNT_MAX - 1);
    let range = SlotRange {
        start: next_slot,
        end_inclusive: Slot::new(page_end.min(end_slot.get())),
    };
    debug_assert!(range.start <= range.end_inclusive);
    debug_assert!(range.end_inclusive.get() - range.start.get() < PAGE_SLOT_COUNT_MAX);
    Some(range)
}

// The archive's lane: its own gateway, and the sender through which it publishes the window
// every route and cut decision reads.
#[derive(Debug)]
pub struct ArchiveLane {
    pub gateway: Arc<RpcGateway>,
    pub routing: watch::Sender<ArchiveRouting>,
}

// Everything outside the archive window goes to the provider, and so does everything while
// there is no window. The filler's SQL selects the same jobs; this is the rule it must agree with.
// The SQL also holds back an unstarted job that straddles an edge until the reconciler splits it.
pub fn route(range: SlotRange, window: Option<ArchiveWindow>) -> Endpoint {
    match window {
        Some(window) if window.serves(range) => Endpoint::Archive,
        _ => Endpoint::Provider,
    }
}

// One loop per endpoint, so a page of archive blocks (minutes at well under one block a
// second) never holds up the provider's jobs, nor the other way round. Routing keeps the two
// job sets disjoint; the archive's window is read beside them, off both walks.
pub async fn run(
    database: PgPool,
    provider: Arc<RpcGateway>,
    archive: Option<ArchiveLane>,
    routing: watch::Receiver<ArchiveRouting>,
    mut receiver: mpsc::Receiver<FillerMessage>,
    fill_sender: mpsc::Sender<ProcessorMessage>,
) -> Result<(), FillerError> {
    debug_assert_eq!(provider.endpoint(), Endpoint::Provider);
    let mut provider_filler = Filler::new(
        database.clone(),
        provider,
        routing.clone(),
        fill_sender.clone(),
    );
    let Some(archive) = archive else {
        debug_assert_eq!(*routing.borrow(), ArchiveRouting::Ready(None));
        return filler_loop(&mut provider_filler, &mut receiver).await;
    };
    debug_assert_eq!(archive.gateway.endpoint(), Endpoint::Archive);
    let mut archive_filler =
        Filler::new(database, Arc::clone(&archive.gateway), routing, fill_sender);
    let (provider_sender, mut provider_receiver) = mpsc::channel(ChannelCapacity::NUDGES.get());
    let (archive_sender, mut archive_receiver) = mpsc::channel(ChannelCapacity::NUDGES.get());
    let (window_sender, window_receiver) = mpsc::channel(ChannelCapacity::NUDGES.get());
    tokio::try_join!(
        fan_out(receiver, [provider_sender, archive_sender, window_sender]),
        filler_loop(&mut provider_filler, &mut provider_receiver),
        filler_loop(&mut archive_filler, &mut archive_receiver),
        archive_window::run(archive.gateway, archive.routing, window_receiver),
    )?;
    Ok(())
}

// Every lane hears every nudge and the shutdown. A nudge lost to a full channel only delays a
// fill until the next scan, and a lane that already stopped has nothing left to hear.
async fn fan_out<const LANE_COUNT: usize>(
    mut receiver: mpsc::Receiver<FillerMessage>,
    senders: [mpsc::Sender<FillerMessage>; LANE_COUNT],
) -> Result<(), FillerError> {
    loop {
        let message = receiver.recv().await.unwrap_or(FillerMessage::Shutdown);
        for sender in &senders {
            match message {
                FillerMessage::OnJobsOpened => {
                    let _ = sender.try_send(message);
                }
                FillerMessage::Shutdown => {
                    let _ = sender.send(message).await;
                }
            }
        }
        if message == FillerMessage::Shutdown {
            return Ok(());
        }
    }
}

enum FillerEvent {
    Pass,
    Shutdown,
}

async fn filler_loop(
    filler: &mut Filler,
    receiver: &mut mpsc::Receiver<FillerMessage>,
) -> Result<(), FillerError> {
    let mut scan = tokio::time::interval(SCAN_INTERVAL);
    scan.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut outcome = PassOutcome::Idle;
    loop {
        let event = match outcome {
            // Pages are still owed, so the next pass starts at once unless a stop is queued.
            PassOutcome::MorePages => match receiver.try_recv() {
                Ok(FillerMessage::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) => {
                    FillerEvent::Shutdown
                }
                Ok(FillerMessage::OnJobsOpened) | Err(mpsc::error::TryRecvError::Empty) => {
                    FillerEvent::Pass
                }
            },
            PassOutcome::Idle | PassOutcome::ShutdownRequested => tokio::select! {
                message = receiver.recv() => match message {
                    Some(FillerMessage::OnJobsOpened) => FillerEvent::Pass,
                    Some(FillerMessage::Shutdown) | None => FillerEvent::Shutdown,
                },
                _ = scan.tick() => FillerEvent::Pass,
            },
        };
        outcome = match event {
            FillerEvent::Pass => match filler.fill_open_jobs(receiver).await {
                Ok(outcome) => outcome,
                // A database hiccup skips this pass; the next scan retries it, and the job
                // memory keeps every block already sent from being fetched again.
                Err(FillerError::Store(error)) => {
                    tracing::warn!(%error, "fill_pass_skipped");
                    PassOutcome::Idle
                }
                Err(error) => return Err(error),
            },
            FillerEvent::Shutdown => PassOutcome::ShutdownRequested,
        };
        if outcome == PassOutcome::ShutdownRequested {
            tracing::info!("range_filler_stopped");
            return Ok(());
        }
    }
}

// A pass walks one page of every open job, so a long hole never starves a fresh one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassOutcome {
    // Nothing is owed until the next scan or nudge: every job is finished, waiting, or blocked.
    Idle,
    MorePages,
    ShutdownRequested,
}

// Blocks sent but maybe not yet committed: the next scan resumes from here, not from the
// table, so a block waiting in the channel is never fetched twice. An entry lives until the
// table stops listing the job as open, i.e. until its last block has committed.
#[derive(Debug, Clone, Copy)]
struct JobMemory {
    next_slot: Slot,
    tip: Slot,
}

// Consecutive passes a job has waited for its end block.
#[derive(Debug, Clone, Copy, Default)]
struct Waiting {
    pass_count: u32,
    // Set while a job cut below the archive waits on the block above: the last slot its walk
    // claimed. Nothing below that block can change the answer, so later passes skip the walk.
    claimed_through_below_cut: Option<Slot>,
}

struct Filler {
    database: PgPool,
    gateway: Arc<RpcGateway>,
    routing: watch::Receiver<ArchiveRouting>,
    fill_sender: mpsc::Sender<ProcessorMessage>,
    memory: HashMap<JobId, JobMemory>,
    waiting: HashMap<JobId, Waiting>,
    // Consecutive passes each archive job ended without progress, on a retryable error or on a
    // block the chain names that the archive answered as skipped.
    retry_pass_counts: HashMap<JobId, u32>,
}

// One job's walk; `pending` is held back until the next block proves the slots between them
// skipped, so next_slot never passes a slot the parent chain has not vouched for.
struct JobWalk {
    job: SlotRangeJob,
    link: ChainLink,
    pending: Option<FinalizedBlock>,
}

enum WalkStep {
    Continue,
    Stop(PassOutcome),
}

// An EndRule with what it needs, resolved before the last page is walked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndCheck {
    ArchiveLacksIt,
    BlockAboveProves,
    Relist { finalized_before_listing: Slot },
}

// Which call failed: only a getBlock failure belongs to one slot and can be unmappable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchCall {
    BlockList,
    Block,
}

impl Filler {
    fn new(
        database: PgPool,
        gateway: Arc<RpcGateway>,
        routing: watch::Receiver<ArchiveRouting>,
        fill_sender: mpsc::Sender<ProcessorMessage>,
    ) -> Self {
        Self {
            database,
            gateway,
            routing,
            fill_sender,
            memory: HashMap::new(),
            waiting: HashMap::new(),
            retry_pass_counts: HashMap::new(),
        }
    }

    // Routed per pass, in SQL before the scan limit: the window's top rises as time passes, so
    // a job can move to the archive between passes (never back). Its memory stays behind and
    // the new lane resumes from the table. Nothing is walked until the window is known.
    async fn read_routed_jobs(&self) -> Result<Vec<SlotRangeJob>, FillerError> {
        let ArchiveRouting::Ready(window) = *self.routing.borrow() else {
            return Ok(Vec::new());
        };
        let endpoint = self.gateway.endpoint();
        let selection = match (endpoint, window) {
            (Endpoint::Provider, None) => JobSelection::All,
            (Endpoint::Provider, Some(window)) => JobSelection::OutsideArchiveWindow(window),
            (Endpoint::Archive, Some(window)) => JobSelection::InsideArchiveWindow(window),
            (Endpoint::Archive, None) => return Ok(Vec::new()),
        };
        let jobs = read_open_jobs(&self.database, JOB_SCAN_COUNT_MAX, selection).await?;
        debug_assert!(jobs.iter().all(|job| route(job.range, window) == endpoint));
        debug_assert!(jobs.iter().all(|job| {
            job.next_slot > job.range.start
                || window.is_none_or(|window| !window.straddled_by(job.range))
        }));
        Ok(jobs)
    }

    async fn fill_open_jobs(
        &mut self,
        receiver: &mut mpsc::Receiver<FillerMessage>,
    ) -> Result<PassOutcome, FillerError> {
        let jobs = self.read_routed_jobs().await?;
        // A job past the scan limit loses its memory too; it then resumes from the table and
        // at worst refetches blocks whose commit is idempotent.
        self.memory
            .retain(|job_id, _| jobs.iter().any(|job| job.id == *job_id));
        self.waiting
            .retain(|job_id, _| jobs.iter().any(|job| job.id == *job_id));
        self.retry_pass_counts
            .retain(|job_id, _| jobs.iter().any(|job| job.id == *job_id));
        let mut pass = PassOutcome::Idle;
        for job in jobs {
            match self.fill_job_page(job, receiver).await? {
                PassOutcome::ShutdownRequested => return Ok(PassOutcome::ShutdownRequested),
                PassOutcome::MorePages => pass = PassOutcome::MorePages,
                PassOutcome::Idle => {}
            }
        }
        Ok(pass)
    }

    async fn fill_job_page(
        &mut self,
        job: SlotRangeJob,
        receiver: &mut mpsc::Receiver<FillerMessage>,
    ) -> Result<PassOutcome, FillerError> {
        let claimed_through_below_cut = self
            .waiting
            .get(&job.id)
            .and_then(|waiting| waiting.claimed_through_below_cut);
        if let Some(claimed_through) = claimed_through_below_cut {
            self.await_block_above(job.id, job.range.end_inclusive, claimed_through)
                .await?;
            return Ok(PassOutcome::Idle);
        }
        let memory = self.memory.get(&job.id).copied();
        let (next_slot, link) = walk_start(&job, memory);
        // Everything left was already sent and waits in the channel for its commit.
        let Some(page) = next_page(next_slot, job.range.end_inclusive) else {
            return Ok(PassOutcome::Idle);
        };
        if memory.is_none() {
            tracing::info!(
                job_id = job.id.get(),
                start_slot = job.range.start.get(),
                end_slot = job.range.end_inclusive.get(),
                next_slot = next_slot.get(),
                endpoint = ?self.gateway.endpoint(),
                "fill_job_started"
            );
        }
        let end_check = if page.end_inclusive == job.range.end_inclusive {
            let rule = end_rule(self.gateway.endpoint(), job.end_kind);
            match self.end_check(job.id, rule).await {
                Some(end_check) => Some(end_check),
                None => return Ok(PassOutcome::Idle),
            }
        } else {
            None
        };
        let mut walk = JobWalk {
            job,
            link,
            pending: None,
        };
        if let WalkStep::Stop(outcome) = self.walk_page(&mut walk, page, receiver).await? {
            self.flush_pending(&mut walk).await?;
            return Ok(outcome);
        }
        if let Some(end_check) = end_check {
            self.finish_job(walk, page.start, end_check).await?;
            return Ok(PassOutcome::Idle);
        }
        // A full page listed no block only above the RPC node's finalized root (getBlocks
        // clamps there), so the job waits for the next scan instead of spinning.
        if walk.pending.is_none() {
            return Ok(PassOutcome::Idle);
        }
        // The next pass resumes from memory at the flushed block, so a page whose tail was
        // clamped is listed again rather than skipped.
        self.flush_pending(&mut walk).await?;
        Ok(PassOutcome::MorePages)
    }

    // Only a relisted end needs the finalized root, and its snapshot is taken before the
    // listing: a root that advances past end_slot during the walk would otherwise read an end
    // block the clamped listing never saw as absent. The archive's getSlot costs 12 to 17 s
    // and answers nothing its jobs need, so the archive lane never asks. None: retry later.
    async fn end_check(&self, job_id: JobId, rule: EndRule) -> Option<EndCheck> {
        match rule {
            EndRule::ArchiveLacksIt => Some(EndCheck::ArchiveLacksIt),
            EndRule::BlockAboveProves => Some(EndCheck::BlockAboveProves),
            EndRule::Relist => match self.gateway.slot_finalized(RequestLane::Fill).await {
                Ok(finalized_before_listing) => Some(EndCheck::Relist {
                    finalized_before_listing,
                }),
                Err(error) => {
                    tracing::warn!(job_id = job_id.get(), %error, "fill_retry_later");
                    None
                }
            },
        }
    }

    async fn walk_page(
        &mut self,
        walk: &mut JobWalk,
        page: SlotRange,
        receiver: &mut mpsc::Receiver<FillerMessage>,
    ) -> Result<WalkStep, FillerError> {
        let slots = match self.gateway.blocks_in_range(page, RequestLane::Fill).await {
            Ok(slots) => slots,
            Err(error) => {
                return self
                    .on_fetch_error(walk, page.start, FetchCall::BlockList, error)
                    .await;
            }
        };
        let gateway = Arc::clone(&self.gateway);
        let mut handler = FillHandler { filler: self, walk };
        let flow = fetch_listed(
            &gateway,
            slots,
            RequestLane::Fill,
            shutdown_received(receiver),
            &mut handler,
        )
        .await?;
        Ok(match flow {
            ControlFlow::Continue(()) => WalkStep::Continue,
            ControlFlow::Break(outcome) => WalkStep::Stop(outcome),
        })
    }

    async fn accept_block(
        &mut self,
        walk: &mut JobWalk,
        block: FinalizedBlock,
    ) -> Result<WalkStep, FillerError> {
        if let Err(missing) = verify_parent_chain(walk.link, block.slot, block.parent_slot) {
            self.flush_pending(walk).await?;
            self.on_block_lacked(walk.job.id, missing).await?;
            return Ok(WalkStep::Stop(PassOutcome::Idle));
        }
        walk.link = ChainLink::Parent(block.slot);
        let next_slot_after = block.slot;
        if let Some(previous) = walk.pending.replace(block) {
            self.send_fill(walk.job.id, previous, next_slot_after)
                .await?;
        }
        Ok(WalkStep::Continue)
    }

    async fn on_fetch_error(
        &mut self,
        walk: &mut JobWalk,
        slot: Slot,
        call: FetchCall,
        error: RpcError,
    ) -> Result<WalkStep, FillerError> {
        let job_id = walk.job.id.get();
        if call == FetchCall::Block && is_unmappable(&error) {
            tracing::error!(job_id, slot = slot.get(), %error, "fill_block_unmappable");
            self.flush_pending(walk).await?;
            self.block_job(walk.job.id, JobBlock::Unmappable(slot))
                .await?;
            return Ok(WalkStep::Stop(PassOutcome::Idle));
        }
        match (self.gateway.classify(&error), call) {
            (RpcErrorClass::SkippedSlot, FetchCall::Block) => Ok(WalkStep::Continue),
            (RpcErrorClass::MissingInStorage, _) => {
                self.flush_pending(walk).await?;
                self.block_job(walk.job.id, JobBlock::MissingInStorage(slot))
                    .await?;
                Ok(WalkStep::Stop(PassOutcome::Idle))
            }
            // A skipped-slot code on getBlocks names no block, so the page is simply retried.
            (RpcErrorClass::Retry, _) | (RpcErrorClass::SkippedSlot, FetchCall::BlockList) => {
                tracing::warn!(job_id, slot = slot.get(), %error, "fill_retry_later");
                self.flush_pending(walk).await?;
                if self.retried_too_long(walk.job.id, &error) {
                    self.block_job(walk.job.id, JobBlock::ArchiveUnavailable(slot))
                        .await?;
                }
                Ok(WalkStep::Stop(PassOutcome::Idle))
            }
            (RpcErrorClass::ConfigurationBug, _) => {
                tracing::error!(job_id, slot = slot.get(), %error, "rpc_configuration_bug");
                Err(FillerError::Rpc(error))
            }
        }
    }

    async fn finish_job(
        &mut self,
        mut walk: JobWalk,
        page_start: Slot,
        end_check: EndCheck,
    ) -> Result<(), FillerError> {
        let end_slot = walk.job.range.end_inclusive;
        let pending_slot = walk.pending.as_ref().map(|block| block.slot);
        if let JobEnd::Missing(missing) = job_end(pending_slot, end_slot) {
            self.flush_pending(&mut walk).await?;
            // Every slot from page_start up to the end was asked for, so the walk claimed
            // through its last block, or through the slot before the page.
            let claimed_through =
                pending_slot.unwrap_or_else(|| Slot::new(page_start.get().saturating_sub(1)));
            return match end_check {
                EndCheck::ArchiveLacksIt => self.on_block_lacked(walk.job.id, missing).await,
                EndCheck::BlockAboveProves => {
                    self.await_block_above(walk.job.id, end_slot, claimed_through)
                        .await
                }
                EndCheck::Relist {
                    finalized_before_listing,
                } => {
                    let relist = SlotRange {
                        start: pending_slot.map_or(page_start, |slot| Slot::new(slot.get() + 1)),
                        end_inclusive: end_slot,
                    };
                    debug_assert!(relist.start <= relist.end_inclusive);
                    self.settle_missing_end(walk.job.id, missing, relist, finalized_before_listing)
                        .await
                }
            };
        }
        if let Some(block) = walk.pending.take() {
            self.send_fill(walk.job.id, block, Slot::new(end_slot.get() + 1))
                .await?;
        }
        // Memory stays until the table stops listing the job; the reconciler completes it
        // once coverage contains its range.
        tracing::info!(job_id = walk.job.id.get(), "fill_job_dispatched");
        Ok(())
    }

    // The job claims only its blocks and waits; the reconciler completes it once the block
    // above lands and coverage spans it. A block above naming a parent this walk never saw
    // blocks the job on that slot. The wait ends unproven only once the job that would fetch
    // the block above is itself blocked: a slow or restarting archive is only a longer wait,
    // and the rule reads the tables, so a restart never resets or shortens it.
    async fn await_block_above(
        &mut self,
        job_id: JobId,
        end_slot: Slot,
        claimed_through: Slot,
    ) -> Result<(), FillerError> {
        let coverage_start_above =
            read_coverage_start_above(&self.database, Slot::new(claimed_through.get() + 1)).await?;
        if let Some(missing) = block_missing_below(claimed_through, end_slot, coverage_start_above)
        {
            return self
                .block_job(job_id, JobBlock::MissingInStorage(missing))
                .await;
        }
        let above_cut = Slot::new(end_slot.get() + 1);
        if read_block_above_stuck(&self.database, above_cut).await? {
            return self
                .block_job(job_id, JobBlock::EndUnproven(end_slot))
                .await;
        }
        self.note_waiting(job_id, end_slot, None, Some(claimed_through));
        Ok(())
    }

    // getBlocks clamps silently at the node's finalized root, and a Geyser tip runs ahead of
    // it, so an end block above the root seen before the listing is not yet listed rather than
    // missing. Below it, one re-list guards against a provider answering from a lagging node:
    // any block that turns up there means the walk is not done, so the job waits.
    async fn settle_missing_end(
        &mut self,
        job_id: JobId,
        end_slot: Slot,
        relist: SlotRange,
        finalized_before_listing: Slot,
    ) -> Result<(), FillerError> {
        debug_assert_eq!(relist.end_inclusive, end_slot);
        if missing_end(end_slot, finalized_before_listing) == MissingEnd::AboveFinalized {
            self.note_waiting(job_id, end_slot, Some(finalized_before_listing), None);
            return Ok(());
        }
        match self
            .gateway
            .blocks_in_range(relist, RequestLane::Fill)
            .await
        {
            Ok(slots) if slots.is_empty() => {
                self.block_job(job_id, JobBlock::MissingInStorage(end_slot))
                    .await
            }
            Ok(_) => {
                self.note_waiting(job_id, end_slot, Some(finalized_before_listing), None);
                Ok(())
            }
            Err(error) => {
                tracing::warn!(job_id = job_id.get(), %error, "fill_retry_later");
                Ok(())
            }
        }
    }

    fn note_waiting(
        &mut self,
        job_id: JobId,
        end_slot: Slot,
        finalized: Option<Slot>,
        claimed_through_below_cut: Option<Slot>,
    ) -> u32 {
        let waiting = self.waiting.entry(job_id).or_default();
        waiting.pass_count = waiting.pass_count.saturating_add(1);
        waiting.claimed_through_below_cut = claimed_through_below_cut;
        let waiting_pass_count = waiting.pass_count;
        let endpoint = self.gateway.endpoint();
        if waited_long(waiting_pass_count) {
            tracing::warn!(
                job_id = job_id.get(),
                end_slot = end_slot.get(),
                finalized = finalized.map(Slot::get),
                waiting_pass_count,
                ?endpoint,
                "fill_job_waiting_long"
            );
        } else if claimed_through_below_cut.is_some() {
            tracing::info!(
                job_id = job_id.get(),
                end_slot = end_slot.get(),
                waiting_pass_count,
                "fill_job_waiting_for_block_above"
            );
        } else {
            tracing::info!(
                job_id = job_id.get(),
                end_slot = end_slot.get(),
                finalized = finalized.map(Slot::get),
                waiting_pass_count,
                "fill_job_waiting_for_rpc_finalized"
            );
        }
        waiting_pass_count
    }

    // A provider lists its blocks before fetching them, so one it returned no block for is
    // missing at once. The archive answers -32009 both for a skipped slot and, now and then,
    // for a real block it failed to read, so there a block the chain names but never arrived is
    // asked for again pass after pass (the next one resumes from memory and fetches it on its
    // own), and only one that never arrives within the retry budget is missing.
    async fn on_block_lacked(&mut self, job_id: JobId, missing: Slot) -> Result<(), FillerError> {
        if self.gateway.endpoint() == Endpoint::Archive && !self.retry_budget_spent(job_id) {
            tracing::warn!(
                job_id = job_id.get(),
                slot = missing.get(),
                "fill_block_lacked_retry_later"
            );
            return Ok(());
        }
        self.block_job(job_id, JobBlock::MissingInStorage(missing))
            .await
    }

    // A provider's retryable answers (429, 5xx) are its rate and health, retried for as long
    // as they last; the archive answering one slot that way pass after pass is a gap in what it
    // serves, which only an operator can close. An archive that does not answer at all is an
    // outage (faithful-cli restarting or loading epochs): it neither counts nor resets, so the
    // jobs simply resume once it is back.
    fn retried_too_long(&mut self, job_id: JobId, error: &RpcError) -> bool {
        if self.gateway.endpoint() != Endpoint::Archive || !is_answer_about_request(error) {
            return false;
        }
        self.retry_budget_spent(job_id)
    }

    // Any block sent clears the count, so only consecutive passes without progress add up.
    fn retry_budget_spent(&mut self, job_id: JobId) -> bool {
        debug_assert_eq!(self.gateway.endpoint(), Endpoint::Archive);
        let retry_pass_count = self.retry_pass_counts.entry(job_id).or_insert(0);
        *retry_pass_count = retry_pass_count.saturating_add(1);
        *retry_pass_count >= ARCHIVE_RETRY_PASS_COUNT_MAX
    }

    // Without a verified successor, only the pending block's own slot is claimed.
    async fn flush_pending(&mut self, walk: &mut JobWalk) -> Result<(), FillerError> {
        if let Some(block) = walk.pending.take() {
            let next_slot_after = Slot::new(block.slot.get() + 1);
            self.send_fill(walk.job.id, block, next_slot_after).await?;
        }
        Ok(())
    }

    async fn send_fill(
        &mut self,
        job_id: JobId,
        block: FinalizedBlock,
        next_slot_after: Slot,
    ) -> Result<(), FillerError> {
        debug_assert!(next_slot_after > block.slot);
        let tip = block.slot;
        let origin = BlockOrigin::Fill {
            job_id,
            next_slot_after,
        };
        self.fill_sender
            .send(ProcessorMessage::OnBlock(block, origin))
            .await
            .map_err(|_| FillerError::ProcessorGone)?;
        let memory = JobMemory {
            next_slot: next_slot_after,
            tip,
        };
        self.memory.insert(job_id, memory);
        self.waiting.remove(&job_id);
        self.retry_pass_counts.remove(&job_id);
        Ok(())
    }

    async fn block_job(&mut self, job_id: JobId, block: JobBlock) -> Result<(), FillerError> {
        tracing::warn!(job_id = job_id.get(), ?block, "fill_job_blocked");
        block_job(&self.database, job_id, block).await?;
        self.memory.remove(&job_id);
        self.waiting.remove(&job_id);
        self.retry_pass_counts.remove(&job_id);
        Ok(())
    }
}

struct FillHandler<'walk> {
    filler: &'walk mut Filler,
    walk: &'walk mut JobWalk,
}

impl ListedBlockHandler for FillHandler<'_> {
    type Stop = PassOutcome;
    type Error = FillerError;

    async fn on_result(
        &mut self,
        slot: Slot,
        result: FetchResult,
    ) -> Result<ControlFlow<PassOutcome>, FillerError> {
        let step = match result {
            Ok(block) => self.filler.accept_block(self.walk, block).await?,
            Err(error) => {
                self.filler
                    .on_fetch_error(self.walk, slot, FetchCall::Block, error)
                    .await?
            }
        };
        Ok(match step {
            WalkStep::Continue => ControlFlow::Continue(()),
            WalkStep::Stop(outcome) => ControlFlow::Break(outcome),
        })
    }
}

// Resolves on Shutdown or a closed channel. A nudge during a pass is redundant, since the pass
// already walks every open job, so nudges are consumed and ignored. A call can sit in a 429
// pause for tens of seconds, so shutdown must not wait it out.
async fn shutdown_received(receiver: &mut mpsc::Receiver<FillerMessage>) -> PassOutcome {
    while let Some(FillerMessage::OnJobsOpened) = receiver.recv().await {}
    PassOutcome::ShutdownRequested
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::actor::fake_rpc_node::spawn_fake_rpc_node;
    use crate::domain::config::{RpsMax, TransactionVersionMax};
    use crate::domain::ids::UnixSeconds;

    // Mid-walk, each block must hang off the previous one.
    #[test]
    fn parent_chain_accepts_contiguous_and_names_omitted_slot() {
        let link = ChainLink::Parent(Slot::new(99));
        assert_eq!(
            verify_parent_chain(link, Slot::new(101), Slot::new(99)),
            Ok(())
        );
        // getBlocks skipped 100, but block 101 says 100 exists: 100 is missing from storage.
        assert_eq!(
            verify_parent_chain(link, Slot::new(101), Slot::new(100)),
            Err(Slot::new(100))
        );
    }

    // After a restart, or on a fresh job, the first block must name a parent below next_slot;
    // a parent at or above it is a block the listing omitted.
    #[test]
    fn parent_chain_on_resume() {
        let link = ChainLink::Resumed {
            next_slot: Slot::new(200),
        };
        assert_eq!(
            verify_parent_chain(link, Slot::new(200), Slot::new(190)),
            Ok(())
        );
        assert_eq!(
            verify_parent_chain(link, Slot::new(203), Slot::new(199)),
            Ok(())
        );
        assert_eq!(
            verify_parent_chain(link, Slot::new(203), Slot::new(201)),
            Err(Slot::new(201))
        );
        // A job cut at an archive window edge may start right after a skipped slot: its first
        // block names a parent further down, which proves the slots between skipped.
        assert_eq!(
            verify_parent_chain(link, Slot::new(201), Slot::new(197)),
            Ok(())
        );
    }

    fn range(start: u64, end_inclusive: u64) -> SlotRange {
        SlotRange {
            start: Slot::new(start),
            end_inclusive: Slot::new(end_inclusive),
        }
    }

    fn window(bottom: u64, top: u64) -> ArchiveWindow {
        ArchiveWindow::new(Slot::new(bottom), Slot::new(top)).expect("window")
    }

    // Only a job the archive window holds end to end goes there; one reaching past either
    // edge, and every job while there is no window, stays with the provider.
    #[test]
    fn route_sends_only_wholly_served_jobs_to_the_archive() {
        let archive = Some(window(1_000_000, 1_500_000));
        let cases = [
            (range(1_000_000, 1_500_000), Endpoint::Archive),
            (range(1_400_000, 1_500_001), Endpoint::Provider),
            (range(999_999, 1_100_000), Endpoint::Provider),
            (range(1_600_000, 1_700_000), Endpoint::Provider),
        ];
        for (job_range, endpoint) in cases {
            assert_eq!(route(job_range, archive), endpoint, "{job_range:?}");
        }
        assert_eq!(route(range(1_000_000, 1_500_000), None), Endpoint::Provider);
    }

    // The coverage above a cut starts right after its first block's parent: a parent the walk
    // already claimed proves the tail skipped (its range simply has not merged yet, so this is
    // a wait, not a block), one inside the unclaimed tail, up to the end itself, is a block this
    // lane never returned, and one past the job's end means the block above has not landed.
    #[test]
    fn block_above_names_a_missing_block_only_inside_the_unclaimed_tail() {
        let missing = |coverage_start: Option<u64>| {
            block_missing_below(
                Slot::new(150),
                Slot::new(199),
                coverage_start.map(Slot::new),
            )
        };
        assert_eq!(missing(Some(181)), Some(Slot::new(180)));
        assert_eq!(missing(Some(200)), Some(Slot::new(199)));
        assert_eq!(missing(Some(151)), None);
        assert_eq!(missing(Some(201)), None);
        assert_eq!(missing(None), None);
    }

    fn job(next_slot: u64) -> SlotRangeJob {
        SlotRangeJob {
            id: JobId::new(1),
            range: range(100, 200),
            next_slot: Slot::new(next_slot),
            end_kind: JobEndKind::Block,
        }
    }

    // A walk resumes from the table unless memory is ahead of it; only memory knows the last
    // block sent, so only a resume from memory checks the next block against it.
    #[test]
    fn walk_start_resumes_from_memory_only_while_it_is_ahead() {
        let resumed = |next_slot: u64| ChainLink::Resumed {
            next_slot: Slot::new(next_slot),
        };
        assert_eq!(walk_start(&job(100), None), (Slot::new(100), resumed(100)));
        let memory = JobMemory {
            next_slot: Slot::new(151),
            tip: Slot::new(150),
        };
        assert_eq!(
            walk_start(&job(100), Some(memory)),
            (Slot::new(151), ChainLink::Parent(Slot::new(150)))
        );
        assert_eq!(
            walk_start(&job(160), Some(memory)),
            (Slot::new(160), resumed(160))
        );
    }

    fn block(slot: u64) -> FinalizedBlock {
        FinalizedBlock {
            slot: Slot::new(slot),
            parent_slot: Slot::new(slot - 1),
            block_time: UnixSeconds::new(1_790_000_000),
            transactions: Vec::new(),
            mapping_failures: Vec::new(),
        }
    }

    fn gateway(url: &str, endpoint: Endpoint) -> Arc<RpcGateway> {
        Arc::new(
            RpcGateway::new(
                url.parse().expect("url"),
                endpoint,
                RpsMax::new(100),
                TransactionVersionMax::new(1),
            )
            .expect("gateway"),
        )
    }

    fn routing(routing: ArchiveRouting) -> watch::Receiver<ArchiveRouting> {
        watch::channel(routing).1
    }

    async fn insert_job(database: &PgPool, start_slot: i64, end_slot: i64) -> JobId {
        insert_job_ending(database, (start_slot, end_slot), JobEndKind::Block).await
    }

    async fn insert_job_ending(
        database: &PgPool,
        (start_slot, end_slot): (i64, i64),
        end_kind: JobEndKind,
    ) -> JobId {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, end_kind)
             VALUES ($1, $2, $1, $3::job_end_kind) RETURNING id",
        )
        .bind(start_slot)
        .bind(end_slot)
        .bind(end_kind.as_str())
        .fetch_one(database)
        .await
        .expect("job inserts");
        JobId::new(id)
    }

    async fn filler_with_job(
        database: &PgPool,
        rpc_url: &str,
    ) -> (Filler, JobId, mpsc::Receiver<ProcessorMessage>) {
        let job_id = insert_job(database, 100, 200).await;
        let (fill_sender, fill_receiver) = mpsc::channel(4);
        let filler = Filler::new(
            database.clone(),
            gateway(rpc_url, Endpoint::Provider),
            routing(ArchiveRouting::Ready(None)),
            fill_sender,
        );
        (filler, job_id, fill_receiver)
    }

    async fn blocked_reason(database: &PgPool, job_id: JobId) -> Option<String> {
        sqlx::query_scalar("SELECT blocked_reason FROM slot_range_job WHERE id = $1")
            .bind(job_id.get())
            .fetch_one(database)
            .await
            .expect("reads job")
    }

    fn sent_slots(receiver: &mut mpsc::Receiver<ProcessorMessage>) -> Vec<(u64, u64)> {
        let mut sent = Vec::new();
        while let Ok(message) = receiver.try_recv() {
            match message {
                ProcessorMessage::OnBlock(
                    block,
                    BlockOrigin::Fill {
                        next_slot_after, ..
                    },
                ) => sent.push((block.slot.get(), next_slot_after.get())),
                other => panic!("unexpected message {other:?}"),
            }
        }
        sent
    }

    // A walk that fetched end_slot carries the job past it.
    #[sqlx::test(migrations = "../migrations")]
    async fn finish_job_carries_reached_end_past_it(database: PgPool) {
        // Never called: a reached end needs no RPC answer.
        let (mut filler, job_id, mut receiver) =
            filler_with_job(&database, "http://127.0.0.1:9").await;
        let walk = JobWalk {
            job: SlotRangeJob {
                id: job_id,
                ..job(100)
            },
            link: ChainLink::Parent(Slot::new(200)),
            pending: Some(block(200)),
        };
        let end_check = EndCheck::Relist {
            finalized_before_listing: Slot::new(200),
        };
        filler
            .finish_job(walk, Slot::new(100), end_check)
            .await
            .expect("finishes");
        assert_eq!(blocked_reason(&database, job_id).await, None);
        assert_eq!(sent_slots(&mut receiver), vec![(200, 201)]);
    }

    // A JSON-RPC node holding one block, at slot 200, whose finalized root moves to
    // `finalized_after_listing` once getBlocks has answered. Listing i answers `listings[i]`
    // (the last one repeats), clamped at the root as a real node clamps.
    struct FakeNode {
        finalized_before_listing: u64,
        finalized_after_listing: u64,
        listings: Vec<Vec<u64>>,
    }

    fn spawn_fake_node(node: FakeNode) -> String {
        let mut finalized = node.finalized_before_listing;
        let mut listing_count = 0;
        spawn_fake_rpc_node(move |method, params| match method {
            "getSlot" => Ok(serde_json::json!(finalized)),
            "getBlocks" => {
                let start = params[0].as_u64().expect("start");
                let end = params[1].as_u64().expect("end");
                let index = listing_count.min(node.listings.len() - 1);
                let listed: Vec<u64> = node.listings[index]
                    .iter()
                    .copied()
                    .filter(|slot| (start..=end.min(finalized)).contains(slot))
                    .collect();
                listing_count += 1;
                finalized = node.finalized_after_listing;
                Ok(serde_json::json!(listed))
            }
            other => panic!("unexpected method {other:?}"),
        })
    }

    async fn fill_last_page(database: &PgPool, node: FakeNode) -> Option<String> {
        let url = spawn_fake_node(node);
        let (mut filler, job_id, _fills) = filler_with_job(database, &url).await;
        let (_messages, mut receiver) = mpsc::channel(1);
        let job = SlotRangeJob {
            id: job_id,
            ..job(100)
        };
        let outcome = filler
            .fill_job_page(job, &mut receiver)
            .await
            .expect("fills");
        assert_eq!(outcome, PassOutcome::Idle);
        blocked_reason(database, job_id).await
    }

    // The root moves from 150 to 250 while the page is listed: the listing never saw block
    // 200, yet a root read after it would call 200 absent. The job must wait, not block.
    #[sqlx::test(migrations = "../migrations")]
    async fn root_advancing_during_listing_leaves_job_open(database: PgPool) {
        let node = FakeNode {
            finalized_before_listing: 150,
            finalized_after_listing: 250,
            listings: vec![vec![200]],
        };
        assert_eq!(fill_last_page(&database, node).await, None);
    }

    // The root already covers block 200, but the first listing came from a lagging node; the
    // re-list finds the block, so the job waits for the next pass to walk it.
    #[sqlx::test(migrations = "../migrations")]
    async fn lagging_listing_found_on_relist_leaves_job_open(database: PgPool) {
        let node = FakeNode {
            finalized_before_listing: 250,
            finalized_after_listing: 250,
            listings: vec![vec![], vec![200]],
        };
        assert_eq!(fill_last_page(&database, node).await, None);
    }

    // A finalized end block that neither listing returns is missing from the node's storage.
    #[sqlx::test(migrations = "../migrations")]
    async fn end_block_unlisted_below_root_blocks_job(database: PgPool) {
        let node = FakeNode {
            finalized_before_listing: 250,
            finalized_after_listing: 250,
            listings: vec![vec![]],
        };
        assert_eq!(
            fill_last_page(&database, node).await.as_deref(),
            Some("missing_in_storage:200")
        );
    }

    type MethodLog = Arc<Mutex<Vec<String>>>;

    // A node holding blocks at the given (slot, parent) pairs, finalized at 10_000, that logs
    // every method it is asked. As a provider it lists them; as an archive it never lists (the
    // filler must not ask) and answers every other slot -32009, as faithful-cli does for a
    // skipped slot. A slot in `failing` answers -32000, a retryable error the gateway does not
    // retry itself, so one pass costs one request. A block in `lacked_once` answers -32009 the
    // first time it is asked, as a transient archive read failure would.
    fn spawn_fake_chain(
        blocks: &'static [(u64, u64)],
        failing: &'static [u64],
        lacked_once: &'static [u64],
    ) -> (String, MethodLog) {
        let log: MethodLog = Arc::default();
        let logged = Arc::clone(&log);
        let mut lacked: Vec<u64> = lacked_once.to_vec();
        let url = spawn_fake_rpc_node(move |method, params| {
            logged.lock().expect("log").push(method.to_owned());
            match method {
                "getSlot" => Ok(serde_json::json!(10_000)),
                "getBlocks" => {
                    let start = params[0].as_u64().expect("start");
                    let end = params[1].as_u64().expect("end");
                    let listed: Vec<u64> = blocks
                        .iter()
                        .map(|(slot, _)| *slot)
                        .filter(|slot| (start..=end).contains(slot))
                        .collect();
                    Ok(serde_json::json!(listed))
                }
                "getBlock" => {
                    let slot = params[0].as_u64().expect("slot");
                    if failing.contains(&slot) {
                        return Err(-32000);
                    }
                    if let Some(index) = lacked.iter().position(|lacked| *lacked == slot) {
                        lacked.swap_remove(index);
                        return Err(-32009);
                    }
                    let (_, parent_slot) = blocks
                        .iter()
                        .find(|(block_slot, _)| *block_slot == slot)
                        .ok_or(-32009)?;
                    Ok(serde_json::json!({
                        "blockTime": 1_790_000_000,
                        "parentSlot": parent_slot,
                        "transactions": [],
                    }))
                }
                _ => Err(-32601),
            }
        });
        (url, log)
    }

    struct LaneRun {
        filler: Filler,
        job: SlotRangeJob,
        fills: mpsc::Receiver<ProcessorMessage>,
        log: MethodLog,
    }

    // One lane with job [start, end] open, its end set as `end_kind`, against a fake chain,
    // under an archive window of [archive_bottom, 100_000].
    async fn lane_with_job(
        database: &PgPool,
        endpoint: Endpoint,
        (start_slot, end_slot): (u64, u64),
        end_kind: JobEndKind,
        archive_bottom: u64,
        chain: (&'static [(u64, u64)], &'static [u64], &'static [u64]),
    ) -> LaneRun {
        let job_range = (start_slot as i64, end_slot as i64);
        let job_id = insert_job_ending(database, job_range, end_kind).await;
        let (url, log) = spawn_fake_chain(chain.0, chain.1, chain.2);
        let (fill_sender, fills) = mpsc::channel(16);
        let filler = Filler::new(
            database.clone(),
            gateway(&url, endpoint),
            routing(ArchiveRouting::Ready(Some(window(archive_bottom, 100_000)))),
            fill_sender,
        );
        let job = SlotRangeJob {
            id: job_id,
            range: range(start_slot, end_slot),
            next_slot: Slot::new(start_slot),
            end_kind,
        };
        LaneRun {
            filler,
            job,
            fills,
            log,
        }
    }

    impl LaneRun {
        async fn pass(&mut self) {
            let (_messages, mut receiver) = mpsc::channel(1);
            let outcome = self
                .filler
                .fill_job_page(self.job.clone(), &mut receiver)
                .await
                .expect("fills");
            assert_eq!(outcome, PassOutcome::Idle);
        }

        fn methods(&self) -> Vec<String> {
            std::mem::take(&mut *self.log.lock().expect("log"))
        }
    }

    // On the archive -32009 is a skipped slot, vouched for by the next block's parent, and the
    // walk reaches the job's end block without ever asking the archive's slow getSlot.
    #[sqlx::test(migrations = "../migrations")]
    async fn archive_walk_reads_skips_from_errors_and_reaches_its_end(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(100, 98), (101, 100), (103, 101), (107, 103)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Archive,
            (100, 107),
            JobEndKind::Block,
            0,
            (CHAIN, &[], &[]),
        )
        .await;
        lane.pass().await;
        assert_eq!(
            sent_slots(&mut lane.fills),
            vec![(100, 101), (101, 103), (103, 107), (107, 108)]
        );
        assert_eq!(blocked_reason(&database, lane.job.id).await, None);
        assert!(lane.methods().iter().all(|method| method == "getBlock"));
    }

    // Every archive job ends on a block, so an end the archive keeps answering as skipped is a
    // block it lacks: the job claims what it fetched, asks for the end again each pass, and
    // blocks on it once the retry budget is spent, instead of waiting for a block above that
    // cannot prove a slot the archive never served.
    #[sqlx::test(migrations = "../migrations")]
    async fn archive_job_whose_end_block_the_archive_lacks_blocks(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(100, 98), (101, 100), (103, 101)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Archive,
            (100, 107),
            JobEndKind::Block,
            0,
            (CHAIN, &[], &[]),
        )
        .await;
        for _ in 1..ARCHIVE_RETRY_PASS_COUNT_MAX {
            lane.pass().await;
            assert_eq!(blocked_reason(&database, lane.job.id).await, None);
        }
        lane.pass().await;
        assert_eq!(
            blocked_reason(&database, lane.job.id).await.as_deref(),
            Some("missing_in_storage:107")
        );
        // Each later pass resumes from memory after block 103 and asks only for 104 to 107.
        assert_eq!(
            sent_slots(&mut lane.fills),
            vec![(100, 101), (101, 103), (103, 104)]
        );
        assert!(lane.methods().iter().all(|method| method == "getBlock"));
    }

    // A block the archive lacks answers -32009 like a skipped slot; its child's parent names
    // it, and the job asks for it again each pass and blocks there once the retry budget is
    // spent, instead of skipping it.
    #[sqlx::test(migrations = "../migrations")]
    async fn archive_walk_blocks_on_a_block_it_lacks(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(100, 98), (101, 100), (103, 102)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Archive,
            (100, 103),
            JobEndKind::Block,
            0,
            (CHAIN, &[], &[]),
        )
        .await;
        for _ in 1..ARCHIVE_RETRY_PASS_COUNT_MAX {
            lane.pass().await;
            assert_eq!(blocked_reason(&database, lane.job.id).await, None);
        }
        lane.pass().await;
        assert_eq!(
            blocked_reason(&database, lane.job.id).await.as_deref(),
            Some("missing_in_storage:102")
        );
        assert_eq!(sent_slots(&mut lane.fills), vec![(100, 101), (101, 102)]);
    }

    // An archive that answers a real block -32009 once must not block the job on it: its
    // child's parent names it, so the pass stops there, and the next pass asks for it again,
    // gets it, and finishes the job.
    #[sqlx::test(migrations = "../migrations")]
    async fn archive_block_answered_as_skipped_once_is_fetched_on_the_next_pass(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(100, 98), (101, 100), (102, 101), (103, 102)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Archive,
            (100, 103),
            JobEndKind::Block,
            0,
            (CHAIN, &[], &[102]),
        )
        .await;
        lane.pass().await;
        assert_eq!(sent_slots(&mut lane.fills), vec![(100, 101), (101, 102)]);
        assert_eq!(blocked_reason(&database, lane.job.id).await, None);
        lane.pass().await;
        assert_eq!(sent_slots(&mut lane.fills), vec![(102, 103), (103, 104)]);
        assert_eq!(blocked_reason(&database, lane.job.id).await, None);
    }

    // An archive slot that keeps answering a retryable error is retried pass after pass, then
    // blocks the job with a reason naming the archive, so health counts it.
    #[sqlx::test(migrations = "../migrations")]
    async fn archive_slot_failing_pass_after_pass_blocks_the_job(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(100, 98), (101, 100), (103, 101)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Archive,
            (100, 103),
            JobEndKind::Block,
            0,
            (CHAIN, &[101], &[]),
        )
        .await;
        for _ in 1..ARCHIVE_RETRY_PASS_COUNT_MAX {
            lane.pass().await;
            assert_eq!(blocked_reason(&database, lane.job.id).await, None);
        }
        lane.pass().await;
        assert_eq!(
            blocked_reason(&database, lane.job.id).await.as_deref(),
            Some("archive_unavailable:101")
        );
        // Block 100 is sent once; every later pass resumes from memory at the failing slot.
        assert_eq!(sent_slots(&mut lane.fills), vec![(100, 101)]);
    }

    // A connection refused by a port nothing listens on: what an archive that is down answers.
    async fn refused_connection() -> RpcError {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        drop(listener);
        let error = reqwest::Client::new()
            .post(url)
            .send()
            .await
            .expect_err("nothing listens there");
        RpcError::Transport(error)
    }

    // An archive that does not answer at all is an outage, not a gap in what it serves: however
    // many passes it lasts, the job stays open and resumes once the archive is back.
    #[sqlx::test(migrations = "../migrations")]
    async fn archive_unreachable_pass_after_pass_leaves_the_job_open(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(100, 98), (101, 100), (103, 101)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Archive,
            (100, 103),
            JobEndKind::Block,
            0,
            (CHAIN, &[], &[]),
        )
        .await;
        for _ in 0..=ARCHIVE_RETRY_PASS_COUNT_MAX {
            let mut walk = JobWalk {
                job: lane.job.clone(),
                link: ChainLink::Resumed {
                    next_slot: lane.job.next_slot,
                },
                pending: None,
            };
            let error = refused_connection().await;
            lane.filler
                .on_fetch_error(&mut walk, Slot::new(100), FetchCall::Block, error)
                .await
                .expect("retries later");
        }
        assert_eq!(blocked_reason(&database, lane.job.id).await, None);
    }

    // A provider job cut just below the archive window may end on a skipped slot: it claims
    // its blocks and waits, without the relist that would block it as missing. Once the
    // archive's first block above names a parent the provider never listed, it blocks there,
    // reading only coverage.
    #[sqlx::test(migrations = "../migrations")]
    async fn provider_job_cut_below_the_archive_waits_for_the_block_above(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(120, 98), (150, 120)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Provider,
            (100, 199),
            JobEndKind::ArchiveLowerCut,
            200,
            (CHAIN, &[], &[]),
        )
        .await;
        lane.pass().await;
        assert_eq!(sent_slots(&mut lane.fills), vec![(120, 150), (150, 151)]);
        assert_eq!(blocked_reason(&database, lane.job.id).await, None);
        assert_eq!(lane.methods(), vec!["getBlocks", "getBlock", "getBlock"]);

        sqlx::query(
            "INSERT INTO slot_coverage (start_slot, end_slot, end_block_time)
             VALUES (181, 300, now())",
        )
        .execute(&database)
        .await
        .expect("the archive's first block above names parent 180");
        lane.pass().await;
        assert_eq!(
            blocked_reason(&database, lane.job.id).await.as_deref(),
            Some("missing_in_storage:180")
        );
        assert_eq!(lane.methods(), Vec::<String>::new());
    }

    // The archive's bottom may move after the cut, when an operator drops an epoch (the job
    // now ends well below it) or loads an older one (the job now routes to the archive). The
    // end was recorded as a cut, so on either lane the job claims its blocks and waits for the
    // block above instead of reading its possibly skipped end as missing.
    #[sqlx::test(migrations = "../migrations")]
    async fn cut_job_waits_for_the_block_above_wherever_the_archive_bottom_moves(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(120, 98), (150, 120)];
        let lanes = [(Endpoint::Provider, 300), (Endpoint::Archive, 0)];
        for (endpoint, archive_bottom) in lanes {
            let mut lane = lane_with_job(
                &database,
                endpoint,
                (100, 199),
                JobEndKind::ArchiveLowerCut,
                archive_bottom,
                (CHAIN, &[], &[]),
            )
            .await;
            lane.pass().await;
            assert_eq!(
                sent_slots(&mut lane.fills),
                vec![(120, 150), (150, 151)],
                "{endpoint:?}"
            );
            assert_eq!(blocked_reason(&database, lane.job.id).await, None);
            assert!(!lane.methods().iter().any(|method| method == "getSlot"));
            sqlx::query("DELETE FROM slot_range_job")
                .execute(&database)
                .await
                .expect("clears the job for the next lane");
        }
    }

    // A cut tail waits for as long as the job above it might still fetch its first block, a
    // slow archive included; once that job is blocked the block above cannot arrive, and the
    // wait becomes a block health can see. The rule reads only the tables, so it holds across
    // a restart.
    #[sqlx::test(migrations = "../migrations")]
    async fn cut_tail_ends_unproven_once_the_job_above_is_blocked(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(150, 98)];
        let mut lane = lane_with_job(
            &database,
            Endpoint::Provider,
            (100, 199),
            JobEndKind::ArchiveLowerCut,
            200,
            (CHAIN, &[], &[]),
        )
        .await;
        let above = insert_job(&database, 200, 300).await;
        for _ in 0..3 {
            lane.pass().await;
        }
        assert_eq!(blocked_reason(&database, lane.job.id).await, None);
        sqlx::query(
            "UPDATE slot_range_job SET blocked_reason = 'archive_unavailable:200' WHERE id = $1",
        )
        .bind(above.get())
        .execute(&database)
        .await
        .expect("blocks the job above");
        lane.pass().await;
        assert_eq!(
            blocked_reason(&database, lane.job.id).await.as_deref(),
            Some("end_unproven:199")
        );
    }

    // Seeds `count` jobs of 100 slots from `first_start` upward.
    async fn seed_jobs(database: &PgPool, first_start: i64, count: i64) {
        sqlx::query(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot)
             SELECT $1 + i * 100, $1 + i * 100 + 99, $1 + i * 100
             FROM generate_series(0, $2 - 1) AS i",
        )
        .bind(first_start)
        .bind(count)
        .execute(database)
        .await
        .expect("seeds jobs");
    }

    async fn routed_job_ranges(database: &PgPool, endpoint: Endpoint) -> Vec<(u64, u64)> {
        let filler = Filler::new(
            database.clone(),
            gateway("http://127.0.0.1:9", endpoint),
            routing(ArchiveRouting::Ready(Some(window(1_000, 100_000)))),
            mpsc::channel(1).0,
        );
        filler
            .read_routed_jobs()
            .await
            .expect("reads")
            .iter()
            .map(|job| (job.range.start.get(), job.range.end_inclusive.get()))
            .collect()
    }

    // A full scan of provider jobs nearer the tip never hides the archive's job below them.
    #[sqlx::test(migrations = "../migrations")]
    async fn archive_lane_reads_its_job_beneath_a_full_scan_of_provider_jobs(database: PgPool) {
        seed_jobs(&database, 200_000, i64::from(JOB_SCAN_COUNT_MAX.get()) + 1).await;
        insert_job(&database, 1_000, 1_999).await;
        assert_eq!(
            routed_job_ranges(&database, Endpoint::Archive).await,
            vec![(1_000, 1_999)]
        );
    }

    // Nor does a full scan of archive jobs hide the provider's job below the archive's bottom.
    #[sqlx::test(migrations = "../migrations")]
    async fn provider_lane_reads_its_job_beneath_a_full_scan_of_archive_jobs(database: PgPool) {
        seed_jobs(&database, 2_000, i64::from(JOB_SCAN_COUNT_MAX.get()) + 1).await;
        insert_job(&database, 100, 999).await;
        assert_eq!(
            routed_job_ranges(&database, Endpoint::Provider).await,
            vec![(100, 999)]
        );
    }

    // Until the archive window is known neither lane walks anything: a job routed then could
    // send the archive's share of history to the metered provider.
    #[sqlx::test(migrations = "../migrations")]
    async fn no_lane_walks_while_the_archive_window_is_pending(database: PgPool) {
        const CHAIN: &[(u64, u64)] = &[(100, 98), (101, 100)];
        insert_job(&database, 100, 101).await;
        for endpoint in [Endpoint::Provider, Endpoint::Archive] {
            let (url, log) = spawn_fake_chain(CHAIN, &[], &[]);
            let (fill_sender, mut fills) = mpsc::channel(4);
            let mut filler = Filler::new(
                database.clone(),
                gateway(&url, endpoint),
                routing(ArchiveRouting::Pending),
                fill_sender,
            );
            let (_messages, mut receiver) = mpsc::channel(1);
            let outcome = filler.fill_open_jobs(&mut receiver).await.expect("passes");
            assert_eq!(outcome, PassOutcome::Idle, "{endpoint:?}");
            assert_eq!(sent_slots(&mut fills), vec![]);
            assert_eq!(*log.lock().expect("log"), Vec::<String>::new());
        }
    }

    // Pages cover the job exactly, never past end_slot, at most one page size each.
    #[test]
    fn next_page_clamps_to_job_end() {
        let page = next_page(Slot::new(10), Slot::new(5000)).unwrap();
        assert_eq!(page.end_inclusive, Slot::new(1009));
        let last = next_page(Slot::new(4500), Slot::new(5000)).unwrap();
        assert_eq!(last.end_inclusive, Slot::new(5000));
        assert_eq!(next_page(Slot::new(5001), Slot::new(5000)), None);
    }
}
