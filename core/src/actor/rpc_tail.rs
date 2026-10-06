// The RPC live source: lists the slots past the cursor with getBlocks, fetches only the listed
// ones, and sends each block to the processor tagged with the live source it stands in for.
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::actor::messages::ProcessorMessage;
use crate::actor::ordered_fetch::{FetchResult, ListedBlockHandler, fetch_listed};
use crate::domain::block::{BlockOrigin, LiveSource};
use crate::domain::error::{RpcError, RpcErrorClass};
use crate::domain::ids::{Slot, SlotRange};
use crate::gateway::rpc::{RequestLane, RpcGateway, is_unmappable};
use crate::store::read_cursor;

const TAIL_INTERVAL: Duration = Duration::from_secs(2);
// One listing window past the cursor, open-ended on purpose: getBlocks clamps the end at the
// node's finalized root, so a caught-up tail sees a short page and never calls getSlot. The
// window covers at least a tick's worth of slots so a longer tick cannot fall behind by design.
const TAIL_PAGE_SLOT_MIN: u64 = 64;
// A pace faster than any measured (0.27 s slots in 2026-10; the protocol's nominal pace is
// 0.4 s), so the window stays ahead of the chain even if slots speed up further.
const FAST_SLOT_SECONDS: f64 = 0.2;
// Beyond this lag the tail jumps to the finalized slot; the skipped stretch is a hole in
// coverage that the reconciler turns into a job, so history never delays live blocks (about
// 40 s at the 0.27 s slots measured in 2026-10).
// Short stalls (a slow getBlock, a 429 pause) replay live instead of spawning a job each.
const TAIL_LAG_SLOT_MAX: u64 = 150;

#[derive(Debug, Error)]
pub enum TailError {
    #[error("rpc: {0}")]
    Rpc(#[from] RpcError),
    #[error("block processor channel closed")]
    ProcessorGone,
}

// First slot the tail fetches: right after the cursor, or the finalized slot itself on
// first boot and after a lag too long to replay live.
fn tail_start(cursor: Option<Slot>, finalized: Slot) -> Slot {
    match cursor {
        Some(cursor) if finalized.get().saturating_sub(cursor.get()) <= TAIL_LAG_SLOT_MAX => {
            Slot::new(cursor.get() + 1)
        }
        _ => finalized,
    }
}

fn tail_window_slots(tick: Duration) -> u64 {
    let slots_per_tick = (tick.as_secs_f64() / FAST_SLOT_SECONDS).ceil() as u64;
    let window = slots_per_tick.max(TAIL_PAGE_SLOT_MIN);
    debug_assert!(window >= TAIL_PAGE_SLOT_MIN);
    window
}

fn tail_page(start: Slot) -> SlotRange {
    let window = tail_window_slots(TAIL_INTERVAL);
    let page = SlotRange {
        start,
        end_inclusive: Slot::new(start.get().saturating_add(window - 1)),
    };
    debug_assert!(page.start <= page.end_inclusive);
    debug_assert!(page.end_inclusive.get() - page.start.get() < window);
    page
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageFill {
    // The root clamped the listing inside the window: the tail is within a page of the tip.
    Short,
    // The window's last slot was listed, so the root may lie far beyond it.
    Full,
}

fn page_fill(page: SlotRange, slots: &[Slot]) -> PageFill {
    debug_assert!(
        slots
            .iter()
            .all(|slot| (page.start..=page.end_inclusive).contains(slot))
    );
    if slots.last() == Some(&page.end_inclusive) {
        PageFill::Full
    } else {
        PageFill::Short
    }
}

// Runs until `stop` turns true or its sender is dropped. Stopping drops a tick in flight,
// which aborts its getBlock calls; a block not yet sent is fetched again from the cursor.
pub async fn run(
    database: PgPool,
    gateway: Arc<RpcGateway>,
    live_sender: mpsc::Sender<ProcessorMessage>,
    mut stop: watch::Receiver<bool>,
    tag: LiveSource,
) -> Result<(), TailError> {
    let result = tokio::select! {
        biased;
        () = stop_requested(&mut stop) => Ok(()),
        result = tail_loop(&database, &gateway, &live_sender, tag) => result,
    };
    tracing::info!("rpc_tail_stopped");
    result
}

async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    // A dropped sender can never ask again, so it counts as a stop.
    let _ = stop.wait_for(|stopped| *stopped).await;
}

async fn tail_loop(
    database: &PgPool,
    gateway: &Arc<RpcGateway>,
    live_sender: &mpsc::Sender<ProcessorMessage>,
    tag: LiveSource,
) -> Result<(), TailError> {
    let mut ticker = tokio::time::interval(TAIL_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The tail is the live source, so a database blip at boot delays it rather than ending it.
    let mut cursor = loop {
        ticker.tick().await;
        match read_cursor(database).await {
            Ok(cursor) => break cursor.map(|cursor| cursor.slot),
            Err(error) => tracing::warn!(%error, "tail_cursor_read_failed"),
        }
    };
    tracing::info!(cursor = cursor.map(Slot::get), "rpc_tail_started");
    loop {
        ticker.tick().await;
        cursor = tail_tick(gateway, live_sender, cursor, tag).await?;
    }
}

// Returns the slot through which blocks were dispatched; an unfinished tick resumes next time.
// getSlot is called only on first boot and after a full page, where the lag decides a jump.
async fn tail_tick(
    gateway: &Arc<RpcGateway>,
    live_sender: &mpsc::Sender<ProcessorMessage>,
    cursor: Option<Slot>,
    tag: LiveSource,
) -> Result<Option<Slot>, TailError> {
    let start = match cursor {
        Some(cursor) => Slot::new(cursor.get() + 1),
        None => match gateway.slot_finalized(RequestLane::Live).await {
            Ok(finalized) => finalized,
            Err(error) => return tail_error(gateway, cursor, None, error),
        },
    };
    let mut page = tail_page(start);
    let mut slots = match gateway.blocks_in_range(page, RequestLane::Live).await {
        Ok(slots) => slots,
        Err(error) => return tail_error(gateway, cursor, Some(page.start), error),
    };
    if cursor.is_some() && page_fill(page, &slots) == PageFill::Full {
        let finalized = match gateway.slot_finalized(RequestLane::Live).await {
            Ok(slot) => slot,
            Err(error) => return tail_error(gateway, cursor, None, error),
        };
        let jumped = tail_start(cursor, finalized);
        if jumped != start {
            tracing::warn!(
                cursor = cursor.map(Slot::get),
                finalized = finalized.get(),
                "tail_jump"
            );
            page = tail_page(jumped);
            slots = match gateway.blocks_in_range(page, RequestLane::Live).await {
                Ok(slots) => slots,
                Err(error) => return tail_error(gateway, cursor, Some(page.start), error),
            };
        }
    }
    let mut handler = TailHandler {
        gateway,
        live_sender,
        tag,
        dispatched: cursor,
    };
    let stop = std::future::pending();
    match fetch_listed(gateway, slots, RequestLane::Live, stop, &mut handler).await? {
        ControlFlow::Break(resume) => Ok(resume),
        ControlFlow::Continue(()) => Ok(handler.dispatched),
    }
}

// Stops with the cursor the next tick resumes from.
struct TailHandler<'sender> {
    gateway: &'sender RpcGateway,
    live_sender: &'sender mpsc::Sender<ProcessorMessage>,
    tag: LiveSource,
    dispatched: Option<Slot>,
}

impl ListedBlockHandler for TailHandler<'_> {
    type Stop = Option<Slot>;
    type Error = TailError;

    async fn on_result(
        &mut self,
        slot: Slot,
        result: FetchResult,
    ) -> Result<ControlFlow<Option<Slot>>, TailError> {
        debug_assert!(self.dispatched.is_none_or(|dispatched| dispatched < slot));
        match result {
            Ok(block) => {
                self.live_sender
                    .send(ProcessorMessage::OnBlock(
                        block,
                        BlockOrigin::Live(self.tag),
                    ))
                    .await
                    .map_err(|_| TailError::ProcessorGone)?;
                self.dispatched = Some(slot);
            }
            // Before anything is stored no later block can reveal this one as a hole, so the
            // tick stops here and the next one starts over from the finalized slot.
            Err(error) if is_unmappable(&error) && self.dispatched.is_none() => {
                tracing::warn!(slot = slot.get(), %error, "tail_block_unmappable_before_cursor");
                return Ok(ControlFlow::Break(None));
            }
            // Retrying would stall live forever. The next block names this slot as its
            // parent, so coverage keeps a hole there; the reconciler's job for it is blocked
            // as unmappable by the filler, where health reports it.
            Err(error) if is_unmappable(&error) => {
                tracing::error!(slot = slot.get(), %error, "tail_block_unmappable");
                self.dispatched = Some(slot);
            }
            Err(error) => match self.gateway.classify(&error) {
                RpcErrorClass::SkippedSlot => self.dispatched = Some(slot),
                _ => {
                    return tail_error(self.gateway, self.dispatched, Some(slot), error)
                        .map(ControlFlow::Break);
                }
            },
        }
        Ok(ControlFlow::Continue(()))
    }
}

// Missing or transient: keep the cursor and try the same slots on the next tick.
fn tail_error(
    gateway: &RpcGateway,
    cursor: Option<Slot>,
    slot: Option<Slot>,
    error: RpcError,
) -> Result<Option<Slot>, TailError> {
    let slot = slot.map(Slot::get);
    match gateway.classify(&error) {
        RpcErrorClass::ConfigurationBug => {
            tracing::error!(slot, %error, "rpc_configuration_bug");
            Err(TailError::Rpc(error))
        }
        class => {
            tracing::warn!(slot, ?class, %error, "tail_retry_later");
            Ok(cursor)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::actor::fake_rpc_node::spawn_fake_rpc_node;
    use crate::domain::config::{RpsMax, TransactionVersionMax};
    use crate::gateway::rpc::Endpoint;

    // The listing window is the 64-slot minimum at the 2 s tick and grows with a longer tick to
    // what a 0.2 s chain produces in it.
    #[test]
    fn tail_window_covers_a_tick_of_fast_slots() {
        assert_eq!(tail_window_slots(Duration::from_secs(2)), 64);
        assert_eq!(tail_window_slots(Duration::from_secs(60)), 300);
    }

    // The tail replays a short lag live and jumps over a long one.
    #[test]
    fn tail_start_jumps_over_long_lag() {
        assert_eq!(tail_start(None, Slot::new(1000)), Slot::new(1000));
        assert_eq!(
            tail_start(Some(Slot::new(990)), Slot::new(1000)),
            Slot::new(991)
        );
        assert_eq!(
            tail_start(Some(Slot::new(1000)), Slot::new(1000)),
            Slot::new(1001)
        );
        assert_eq!(
            tail_start(Some(Slot::new(100)), Slot::new(1000)),
            Slot::new(1000)
        );
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct Calls {
        slot_finalized_count: u32,
        block_slots: Vec<u64>,
    }

    // A node whose blocks are `blocks`, rooted at `finalized`. Every getBlock answers "slot
    // skipped", so the tail sends nothing and the test reads only which calls it made.
    async fn tick_against(cursor: u64, finalized: u64, blocks: Vec<u64>) -> (Option<Slot>, Calls) {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let node_calls = Arc::clone(&calls);
        let url = spawn_fake_rpc_node(move |method, params| {
            let mut calls = node_calls.lock().expect("lock");
            match method {
                "getSlot" => {
                    calls.slot_finalized_count += 1;
                    Ok(serde_json::json!(finalized))
                }
                "getBlocks" => {
                    let start = params[0].as_u64().expect("start");
                    let end = params[1].as_u64().expect("end").min(finalized);
                    let listed: Vec<u64> = blocks
                        .iter()
                        .copied()
                        .filter(|slot| (start..=end).contains(slot))
                        .collect();
                    Ok(serde_json::json!(listed))
                }
                "getBlock" => {
                    calls.block_slots.push(params[0].as_u64().expect("slot"));
                    Err(-32007)
                }
                other => panic!("unexpected method {other:?}"),
            }
        });
        let gateway = RpcGateway::new(
            url.parse().expect("url"),
            Endpoint::Provider,
            RpsMax::new(100),
            TransactionVersionMax::new(1),
        )
        .expect("gateway");
        let (live_sender, _live_receiver) = mpsc::channel(4);
        let resumed = tail_tick(
            &Arc::new(gateway),
            &live_sender,
            Some(Slot::new(cursor)),
            LiveSource::RpcTail,
        )
        .await
        .expect("ticks");
        let mut calls = std::mem::take(&mut *calls.lock().expect("lock"));
        // getBlock calls run concurrently, so only the set of fetched slots is stable.
        calls.block_slots.sort_unstable();
        (resumed, calls)
    }

    // A caught-up tail lists past the cursor, fetches only the listed slots and never asks
    // for the finalized slot: skipped slots cost no call at all.
    #[tokio::test]
    async fn short_listing_fetches_only_listed_slots() {
        let (resumed, calls) = tick_against(100, 110, vec![100, 101, 103, 106, 111]).await;
        assert_eq!(resumed, Some(Slot::new(106)));
        assert_eq!(
            calls,
            Calls {
                slot_finalized_count: 0,
                block_slots: vec![101, 103, 106],
            }
        );
    }

    // A full page means the tail may be far behind: one getSlot measures the lag, and past
    // TAIL_LAG_SLOT_MAX the tail fetches from the tip instead of the stale page.
    #[tokio::test]
    async fn full_listing_far_behind_jumps_to_tip() {
        let (resumed, calls) = tick_against(100, 1000, (101..=1000).collect()).await;
        assert_eq!(resumed, Some(Slot::new(1000)));
        assert_eq!(
            calls,
            Calls {
                slot_finalized_count: 1,
                block_slots: vec![1000],
            }
        );
    }
}
