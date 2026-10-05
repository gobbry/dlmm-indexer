// The archive window, read off every hot path: on faithful-cli getFirstAvailableBlock takes 9
// to 13 s and getSlot 12 to 17 s, so they run once at boot, beside live rather than before
// it, and again on a slow interval as the week-old line moves up. The week-old block itself
// is found by getBlockTime bisection on the archive, which answers each probe in under a
// millisecond.
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::actor::messages::FillerMessage;
use crate::actor::range_filler::FillerError;
use crate::domain::config::ARCHIVE_SAFE_LAG_SECONDS;
use crate::domain::error::RpcError;
use crate::domain::ids::{Slot, UnixSeconds};
use crate::domain::job::{ArchiveRouting, ArchiveWindow};
use crate::gateway::rpc::{RequestLane, RpcGateway};

// The window's top moves about 2,200 slots in ten minutes, which a job cut at an older top
// simply leaves to the provider: a margin, never a correctness question.
const REFRESH_INTERVAL: Duration = Duration::from_secs(600);
// Until the window is known nothing is filled at all, so an archive that is down or still
// loading its epochs is asked again soon; each read already spends the gateway's retries.
const UNREADABLE_RETRY_INTERVAL: Duration = Duration::from_secs(20);

// The oldest block at most a week old (the week line), or the archive's newest block when even
// that is older; a block either way, so the archive job cut at it ends on one.
fn window_top(newest_block: Slot, week_line_block: Option<Slot>) -> Slot {
    week_line_block.unwrap_or(newest_block)
}

// None when the archive's first block is already younger than a week: a window of that block
// alone would hold nothing week-old, yet still cut holes and make the provider job below it
// wait on the archive.
pub async fn resolve(
    gateway: &RpcGateway,
    now: UnixSeconds,
) -> Result<Option<ArchiveWindow>, RpcError> {
    let (bottom, finalized) = tokio::try_join!(
        gateway.first_available_block(RequestLane::Fill),
        gateway.slot_finalized(RequestLane::Fill),
    )?;
    let newest = gateway
        .block_near(finalized, bottom.min(finalized), RequestLane::Fill)
        .await?;
    let first = gateway
        .first_block_from(bottom.min(newest.slot), newest.slot, RequestLane::Fill)
        .await?;
    let week_old = UnixSeconds::new(now.get() - ARCHIVE_SAFE_LAG_SECONDS);
    if first.block_time >= week_old {
        return Ok(None);
    }
    let week_line_block = gateway
        .slot_at_or_after_from(newest, first.slot, week_old, RequestLane::Fill)
        .await?;
    let top = window_top(newest.slot, week_line_block);
    debug_assert!(top > first.slot);
    tracing::info!(
        bottom = bottom.get(),
        top = top.get(),
        archive_finalized = finalized.get(),
        week_old = week_old.get(),
        "archive_window_resolved"
    );
    Ok(ArchiveWindow::new(bottom, top))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowRead {
    Resolved(Option<ArchiveWindow>),
    Failed,
}

// What a read leaves published, and how long until the next one. A failed read never ends the
// lane: while the window is unknown, routing stays Pending, so live and the processor run on
// and only filling waits (a week-old hole never reaches the metered provider in the
// meantime); once a window is known, a failed refresh keeps it.
fn after_read(published: ArchiveRouting, read: WindowRead) -> (ArchiveRouting, Duration) {
    match (read, published) {
        (WindowRead::Resolved(window), _) => (ArchiveRouting::Ready(window), REFRESH_INTERVAL),
        (WindowRead::Failed, ArchiveRouting::Pending) => {
            (ArchiveRouting::Pending, UNREADABLE_RETRY_INTERVAL)
        }
        (WindowRead::Failed, ready) => (ready, REFRESH_INTERVAL),
    }
}

pub async fn run(
    gateway: Arc<RpcGateway>,
    sender: watch::Sender<ArchiveRouting>,
    mut receiver: mpsc::Receiver<FillerMessage>,
) -> Result<(), FillerError> {
    let mut wait = Duration::ZERO;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = stop_requested(&mut receiver) => return Ok(()),
        }
        let started = Instant::now();
        let now = UnixSeconds::new(Utc::now().timestamp());
        let resolved = tokio::select! {
            resolved = resolve(&gateway, now) => resolved,
            _ = stop_requested(&mut receiver) => return Ok(()),
        };
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let published = *sender.borrow();
        let read = match &resolved {
            Ok(window) => WindowRead::Resolved(*window),
            Err(_) => WindowRead::Failed,
        };
        let (routing, next_wait) = after_read(published, read);
        let retry_in_ms = next_wait.as_millis() as u64;
        match (resolved, routing) {
            (Ok(None), _) => tracing::warn!(elapsed_ms, "archive_holds_nothing_a_week_old"),
            (Ok(Some(_)), _) => {}
            (Err(error), ArchiveRouting::Pending) => {
                tracing::error!(%error, elapsed_ms, retry_in_ms, "archive_window_unreadable");
            }
            (Err(error), ArchiveRouting::Ready(_)) => {
                tracing::warn!(%error, elapsed_ms, "archive_window_refresh_failed");
            }
        }
        sender.send_if_modified(|current| {
            let changed = *current != routing;
            *current = routing;
            changed
        });
        wait = next_wait;
    }
}

// Nudges mean nothing here; Shutdown or a closed channel ends the loop, even mid-request.
async fn stop_requested(receiver: &mut mpsc::Receiver<FillerMessage>) {
    while let Some(FillerMessage::OnJobsOpened) = receiver.recv().await {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::fake_rpc_node::spawn_fake_rpc_node;
    use crate::domain::config::{RpsMax, TransactionVersionMax};
    use crate::gateway::rpc::Endpoint;

    const NOW: i64 = 1_800_000_000;
    const FIRST_SLOT: u64 = 1_000;
    const SLOTS_PER_SECOND: u64 = 2;

    // An archive serving FIRST_SLOT up to `newest_slot`, a block on every even slot, two slots
    // a second, its newest block `newest_age_seconds` old; it answers what faithful-cli
    // answers, -32009 for a skipped slot and -32004 outside its epochs.
    // The first `unreadable_count` getFirstAvailableBlock calls answer -32000, an error the
    // gateway does not retry itself, as an archive still loading its epochs might.
    fn spawn_fake_archive(
        newest_slot: u64,
        newest_age_seconds: i64,
        unreadable_count: u32,
    ) -> String {
        debug_assert!(newest_slot.is_multiple_of(2));
        let time_of = move |slot: u64| {
            let behind_newest =
                i64::try_from((newest_slot - slot) / SLOTS_PER_SECOND).expect("fits");
            NOW - newest_age_seconds - behind_newest
        };
        let mut unreadable_left = unreadable_count;
        spawn_fake_rpc_node(move |method, params| match method {
            "getFirstAvailableBlock" if unreadable_left > 0 => {
                unreadable_left -= 1;
                Err(-32000)
            }
            "getFirstAvailableBlock" => Ok(serde_json::json!(FIRST_SLOT)),
            "getSlot" => Ok(serde_json::json!(newest_slot)),
            "getBlockTime" => {
                let slot = params[0].as_u64().expect("slot");
                if !(FIRST_SLOT..=newest_slot).contains(&slot) {
                    return Err(-32004);
                }
                if slot % 2 == 1 {
                    return Err(-32009);
                }
                Ok(serde_json::json!(time_of(slot)))
            }
            other => panic!("unexpected method {other:?}"),
        })
    }

    fn archive_gateway(url: &str) -> RpcGateway {
        RpcGateway::new(
            url.parse().expect("url"),
            Endpoint::Archive,
            RpsMax::new(1_000),
            TransactionVersionMax::SUPPORTED,
        )
        .expect("gateway")
    }

    async fn resolve_against(newest_slot: u64, newest_age_seconds: i64) -> Option<ArchiveWindow> {
        let gateway = archive_gateway(&spawn_fake_archive(newest_slot, newest_age_seconds, 0));
        resolve(&gateway, UnixSeconds::new(NOW))
            .await
            .expect("resolves")
    }

    fn window(bottom: u64, top: u64) -> Option<ArchiveWindow> {
        ArchiveWindow::new(Slot::new(bottom), Slot::new(top))
    }

    // The top is the block on the week line by the clock, found without probing outside the
    // archive's epochs (a probe there would answer -32004 and fail the boot). When the
    // archive's newest block is older than a week the top is that block, and when even its
    // first block is younger there is no window at all.
    #[tokio::test]
    async fn window_top_is_the_week_line_block_within_the_archive() {
        let week_slots =
            u64::try_from(ARCHIVE_SAFE_LAG_SECONDS).expect("positive") * SLOTS_PER_SECOND;
        let newest_age_seconds = 1_000;
        let newest_slot = FIRST_SLOT + 4_000 + week_slots;
        let week_old_slot =
            newest_slot - (week_slots - SLOTS_PER_SECOND * newest_age_seconds as u64);
        assert_eq!(
            resolve_against(newest_slot, newest_age_seconds).await,
            window(FIRST_SLOT, week_old_slot)
        );
        assert_eq!(
            resolve_against(9_998, ARCHIVE_SAFE_LAG_SECONDS + 3_600).await,
            window(FIRST_SLOT, 9_998)
        );
        assert_eq!(
            resolve_against(FIRST_SLOT + 1_000, newest_age_seconds).await,
            None
        );
    }

    fn some_window() -> ArchiveRouting {
        ArchiveRouting::Ready(window(FIRST_SLOT, 9_998))
    }

    // An unreadable archive never stops the lane: before the first window it stays Pending and
    // is asked again soon, after it the published window stands until the next refresh.
    #[test]
    fn a_failed_read_keeps_what_is_published() {
        assert_eq!(
            after_read(ArchiveRouting::Pending, WindowRead::Failed),
            (ArchiveRouting::Pending, UNREADABLE_RETRY_INTERVAL)
        );
        assert_eq!(
            after_read(some_window(), WindowRead::Failed),
            (some_window(), REFRESH_INTERVAL)
        );
        assert_eq!(
            after_read(ArchiveRouting::Pending, WindowRead::Resolved(None)),
            (ArchiveRouting::Ready(None), REFRESH_INTERVAL)
        );
        assert_eq!(
            after_read(
                some_window(),
                WindowRead::Resolved(window(FIRST_SLOT, 10_000))
            ),
            (
                ArchiveRouting::Ready(window(FIRST_SLOT, 10_000)),
                REFRESH_INTERVAL
            )
        );
    }

    // An archive that does not answer at boot leaves the lane running (so the filler task, and
    // live with it, keep going) until it answers and the window is published.
    #[tokio::test]
    async fn archive_unreadable_at_boot_is_retried_until_its_window_is_published() {
        // run() reads the wall clock, so the archive's newest block is an hour past a week old
        // by it.
        let newest_age_seconds = NOW - Utc::now().timestamp() + ARCHIVE_SAFE_LAG_SECONDS + 3_600;
        let url = spawn_fake_archive(9_998, newest_age_seconds, 1);
        let (sender, mut routing) = watch::channel(ArchiveRouting::Pending);
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        let lane = tokio::spawn(run(Arc::new(archive_gateway(&url)), sender, stop_receiver));
        let deadline = UNREADABLE_RETRY_INTERVAL + Duration::from_secs(10);
        let published = tokio::time::timeout(
            deadline,
            routing.wait_for(|routing| matches!(routing, ArchiveRouting::Ready(Some(_)))),
        )
        .await
        .expect("published before the deadline")
        .map(|routing| *routing)
        .expect("lane alive");
        assert_eq!(published, ArchiveRouting::Ready(window(FIRST_SLOT, 9_998)));
        assert!(!lane.is_finished());
        stop_sender
            .send(FillerMessage::Shutdown)
            .await
            .expect("lane listening");
        assert!(lane.await.expect("joins").is_ok());
    }
}
