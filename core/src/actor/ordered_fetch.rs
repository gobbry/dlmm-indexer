// A sliding window of getBlock calls shared by the filler and the RPC tail.
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use tokio::task::JoinSet;

use crate::domain::block::FinalizedBlock;
use crate::domain::config::ARCHIVE_IN_FLIGHT_MAX;
use crate::domain::error::RpcError;
use crate::domain::ids::Slot;
use crate::gateway::rpc::{Endpoint, RequestLane, RpcGateway};

// A getBlock on an 8 MB body takes longer than one permit interval, so the fetch window holds
// about this many seconds of the lane's permits to keep the limiter, not latency, the pacer.
const FETCH_WINDOW_SECONDS: usize = 2;
// Results that finished ahead of a slow one wait in a buffer this many windows deep; past it
// no new call starts, so one stuck getBlock bounds memory at a few dozen blocks.
const FETCH_BUFFER_WINDOW_COUNT: usize = 2;

fn fetch_window_size(gateway: &RpcGateway, lane: RequestLane) -> usize {
    match gateway.endpoint() {
        Endpoint::Archive => ARCHIVE_IN_FLIGHT_MAX,
        Endpoint::Provider => usize::try_from(gateway.rps_max(lane).get())
            .unwrap_or(1)
            .saturating_mul(FETCH_WINDOW_SECONDS)
            .max(1),
    }
}

pub(crate) type FetchResult = Result<FinalizedBlock, RpcError>;

// A sliding window of getBlock calls that hands results back in slot order. A call that
// finishes early waits in `completed` while the next one starts, so one slow or retried call
// holds back only its own result, never the whole window, until the buffer fills. Dropping it
// aborts calls in flight.
pub(crate) struct OrderedFetch {
    gateway: Arc<RpcGateway>,
    lane: RequestLane,
    slots: Vec<Slot>,
    window: usize,
    buffer_count_max: usize,
    spawned_count: usize,
    yielded_count: usize,
    in_flight: JoinSet<(usize, FetchResult)>,
    completed: BTreeMap<usize, FetchResult>,
}

impl OrderedFetch {
    pub(crate) fn start(gateway: &Arc<RpcGateway>, slots: Vec<Slot>, lane: RequestLane) -> Self {
        let window = fetch_window_size(gateway, lane);
        let mut fetch = Self {
            gateway: Arc::clone(gateway),
            lane,
            slots,
            window,
            buffer_count_max: window * FETCH_BUFFER_WINDOW_COUNT,
            spawned_count: 0,
            yielded_count: 0,
            in_flight: JoinSet::new(),
            completed: BTreeMap::new(),
        };
        fetch.refill();
        fetch
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    // Only calls in flight use permits, so only they count against the window; a full buffer
    // is the back-pressure that keeps memory bounded behind one slow call.
    fn refill(&mut self) {
        while self.spawned_count < self.slots.len()
            && self.in_flight.len() < self.window
            && self.completed.len() < self.buffer_count_max
        {
            let position = self.spawned_count;
            let slot = self.slots[position];
            let gateway = Arc::clone(&self.gateway);
            let lane = self.lane;
            self.in_flight
                .spawn(async move { (position, gateway.block(slot, lane).await) });
            self.spawned_count += 1;
        }
        debug_assert!(self.yielded_count < self.spawned_count || self.yielded_count == self.len());
    }

    pub(crate) async fn next(&mut self) -> Option<(Slot, FetchResult)> {
        let position = self.yielded_count;
        let slot = *self.slots.get(position)?;
        // The wanted call is in flight, so at most `window` joins pass before it lands.
        for _ in 0..=self.window {
            if let Some(result) = self.completed.remove(&position) {
                self.yielded_count += 1;
                self.refill();
                return Some((slot, result));
            }
            match self.in_flight.join_next().await {
                Some(Ok((done, result))) => {
                    self.completed.insert(done, result);
                }
                Some(Err(join_error)) => std::panic::resume_unwind(join_error.into_panic()),
                None => break,
            }
        }
        debug_assert!(false, "slot {} was never in flight", slot.get());
        None
    }
}

// What a caller does with each fetched block. A trait rather than a closure: an async closure
// borrowing the caller's state trips rustc's higher-ranked `Send` check at the spawn site.
pub(crate) trait ListedBlockHandler {
    type Stop;
    type Error;
    fn on_result(
        &mut self,
        slot: Slot,
        result: FetchResult,
    ) -> impl Future<Output = Result<ControlFlow<Self::Stop>, Self::Error>> + Send;
}

// The one RPC fetch shape behind the tail and the range filler: the caller lists a page with
// getBlocks, and only the listed slots get a getBlock, handed over in slot order. A skipped
// slot is never listed by a provider, so it costs no call and needs no message; the archive
// lists every slot, and its skipped ones come back as errors the caller classifies. `stop` resolving drops the
// fetch, which aborts the calls in flight; next() is cancel-safe because a joined result is
// stored before it returns.
pub(crate) async fn fetch_listed<Handler: ListedBlockHandler>(
    gateway: &Arc<RpcGateway>,
    slots: Vec<Slot>,
    lane: RequestLane,
    stop: impl Future<Output = Handler::Stop>,
    handler: &mut Handler,
) -> Result<ControlFlow<Handler::Stop>, Handler::Error> {
    debug_assert!(slots.is_sorted());
    let mut fetch = OrderedFetch::start(gateway, slots, lane);
    let mut stop = std::pin::pin!(stop);
    for _ in 0..fetch.len() {
        let next = tokio::select! {
            biased;
            stopped = &mut stop => return Ok(ControlFlow::Break(stopped)),
            next = fetch.next() => next,
        };
        let Some((slot, result)) = next else {
            break;
        };
        if let ControlFlow::Break(stopped) = handler.on_result(slot, result).await? {
            return Ok(ControlFlow::Break(stopped));
        }
    }
    debug_assert!(fetch.yielded_count <= fetch.len());
    Ok(ControlFlow::Continue(()))
}
