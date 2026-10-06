// The Geyser live source: one finalized blocks subscription, resumed from the cursor on every
// reconnect, publishing its health so the live supervisor can stand the RPC tail in for it.
use std::future::Future;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use sqlx::PgPool;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::actor::messages::ProcessorMessage;
use crate::domain::block::{BlockOrigin, LiveSource};
use crate::domain::error::StoreError;
use crate::domain::ids::Slot;
use crate::gateway::geyser::{
    GeyserError, GeyserErrorClass, GeyserGateway, GeyserUpdate, classify_geyser_error,
};
use crate::store::read_cursor;

// Every finalized block arrives, empty or not, about 3.7 a second, so 30 s without one is a
// wedged stream even while the server keeps pinging.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(30);
// Full jitter adds up to the delay itself, so the effective cap is 60 s.
const RECONNECT_DELAY_MIN: Duration = Duration::from_millis(250);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeyserHealth {
    // Boot, before the first stream has proved itself either way.
    Starting,
    // The current stream has delivered a block.
    Connected,
    // The last stream failed or stalled; reconnecting.
    Degraded,
}

#[derive(Debug, Error)]
pub enum GeyserSourceError {
    #[error("geyser configuration bug: {0}")]
    ConfigurationBug(GeyserError),
    #[error("block processor channel closed")]
    ProcessorGone,
}

// From where the next subscription starts: right after the cursor, or the live tip after the
// server refused the cursor (the stretch skipped is then a coverage hole).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResumeFrom {
    Cursor,
    Tip,
}

enum StreamEnd {
    Failed {
        error: GeyserError,
        delivered: Delivered,
    },
    ProcessorGone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivered {
    Nothing,
    Blocks,
}

// Runs until `stop` turns true or its sender is dropped; returns an error only for a
// configuration bug or a closed processor channel. Health starts as `Starting`.
pub async fn run(
    gateway: GeyserGateway,
    database: PgPool,
    live_sender: mpsc::Sender<ProcessorMessage>,
    stop: watch::Receiver<bool>,
    health: watch::Sender<GeyserHealth>,
) -> Result<(), GeyserSourceError> {
    let read = || async {
        read_cursor(&database)
            .await
            .map(|cursor| cursor.map(|cursor| cursor.slot))
    };
    run_with_cursor(gateway, read, live_sender, stop, health).await
}

// The cursor read is a parameter so the stream loop can run without a database in tests.
pub(crate) async fn run_with_cursor<Read, ReadFuture>(
    gateway: GeyserGateway,
    read_cursor: Read,
    live_sender: mpsc::Sender<ProcessorMessage>,
    mut stop: watch::Receiver<bool>,
    health: watch::Sender<GeyserHealth>,
) -> Result<(), GeyserSourceError>
where
    Read: Fn() -> ReadFuture,
    ReadFuture: Future<Output = Result<Option<Slot>, StoreError>>,
{
    let result = tokio::select! {
        biased;
        () = stop_requested(&mut stop) => Ok(()),
        result = connection_loop(&gateway, &read_cursor, &live_sender, &health) => result,
    };
    tracing::info!("geyser_source_stopped");
    result
}

async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    // A dropped sender can never ask again, so it counts as a stop.
    let _ = stop.wait_for(|stopped| *stopped).await;
}

fn reconnect_backoff() -> ExponentialBackoff {
    ExponentialBuilder::new()
        .with_min_delay(RECONNECT_DELAY_MIN)
        .with_max_delay(RECONNECT_DELAY_MAX)
        .with_jitter()
        .without_max_times()
        .build()
}

// Unbounded on purpose: Geyser is the primary source, so it retries until stopped while the
// tail covers for it. Each pass is bounded by the stall timeout and the backoff cap.
async fn connection_loop<Read, ReadFuture>(
    gateway: &GeyserGateway,
    read_cursor: &Read,
    live_sender: &mpsc::Sender<ProcessorMessage>,
    health: &watch::Sender<GeyserHealth>,
) -> Result<(), GeyserSourceError>
where
    Read: Fn() -> ReadFuture,
    ReadFuture: Future<Output = Result<Option<Slot>, StoreError>>,
{
    let mut backoff = reconnect_backoff();
    let mut resume = ResumeFrom::Cursor;
    loop {
        let from_slot = match resume {
            ResumeFrom::Cursor => from_slot_after_cursor(read_cursor).await,
            ResumeFrom::Tip => None,
        };
        let (error, delivered) = match stream_once(gateway, from_slot, live_sender, health).await {
            StreamEnd::ProcessorGone => return Err(GeyserSourceError::ProcessorGone),
            StreamEnd::Failed { error, delivered } => (error, delivered),
        };
        // A stream that delivered proved the endpoint healthy, so the next outage starts over.
        if delivered == Delivered::Blocks {
            backoff = reconnect_backoff();
        }
        match classify_geyser_error(&error) {
            GeyserErrorClass::ConfigurationBug => {
                tracing::error!(%error, "geyser_configuration_bug");
                return Err(GeyserSourceError::ConfigurationBug(error));
            }
            // Expected after any outage longer than the replay ring; no backoff, no flap.
            GeyserErrorClass::ResubscribeWithoutFromSlot => {
                tracing::warn!(%error, from_slot = from_slot.map(Slot::get), "geyser_resubscribe_at_tip");
                resume = ResumeFrom::Tip;
            }
            GeyserErrorClass::Reconnect => {
                publish(health, GeyserHealth::Degraded);
                // The backoff is built without a maximum, so it always yields.
                let delay = backoff.next().unwrap_or(RECONNECT_DELAY_MAX);
                tracing::warn!(%error, delay_ms = delay.as_millis() as u64, "geyser_reconnect");
                resume = ResumeFrom::Cursor;
                tokio::time::sleep(delay).await;
            }
        }
    }
}

// A cursor read failure subscribes at the tip: the reconciler sees the hole it leaves.
async fn from_slot_after_cursor<Read, ReadFuture>(read_cursor: &Read) -> Option<Slot>
where
    Read: Fn() -> ReadFuture,
    ReadFuture: Future<Output = Result<Option<Slot>, StoreError>>,
{
    match read_cursor().await {
        Ok(cursor) => cursor.map(|slot| Slot::new(slot.get() + 1)),
        Err(error) => {
            tracing::warn!(%error, "geyser_cursor_read_failed");
            None
        }
    }
}

fn publish(health: &watch::Sender<GeyserHealth>, value: GeyserHealth) {
    health.send_if_modified(|current| {
        let changed = *current != value;
        *current = value;
        changed
    });
}

async fn stream_once(
    gateway: &GeyserGateway,
    from_slot: Option<Slot>,
    live_sender: &mpsc::Sender<ProcessorMessage>,
    health: &watch::Sender<GeyserHealth>,
) -> StreamEnd {
    let failed = |error| StreamEnd::Failed {
        error,
        delivered: Delivered::Nothing,
    };
    let connected = match gateway.stream().connect().await {
        Ok(connected) => connected,
        Err(error) => return failed(error),
    };
    let mut stream = match connected.subscribe(from_slot).await {
        Ok(stream) => stream,
        Err(error) => return failed(error),
    };
    tracing::info!(from_slot = from_slot.map(Slot::get), "geyser_subscribed");
    let mut delivered = Delivered::Nothing;
    let mut block_deadline = Instant::now() + STALL_TIMEOUT;
    loop {
        let next = match tokio::time::timeout_at(block_deadline, stream.next_update()).await {
            Ok(next) => next,
            Err(_) => Err(GeyserError::Stalled {
                silence_ms: STALL_TIMEOUT.as_millis() as u64,
            }),
        };
        let block = match next {
            Ok(GeyserUpdate::Block(block)) => block,
            Ok(GeyserUpdate::BlockUnmappable { slot, error }) => {
                tracing::warn!(slot = slot.get(), %error, "geyser_block_unmappable");
                block_deadline = Instant::now() + STALL_TIMEOUT;
                continue;
            }
            Ok(GeyserUpdate::Other) => continue,
            Err(error) => return StreamEnd::Failed { error, delivered },
        };
        block_deadline = Instant::now() + STALL_TIMEOUT;
        if delivered == Delivered::Nothing {
            tracing::info!(slot = block.slot.get(), "geyser_connected");
            publish(health, GeyserHealth::Connected);
            delivered = Delivered::Blocks;
        }
        // The same loop reads the stream, so ping replies pause while this send waits on a
        // full live channel. The processor drains far faster than blocks arrive; a wait long
        // enough to miss pings is already a processor failure. Replying from a task that owns
        // the sink would decouple them if that ever changes.
        let message = ProcessorMessage::OnBlock(block, BlockOrigin::Live(LiveSource::Geyser));
        if live_sender.send(message).await.is_err() {
            return StreamEnd::ProcessorGone;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use futures::channel::mpsc as stream_channel;
    use tonic::codec::CompressionEncoding;
    use tonic::transport::Server;
    use tonic::transport::server::TcpIncoming;
    use tonic::{Request, Response, Status, Streaming};
    use yellowstone_grpc_proto::prelude::geyser_server::{Geyser, GeyserServer};
    use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
    use yellowstone_grpc_proto::prelude::*;

    use super::*;
    use crate::domain::block::FinalizedBlock;
    use crate::domain::config::TransactionVersionMax;

    type UpdateSender = stream_channel::Sender<Result<SubscribeUpdate, Status>>;

    // What the server does with one subscription, in arrival order.
    pub(crate) enum Script {
        // Sends these updates, then stays silent with the stream open.
        ServeThenSilence(Vec<SubscribeUpdate>),
        // Sends these updates, then only a ping every 10 s, as a wedged server would.
        ServeThenPing(Vec<SubscribeUpdate>),
        Fail(Status),
    }

    #[derive(Default)]
    pub(crate) struct ServerLog {
        // Every request any client sent: subscriptions and ping replies.
        pub(crate) requests: Mutex<Vec<SubscribeRequest>>,
        // The grpc-accept-encoding header of each subscription, empty when absent.
        pub(crate) accept_encodings: Mutex<Vec<String>>,
    }

    #[derive(Default)]
    struct ScriptedGeyser {
        scripts: Mutex<VecDeque<Script>>,
        log: Arc<ServerLog>,
        // Holding a sender keeps its stream open and silent.
        silent: Mutex<Vec<UpdateSender>>,
    }

    fn serve(sender: &mut UpdateSender, updates: Vec<SubscribeUpdate>) {
        for update in updates {
            sender
                .try_send(Ok(update))
                .expect("buffer holds the script");
        }
    }

    // Bounded well past the stall timeout; ends early once the client drops the stream.
    async fn ping_forever(mut sender: UpdateSender) {
        for _ in 0..12 {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if futures::SinkExt::send(&mut sender, Ok(ping_update()))
                .await
                .is_err()
            {
                return;
            }
        }
    }

    fn unimplemented<T>() -> Result<Response<T>, Status> {
        Err(Status::unimplemented("not part of the test server"))
    }

    #[tonic::async_trait]
    impl Geyser for ScriptedGeyser {
        type SubscribeStream = stream_channel::Receiver<Result<SubscribeUpdate, Status>>;
        type SubscribeDeshredStream =
            futures::stream::Empty<Result<SubscribeUpdateDeshred, Status>>;
        type SubscribeGossipStream = futures::stream::Empty<Result<SubscribeUpdateGossip, Status>>;

        async fn subscribe(
            &self,
            request: Request<Streaming<SubscribeRequest>>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let accept_encoding = request
                .metadata()
                .get("grpc-accept-encoding")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            self.log
                .accept_encodings
                .lock()
                .expect("lock")
                .push(accept_encoding);
            let mut incoming = request.into_inner();
            let log = Arc::clone(&self.log);
            tokio::spawn(async move {
                while let Ok(Some(request)) = incoming.message().await {
                    log.requests.lock().expect("lock").push(request);
                }
            });
            let script = self.scripts.lock().expect("lock").pop_front();
            let (mut sender, receiver) = stream_channel::channel(64);
            match script.unwrap_or(Script::ServeThenSilence(Vec::new())) {
                Script::ServeThenSilence(updates) => {
                    serve(&mut sender, updates);
                    self.silent.lock().expect("lock").push(sender);
                }
                Script::ServeThenPing(updates) => {
                    serve(&mut sender, updates);
                    tokio::spawn(ping_forever(sender));
                }
                Script::Fail(status) => sender.try_send(Err(status)).expect("buffer"),
            }
            Ok(Response::new(receiver))
        }

        async fn subscribe_deshred(
            &self,
            _: Request<Streaming<SubscribeDeshredRequest>>,
        ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
            unimplemented()
        }

        async fn subscribe_gossip(
            &self,
            _: Request<SubscribeGossipRequest>,
        ) -> Result<Response<Self::SubscribeGossipStream>, Status> {
            unimplemented()
        }

        async fn subscribe_replay_info(
            &self,
            _: Request<SubscribeReplayInfoRequest>,
        ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
            unimplemented()
        }

        async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
            unimplemented()
        }

        async fn get_latest_blockhash(
            &self,
            _: Request<GetLatestBlockhashRequest>,
        ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
            unimplemented()
        }

        async fn get_block_height(
            &self,
            _: Request<GetBlockHeightRequest>,
        ) -> Result<Response<GetBlockHeightResponse>, Status> {
            unimplemented()
        }

        async fn get_slot(
            &self,
            _: Request<GetSlotRequest>,
        ) -> Result<Response<GetSlotResponse>, Status> {
            unimplemented()
        }

        async fn is_blockhash_valid(
            &self,
            _: Request<IsBlockhashValidRequest>,
        ) -> Result<Response<IsBlockhashValidResponse>, Status> {
            unimplemented()
        }

        async fn get_version(
            &self,
            _: Request<GetVersionRequest>,
        ) -> Result<Response<GetVersionResponse>, Status> {
            unimplemented()
        }
    }

    pub(crate) fn block_update(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["dlmm".to_owned()],
            created_at: None,
            update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
                slot,
                parent_slot: slot - 1,
                block_time: Some(UnixTimestamp {
                    timestamp: 1_790_000_000 + slot as i64,
                }),
                ..SubscribeUpdateBlock::default()
            })),
        }
    }

    fn ping_update() -> SubscribeUpdate {
        SubscribeUpdate {
            filters: Vec::new(),
            created_at: None,
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        }
    }

    struct Harness {
        log: Arc<ServerLog>,
        // Stands in for the processor's cursor: the last slot the test received.
        cursor: Arc<Mutex<Option<Slot>>>,
        live_receiver: mpsc::Receiver<ProcessorMessage>,
        health: watch::Receiver<GeyserHealth>,
        stop: watch::Sender<bool>,
        source: tokio::task::JoinHandle<Result<(), GeyserSourceError>>,
    }

    // Serves the scripts on a loopback port, zstd-compressing whatever the client accepts as
    // the real provider does; returns a gateway pointed at it and the log of what it saw.
    pub(crate) fn start_server(scripts: Vec<Script>) -> (GeyserGateway, Arc<ServerLog>) {
        let server = ScriptedGeyser {
            scripts: Mutex::new(scripts.into()),
            ..ScriptedGeyser::default()
        };
        let log = Arc::clone(&server.log);
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("address")).expect("bind");
        let address = incoming.local_addr().expect("local address");
        tokio::spawn(
            Server::builder()
                .add_service(GeyserServer::new(server).send_compressed(CompressionEncoding::Zstd))
                .serve_with_incoming(incoming),
        );
        let gateway = GeyserGateway::new(
            format!("http://{address}").parse().expect("url"),
            None,
            TransactionVersionMax::SUPPORTED,
        )
        .expect("gateway");
        (gateway, log)
    }

    // Reads a cursor the test advances itself, standing in for the processor's.
    pub(crate) fn shared_cursor_reader(
        cursor: &Arc<Mutex<Option<Slot>>>,
    ) -> impl Fn() -> std::future::Ready<Result<Option<Slot>, StoreError>> + use<> {
        let cursor = Arc::clone(cursor);
        move || std::future::ready(Ok(*cursor.lock().expect("lock")))
    }

    fn start(scripts: Vec<Script>, cursor: Option<Slot>) -> Harness {
        let (gateway, log) = start_server(scripts);
        let cursor = Arc::new(Mutex::new(cursor));
        let read_cursor = shared_cursor_reader(&cursor);
        let (live_sender, live_receiver) = mpsc::channel(16);
        let (stop, stop_receiver) = watch::channel(false);
        let (health_sender, health) = watch::channel(GeyserHealth::Starting);
        let source = tokio::spawn(run_with_cursor(
            gateway,
            read_cursor,
            live_sender,
            stop_receiver,
            health_sender,
        ));
        Harness {
            log,
            cursor,
            live_receiver,
            health,
            stop,
            source,
        }
    }

    impl Harness {
        async fn receive_block(&mut self) -> FinalizedBlock {
            match self.live_receiver.recv().await.expect("source is running") {
                ProcessorMessage::OnBlock(block, origin) => {
                    assert_eq!(origin, BlockOrigin::Live(LiveSource::Geyser));
                    *self.cursor.lock().expect("lock") = Some(block.slot);
                    block
                }
                other => panic!("unexpected message {other:?}"),
            }
        }

        fn subscription_from_slots(&self) -> Vec<Option<u64>> {
            let requests = self.log.requests.lock().expect("lock");
            let subscriptions = requests.iter().filter(|request| !request.blocks.is_empty());
            subscriptions.map(|request| request.from_slot).collect()
        }

        async fn wait_for_subscriptions(&self, count: usize) {
            for _ in 0..1000 {
                if self.subscription_from_slots().len() >= count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("no subscription number {count}");
        }

        async fn stop(self) {
            self.stop.send(true).expect("source listens");
            let result = self.source.await.expect("source task");
            assert!(result.is_ok(), "{result:?}");
        }
    }

    // Blocks arrive in slot order with a continuous parent chain, pings are answered, 30 s
    // without a block publishes Degraded although pings keep arriving, and the reconnect
    // resumes right after the cursor. Time is paused only for the stall: tokio's auto-advance
    // would race real socket IO, which only delays pings and so cannot hide the stall.
    #[tokio::test]
    async fn ping_only_stream_degrades_after_stall_and_resumes_from_cursor() {
        let served = vec![
            block_update(100),
            block_update(101),
            ping_update(),
            block_update(102),
            block_update(103),
        ];
        let mut harness = start(vec![Script::ServeThenPing(served)], Some(Slot::new(99)));
        let mut previous = Slot::new(99);
        for _ in 0..4 {
            let block = harness.receive_block().await;
            assert_eq!(block.parent_slot, previous);
            previous = block.slot;
        }
        assert_eq!(previous, Slot::new(103));
        assert_eq!(*harness.health.borrow(), GeyserHealth::Connected);

        tokio::time::pause();
        let last_block_at = Instant::now();
        harness
            .health
            .wait_for(|health| *health == GeyserHealth::Degraded)
            .await
            .expect("source publishes");
        let silence = last_block_at.elapsed();
        tokio::time::resume();
        let tolerance = Duration::from_secs(1);
        assert!(silence + tolerance >= STALL_TIMEOUT, "{silence:?}");
        assert!(silence <= STALL_TIMEOUT + tolerance, "{silence:?}");

        harness.wait_for_subscriptions(2).await;
        assert_eq!(
            harness.subscription_from_slots(),
            vec![Some(100), Some(104)]
        );
        let ping_replies = harness
            .log
            .requests
            .lock()
            .expect("lock")
            .iter()
            .filter(|request| request.ping == Some(SubscribeRequestPing { id: 1 }))
            .count();
        assert!(ping_replies >= 1, "{ping_replies}");
        harness.stop().await;
    }

    // A from_slot beyond the replay ring is answered by resubscribing at the tip, without
    // reporting the stream as degraded.
    #[tokio::test]
    async fn out_of_range_resubscribes_at_the_tip() {
        let scripts = vec![
            Script::Fail(Status::out_of_range("broadcast from 100 is not available")),
            Script::ServeThenSilence(vec![block_update(500)]),
        ];
        let mut harness = start(scripts, Some(Slot::new(99)));
        let block = harness.receive_block().await;
        assert_eq!(block.slot, Slot::new(500));
        assert_eq!(harness.subscription_from_slots(), vec![Some(100), None]);
        assert_eq!(*harness.health.borrow(), GeyserHealth::Connected);
        harness.stop().await;
    }

    // The provider's stream is mostly large token-balance tables, so the subscription asks
    // for zstd, and a block the server sends compressed arrives intact.
    #[tokio::test]
    async fn subscription_accepts_zstd_and_decodes_compressed_blocks() {
        let mut harness = start(
            vec![Script::ServeThenSilence(vec![block_update(200)])],
            Some(Slot::new(199)),
        );
        let block = harness.receive_block().await;
        assert_eq!(block.slot, Slot::new(200));
        assert_eq!(block.parent_slot, Slot::new(199));
        let accept_encodings = harness.log.accept_encodings.lock().expect("lock").clone();
        assert_eq!(accept_encodings.len(), 1);
        assert!(
            accept_encodings[0]
                .split(',')
                .any(|name| name.trim() == "zstd"),
            "{accept_encodings:?}"
        );
        harness.stop().await;
    }
}
