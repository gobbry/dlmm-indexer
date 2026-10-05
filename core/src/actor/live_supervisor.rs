// Owns the live sources. With Geyser configured it is primary and the RPC tail stands in only
// while the stream is down; without it the tail is the live source. Both feed one channel and
// the handover in either direction is the cursor.
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinError, JoinHandle};

use crate::actor::geyser_source::{self, GeyserHealth, GeyserSourceError};
use crate::actor::messages::ProcessorMessage;
use crate::actor::rpc_tail::{self, TailError};
use crate::domain::block::LiveSource;
use crate::gateway::geyser::GeyserGateway;
use crate::gateway::rpc::RpcGateway;

// Both sources stop by dropping their in-flight work; this only bounds a wedged task.
const SOURCE_STOP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeyserMode {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveState {
    GeyserOnly,
    TailWhileGeyserDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveEvent {
    GeyserHealthy,
    GeyserDegraded,
    StopRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveAction {
    StartTail,
    StopTail,
    StartGeyser,
    StopGeyser,
    Exit,
}

// Without Geyser the tail runs for good: no health event ever arrives to hand it back.
pub fn initial_state(mode: GeyserMode) -> (LiveState, &'static [LiveAction]) {
    match mode {
        GeyserMode::Enabled => (LiveState::GeyserOnly, &[LiveAction::StartGeyser]),
        GeyserMode::Disabled => (LiveState::TailWhileGeyserDown, &[LiveAction::StartTail]),
    }
}

// The tail starts on the first Degraded, not after a grace period: Degraded already means a
// failed stream or 30 s of silence, and the tail must take over within 30 s of silence.
pub fn next_state(state: LiveState, event: LiveEvent) -> (LiveState, &'static [LiveAction]) {
    match (state, event) {
        (LiveState::GeyserOnly, LiveEvent::GeyserHealthy) => (state, &[]),
        (LiveState::GeyserOnly, LiveEvent::GeyserDegraded) => {
            (LiveState::TailWhileGeyserDown, &[LiveAction::StartTail])
        }
        (LiveState::TailWhileGeyserDown, LiveEvent::GeyserDegraded) => (state, &[]),
        (LiveState::TailWhileGeyserDown, LiveEvent::GeyserHealthy) => {
            (LiveState::GeyserOnly, &[LiveAction::StopTail])
        }
        (LiveState::GeyserOnly, LiveEvent::StopRequested) => {
            (state, &[LiveAction::StopGeyser, LiveAction::Exit])
        }
        (LiveState::TailWhileGeyserDown, LiveEvent::StopRequested) => (
            state,
            &[
                LiveAction::StopTail,
                LiveAction::StopGeyser,
                LiveAction::Exit,
            ],
        ),
    }
}

fn health_event(health: GeyserHealth) -> Option<LiveEvent> {
    match health {
        GeyserHealth::Starting => None,
        GeyserHealth::Connected => Some(LiveEvent::GeyserHealthy),
        GeyserHealth::Degraded => Some(LiveEvent::GeyserDegraded),
    }
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("rpc tail: {0}")]
    Tail(#[from] TailError),
    #[error("geyser source: {0}")]
    Geyser(#[from] GeyserSourceError),
    #[error("{source_name} stopped without being asked")]
    SourceExited { source_name: &'static str },
    #[error("{source_name} panicked or was cancelled")]
    SourcePanicked { source_name: &'static str },
}

// Runs until `stop` turns true or its sender is dropped; an error means a source failed in a
// way a restart cannot fix (a configuration bug, a closed processor channel).
pub async fn run(
    database: PgPool,
    rpc_gateway: Arc<RpcGateway>,
    geyser_gateway: Option<GeyserGateway>,
    live_sender: mpsc::Sender<ProcessorMessage>,
    stop: watch::Receiver<bool>,
) -> Result<(), SupervisorError> {
    let tail_database = database.clone();
    let tail_sender = live_sender.clone();
    let start_tail = move |tail_stop| {
        tokio::spawn(rpc_tail::run(
            tail_database.clone(),
            Arc::clone(&rpc_gateway),
            tail_sender.clone(),
            tail_stop,
            LiveSource::RpcTail,
        ))
    };
    let start_geyser = geyser_gateway.map(|gateway| {
        move |geyser_stop, health| {
            tokio::spawn(geyser_source::run(
                gateway,
                database,
                live_sender,
                geyser_stop,
                health,
            ))
        }
    });
    supervise(start_tail, start_geyser, stop).await
}

type TailHandle = JoinHandle<Result<(), TailError>>;
type GeyserHandle = JoinHandle<Result<(), GeyserSourceError>>;

struct Running<Task> {
    handle: JoinHandle<Task>,
    stop: watch::Sender<bool>,
}

// A dropped JoinHandle detaches its task, so an aborted or timed-out supervisor would leave its
// sources producing behind the processor's Shutdown; dropping a Running aborts it instead.
// Aborting a task that already finished does nothing.
impl<Task> Drop for Running<Task> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct Sources {
    tail: Option<Running<Result<(), TailError>>>,
    geyser: Option<Running<Result<(), GeyserSourceError>>>,
    health: Option<watch::Receiver<GeyserHealth>>,
}

enum Input {
    Event(LiveEvent),
    HealthUnchanged,
    TailExited(Result<Result<(), TailError>, JoinError>),
    GeyserExited(Result<Result<(), GeyserSourceError>, JoinError>),
}

// The imperative shell around `next_state`; source spawning is a parameter so tests can run
// it with fake sources. No Geyser starter means the tail is the only live source.
pub(crate) async fn supervise<StartTail, StartGeyser>(
    start_tail: StartTail,
    start_geyser: Option<StartGeyser>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), SupervisorError>
where
    StartTail: FnMut(watch::Receiver<bool>) -> TailHandle,
    StartGeyser: FnOnce(watch::Receiver<bool>, watch::Sender<GeyserHealth>) -> GeyserHandle,
{
    let mode = match start_geyser {
        Some(_) => GeyserMode::Enabled,
        None => GeyserMode::Disabled,
    };
    tracing::info!(?mode, "live_supervisor_started");
    let mut sources = Sources {
        tail: None,
        geyser: None,
        health: None,
    };
    let mut starters = Starters {
        tail: start_tail,
        geyser: start_geyser,
    };
    let (mut state, actions) = initial_state(mode);
    for &action in actions {
        apply(action, &mut sources, &mut starters).await?;
    }
    // Ends on StopRequested or when a source exits; each pass handles one input.
    loop {
        let event = match next_input(&mut sources, &mut stop).await {
            Input::Event(event) => event,
            Input::HealthUnchanged => continue,
            Input::TailExited(joined) => {
                return source_exited(&mut sources, "rpc tail", joined).await;
            }
            Input::GeyserExited(joined) => {
                return source_exited(&mut sources, "geyser source", joined).await;
            }
        };
        let (next, actions) = next_state(state, event);
        if next != state {
            tracing::info!(from = ?state, to = ?next, "live_source_switched");
        }
        state = next;
        for &action in actions {
            if action == LiveAction::Exit {
                return Ok(());
            }
            apply(action, &mut sources, &mut starters).await?;
        }
    }
}

async fn next_input(sources: &mut Sources, stop: &mut watch::Receiver<bool>) -> Input {
    let tail = optional_join(sources.tail.as_mut().map(|running| &mut running.handle));
    let geyser = optional_join(sources.geyser.as_mut().map(|running| &mut running.handle));
    let health = health_change(sources.health.as_mut());
    tokio::select! {
        biased;
        () = stop_requested(stop) => Input::Event(LiveEvent::StopRequested),
        joined = tail => Input::TailExited(joined),
        joined = geyser => Input::GeyserExited(joined),
        changed = health => changed.map_or(Input::HealthUnchanged, Input::Event),
    }
}

async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    // A dropped sender can never ask again, so it counts as a stop.
    let _ = stop.wait_for(|stopped| *stopped).await;
}

async fn optional_join<Task>(handle: Option<&mut JoinHandle<Task>>) -> Result<Task, JoinError> {
    match handle {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

// None for a value that maps to no event; a closed health channel waits for the task's exit.
async fn health_change(health: Option<&mut watch::Receiver<GeyserHealth>>) -> Option<LiveEvent> {
    let Some(health) = health else {
        return std::future::pending().await;
    };
    if health.changed().await.is_err() {
        return std::future::pending().await;
    }
    let value = *health.borrow_and_update();
    health_event(value)
}

struct Starters<StartTail, StartGeyser> {
    tail: StartTail,
    // Geyser starts once and reconnects by itself, so its starter is consumed.
    geyser: Option<StartGeyser>,
}

async fn apply<StartTail, StartGeyser>(
    action: LiveAction,
    sources: &mut Sources,
    starters: &mut Starters<StartTail, StartGeyser>,
) -> Result<(), SupervisorError>
where
    StartTail: FnMut(watch::Receiver<bool>) -> TailHandle,
    StartGeyser: FnOnce(watch::Receiver<bool>, watch::Sender<GeyserHealth>) -> GeyserHandle,
{
    match action {
        LiveAction::StartTail => {
            debug_assert!(sources.tail.is_none());
            let (tail_stop, tail_stop_receiver) = watch::channel(false);
            sources.tail = Some(Running {
                handle: (starters.tail)(tail_stop_receiver),
                stop: tail_stop,
            });
            Ok(())
        }
        LiveAction::StartGeyser => {
            debug_assert!(sources.geyser.is_none());
            // Only GeyserMode::Enabled starts Geyser, and only once.
            let Some(start) = starters.geyser.take() else {
                return Ok(());
            };
            let (geyser_stop, geyser_stop_receiver) = watch::channel(false);
            let (health_sender, health_receiver) = watch::channel(GeyserHealth::Starting);
            sources.geyser = Some(Running {
                handle: start(geyser_stop_receiver, health_sender),
                stop: geyser_stop,
            });
            sources.health = Some(health_receiver);
            Ok(())
        }
        LiveAction::StopTail => match sources.tail.take() {
            Some(running) => stop_source(running, "rpc tail").await,
            None => Ok(()),
        },
        LiveAction::StopGeyser => match sources.geyser.take() {
            Some(running) => stop_source(running, "geyser source").await,
            None => Ok(()),
        },
        // The caller's loop ends on Exit before applying it.
        LiveAction::Exit => Ok(()),
    }
}

async fn stop_source<SourceError>(
    mut running: Running<Result<(), SourceError>>,
    source_name: &'static str,
) -> Result<(), SupervisorError>
where
    SupervisorError: From<SourceError>,
{
    // An error means the source already exited and dropped its receiver.
    let _ = running.stop.send(true);
    match tokio::time::timeout(SOURCE_STOP_TIMEOUT, &mut running.handle).await {
        Ok(Ok(result)) => result.map_err(SupervisorError::from),
        Ok(Err(_)) => Err(SupervisorError::SourcePanicked { source_name }),
        Err(_) => {
            tracing::warn!(source_name, "live_source_stop_timed_out");
            running.handle.abort();
            Ok(())
        }
    }
}

// A source that exits on its own is fatal (they only return on stop); the other is stopped
// first so nothing keeps producing behind the caller's Shutdown.
async fn source_exited<SourceError>(
    sources: &mut Sources,
    source_name: &'static str,
    joined: Result<Result<(), SourceError>, JoinError>,
) -> Result<(), SupervisorError>
where
    SupervisorError: From<SourceError>,
{
    tracing::error!(source_name, "live_source_exited");
    if let Some(running) = sources.tail.take()
        && let Err(error) = stop_source::<TailError>(running, "rpc tail").await
    {
        tracing::warn!(%error, "rpc_tail_stop_failed");
    }
    if let Some(running) = sources.geyser.take()
        && let Err(error) = stop_source::<GeyserSourceError>(running, "geyser source").await
    {
        tracing::warn!(%error, "geyser_source_stop_failed");
    }
    match joined {
        Ok(Ok(())) => Err(SupervisorError::SourceExited { source_name }),
        Ok(Err(error)) => Err(SupervisorError::from(error)),
        Err(_) => Err(SupervisorError::SourcePanicked { source_name }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::time::Instant;

    use super::*;
    use crate::actor::geyser_source::tests::{
        Script, block_update, shared_cursor_reader, start_server,
    };
    use crate::actor::geyser_source::{STALL_TIMEOUT, run_with_cursor};
    use crate::domain::block::BlockOrigin;
    use crate::domain::ids::Slot;

    // The whole transition table, so a changed edge is a deliberate edit here.
    #[test]
    fn transitions_follow_the_table() {
        use LiveAction::*;
        use LiveEvent::*;
        use LiveState::*;
        assert_eq!(
            initial_state(GeyserMode::Enabled),
            (GeyserOnly, &[StartGeyser][..])
        );
        assert_eq!(
            initial_state(GeyserMode::Disabled),
            (TailWhileGeyserDown, &[StartTail][..])
        );
        let table: [(LiveState, LiveEvent, LiveState, &[LiveAction]); 6] = [
            (GeyserOnly, GeyserHealthy, GeyserOnly, &[]),
            (
                GeyserOnly,
                GeyserDegraded,
                TailWhileGeyserDown,
                &[StartTail],
            ),
            (GeyserOnly, StopRequested, GeyserOnly, &[StopGeyser, Exit]),
            (TailWhileGeyserDown, GeyserHealthy, GeyserOnly, &[StopTail]),
            (
                TailWhileGeyserDown,
                GeyserDegraded,
                TailWhileGeyserDown,
                &[],
            ),
            (
                TailWhileGeyserDown,
                StopRequested,
                TailWhileGeyserDown,
                &[StopTail, StopGeyser, Exit],
            ),
        ];
        for (state, event, expected_state, expected_actions) in table {
            assert_eq!(
                next_state(state, event),
                (expected_state, expected_actions),
                "{state:?} on {event:?}"
            );
        }
    }

    type Events = mpsc::UnboundedSender<&'static str>;
    type StartGeyserFn = fn(watch::Receiver<bool>, watch::Sender<GeyserHealth>) -> GeyserHandle;

    fn fake_task<SourceError: Send + 'static>(
        events: Events,
        name_started: &'static str,
        name_stopped: &'static str,
        mut stop: watch::Receiver<bool>,
    ) -> JoinHandle<Result<(), SourceError>> {
        tokio::spawn(async move {
            let _ = events.send(name_started);
            let _ = stop.wait_for(|stopped| *stopped).await;
            let _ = events.send(name_stopped);
            Ok(())
        })
    }

    async fn expect_event(events: &mut mpsc::UnboundedReceiver<&'static str>, expected: &str) {
        assert_eq!(events.recv().await, Some(expected));
    }

    // Degraded starts the tail, Healthy stops it, and a stop request stops both sources.
    #[tokio::test]
    async fn supervisor_hands_live_between_geyser_and_tail() {
        let (event_sender, mut events) = mpsc::unbounded_channel();
        let (health_out, mut health_in) = mpsc::unbounded_channel();
        let tail_events = event_sender.clone();
        let start_tail =
            move |stop| fake_task(tail_events.clone(), "tail_started", "tail_stopped", stop);
        let start_geyser = move |stop, health| {
            let _ = health_out.send(health);
            fake_task(event_sender, "geyser_started", "geyser_stopped", stop)
        };
        let (stop, stop_receiver) = watch::channel(false);
        let supervisor = tokio::spawn(supervise(start_tail, Some(start_geyser), stop_receiver));
        let health: watch::Sender<GeyserHealth> = health_in.recv().await.expect("geyser started");
        expect_event(&mut events, "geyser_started").await;

        health.send_replace(GeyserHealth::Degraded);
        expect_event(&mut events, "tail_started").await;
        health.send_replace(GeyserHealth::Connected);
        expect_event(&mut events, "tail_stopped").await;
        health.send_replace(GeyserHealth::Degraded);
        expect_event(&mut events, "tail_started").await;

        stop.send_replace(true);
        expect_event(&mut events, "tail_stopped").await;
        expect_event(&mut events, "geyser_stopped").await;
        let result = supervisor.await.expect("supervisor task");
        assert!(result.is_ok(), "{result:?}");
    }

    // Without Geyser the tail is the live source from the start until stopped.
    #[tokio::test]
    async fn without_geyser_the_tail_runs_until_stop() {
        let (event_sender, mut events) = mpsc::unbounded_channel();
        let start_tail =
            move |stop| fake_task(event_sender.clone(), "tail_started", "tail_stopped", stop);
        let start_geyser: Option<StartGeyserFn> = None;
        let (stop, stop_receiver) = watch::channel(false);
        let supervisor = tokio::spawn(supervise(start_tail, start_geyser, stop_receiver));
        expect_event(&mut events, "tail_started").await;
        stop.send_replace(true);
        expect_event(&mut events, "tail_stopped").await;
        let result = supervisor.await.expect("supervisor task");
        assert!(result.is_ok(), "{result:?}");
    }

    // Stands in for rpc_tail: records when it started, then waits to be stopped. Where the real
    // tail resumes is its own contract (tail_start), so this one sends nothing.
    fn fake_rpc_tail(
        started: mpsc::UnboundedSender<Instant>,
        mut stop: watch::Receiver<bool>,
    ) -> TailHandle {
        tokio::spawn(async move {
            let _ = started.send(Instant::now());
            let _ = stop.wait_for(|stopped| *stopped).await;
            Ok(())
        })
    }

    // Supervises a real Geyser source reading the shared cursor and a fake tail that reports
    // when it started.
    fn spawn_supervisor_over_real_geyser(
        gateway: GeyserGateway,
        cursor: &Arc<Mutex<Option<Slot>>>,
        live_sender: mpsc::Sender<ProcessorMessage>,
        tail_started: mpsc::UnboundedSender<Instant>,
        stop: watch::Receiver<bool>,
    ) -> JoinHandle<Result<(), SupervisorError>> {
        let start_tail = move |stop| fake_rpc_tail(tail_started.clone(), stop);
        let read_cursor = shared_cursor_reader(cursor);
        let start_geyser = move |stop, health| {
            tokio::spawn(run_with_cursor(
                gateway,
                read_cursor,
                live_sender,
                stop,
                health,
            ))
        };
        tokio::spawn(supervise(start_tail, Some(start_geyser), stop))
    }

    // A real Geyser source against an in-process server that serves four blocks and goes
    // silent: the tail does not start while blocks flow, and starts once the stall timeout
    // has passed, not later.
    #[tokio::test]
    async fn tail_takes_over_a_silent_stream_at_the_stall_timeout() {
        let served = (100..=103).map(block_update).collect();
        let (gateway, _requests) = start_server(vec![Script::ServeThenSilence(served)]);
        let cursor = Arc::new(Mutex::new(Some(Slot::new(99))));
        let (live_sender, mut live_receiver) = mpsc::channel(16);
        let (started_sender, mut tail_started) = mpsc::unbounded_channel();
        let (stop, stop_receiver) = watch::channel(false);
        let supervisor = spawn_supervisor_over_real_geyser(
            gateway,
            &cursor,
            live_sender,
            started_sender,
            stop_receiver,
        );

        for slot in 100..=103 {
            let Some(ProcessorMessage::OnBlock(block, origin)) = live_receiver.recv().await else {
                panic!("expected a block");
            };
            assert_eq!(
                (block.slot, origin),
                (Slot::new(slot), BlockOrigin::Live(LiveSource::Geyser))
            );
            *cursor.lock().expect("lock") = Some(block.slot);
        }
        assert!(
            tail_started.is_empty(),
            "the tail started while Geyser delivered"
        );
        // Paused only for the silence: tokio's auto-advance would race real socket IO.
        tokio::time::pause();
        let silence_started = Instant::now();
        let took_over_at = tail_started.recv().await.expect("tail started");
        tokio::time::resume();
        let silence = took_over_at.saturating_duration_since(silence_started);
        let tolerance = Duration::from_secs(1);
        assert!(silence + tolerance >= STALL_TIMEOUT, "{silence:?}");
        assert!(silence <= STALL_TIMEOUT + tolerance, "{silence:?}");
        stop.send_replace(true);
        let result = supervisor.await.expect("supervisor task");
        assert!(result.is_ok(), "{result:?}");
    }
}
