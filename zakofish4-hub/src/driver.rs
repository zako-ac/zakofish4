use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::Instrument;
use zakofish4_common::action::{DisconnectReason, HubAction};
use zakofish4_common::codec;
use zakofish4_common::config::HubConfig;
use zakofish4_common::event::HubEvent;
use zakofish4_common::state::{self, TapState};

use crate::backend::HubBackend;
use crate::handle::TapHandle;
use crate::timers::Timers;
use crate::transport::Transport;

/// How many dispatches may queue for one tap before callers are told it is
/// busy. Small on purpose: a backlog here means the tap is not keeping up, and
/// queueing deeper only delays discovering that.
const DISPATCH_QUEUE: usize = 64;

/// Runs one tap connection to completion.
///
/// Owns the socket, the state machine and the timers, and does nothing else.
/// Every decision comes from [`zakofish4_common::state::handle_event`]; this
/// loop only performs the actions it is handed.
pub async fn serve<T, B>(transport: T, backend: Arc<B>, cfg: HubConfig)
where
    T: Transport,
    B: HubBackend,
{
    // The span wraps the whole future rather than being entered as a guard: an
    // `EnteredSpan` is not `Send`, so holding one across an await would stop the
    // connection task being spawnable at all.
    let span = tracing::info_span!(
        "tap.connection",
        transport = "ws",
        tap_id = tracing::field::Empty,
        disconnect_reason = tracing::field::Empty,
        outstanding_at_close = tracing::field::Empty,
    );
    serve_inner(transport, backend, cfg).instrument(span).await
}

async fn serve_inner<T, B>(mut transport: T, backend: Arc<B>, cfg: HubConfig)
where
    T: Transport,
    B: HubBackend,
{
    let span = tracing::Span::current();

    let (event_tx, mut event_rx) = mpsc::channel::<HubEvent>(DISPATCH_QUEUE);
    let handle = TapHandle::new(event_tx.clone());

    let mut state = TapState::new();
    let mut timers = Timers::default();
    let mut announced = false;
    let mut disconnect: Option<DisconnectReason> = None;

    for action in state::on_connect(&cfg) {
        apply(
            &mut transport,
            &mut timers,
            &backend,
            &state,
            &event_tx,
            action,
            &mut disconnect,
        )
        .await;
    }

    'outer: loop {
        if disconnect.is_some() {
            break;
        }

        let event = {
            let sleep = timers.next_deadline();
            tokio::select! {
                frame = transport.recv() => match frame {
                    Some(Ok(bytes)) => match codec::decode_from_tap(&bytes) {
                        Ok(msg) => Some(HubEvent::MessageFromTap(msg)),
                        Err(e) => {
                            tracing::warn!(%e, "undecodable frame from tap");
                            disconnect = Some(DisconnectReason::ProtocolViolation);
                            break 'outer;
                        }
                    },
                    Some(Err(e)) => {
                        tracing::debug!(?e, "transport error");
                        break 'outer;
                    }
                    None => break 'outer,
                },
                Some(ev) = event_rx.recv() => Some(ev),
                () = sleep_until_opt(sleep) => None,
            }
        };

        let mut queue: Vec<HubEvent> = Vec::new();
        if let Some(ev) = event {
            queue.push(ev);
        }
        queue.extend(
            timers
                .take_expired(Instant::now())
                .into_iter()
                .map(HubEvent::TimerFired),
        );

        for ev in queue {
            // `handle_event` consumes the state and hands back the next one, so
            // it is moved out and put back rather than borrowed.
            let current = std::mem::take(&mut state);
            let (next, actions) = match state::handle_event(current, ev, &cfg) {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(%e, "protocol error");
                    disconnect = Some(DisconnectReason::ProtocolViolation);
                    break 'outer;
                }
            };
            state = next;

            for action in actions {
                apply(
                    &mut transport,
                    &mut timers,
                    &backend,
                    &state,
                    &event_tx,
                    action,
                    &mut disconnect,
                )
                .await;
            }

            // Announce exactly once, the moment the tap becomes usable.
            if !announced && let TapState::Authenticated { hello, .. } = &state {
                announced = true;
                span.record("tap_id", tracing::field::display(&hello.tap_id));
                backend.on_authenticated(hello, handle.clone()).await;
            }

            if disconnect.is_some() {
                break 'outer;
            }
        }
    }

    // Anything still waiting for an answer is failed here. Requests already
    // streaming are deliberately left alone: their audio is on UDP and does not
    // care that this socket died.
    let tap_id = state.tap_id().cloned();
    let outstanding = state.outstanding_ids().len();
    span.record("outstanding_at_close", outstanding);
    if let Some(reason) = disconnect {
        span.record("disconnect_reason", reason.as_str());
    }

    // The same two actions the loop performs, applied to what the disconnect
    // produced: a request that was still awaiting an answer, and a probe that
    // was still outstanding.
    for action in state::on_disconnect(&state) {
        let Some(id) = tap_id.as_ref() else {
            continue;
        };
        match action {
            HubAction::CompleteRequest {
                request_id,
                outcome,
            } => {
                backend.on_complete(id, request_id, outcome).await;
            }
            HubAction::ProbeCompleted { probe_id, result } => {
                backend.on_probe_result(id, probe_id, result).await;
            }
            _ => {}
        }
    }

    let _ = transport.close().await;
    backend.on_disconnected(tap_id.as_ref(), disconnect).await;
}

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

async fn apply<T, B>(
    transport: &mut T,
    timers: &mut Timers,
    backend: &Arc<B>,
    state: &TapState,
    events: &mpsc::Sender<HubEvent>,
    action: HubAction,
    disconnect: &mut Option<DisconnectReason>,
) where
    T: Transport,
    B: HubBackend,
{
    match action {
        HubAction::SendMessageToTap(msg) => match codec::encode_to_tap(&msg) {
            Ok(bytes) => {
                if let Err(e) = transport.send(bytes).await {
                    tracing::debug!(?e, "send failed");
                }
            }
            Err(e) => tracing::error!(%e, "failed to encode a message for the tap"),
        },

        HubAction::ValidateCredential(hello) => {
            // Runs off the connection task so a slow credential check cannot
            // stall this tap's heartbeat or its other traffic. The result comes
            // back as an ordinary event, so the state machine stays the only
            // thing that decides what a rejection means.
            let backend = Arc::clone(backend);
            let events = events.clone();
            let span = tracing::Span::current();
            tokio::spawn(
                async move {
                    let event = match backend.validate(&hello).await {
                        Ok(()) => HubEvent::TapAccepted,
                        Err(reject) => HubEvent::TapRejected(reject),
                    };
                    let _ = events.send(event).await;
                }
                .instrument(span),
            );
        }

        HubAction::StartTimer(id, after) => timers.start(id, after),
        HubAction::CancelTimer(id) => timers.cancel(id),

        HubAction::CompleteRequest {
            request_id,
            outcome,
        } => {
            if let Some(tap_id) = state.tap_id() {
                backend.on_complete(tap_id, request_id, outcome).await;
            }
        }

        HubAction::ProbeCompleted { probe_id, result } => {
            if let Some(tap_id) = state.tap_id() {
                backend.on_probe_result(tap_id, probe_id, result).await;
            }
        }

        HubAction::Disconnect(reason) => *disconnect = Some(reason),
    }
}
