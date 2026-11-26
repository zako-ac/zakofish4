//! Connection runtime: one gateway WebSocket, plus a protofish4 transfer per
//! audio request.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use protofish4::{SenderConfig, SessionKey, TimestampMs};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use zakofish4_common::codec;
use zakofish4_common::messages::*;
use zakofish4_common::model::{RequestId, StreamOutcome};

use crate::error::SdkError;
use crate::handler::TapHandler;
use crate::metrics::{RequestKind, TapMetrics};
use crate::source::AudioSource;
use crate::stream::{AudioStreamSender, OutFrame, Pacer};

/// Frames buffered between the handler and the wire. Small on purpose: this is
/// a handoff, not a jitter buffer, and a deep queue would just defeat the
/// pacing it exists to allow.
const FRAME_QUEUE: usize = 64;

pub(crate) struct Config {
    pub hub_url: String,
    pub tap_id: zakofish4_common::model::TapId,
    pub friendly_name: String,
    pub api_token: String,
    pub selection_weight: f32,
    pub pacing_lead: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    /// How long this connection may go without a single frame from the hub
    /// before it is treated as gone. Must be comfortably longer than the hub's
    /// `heartbeat_interval`, which is how often it pings.
    pub idle_timeout: Duration,
    /// How long a drain signal may wait for the transfers already accepted to
    /// finish before the process exits.
    pub drain_timeout: Duration,
    /// Request-path counters, shared with `/metrics` and `/health`.
    pub metrics: Arc<TapMetrics>,
}

/// Messages the transfer tasks send back up to the socket writer.
enum Upstream {
    Message(TapToHubMessage),
}

/// Connect, serve, and keep reconnecting until the process is asked to stop.
///
/// SIGTERM (and Ctrl-C) is a *drain*, not an exit: the hub stops routing to a
/// closed connection immediately, while the audio already accepted keeps going
/// straight to the sink, so the process has to stay alive long enough for those
/// transfers to finish. Without this a rolling update or an HPA scale-down
/// truncates playback mid-sentence — the listener hears silence for the rest of
/// the track, and nothing in the tap or the hub records it as a drop.
pub(crate) async fn run(cfg: Config, handler: Arc<dyn TapHandler>) -> Result<(), SdkError> {
    let mut backoff = cfg.reconnect_min;
    // Requests whose audio is still in flight. A dropped WebSocket does not
    // stop a transfer, so these are re-announced on the next connection and the
    // hub re-adopts them instead of orphaning them.
    let streaming: Arc<tokio::sync::Mutex<Vec<RequestId>>> = Default::default();
    let mut shutting_down = Box::pin(shutdown_signal());

    'serve: loop {
        let resuming = streaming.lock().await.clone();
        let served = tokio::select! {
            _ = &mut shutting_down => {
                tracing::info!("drain signal: closing the hub connection and finishing what is in flight");
                break 'serve;
            }
            served = serve_once(
                &cfg,
                Arc::clone(&handler),
                Arc::clone(&streaming),
                resuming,
                Arc::clone(&cfg.metrics),
            ) => served,
        };

        match served {
            Ok(()) => {
                tracing::info!("hub connection closed cleanly");
                backoff = cfg.reconnect_min;
            }
            Err(SdkError::Rejected(reason)) => {
                // Bad credentials will not fix themselves by retrying.
                return Err(SdkError::Rejected(reason));
            }
            Err(e) => {
                tracing::warn!(%e, retry_in = ?backoff, "hub connection lost");
            }
        }

        tokio::select! {
            _ = &mut shutting_down => {
                tracing::info!("drain signal while reconnecting; not reconnecting again");
                break 'serve;
            }
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(cfg.reconnect_max);
    }

    drain(&streaming, cfg.drain_timeout).await;
    Ok(())
}

/// Resolves on SIGTERM or Ctrl-C — the two ways an orchestrator or an operator
/// asks a tap to stop.
///
/// A process that never installs a SIGTERM handler dies on the default
/// disposition, which is what made every rolling update cut audio.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(%e, "failed to install a SIGTERM handler; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = term.recv() => tracing::info!("SIGTERM"),
            _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("ctrl-c");
    }
}

/// Wait for the transfers already accepted to finish, up to `timeout`.
///
/// The hub connection is already closed by the time this runs, which is the
/// point: the hub stops routing to us the moment it sees the socket go, while
/// the audio already accepted is flowing straight from this process to the
/// sink. Staying alive here is the difference between finishing the sentence
/// and cutting it.
async fn drain(streaming: &Arc<tokio::sync::Mutex<Vec<RequestId>>>, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let in_flight = streaming.lock().await.len();
        if in_flight == 0 {
            tracing::info!("drained; nothing left in flight");
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                in_flight,
                ?timeout,
                "drain timeout; exiting with transfers still in flight"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn serve_once(
    cfg: &Config,
    handler: Arc<dyn TapHandler>,
    streaming: Arc<tokio::sync::Mutex<Vec<RequestId>>>,
    resuming: Vec<RequestId>,
    metrics: Arc<TapMetrics>,
) -> Result<(), SdkError> {
    let (ws, _) = tokio_tungstenite::connect_async(&cfg.hub_url).await?;
    let (mut sink, mut stream) = ws.split();

    let hello = TapToHubMessage::ClientHello(TapClientHello {
        protocol_version: PROTOCOL_VERSION,
        tap_id: cfg.tap_id.clone(),
        friendly_name: cfg.friendly_name.clone(),
        api_token: cfg.api_token.clone(),
        selection_weight: cfg.selection_weight,
        resuming,
    });
    sink.send(Message::Binary(codec::encode_to_hub(&hello)?))
        .await?;

    let (up_tx, mut up_rx) = mpsc::channel::<Upstream>(64);
    // The writer ends when the connection does, not when the last sender is
    // dropped. A request whose yt-dlp or ffmpeg child never exits keeps its
    // sender for good, and `run` waiting on a writer that is waiting for that
    // sender is what turned a dropped socket into a tap that never came back:
    // the process stayed up, its healthcheck kept answering, and `serve_once`
    // never returned to reconnect.
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    // One task owns the write half, so transfer tasks and the read loop can
    // both talk to the hub without sharing a lock over the socket.
    let writer = tokio::spawn(async move {
        loop {
            let next = tokio::select! {
                _ = &mut stop_rx => break,
                next = up_rx.recv() => next,
            };
            let Some(Upstream::Message(msg)) = next else {
                break;
            };
            let Ok(bytes) = codec::encode_to_hub(&msg) else {
                continue;
            };
            if sink.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    let mut cancels: HashMap<RequestId, tokio::task::AbortHandle> = HashMap::new();
    let mut accepted = false;

    let result = loop {
        let frame = tokio::select! {
            frame = stream.next() => frame,
            () = tokio::time::sleep(cfg.idle_timeout) => {
                // A connection the hub has dropped but this side never saw go
                // — a black-holed path — leaves a socket that still reads as
                // open and a read half that never wakes. The hub pings, so
                // silence is evidence in itself: without this the tap sits
                // here for good while the hub has long since forgotten it, and
                // the pod looks healthy the whole time.
                tracing::warn!(
                    idle = ?cfg.idle_timeout,
                    "no frame from the hub; treating the connection as gone"
                );
                break Err(SdkError::Idle(cfg.idle_timeout));
            }
        };
        let Some(frame) = frame else {
            break Ok(());
        };
        let msg = match frame {
            Ok(Message::Binary(b)) => match codec::decode_from_hub(&b) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(%e, "undecodable frame from hub");
                    continue;
                }
            },
            Ok(Message::Close(_)) => break Ok(()),
            // The gateway speaks its own Ping/Pong in-band; protocol-level
            // frames here are handled by tungstenite and ignored.
            Ok(_) => continue,
            Err(e) => break Err(SdkError::WebSocket(e)),
        };

        match msg {
            HubToTapMessage::Accept => {
                accepted = true;
                metrics.connection_accepted();
                tracing::info!(tap_id = %cfg.tap_id, "accepted by hub");
            }
            HubToTapMessage::Reject(reject) => {
                break Err(SdkError::Rejected(reject.reason));
            }
            HubToTapMessage::Ping { nonce } => {
                let _ = up_tx
                    .send(Upstream::Message(TapToHubMessage::Pong { nonce }))
                    .await;
            }
            // Spawned rather than awaited: a probe synthesizes, which takes
            // real time, and doing it here would stall this connection's
            // heartbeats and every request behind it. The hub is timing the
            // probe, so it must not also be blocked by it.
            HubToTapMessage::Probe { probe_id } => {
                if !accepted {
                    tracing::warn!("hub sent a probe before accepting us");
                    continue;
                }
                let handler = Arc::clone(&handler);
                let up = up_tx.clone();
                tokio::spawn(async move {
                    let result = handler.probe().await;
                    let _ = up
                        .send(Upstream::Message(TapToHubMessage::ProbeResult {
                            probe_id,
                            result,
                        }))
                        .await;
                });
            }
            HubToTapMessage::Cancel { request_id } => {
                if let Some(h) = cancels.remove(&request_id) {
                    tracing::info!(%request_id, "cancelled by hub");
                    h.abort();
                }
            }
            HubToTapMessage::Request(request) => {
                if !accepted {
                    tracing::warn!("hub sent a request before accepting us");
                    continue;
                }
                let task = tokio::spawn(serve_request(
                    request.request_id,
                    request.variant,
                    Arc::clone(&handler),
                    up_tx.clone(),
                    Arc::clone(&streaming),
                    cfg.pacing_lead,
                    Arc::clone(&metrics),
                ));
                cancels.insert(request.request_id, task.abort_handle());
            }
        }
    };

    drop(up_tx);
    // The read half is done, so this socket is finished, whatever is still
    // holding a sender. Releases the socket and lets `run` reconnect.
    let _ = stop_tx.send(());
    let _ = writer.await;
    result
}

async fn serve_request(
    request_id: RequestId,
    variant: RequestVariant,
    handler: Arc<dyn TapHandler>,
    up: mpsc::Sender<Upstream>,
    streaming: Arc<tokio::sync::Mutex<Vec<RequestId>>>,
    pacing_lead: Duration,
    metrics: Arc<TapMetrics>,
) {
    match variant {
        RequestVariant::AudioMetadataRequest(req) => {
            // Dropped when this function returns, however it returns: the
            // in-flight count and `tap_requests_completed_total` both move on
            // that drop.
            let _guard = metrics.request_received(RequestKind::Meta);

            let source = AudioSource::new(req.ars, req.discord_user_id);
            let variant = match handler.handle_audio_metadata_request(source).await {
                Ok(success) => ResponseVariant::AudioMetadataSuccess(success),
                Err(e) => ResponseVariant::AudioMetadataFailure(AudioMetadataFailureMessage {
                    try_others: e.try_others(),
                    reason: e.reason(),
                }),
            };
            let _ = up
                .send(Upstream::Message(TapToHubMessage::Response(Response {
                    request_id,
                    variant,
                })))
                .await;
            metrics.reply_sent();
        }

        RequestVariant::AudioRequest(req) => {
            serve_audio(
                request_id,
                req,
                handler,
                up,
                streaming,
                pacing_lead,
                metrics,
            )
            .await;
        }
    }
}

async fn serve_audio(
    request_id: RequestId,
    req: AudioRequestMessage,
    handler: Arc<dyn TapHandler>,
    up: mpsc::Sender<Upstream>,
    streaming: Arc<tokio::sync::Mutex<Vec<RequestId>>>,
    pacing_lead: Duration,
    metrics: Arc<TapMetrics>,
) {
    // An audio request is in flight until its stream ends, not until it is
    // answered: that is the interval a drain has to wait out, and the interval
    // `/health` judges.
    let _guard = metrics.request_received(RequestKind::Audio);

    let deliver_to = req.deliver_to.clone();
    let key = match SessionKey::from_bytes(&req.encryption_key.0) {
        Ok(k) => k,
        Err(e) => {
            report_failure(&up, request_id, format!("bad session key: {e}"), false).await;
            metrics.reply_sent();
            return;
        }
    };

    let (frame_tx, frame_rx) = mpsc::channel::<OutFrame>(FRAME_QUEUE);
    let source = AudioSource::new(req.ars, req.discord_user_id);

    // The handler answers first; the hub is holding a listener on this.
    let success = match handler
        .handle_audio_request(source, AudioStreamSender::new(frame_tx))
        .await
    {
        Ok(s) => s,
        Err(e) => {
            report_failure(&up, request_id, e.to_string(), e.try_others()).await;
            metrics.reply_sent();
            return;
        }
    };

    let _ = up
        .send(Upstream::Message(TapToHubMessage::Response(Response {
            request_id,
            variant: ResponseVariant::AudioRequestSuccess(success),
        })))
        .await;
    metrics.reply_sent();

    streaming.lock().await.push(request_id);
    let outcome = pump(request_id, key, deliver_to, frame_rx, pacing_lead, &metrics).await;
    streaming.lock().await.retain(|id| *id != request_id);

    let _ = up
        .send(Upstream::Message(TapToHubMessage::StreamOutcome {
            request_id,
            outcome,
        }))
        .await;
    metrics.reply_sent();
}

/// Move frames from the handler onto the wire, paced to roughly realtime.
async fn pump(
    request_id: RequestId,
    key: SessionKey,
    deliver_to: Vec<String>,
    mut frames: mpsc::Receiver<OutFrame>,
    pacing_lead: Duration,
    metrics: &TapMetrics,
) -> StreamOutcome {
    let mut sender = match protofish4::Sender::connect(
        &deliver_to,
        protofish4::RequestId(request_id.0),
        key,
        SenderConfig::default(),
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            return StreamOutcome::Undeliverable {
                reason: e.to_string(),
            };
        }
    };

    let pacer = Pacer::new(pacing_lead);
    let mut sent = 0u64;

    while let Some(frame) = frames.recv().await {
        pacer.wait_for(frame.ts_ms).await;
        if let Err(e) = sender
            .send_frame(TimestampMs(frame.ts_ms), frame.payload.to_vec())
            .await
        {
            return StreamOutcome::Aborted {
                frames_sent: sent,
                reason: e.to_string(),
            };
        }
        sent += 1;
        // Per frame, not per request: a transfer that has been streaming for a
        // minute is making progress, and `/health` must not call that a stall.
        metrics.reply_sent();
    }

    match sender.finish().await {
        Ok(()) => StreamOutcome::Completed { frames_sent: sent },
        Err(e) => StreamOutcome::Aborted {
            frames_sent: sent,
            reason: e.to_string(),
        },
    }
}

async fn report_failure(
    up: &mpsc::Sender<Upstream>,
    request_id: RequestId,
    reason: String,
    try_others: bool,
) {
    let _ = up
        .send(Upstream::Message(TapToHubMessage::Response(Response {
            request_id,
            variant: ResponseVariant::AudioRequestFailure(AudioRequestFailureMessage {
                reason,
                try_others,
            }),
        })))
        .await;
}
