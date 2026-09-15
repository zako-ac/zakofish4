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
}

/// Messages the transfer tasks send back up to the socket writer.
enum Upstream {
    Message(TapToHubMessage),
}

/// Connect, serve, and keep reconnecting until the process ends.
pub(crate) async fn run(cfg: Config, handler: Arc<dyn TapHandler>) -> Result<(), SdkError> {
    let mut backoff = cfg.reconnect_min;
    // Requests whose audio is still in flight. A dropped WebSocket does not
    // stop a transfer, so these are re-announced on the next connection and the
    // hub re-adopts them instead of orphaning them.
    let streaming: Arc<tokio::sync::Mutex<Vec<RequestId>>> = Default::default();

    loop {
        let resuming = streaming.lock().await.clone();
        match serve_once(&cfg, Arc::clone(&handler), Arc::clone(&streaming), resuming).await {
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

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(cfg.reconnect_max);
    }
}

async fn serve_once(
    cfg: &Config,
    handler: Arc<dyn TapHandler>,
    streaming: Arc<tokio::sync::Mutex<Vec<RequestId>>>,
    resuming: Vec<RequestId>,
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

    // One task owns the write half, so transfer tasks and the read loop can
    // both talk to the hub without sharing a lock over the socket.
    let writer = tokio::spawn(async move {
        while let Some(Upstream::Message(msg)) = up_rx.recv().await {
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
        let Some(frame) = stream.next().await else {
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
                ));
                cancels.insert(request.request_id, task.abort_handle());
            }
        }
    };

    drop(up_tx);
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
) {
    match variant {
        RequestVariant::AudioMetadataRequest(req) => {
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
        }

        RequestVariant::AudioRequest(req) => {
            serve_audio(request_id, req, handler, up, streaming, pacing_lead).await;
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
) {
    let deliver_to = req.deliver_to.clone();
    let key = match SessionKey::from_bytes(&req.encryption_key.0) {
        Ok(k) => k,
        Err(e) => {
            report_failure(&up, request_id, format!("bad session key: {e}"), false).await;
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
            return;
        }
    };

    let _ = up
        .send(Upstream::Message(TapToHubMessage::Response(Response {
            request_id,
            variant: ResponseVariant::AudioRequestSuccess(success),
        })))
        .await;

    streaming.lock().await.push(request_id);
    let outcome = pump(request_id, key, deliver_to, frame_rx, pacing_lead).await;
    streaming.lock().await.retain(|id| *id != request_id);

    let _ = up
        .send(Upstream::Message(TapToHubMessage::StreamOutcome {
            request_id,
            outcome,
        }))
        .await;
}

/// Move frames from the handler onto the wire, paced to roughly realtime.
async fn pump(
    request_id: RequestId,
    key: SessionKey,
    deliver_to: Vec<String>,
    mut frames: mpsc::Receiver<OutFrame>,
    pacing_lead: Duration,
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
