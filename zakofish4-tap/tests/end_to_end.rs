//! The whole stack: a real tap SDK talking to a real hub driver over a real
//! WebSocket, delivering real audio over real UDP to a protofish4 sink.
//!
//! This is the test that proves the two transports actually cooperate — that
//! the key and address the hub puts on the control plane are the ones that open
//! the data plane.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use protofish4::{Endpoint, ReceiverConfig, RelOutcome};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use zakofish4_common::action::DisconnectReason;
use zakofish4_common::config::HubConfig;
use zakofish4_common::event::RequestOutcome;
use zakofish4_common::messages::*;
use zakofish4_common::model::*;
use zakofish4_common::state::PendingRequest;
use zakofish4_hub::backend::HubBackend;
use zakofish4_hub::{TapHandle, Transport, serve};
use zakofish4_tap::{
    AudioSource, AudioStreamSender, AudioCachePolicy, AudioCacheType, AudioMetadata,
    AttachedMetadata, AudioMetadataSuccessMessage, AudioRequestSuccessMessage, TapError,
    TapHandler, tap,
};

// --- a Transport over tokio-tungstenite ------------------------------------

struct WsTransport(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>);

#[async_trait]
impl Transport for WsTransport {
    type Error = tokio_tungstenite::tungstenite::Error;

    async fn recv(&mut self) -> Option<Result<Vec<u8>, Self::Error>> {
        loop {
            match self.0.next().await? {
                Ok(Message::Binary(b)) => return Some(Ok(b)),
                Ok(Message::Close(_)) => return None,
                Ok(_) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.0.send(Message::Binary(frame)).await
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.0.close(None).await
    }
}

// --- a backend that dispatches one request ---------------------------------

struct TestBackend {
    handles: mpsc::Sender<TapHandle>,
    outcomes: Arc<Mutex<Vec<String>>>,
    done: mpsc::Sender<()>,
}

#[async_trait]
impl HubBackend for TestBackend {
    async fn validate(&self, hello: &TapClientHello) -> Result<(), TapServerReject> {
        if hello.api_token == "zk_good" {
            Ok(())
        } else {
            Err(TapServerReject {
                reason_type: HubRejectReasonType::Unauthorized,
                reason: "bad token".into(),
            })
        }
    }

    async fn on_authenticated(&self, _hello: &TapClientHello, handle: TapHandle) {
        let _ = self.handles.send(handle).await;
    }

    async fn on_complete(&self, _tap: &TapId, _id: RequestId, outcome: RequestOutcome) {
        let tag = match &outcome {
            RequestOutcome::Answered(v) => match v {
                ResponseVariant::AudioRequestSuccess(_) => "answered:audio_ok".to_string(),
                ResponseVariant::AudioRequestFailure(f) => {
                    format!("answered:audio_fail:{}", f.reason)
                }
                ResponseVariant::AudioMetadataSuccess(_) => "answered:meta_ok".to_string(),
                ResponseVariant::AudioMetadataFailure(f) => {
                    format!("answered:meta_fail:{}", f.reason)
                }
            },
            RequestOutcome::Streamed(StreamOutcome::Completed { frames_sent }) => {
                format!("streamed:completed:{frames_sent}")
            }
            RequestOutcome::Streamed(other) => format!("streamed:{other:?}"),
            RequestOutcome::TimedOut => "timed_out".into(),
            RequestOutcome::Disconnected => "disconnected".into(),
        };
        self.outcomes.lock().unwrap().push(tag);
        let _ = self.done.send(()).await;
    }

    async fn on_disconnected(&self, _tap: Option<&TapId>, _reason: Option<DisconnectReason>) {}
}

// --- a tap that emits a known sequence of frames ---------------------------

const FRAME_COUNT: u64 = 25;

fn frame_payload(i: u64) -> Bytes {
    Bytes::from(vec![(i % 251) as u8; 60])
}

struct TestTap;

#[async_trait]
impl TapHandler for TestTap {
    async fn handle_audio_metadata_request(
        &self,
        source: AudioSource,
    ) -> Result<AudioMetadataSuccessMessage, TapError> {
        if source.as_str().contains("missing") {
            return Err(TapError::Permanent("no such track".into()));
        }
        Ok(AudioMetadataSuccessMessage {
            metadatas: vec![AudioMetadata::Title("Test Song".into())],
            cache: AudioCachePolicy { cache_type: AudioCacheType::ARHash, ttl_seconds: Some(60) },
        })
    }

    async fn handle_audio_request(
        &self,
        _source: AudioSource,
        stream: AudioStreamSender,
    ) -> Result<AudioRequestSuccessMessage, TapError> {
        tokio::spawn(async move {
            for i in 0..FRAME_COUNT {
                if !stream.send_opus_frame(i, frame_payload(i)).await {
                    break;
                }
            }
        });

        Ok(AudioRequestSuccessMessage {
            cache: AudioCachePolicy { cache_type: AudioCacheType::ARHash, ttl_seconds: Some(60) },
            duration_secs: Some(0.5),
            metadatas: AttachedMetadata::Metadatas(vec![AudioMetadata::Title("Test Song".into())]),
        })
    }
}

// --- harness ---------------------------------------------------------------

struct Stack {
    handle: TapHandle,
    outcomes: Arc<Mutex<Vec<String>>>,
    done: mpsc::Receiver<()>,
}

/// Bring up hub, tap and sink, and wait until the tap is serving.
async fn bring_up(token: &'static str) -> Stack {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hub_addr = listener.local_addr().unwrap();

    let (handles_tx, mut handles_rx) = mpsc::channel(4);
    let (done_tx, done) = mpsc::channel(16);
    let outcomes = Arc::new(Mutex::new(Vec::new()));

    let backend = Arc::new(TestBackend {
        handles: handles_tx,
        outcomes: Arc::clone(&outcomes),
        done: done_tx,
    });

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        serve(
            WsTransport(ws),
            backend,
            HubConfig {
                heartbeat_interval: Duration::from_millis(200),
                ..Default::default()
            },
        )
        .await;
    });

    tokio::spawn(async move {
        let _ = tap()
            .hub(format!("ws://{hub_addr}/gateway"))
            .tap_id("tap-1")
            .friendly_name("Test Tap")
            .api_token(token)
            .pacing_lead(Duration::from_secs(60)) // no pacing delay in tests
            .run(Arc::new(TestTap))
            .await;
    });

    let handle = tokio::time::timeout(Duration::from_secs(5), handles_rx.recv())
        .await
        .expect("tap should authenticate")
        .expect("handle");

    Stack { handle, outcomes, done }
}

fn audio_request(id: RequestId, key: [u8; 32], deliver_to: Vec<String>) -> PendingRequest {
    PendingRequest {
        request_id: id,
        variant: RequestVariant::AudioRequest(AudioRequestMessage {
            ars: AudioRequestString("https://example.invalid/song".into()),
            discord_user_id: DiscordUserId("42".into()),
            encryption_key: EncryptionKey(key),
            deliver_to,
            headers: Default::default(),
        }),
        timeout: Duration::from_secs(10),
    }
}

/// Audio requested over the WebSocket arrives over UDP, byte for byte, and the
/// hub learns how it went.
#[tokio::test]
async fn audio_flows_from_tap_to_sink_and_the_hub_is_told() {
    let mut stack = bring_up("zk_good").await;

    // The sink mints its own id and key and arms itself first — the ordering
    // that makes a handshake unnecessary.
    let raw_key = protofish4::random_key();
    let request_id = RequestId::random();
    let endpoint = Endpoint::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let sink_addr = endpoint.local_addr().unwrap();
    let (_armed, mut streams) = endpoint
        .arm(
            protofish4::RequestId(request_id.0),
            protofish4::SessionKey::from_bytes(&raw_key).unwrap(),
            ReceiverConfig::audio_engine(),
        )
        .await
        .unwrap();

    tokio::spawn({
        let e = Arc::clone(&endpoint);
        async move {
            let _ = e.run().await;
        }
    });
    tokio::spawn({
        let e = Arc::clone(&endpoint);
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                e.tick().await;
            }
        }
    });

    assert!(stack.handle.dispatch(audio_request(
        request_id,
        raw_key,
        vec![sink_addr.to_string()]
    )));

    // Collect the cache-bound copy.
    let mut rel = Vec::new();
    let collect = async {
        while let Some(f) = streams.rel.recv().await {
            rel.push(f.payload);
        }
    };
    tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("audio should arrive");

    let expected: Vec<Vec<u8>> = (0..FRAME_COUNT).map(|i| frame_payload(i).to_vec()).collect();
    assert_eq!(rel, expected, "the cache copy must be byte-exact");

    let outcome = tokio::time::timeout(Duration::from_secs(5), streams.outcome)
        .await
        .expect("outcome")
        .expect("outcome");
    assert!(matches!(outcome, RelOutcome::Complete { .. }));

    // The hub sees the answer and then the stream report.
    for _ in 0..2 {
        let _ = tokio::time::timeout(Duration::from_secs(5), stack.done.recv()).await;
    }
    let seen = stack.outcomes.lock().unwrap().clone();
    assert!(seen.contains(&"answered:audio_ok".to_string()), "{seen:?}");
    assert!(
        seen.contains(&format!("streamed:completed:{FRAME_COUNT}")),
        "the tap must report what it actually sent: {seen:?}"
    );
}

/// Metadata requests never touch UDP, which makes them the easiest thing to
/// migrate first.
#[tokio::test]
async fn metadata_requests_complete_over_the_websocket_alone() {
    let mut stack = bring_up("zk_good").await;
    let id = RequestId::random();

    assert!(stack.handle.dispatch(PendingRequest {
        request_id: id,
        variant: RequestVariant::AudioMetadataRequest(AudioMetadataRequestMessage {
            ars: AudioRequestString("https://example.invalid/song".into()),
            discord_user_id: DiscordUserId("42".into()),
            headers: Default::default(),
        }),
        timeout: Duration::from_secs(5),
    }));

    tokio::time::timeout(Duration::from_secs(5), stack.done.recv())
        .await
        .expect("should complete");
    assert_eq!(
        stack.outcomes.lock().unwrap().clone(),
        vec!["answered:meta_ok".to_string()]
    );
}

/// A tap's own refusal must reach the hub verbatim, so the bot can show the
/// real reason rather than a generic error.
#[tokio::test]
async fn a_tap_refusal_reaches_the_hub_with_its_reason() {
    let mut stack = bring_up("zk_good").await;
    let id = RequestId::random();

    assert!(stack.handle.dispatch(PendingRequest {
        request_id: id,
        variant: RequestVariant::AudioMetadataRequest(AudioMetadataRequestMessage {
            ars: AudioRequestString("https://example.invalid/missing".into()),
            discord_user_id: DiscordUserId("42".into()),
            headers: Default::default(),
        }),
        timeout: Duration::from_secs(5),
    }));

    tokio::time::timeout(Duration::from_secs(5), stack.done.recv())
        .await
        .expect("should complete");
    assert_eq!(
        stack.outcomes.lock().unwrap().clone(),
        vec!["answered:meta_fail:no such track".to_string()]
    );
}

/// A hub that goes quiet without closing the socket — a black hole — must not
/// leave the tap believing it is connected. On 2026-10-03 the hub dropped four
/// tap connections as `heartbeat_timeout` and the taps never noticed: their
/// sockets stayed `ESTABLISHED` and the pods stayed up, silently dark.
#[tokio::test]
async fn a_hub_that_goes_silent_is_given_up_on() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = tap()
            .hub(format!("ws://{addr}/gateway"))
            .tap_id("tap-1")
            .friendly_name("Test Tap")
            .api_token("zk_good")
            .idle_timeout(Duration::from_millis(300))
            .run(Arc::new(TestTap))
            .await;
    });

    // First connection: finish the WebSocket handshake, read the hello, then
    // say nothing at all while holding the socket open.
    let (socket, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
    assert!(ws.next().await.is_some(), "the tap should send a hello");
    tokio::spawn(async move {
        let _hold = ws;
        std::future::pending::<()>().await;
    });

    // The tap gives up on the silence and dials again.
    let again = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await;
    assert!(
        again.is_ok(),
        "a tap that hears nothing must stop believing it is connected"
    );
}

/// A sink that never arms leaves the tap unable to deliver. It must say so
/// rather than hanging, so the hub can try elsewhere.
#[tokio::test]
async fn an_unreachable_sink_is_reported_as_undeliverable() {
    let mut stack = bring_up("zk_good").await;
    let id = RequestId::random();

    // Nothing is listening on this port.
    let dead = "127.0.0.1:1".to_string();
    assert!(stack.handle.dispatch(audio_request(
        id,
        protofish4::random_key(),
        vec![dead]
    )));

    // First the answer, then the failure report.
    for _ in 0..2 {
        let _ = tokio::time::timeout(Duration::from_secs(10), stack.done.recv()).await;
    }
    let seen = stack.outcomes.lock().unwrap().clone();
    assert!(seen.contains(&"answered:audio_ok".to_string()), "{seen:?}");
    assert!(
        seen.iter().any(|s| s.starts_with("streamed:")),
        "the tap must report the delivery failure: {seen:?}"
    );
}
