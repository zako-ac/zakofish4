//! The driver end to end, over an in-memory pipe.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use zakofish4_common::action::DisconnectReason;
use zakofish4_common::codec;
use zakofish4_common::config::HubConfig;
use zakofish4_common::event::RequestOutcome;
use zakofish4_common::messages::*;
use zakofish4_common::model::*;
use zakofish4_common::state::PendingRequest;
use zakofish4_hub::backend::HubBackend;
use zakofish4_hub::transport::duplex::{self, Pipe};
use zakofish4_hub::{TapHandle, serve};

/// Records what the driver reported and hands back the tap handle.
#[derive(Default)]
struct Recorder {
    accept: bool,
    handle_tx: Mutex<Option<mpsc::Sender<TapHandle>>>,
    completed: Mutex<Vec<(RequestId, String)>>,
    probes: Mutex<Vec<(u64, String)>>,
    disconnected: Mutex<Option<Option<DisconnectReason>>>,
    done_tx: Mutex<Option<mpsc::Sender<()>>>,
}

#[async_trait]
impl HubBackend for Recorder {
    async fn validate(&self, _hello: &TapClientHello) -> Result<(), TapServerReject> {
        if self.accept {
            Ok(())
        } else {
            Err(TapServerReject {
                reason_type: HubRejectReasonType::Unauthorized,
                reason: "no".into(),
            })
        }
    }

    async fn on_authenticated(&self, _hello: &TapClientHello, handle: TapHandle) {
        let tx = self.handle_tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(handle).await;
        }
    }

    async fn on_complete(&self, _tap: &TapId, request_id: RequestId, outcome: RequestOutcome) {
        let tag = match outcome {
            RequestOutcome::Answered(_) => "answered",
            RequestOutcome::TimedOut => "timed_out",
            RequestOutcome::Disconnected => "disconnected",
            RequestOutcome::Streamed(_) => "streamed",
        };
        self.completed.lock().unwrap().push((request_id, tag.into()));
    }

    async fn on_probe_result(&self, _tap: &TapId, probe_id: u64, result: Option<ProbeResult>) {
        let tag = match result {
            Some(ProbeResult::Ready {
                time_to_first_sample_ms,
            }) => format!("ready:{time_to_first_sample_ms}"),
            Some(ProbeResult::Failed { reason }) => format!("failed:{reason}"),
            Some(ProbeResult::Unsupported) => "unsupported".to_string(),
            None => "unanswered".to_string(),
        };
        self.probes.lock().unwrap().push((probe_id, tag));
    }

    async fn on_disconnected(&self, _tap: Option<&TapId>, reason: Option<DisconnectReason>) {
        *self.disconnected.lock().unwrap() = Some(reason);
        let tx = self.done_tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(()).await;
        }
    }
}

struct Harness {
    tap: Pipe,
    backend: Arc<Recorder>,
    handles: mpsc::Receiver<TapHandle>,
    done: mpsc::Receiver<()>,
}

fn start(accept: bool, cfg: HubConfig) -> Harness {
    let (hub_side, tap_side) = duplex::pair(32);
    let (handle_tx, handles) = mpsc::channel(4);
    let (done_tx, done) = mpsc::channel(1);

    let backend = Arc::new(Recorder {
        accept,
        handle_tx: Mutex::new(Some(handle_tx)),
        done_tx: Mutex::new(Some(done_tx)),
        ..Default::default()
    });

    tokio::spawn(serve(hub_side, Arc::clone(&backend), cfg));

    Harness { tap: tap_side, backend, handles, done }
}

impl Harness {
    async fn send(&mut self, msg: TapToHubMessage) {
        use zakofish4_hub::Transport;
        self.tap.send(codec::encode_to_hub(&msg).unwrap()).await.unwrap();
    }

    async fn recv(&mut self) -> Option<HubToTapMessage> {
        use zakofish4_hub::Transport;
        let bytes = self.tap.recv().await?.ok()?;
        Some(codec::decode_from_hub(&bytes).unwrap())
    }

    /// Read until something other than a `Ping` shows up, so heartbeat traffic
    /// does not have to be threaded through every assertion.
    async fn recv_skipping_pings(&mut self) -> Option<HubToTapMessage> {
        loop {
            match self.recv().await? {
                HubToTapMessage::Ping { .. } => continue,
                other => return Some(other),
            }
        }
    }
}

fn hello() -> TapToHubMessage {
    TapToHubMessage::ClientHello(TapClientHello {
        protocol_version: PROTOCOL_VERSION,
        tap_id: TapId("tap-1".into()),
        friendly_name: "Tap".into(),
        api_token: "zk_x".into(),
        selection_weight: 1.0,
        resuming: Vec::new(),
    })
}

fn audio_request(id: RequestId) -> PendingRequest {
    PendingRequest {
        request_id: id,
        variant: RequestVariant::AudioRequest(AudioRequestMessage {
            ars: AudioRequestString("https://example.invalid/x".into()),
            discord_user_id: DiscordUserId("42".into()),
            encryption_key: EncryptionKey([1u8; 32]),
            deliver_to: vec!["10.0.0.5:5000".into()],
            headers: Default::default(),
        }),
        timeout: Duration::from_millis(200),
    }
}

fn slow_heartbeat() -> HubConfig {
    HubConfig {
        heartbeat_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_valid_tap_is_accepted_and_handed_back() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;

    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    assert!(h.handles.recv().await.is_some(), "backend should receive a handle");
}

#[tokio::test]
async fn a_rejected_tap_is_told_why_and_dropped() {
    let mut h = start(false, slow_heartbeat());
    h.send(hello()).await;

    assert!(matches!(
        h.recv_skipping_pings().await,
        Some(HubToTapMessage::Reject(_))
    ));
    h.done.recv().await;
    assert_eq!(
        *h.backend.disconnected.lock().unwrap(),
        Some(Some(DisconnectReason::Unauthorized))
    );
}

#[tokio::test]
async fn a_dispatched_request_reaches_the_tap_and_its_answer_comes_back() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    let id = RequestId::random();
    assert!(handle.dispatch(audio_request(id)));

    let Some(HubToTapMessage::Request(req)) = h.recv_skipping_pings().await else {
        panic!("expected a Request");
    };
    assert_eq!(req.request_id, id);

    h.send(TapToHubMessage::Response(Response {
        request_id: id,
        variant: ResponseVariant::AudioRequestSuccess(AudioRequestSuccessMessage {
            cache: AudioCachePolicy { cache_type: AudioCacheType::ARHash, ttl_seconds: None },
            duration_secs: Some(1.0),
            metadatas: AttachedMetadata::UseCached,
        }),
    }))
    .await;

    // Then the transfer finishes.
    h.send(TapToHubMessage::StreamOutcome {
        request_id: id,
        outcome: StreamOutcome::Completed { frames_sent: 5 },
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let completed = h.backend.completed.lock().unwrap().clone();
    assert_eq!(
        completed,
        vec![(id, "answered".to_string()), (id, "streamed".to_string())]
    );
}

/// A tap that accepts a request and then goes silent must not wedge the caller.
#[tokio::test]
async fn an_unanswered_request_times_out() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    let id = RequestId::random();
    handle.dispatch(audio_request(id));
    let _ = h.recv_skipping_pings().await;

    tokio::time::sleep(Duration::from_millis(400)).await;
    let completed = h.backend.completed.lock().unwrap().clone();
    assert_eq!(completed, vec![(id, "timed_out".to_string())]);
}

/// The connection dying fails what was still waiting for an answer — but a
/// request already streaming is left alone, because its audio is on UDP and is
/// still arriving.
#[tokio::test]
async fn dropping_the_socket_spares_a_streaming_request() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    let streaming = RequestId::random();
    let waiting = RequestId::random();

    handle.dispatch(audio_request(streaming));
    let _ = h.recv_skipping_pings().await;
    h.send(TapToHubMessage::Response(Response {
        request_id: streaming,
        variant: ResponseVariant::AudioRequestSuccess(AudioRequestSuccessMessage {
            cache: AudioCachePolicy { cache_type: AudioCacheType::None, ttl_seconds: None },
            duration_secs: None,
            metadatas: AttachedMetadata::UseCached,
        }),
    }))
    .await;

    let mut slow = audio_request(waiting);
    slow.timeout = Duration::from_secs(30);
    handle.dispatch(slow);
    let _ = h.recv_skipping_pings().await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(h.tap);
    h.done.recv().await;

    let completed = h.backend.completed.lock().unwrap().clone();
    assert!(completed.contains(&(streaming, "answered".to_string())));
    assert!(completed.contains(&(waiting, "disconnected".to_string())));
    assert!(
        !completed.contains(&(streaming, "disconnected".to_string())),
        "a streaming request must survive the socket"
    );
}

#[tokio::test]
async fn a_silent_client_is_dropped_on_the_handshake_deadline() {
    let cfg = HubConfig {
        handshake_timeout: Duration::from_millis(80),
        ..slow_heartbeat()
    };
    let mut h = start(true, cfg);

    h.done.recv().await;
    assert_eq!(
        *h.backend.disconnected.lock().unwrap(),
        Some(Some(DisconnectReason::HandshakeTimeout))
    );
}

/// A tap that stops answering pings is dropped, which is the whole point of
/// having an application-level heartbeat over one the socket cannot see.
#[tokio::test]
async fn a_tap_that_stops_ponging_is_dropped() {
    let cfg = HubConfig {
        heartbeat_interval: Duration::from_millis(20),
        heartbeat_deadline: Duration::from_millis(60),
        ..Default::default()
    };
    let mut h = start(true, cfg);
    h.send(hello()).await;

    // Never pong.
    h.done.recv().await;
    assert_eq!(
        *h.backend.disconnected.lock().unwrap(),
        Some(Some(DisconnectReason::HeartbeatTimeout))
    );
}

#[tokio::test]
async fn undecodable_frames_end_the_connection() {
    use zakofish4_hub::Transport;
    let mut h = start(true, slow_heartbeat());
    h.tap.send(vec![0xC1, 0x00, 0xFF]).await.unwrap();

    h.done.recv().await;
    assert_eq!(
        *h.backend.disconnected.lock().unwrap(),
        Some(Some(DisconnectReason::ProtocolViolation))
    );
}

/// An answer for a request this connection never dispatched — a stale answer
/// from an earlier connection — must not cost the tap its socket. The hub
/// produces this situation itself (a request that timed out is dropped from
/// `outstanding` while the tap is still working), and reading it as a protocol
/// violation is what took the YouTube tap off the air until it was restarted.
#[tokio::test]
async fn a_response_to_an_unknown_request_is_ignored() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    h.send(TapToHubMessage::Response(Response {
        request_id: RequestId::random(),
        variant: ResponseVariant::AudioMetadataSuccess(AudioMetadataSuccessMessage {
            metadatas: vec![],
            cache: AudioCachePolicy { cache_type: AudioCacheType::None, ttl_seconds: None },
        }),
    }))
    .await;

    assert!(
        tokio::time::timeout(Duration::from_millis(200), h.done.recv())
            .await
            .is_err(),
        "an answer nobody is waiting for must not disconnect the tap"
    );

    // Still connected, and still serving.
    let id = RequestId::random();
    assert!(handle.dispatch(audio_request(id)));
    let Some(HubToTapMessage::Request(req)) = h.recv_skipping_pings().await else {
        panic!("expected a Request on the same connection");
    };
    assert_eq!(req.request_id, id);
}

#[tokio::test]
async fn cancelling_reaches_the_tap() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    let id = RequestId::random();
    handle.dispatch(audio_request(id));
    let _ = h.recv_skipping_pings().await;

    assert!(handle.cancel(id));
    let Some(HubToTapMessage::Cancel { request_id }) = h.recv_skipping_pings().await else {
        panic!("expected Cancel");
    };
    assert_eq!(request_id, id);
}

// --- probes ----------------------------------------------------------------

/// A probe has to survive the whole way out and back: the backend learns the
/// tap's own answer, not a timeout, or every healthy tap looks wedged.
#[tokio::test]
async fn a_probe_reaches_the_tap_and_its_report_comes_back() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    assert!(handle.probe(11));

    let Some(HubToTapMessage::Probe { probe_id }) = h.recv_skipping_pings().await else {
        panic!("expected a Probe");
    };
    assert_eq!(probe_id, 11);

    h.send(TapToHubMessage::ProbeResult {
        probe_id: 11,
        result: ProbeResult::Ready {
            time_to_first_sample_ms: 250,
        },
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        h.backend.probes.lock().unwrap().clone(),
        vec![(11, "ready:250".to_string())]
    );
}

#[tokio::test]
async fn a_probe_the_tap_never_answers_is_reported_as_unanswered() {
    let cfg = HubConfig {
        probe_timeout: Duration::from_millis(80),
        ..slow_heartbeat()
    };
    let mut h = start(true, cfg);
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    handle.probe(4);
    let _ = h.recv_skipping_pings().await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        h.backend.probes.lock().unwrap().clone(),
        vec![(4, "unanswered".to_string())]
    );
}

/// A tap that answers after its request timed out must keep its connection.
///
/// The hub itself creates the situation — the request timer removes the
/// request while the tap is still working — so reading the answer as a
/// protocol violation and closing the socket is what took the YouTube tap
/// down until someone restarted it.
#[tokio::test]
async fn answering_after_a_timeout_does_not_drop_the_connection() {
    let mut h = start(true, slow_heartbeat());
    h.send(hello()).await;
    assert!(matches!(h.recv_skipping_pings().await, Some(HubToTapMessage::Accept)));
    let handle = h.handles.recv().await.unwrap();

    // `audio_request` times out after 200 ms.
    let id = RequestId::random();
    assert!(handle.dispatch(audio_request(id)));
    let Some(HubToTapMessage::Request(req)) = h.recv_skipping_pings().await else {
        panic!("expected a Request");
    };
    assert_eq!(req.request_id, id);

    // The hub gives up and tells the tap to stop.
    let Some(HubToTapMessage::Cancel { request_id }) = h.recv_skipping_pings().await else {
        panic!("expected a Cancel once the request timed out");
    };
    assert_eq!(request_id, id);

    // The tap answers anyway, late.
    h.send(TapToHubMessage::Response(Response {
        request_id: id,
        variant: ResponseVariant::AudioRequestSuccess(AudioRequestSuccessMessage {
            cache: AudioCachePolicy { cache_type: AudioCacheType::ARHash, ttl_seconds: None },
            duration_secs: Some(1.0),
            metadatas: AttachedMetadata::UseCached,
        }),
    }))
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        h.backend.disconnected.lock().unwrap().is_none(),
        "a late answer must not disconnect the tap"
    );

    // And the connection still works: a second request reaches the tap.
    let second = RequestId::random();
    assert!(handle.dispatch(audio_request(second)));
    let Some(HubToTapMessage::Request(req)) = h.recv_skipping_pings().await else {
        panic!("expected a second Request on the same connection");
    };
    assert_eq!(req.request_id, second);
}
