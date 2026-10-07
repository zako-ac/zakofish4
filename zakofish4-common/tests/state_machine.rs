//! The hub protocol, exercised without a socket in sight.

use std::time::Duration;

use zakofish4_common::action::DisconnectReason;
use zakofish4_common::config::HubConfig;
use zakofish4_common::event::{HubEvent, RequestOutcome, TimerId};
use zakofish4_common::messages::*;
use zakofish4_common::model::*;
use zakofish4_common::state::{PendingRequest, TapState, handle_event, on_connect, on_disconnect};
use zakofish4_common::{HubAction, HubError};

fn hello() -> TapClientHello {
    TapClientHello {
        protocol_version: PROTOCOL_VERSION,
        tap_id: TapId("tap-1".into()),
        friendly_name: "Test Tap".into(),
        api_token: "zk_test".into(),
        selection_weight: 1.0,
        resuming: Vec::new(),
    }
}

fn audio_request(id: RequestId) -> PendingRequest {
    PendingRequest {
        request_id: id,
        variant: RequestVariant::AudioRequest(AudioRequestMessage {
            ars: AudioRequestString("https://example.invalid/song".into()),
            discord_user_id: DiscordUserId("42".into()),
            encryption_key: EncryptionKey([7u8; 32]),
            deliver_to: vec!["10.0.0.5:5000".into()],
            headers: Default::default(),
        }),
        timeout: Duration::from_secs(10),
    }
}

fn metadata_request(id: RequestId) -> PendingRequest {
    PendingRequest {
        request_id: id,
        variant: RequestVariant::AudioMetadataRequest(AudioMetadataRequestMessage {
            ars: AudioRequestString("https://example.invalid/song".into()),
            discord_user_id: DiscordUserId("42".into()),
            headers: Default::default(),
        }),
        timeout: Duration::from_secs(5),
    }
}

fn audio_success() -> ResponseVariant {
    ResponseVariant::AudioRequestSuccess(AudioRequestSuccessMessage {
        cache: AudioCachePolicy { cache_type: AudioCacheType::ARHash, ttl_seconds: Some(300) },
        duration_secs: Some(210.0),
        metadatas: AttachedMetadata::Metadatas(vec![AudioMetadata::Title("Song".into())]),
    })
}

/// Drive to the point where the tap is serving requests.
fn authenticated(cfg: &HubConfig) -> TapState {
    authenticated_with(cfg, hello())
}

fn authenticated_with(cfg: &HubConfig, h: TapClientHello) -> TapState {
    let (state, _) = handle_event(
        TapState::new(),
        HubEvent::MessageFromTap(TapToHubMessage::ClientHello(h)),
        cfg,
    )
    .unwrap();
    let (state, _) = handle_event(state, HubEvent::TapAccepted, cfg).unwrap();
    state
}

fn completions(actions: &[HubAction]) -> Vec<(RequestId, String)> {
    actions
        .iter()
        .filter_map(|a| match a {
            HubAction::CompleteRequest { request_id, outcome } => {
                let tag = match outcome {
                    RequestOutcome::Answered(_) => "answered",
                    RequestOutcome::TimedOut => "timed_out",
                    RequestOutcome::Disconnected => "disconnected",
                    RequestOutcome::Streamed(_) => "streamed",
                };
                Some((*request_id, tag.to_string()))
            }
            _ => None,
        })
        .collect()
}

fn sent(actions: &[HubAction]) -> Vec<&HubToTapMessage> {
    actions
        .iter()
        .filter_map(|a| match a {
            HubAction::SendMessageToTap(m) => Some(m),
            _ => None,
        })
        .collect()
}

// --- handshake -------------------------------------------------------------

#[test]
fn connecting_starts_the_handshake_deadline() {
    let cfg = HubConfig::default();
    let actions = on_connect(&cfg);
    assert!(matches!(
        actions.as_slice(),
        [HubAction::StartTimer(TimerId::Handshake, _)]
    ));
}

#[test]
fn hello_triggers_a_credential_check() {
    let cfg = HubConfig::default();
    let (state, actions) = handle_event(
        TapState::new(),
        HubEvent::MessageFromTap(TapToHubMessage::ClientHello(hello())),
        &cfg,
    )
    .unwrap();

    assert!(matches!(state, TapState::Validating { .. }));
    assert!(actions.iter().any(|a| matches!(a, HubAction::ValidateCredential(_))));
    // The deadline must be released, or a slow credential check kills the
    // connection it just approved.
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, HubAction::CancelTimer(TimerId::Handshake)))
    );
}

#[test]
fn accepting_admits_the_tap() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    assert!(matches!(state, TapState::Authenticated { .. }));
}

#[test]
fn rejecting_sends_reject_then_disconnects() {
    let cfg = HubConfig::default();
    let (state, _) = handle_event(
        TapState::new(),
        HubEvent::MessageFromTap(TapToHubMessage::ClientHello(hello())),
        &cfg,
    )
    .unwrap();

    let reject = TapServerReject {
        reason_type: HubRejectReasonType::Unauthorized,
        reason: "bad token".into(),
    };
    let (state, actions) = handle_event(state, HubEvent::TapRejected(reject), &cfg).unwrap();

    assert!(matches!(state, TapState::Anonymous));
    assert!(matches!(sent(&actions).as_slice(), [HubToTapMessage::Reject(_)]));
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::Disconnect(DisconnectReason::Unauthorized)
    )));
}

#[test]
fn silent_client_is_dropped_on_the_handshake_deadline() {
    let cfg = HubConfig::default();
    let (_, actions) =
        handle_event(TapState::new(), HubEvent::TimerFired(TimerId::Handshake), &cfg).unwrap();
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::Disconnect(DisconnectReason::HandshakeTimeout)
    )));
}

#[test]
fn anything_before_hello_is_a_protocol_error() {
    let cfg = HubConfig::default();
    let err = handle_event(
        TapState::new(),
        HubEvent::MessageFromTap(TapToHubMessage::Pong { nonce: 1 }),
        &cfg,
    )
    .unwrap_err();
    assert!(matches!(err, HubError::InvalidMessage(_)));
}

/// Third-party taps cannot be upgraded in lockstep, so a newer one must be
/// told plainly rather than failing on a malformed message later.
#[test]
fn a_newer_protocol_version_is_refused_cleanly() {
    let cfg = HubConfig::default();
    let mut h = hello();
    h.protocol_version = PROTOCOL_VERSION + 5;

    let (state, actions) = handle_event(
        TapState::new(),
        HubEvent::MessageFromTap(TapToHubMessage::ClientHello(h)),
        &cfg,
    )
    .unwrap();

    assert!(matches!(state, TapState::Anonymous));
    assert!(matches!(sent(&actions).as_slice(), [HubToTapMessage::Reject(_)]));
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::Disconnect(DisconnectReason::UnsupportedVersion)
    )));
}

// --- request correlation ---------------------------------------------------

#[test]
fn dispatch_sends_the_request_and_arms_its_deadline() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();

    let (state, actions) =
        handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();

    assert!(matches!(sent(&actions).as_slice(), [HubToTapMessage::Request(_)]));
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::StartTimer(TimerId::Request(r), _) if *r == id
    )));
    assert_eq!(state.outstanding_ids(), vec![id]);
}

#[test]
fn a_response_completes_the_request_and_cancels_its_timer() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) =
        handle_event(state, HubEvent::DispatchRequest(metadata_request(id)), &cfg).unwrap();

    let response = Response {
        request_id: id,
        variant: ResponseVariant::AudioMetadataSuccess(AudioMetadataSuccessMessage {
            metadatas: vec![AudioMetadata::Title("Song".into())],
            cache: AudioCachePolicy { cache_type: AudioCacheType::None, ttl_seconds: None },
        }),
    };
    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(response)),
        &cfg,
    )
    .unwrap();

    assert_eq!(completions(&actions), vec![(id, "answered".into())]);
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::CancelTimer(TimerId::Request(r)) if *r == id
    )));
    // Metadata has no streaming phase, so nothing is left outstanding.
    assert!(state.outstanding_ids().is_empty());
}

/// An accepted audio request is answered but *not* finished: the tap is now
/// streaming over UDP, which the hub never sees.
#[test]
fn an_accepted_audio_request_stays_outstanding_while_it_streams() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) = handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();

    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
            request_id: id,
            variant: audio_success(),
        })),
        &cfg,
    )
    .unwrap();

    assert_eq!(completions(&actions), vec![(id, "answered".into())]);
    assert_eq!(state.outstanding_ids(), vec![id], "still streaming");

    // The transfer finishes and the hub finally hears about it.
    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::StreamOutcome {
            request_id: id,
            outcome: StreamOutcome::Completed { frames_sent: 10_500 },
        }),
        &cfg,
    )
    .unwrap();

    assert_eq!(completions(&actions), vec![(id, "streamed".into())]);
    assert!(state.outstanding_ids().is_empty());
}

/// A refused audio request never streams, so it must not linger.
#[test]
fn a_refused_audio_request_is_finished_immediately() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) = handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();

    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
            request_id: id,
            variant: ResponseVariant::AudioRequestFailure(AudioRequestFailureMessage {
                reason: "video unavailable".into(),
                try_others: false,
            }),
        })),
        &cfg,
    )
    .unwrap();

    assert_eq!(completions(&actions), vec![(id, "answered".into())]);
    assert!(state.outstanding_ids().is_empty());
}

/// The hub creates this situation itself: a request it has given up on is gone
/// from `outstanding` while the tap is still working, so the tap's answer
/// arrives with nothing to match it. That is late, not wrong, and dropping a
/// working connection over it is what took the YouTube tap off the air.
#[test]
fn a_response_to_an_already_timed_out_request_is_ignored() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) =
        handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();
    let (state, _) =
        handle_event(state, HubEvent::TimerFired(TimerId::Request(id)), &cfg).unwrap();

    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
            request_id: id,
            variant: audio_success(),
        })),
        &cfg,
    )
    .unwrap();

    assert!(
        completions(&actions).is_empty(),
        "the request was already reported as timed out"
    );
    assert!(state.outstanding_ids().is_empty());
}

/// A response for a request this connection never dispatched — a stale answer
/// from a previous connection — is equally not worth dropping the socket for.
#[test]
fn a_response_to_an_unknown_request_is_ignored() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);

    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
            request_id: RequestId::random(),
            variant: audio_success(),
        })),
        &cfg,
    )
    .unwrap();

    assert!(completions(&actions).is_empty());
    assert!(state.outstanding_ids().is_empty());
}

/// Answering a metadata question with audio means the two sides disagree about
/// what is in flight, which guessing cannot fix.
#[test]
fn a_response_of_the_wrong_kind_is_rejected() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) =
        handle_event(state, HubEvent::DispatchRequest(metadata_request(id)), &cfg).unwrap();

    let err = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
            request_id: id,
            variant: audio_success(),
        })),
        &cfg,
    )
    .unwrap_err();

    assert!(matches!(err, HubError::InvalidMessage(_)));
}

#[test]
fn a_duplicate_response_is_rejected() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) = handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();

    let respond = |s: TapState| {
        handle_event(
            s,
            HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
                request_id: id,
                variant: audio_success(),
            })),
            &cfg,
        )
    };

    let (state, _) = respond(state).unwrap();
    assert!(matches!(respond(state).unwrap_err(), HubError::InvalidMessage(_)));
}

#[test]
fn a_request_that_is_never_answered_times_out() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) = handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();

    let (state, actions) =
        handle_event(state, HubEvent::TimerFired(TimerId::Request(id)), &cfg).unwrap();

    assert_eq!(completions(&actions), vec![(id, "timed_out".into())]);
    // And the tap is told to stop, so it cannot finish work nobody is waiting
    // for and then answer a request the hub no longer knows about.
    assert!(matches!(
        sent(&actions).as_slice(),
        [HubToTapMessage::Cancel { request_id }] if *request_id == id
    ));
    assert!(state.outstanding_ids().is_empty());
}

/// Once the audio is flowing the hub has no deadline to enforce — the sink
/// owns that — so a late request timer must not kill a healthy stream.
#[test]
fn a_streaming_request_ignores_the_response_deadline() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) = handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();
    let (state, _) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
            request_id: id,
            variant: audio_success(),
        })),
        &cfg,
    )
    .unwrap();

    let (state, actions) =
        handle_event(state, HubEvent::TimerFired(TimerId::Request(id)), &cfg).unwrap();

    assert!(completions(&actions).is_empty(), "a streaming request must not be timed out");
    assert_eq!(state.outstanding_ids(), vec![id]);
}

#[test]
fn cancelling_tells_the_tap_to_stop() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();
    let (state, _) = handle_event(state, HubEvent::DispatchRequest(audio_request(id)), &cfg).unwrap();

    let (state, actions) = handle_event(state, HubEvent::CancelRequest(id), &cfg).unwrap();

    assert!(matches!(
        sent(&actions).as_slice(),
        [HubToTapMessage::Cancel { request_id }] if *request_id == id
    ));
    assert!(state.outstanding_ids().is_empty());
}

// --- disconnects and resumption --------------------------------------------

/// The decisive behaviour: a dropped WebSocket fails only what was still
/// waiting for an answer. Audio already in flight travels over UDP and keeps
/// going, so failing it here would truncate a track that is playing fine.
#[test]
fn disconnect_fails_pending_requests_but_spares_streaming_ones() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);

    let streaming = RequestId::random();
    let waiting = RequestId::random();

    let (state, _) =
        handle_event(state, HubEvent::DispatchRequest(audio_request(streaming)), &cfg).unwrap();
    let (state, _) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Response(Response {
            request_id: streaming,
            variant: audio_success(),
        })),
        &cfg,
    )
    .unwrap();
    let (state, _) =
        handle_event(state, HubEvent::DispatchRequest(audio_request(waiting)), &cfg).unwrap();

    let actions = on_disconnect(&state);
    assert_eq!(completions(&actions), vec![(waiting, "disconnected".into())]);
}

#[test]
fn a_reconnecting_tap_readopts_its_in_flight_requests() {
    let cfg = HubConfig::default();
    let resumed = RequestId::random();
    let mut h = hello();
    h.resuming = vec![resumed];

    let state = authenticated_with(&cfg, h);
    assert_eq!(state.outstanding_ids(), vec![resumed]);

    // And it can report the outcome on this new connection.
    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::StreamOutcome {
            request_id: resumed,
            outcome: StreamOutcome::Completed { frames_sent: 99 },
        }),
        &cfg,
    )
    .unwrap();

    assert_eq!(completions(&actions), vec![(resumed, "streamed".into())]);
    assert!(state.outstanding_ids().is_empty());
}

/// A tap may report on work begun in an earlier session that this connection
/// never heard about, so an unknown id here is forwarded rather than refused.
#[test]
fn a_stream_outcome_for_an_unknown_request_is_still_reported() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let id = RequestId::random();

    let (_, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::StreamOutcome {
            request_id: id,
            outcome: StreamOutcome::Aborted {
                frames_sent: 3,
                reason: "source died".into(),
            },
        }),
        &cfg,
    )
    .unwrap();

    assert_eq!(completions(&actions), vec![(id, "streamed".into())]);
}

#[test]
fn a_second_hello_is_a_protocol_error() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let err = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::ClientHello(hello())),
        &cfg,
    )
    .unwrap_err();
    assert!(matches!(err, HubError::InvalidMessage(_)));
}

// --- heartbeat -------------------------------------------------------------

/// A black-holed TCP connection is invisible to the WebSocket layer. Since
/// undetected disconnects are the problem this protocol exists to fix, the
/// liveness check is explicit rather than inherited.
#[test]
fn heartbeat_pings_and_arms_a_deadline() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);

    let (state, actions) =
        handle_event(state, HubEvent::TimerFired(TimerId::Heartbeat), &cfg).unwrap();

    assert!(matches!(sent(&actions).as_slice(), [HubToTapMessage::Ping { .. }]));
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::StartTimer(TimerId::HeartbeatDeadline, _)
    )));

    // A pong clears the deadline and schedules the next ping.
    let (_, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::Pong { nonce: 1 }),
        &cfg,
    )
    .unwrap();
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, HubAction::CancelTimer(TimerId::HeartbeatDeadline)))
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, HubAction::StartTimer(TimerId::Heartbeat, _)))
    );
}

#[test]
fn a_missing_pong_disconnects() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);
    let (_, actions) =
        handle_event(state, HubEvent::TimerFired(TimerId::HeartbeatDeadline), &cfg).unwrap();

    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::Disconnect(DisconnectReason::HeartbeatTimeout)
    )));
}

// --- probes ----------------------------------------------------------------

fn probes(actions: &[HubAction]) -> Vec<(u64, String)> {
    actions
        .iter()
        .filter_map(|a| match a {
            HubAction::ProbeCompleted { probe_id, result } => {
                let tag = match result {
                    Some(ProbeResult::Ready {
                        time_to_first_sample_ms,
                    }) => format!("ready:{time_to_first_sample_ms}"),
                    Some(ProbeResult::Failed { reason }) => format!("failed:{reason}"),
                    Some(ProbeResult::Unsupported) => "unsupported".to_string(),
                    None => "unanswered".to_string(),
                };
                Some((*probe_id, tag))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn a_probe_is_sent_and_its_deadline_armed() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);

    let (_, actions) = handle_event(state, HubEvent::DispatchProbe(7), &cfg).unwrap();

    assert!(matches!(
        sent(&actions).as_slice(),
        [HubToTapMessage::Probe { probe_id: 7 }]
    ));
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::StartTimer(TimerId::Probe(7), _)
    )));
}

#[test]
fn a_probe_answer_completes_the_probe_and_releases_its_timer() {
    let cfg = HubConfig::default();
    let (state, _) = handle_event(authenticated(&cfg), HubEvent::DispatchProbe(7), &cfg).unwrap();

    let (_, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::ProbeResult {
            probe_id: 7,
            result: ProbeResult::Ready {
                time_to_first_sample_ms: 412,
            },
        }),
        &cfg,
    )
    .unwrap();

    assert_eq!(probes(&actions), vec![(7, "ready:412".to_string())]);
    assert!(actions.iter().any(|a| matches!(
        a,
        HubAction::CancelTimer(TimerId::Probe(7))
    )));
}

/// The whole reason the probe exists: an unanswered one has to be a verdict,
/// not silence that the caller reads as a healthy tap.
#[test]
fn an_unanswered_probe_is_reported_rather_than_left_pending() {
    let cfg = HubConfig::default();
    let (state, _) = handle_event(authenticated(&cfg), HubEvent::DispatchProbe(1), &cfg).unwrap();

    let (state, actions) =
        handle_event(state, HubEvent::TimerFired(TimerId::Probe(1)), &cfg).unwrap();

    assert_eq!(probes(&actions), vec![(1, "unanswered".to_string())]);

    // And it is not reported twice: a second firing finds nothing outstanding.
    let (_, actions) = handle_event(state, HubEvent::TimerFired(TimerId::Probe(1)), &cfg).unwrap();
    assert!(probes(&actions).is_empty());
}

#[test]
fn a_probe_answer_nobody_is_waiting_for_is_ignored() {
    let cfg = HubConfig::default();
    let state = authenticated(&cfg);

    let (state, actions) = handle_event(
        state,
        HubEvent::MessageFromTap(TapToHubMessage::ProbeResult {
            probe_id: 99,
            result: ProbeResult::Failed {
                reason: "invented".into(),
            },
        }),
        &cfg,
    )
    .unwrap();

    assert!(probes(&actions).is_empty());
    // Not a protocol error either: repeating yourself is not worth losing a
    // working connection over.
    assert!(matches!(state, TapState::Authenticated { .. }));
}

/// The connection dying while a probe is outstanding must resolve it, or the
/// caller waits out a deadline for an answer that can no longer arrive.
#[test]
fn dropping_the_connection_fails_an_outstanding_probe() {
    let cfg = HubConfig::default();
    let (state, _) = handle_event(authenticated(&cfg), HubEvent::DispatchProbe(3), &cfg).unwrap();

    assert_eq!(
        probes(&on_disconnect(&state)),
        vec![(3, "unanswered".to_string())]
    );
}

#[test]
fn a_probe_before_hello_is_refused() {
    let cfg = HubConfig::default();
    let err = handle_event(TapState::new(), HubEvent::DispatchProbe(1), &cfg).unwrap_err();
    assert!(matches!(err, HubError::InternalError(_)));
}
