use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::action::{DisconnectReason, HubAction};
use crate::config::HubConfig;
use crate::error::HubError;
use crate::event::{HubEvent, RequestOutcome, TimerId};
use crate::messages::{
    HubToTapMessage, Request, RequestVariant, Response, ResponseVariant, TapClientHello,
    TapServerReject,
};
use crate::model::{HubRejectReasonType, RequestId};

/// A request the hub wants this tap to serve.
#[derive(Debug, Clone)]
pub struct PendingRequest {
    pub request_id: RequestId,
    pub variant: RequestVariant,
    /// How long the tap has to answer. Only bounds the *response*; the audio
    /// transfer that follows has its own deadline, enforced by the sink.
    pub timeout: Duration,
}

/// How far along a dispatched request is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Dispatched; the tap has not answered yet.
    AwaitingResponse,
    /// The tap answered and is now sending audio over UDP. The hub sees none
    /// of that traffic and hears about it only via `StreamOutcome`.
    Streaming,
}

/// Which of the two request shapes is in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Audio,
    Metadata,
}

/// What the hub remembers about one in-flight request.
#[derive(Debug, Clone)]
pub struct Tracked {
    pub kind: Kind,
    pub phase: Phase,
}

#[derive(Debug, Clone)]
pub enum TapState {
    /// Connected, nothing said yet.
    Anonymous,
    /// `ClientHello` received, credentials being checked.
    Validating { hello: TapClientHello },
    /// Serving requests.
    Authenticated {
        hello: TapClientHello,
        outstanding: HashMap<RequestId, Tracked>,
        /// Probes sent and not yet answered.
        ///
        /// A set rather than a single slot because the hub side timers are
        /// keyed by probe, and serialising probes on the hub would make one
        /// slow answer hide every other connection's.
        probes: HashSet<u64>,
    },
}

impl TapState {
    pub fn new() -> Self {
        TapState::Anonymous
    }

    pub fn tap_id(&self) -> Option<&crate::model::TapId> {
        match self {
            TapState::Anonymous => None,
            TapState::Validating { hello } | TapState::Authenticated { hello, .. } => {
                Some(&hello.tap_id)
            }
        }
    }

    /// Requests this connection is still responsible for.
    pub fn outstanding_ids(&self) -> Vec<RequestId> {
        match self {
            TapState::Authenticated { outstanding, .. } => outstanding.keys().copied().collect(),
            _ => Vec::new(),
        }
    }
}

impl Default for TapState {
    fn default() -> Self {
        Self::new()
    }
}

/// Advance the connection by one event.
///
/// Returns the next state and what the caller must do. Nothing here reads a
/// clock or touches a socket: timeouts arrive as [`HubEvent::TimerFired`] and
/// leave as [`HubAction::StartTimer`].
pub fn handle_event(
    state: TapState,
    event: HubEvent,
    cfg: &HubConfig,
) -> Result<(TapState, Vec<HubAction>), HubError> {
    match state {
        TapState::Anonymous => handle_anonymous(event, cfg),
        TapState::Validating { hello } => handle_validating(hello, event),
        TapState::Authenticated {
            hello,
            outstanding,
            probes,
        } => handle_authenticated(hello, outstanding, probes, event, cfg),
    }
}

/// The actions a caller should apply when a fresh connection opens, before any
/// event. Starts the handshake deadline.
pub fn on_connect(cfg: &HubConfig) -> Vec<HubAction> {
    vec![HubAction::StartTimer(
        TimerId::Handshake,
        cfg.handshake_timeout,
    )]
}

/// Requests that must be reported as failed when the connection ends.
///
/// Only those still awaiting a response. Anything already `Streaming` is
/// deliberately left alone: the audio travels over UDP and does not care that
/// the WebSocket went away, so failing it here would truncate a track that is
/// in fact playing fine. The tap re-announces such requests in `resuming` when
/// it reconnects.
///
/// Probes in flight are failed too. They are about to become unanswerable, and
/// leaving the caller to hit its own deadline would spend that deadline for
/// nothing — the connection it was asking about is already gone.
pub fn on_disconnect(state: &TapState) -> Vec<HubAction> {
    let TapState::Authenticated {
        outstanding,
        probes,
        ..
    } = state
    else {
        return Vec::new();
    };

    let mut actions: Vec<HubAction> = outstanding
        .iter()
        .filter(|(_, t)| t.phase == Phase::AwaitingResponse)
        .map(|(id, _)| HubAction::CompleteRequest {
            request_id: *id,
            outcome: RequestOutcome::Disconnected,
        })
        .collect();

    actions.extend(probes.iter().map(|probe_id| HubAction::ProbeCompleted {
        probe_id: *probe_id,
        result: None,
    }));

    actions
}

fn handle_anonymous(
    event: HubEvent,
    cfg: &HubConfig,
) -> Result<(TapState, Vec<HubAction>), HubError> {
    match event {
        HubEvent::MessageFromTap(crate::messages::TapToHubMessage::ClientHello(hello)) => {
            if hello.protocol_version > cfg.protocol_version {
                let reject = TapServerReject {
                    reason_type: HubRejectReasonType::UnsupportedVersion,
                    reason: format!(
                        "hub serves protocol version {}, tap speaks {}",
                        cfg.protocol_version, hello.protocol_version
                    ),
                };
                return Ok((
                    TapState::Anonymous,
                    vec![
                        HubAction::CancelTimer(TimerId::Handshake),
                        HubAction::SendMessageToTap(HubToTapMessage::Reject(reject)),
                        HubAction::Disconnect(DisconnectReason::UnsupportedVersion),
                    ],
                ));
            }

            let actions = vec![
                HubAction::CancelTimer(TimerId::Handshake),
                HubAction::ValidateCredential(hello.clone()),
            ];
            Ok((TapState::Validating { hello }, actions))
        }
        HubEvent::TimerFired(TimerId::Handshake) => Ok((
            TapState::Anonymous,
            vec![HubAction::Disconnect(DisconnectReason::HandshakeTimeout)],
        )),
        // A stray timer for a request this connection never had is not worth
        // dropping the connection over.
        HubEvent::TimerFired(_) => Ok((TapState::Anonymous, Vec::new())),
        HubEvent::MessageFromTap(_) => Err(HubError::InvalidMessage(
            "expected ClientHello before anything else".to_string(),
        )),
        HubEvent::TapAccepted | HubEvent::TapRejected(_) => Err(HubError::InternalError(
            "credential result arrived for a connection that never sent ClientHello".to_string(),
        )),
        HubEvent::DispatchRequest(_) | HubEvent::CancelRequest(_) => Err(HubError::InternalError(
            "cannot dispatch to an unauthenticated tap".to_string(),
        )),
        // A probe is a request for work, so it needs a credential like any
        // other. Ping does not; this is not Ping.
        HubEvent::DispatchProbe(_) => Err(HubError::InternalError(
            "cannot probe an unauthenticated tap".to_string(),
        )),
    }
}

fn handle_validating(
    hello: TapClientHello,
    event: HubEvent,
) -> Result<(TapState, Vec<HubAction>), HubError> {
    match event {
        HubEvent::TapAccepted => {
            let mut outstanding = HashMap::new();
            // A tap that reconnects mid-transfer re-announces what it is still
            // streaming, so those requests are adopted rather than orphaned.
            for id in &hello.resuming {
                outstanding.insert(
                    *id,
                    Tracked {
                        kind: Kind::Audio,
                        phase: Phase::Streaming,
                    },
                );
            }

            let actions = vec![
                HubAction::SendMessageToTap(HubToTapMessage::Accept),
                HubAction::StartTimer(TimerId::Heartbeat, Duration::ZERO),
            ];
            Ok((
                TapState::Authenticated {
                    hello,
                    outstanding,
                    probes: HashSet::new(),
                },
                actions,
            ))
        }
        HubEvent::TapRejected(reject) => Ok((
            TapState::Anonymous,
            vec![
                HubAction::SendMessageToTap(HubToTapMessage::Reject(reject)),
                HubAction::Disconnect(DisconnectReason::Unauthorized),
            ],
        )),
        // The tap may keep talking while we are checking it; nothing it says is
        // valid yet, but it is not worth a disconnect either.
        HubEvent::MessageFromTap(_) => Ok((TapState::Validating { hello }, Vec::new())),
        HubEvent::TimerFired(_) => Ok((TapState::Validating { hello }, Vec::new())),
        HubEvent::DispatchRequest(_) | HubEvent::CancelRequest(_) => Err(HubError::InternalError(
            "cannot dispatch while validating".to_string(),
        )),
        HubEvent::DispatchProbe(_) => Err(HubError::InternalError(
            "cannot probe while validating".to_string(),
        )),
    }
}

fn handle_authenticated(
    hello: TapClientHello,
    mut outstanding: HashMap<RequestId, Tracked>,
    mut probes: HashSet<u64>,
    event: HubEvent,
    cfg: &HubConfig,
) -> Result<(TapState, Vec<HubAction>), HubError> {
    use crate::messages::TapToHubMessage as Msg;

    let mut actions = Vec::new();

    match event {
        HubEvent::MessageFromTap(Msg::Response(response)) => {
            on_response(&mut outstanding, response, &mut actions)?;
        }

        HubEvent::MessageFromTap(Msg::StreamOutcome {
            request_id,
            outcome,
        }) => {
            // Accepted even for a request this connection never dispatched: a
            // tap that reconnected mid-transfer reports on work begun in an
            // earlier session, and the hub resolves it out of shared state.
            outstanding.remove(&request_id);
            actions.push(HubAction::CompleteRequest {
                request_id,
                outcome: RequestOutcome::Streamed(outcome),
            });
        }

        HubEvent::MessageFromTap(Msg::Pong { .. }) => {
            actions.push(HubAction::CancelTimer(TimerId::HeartbeatDeadline));
            actions.push(HubAction::StartTimer(
                TimerId::Heartbeat,
                cfg.heartbeat_interval,
            ));
        }

        HubEvent::MessageFromTap(Msg::ProbeResult { probe_id, result }) => {
            // An answer to a probe this connection is not waiting on is either
            // late (already reported as unanswered) or invented. Neither is
            // worth dropping a working connection over, and treating it as an
            // answer would let a tap retract a verdict by repeating itself.
            if probes.remove(&probe_id) {
                actions.push(HubAction::CancelTimer(TimerId::Probe(probe_id)));
                actions.push(HubAction::ProbeCompleted {
                    probe_id,
                    result: Some(result),
                });
            }
        }

        HubEvent::DispatchProbe(probe_id) => {
            probes.insert(probe_id);
            actions.push(HubAction::SendMessageToTap(HubToTapMessage::Probe {
                probe_id,
            }));
            actions.push(HubAction::StartTimer(
                TimerId::Probe(probe_id),
                cfg.probe_timeout,
            ));
        }

        HubEvent::TimerFired(TimerId::Probe(probe_id)) => {
            if probes.remove(&probe_id) {
                actions.push(HubAction::ProbeCompleted {
                    probe_id,
                    result: None,
                });
            }
        }

        HubEvent::MessageFromTap(Msg::ClientHello(_)) => {
            return Err(HubError::InvalidMessage(
                "ClientHello on an already-authenticated connection".to_string(),
            ));
        }

        HubEvent::DispatchRequest(pending) => {
            let kind = match pending.variant {
                RequestVariant::AudioRequest(_) => Kind::Audio,
                RequestVariant::AudioMetadataRequest(_) => Kind::Metadata,
            };
            outstanding.insert(
                pending.request_id,
                Tracked {
                    kind,
                    phase: Phase::AwaitingResponse,
                },
            );
            actions.push(HubAction::SendMessageToTap(HubToTapMessage::Request(
                Request {
                    request_id: pending.request_id,
                    variant: pending.variant,
                },
            )));
            actions.push(HubAction::StartTimer(
                TimerId::Request(pending.request_id),
                pending.timeout,
            ));
        }

        HubEvent::CancelRequest(request_id) => {
            outstanding.remove(&request_id);
            actions.push(HubAction::CancelTimer(TimerId::Request(request_id)));
            actions.push(HubAction::SendMessageToTap(HubToTapMessage::Cancel {
                request_id,
            }));
        }

        HubEvent::TimerFired(TimerId::Request(request_id)) => {
            // Only meaningful while awaiting a response. A streaming request
            // has no hub-side deadline; the sink owns that.
            if let Some(tracked) = outstanding.get(&request_id)
                && tracked.phase == Phase::AwaitingResponse
            {
                outstanding.remove(&request_id);
                actions.push(HubAction::CompleteRequest {
                    request_id,
                    outcome: RequestOutcome::TimedOut,
                });
            }
        }

        HubEvent::TimerFired(TimerId::Heartbeat) => {
            actions.push(HubAction::SendMessageToTap(HubToTapMessage::Ping {
                nonce: rand_nonce(&hello, outstanding.len()),
            }));
            actions.push(HubAction::StartTimer(
                TimerId::HeartbeatDeadline,
                cfg.heartbeat_deadline,
            ));
        }

        HubEvent::TimerFired(TimerId::HeartbeatDeadline) => {
            actions.push(HubAction::Disconnect(DisconnectReason::HeartbeatTimeout));
        }

        HubEvent::TimerFired(TimerId::Handshake) => {}

        HubEvent::TapAccepted | HubEvent::TapRejected(_) => {
            return Err(HubError::InternalError(
                "duplicate credential result for an authenticated connection".to_string(),
            ));
        }
    }

    Ok((
        TapState::Authenticated {
            hello,
            outstanding,
            probes,
        },
        actions,
    ))
}

fn on_response(
    outstanding: &mut HashMap<RequestId, Tracked>,
    response: Response,
    actions: &mut Vec<HubAction>,
) -> Result<(), HubError> {
    let Some(tracked) = outstanding.get(&response.request_id).cloned() else {
        return Err(HubError::InvalidRequestId(response.request_id));
    };

    // A tap answering a metadata request with audio (or the reverse) means the
    // two sides disagree about what is in flight, which is not recoverable by
    // guessing.
    let answered_kind = if response.variant.is_audio() {
        Kind::Audio
    } else {
        Kind::Metadata
    };
    if answered_kind != tracked.kind {
        return Err(HubError::InvalidMessage(format!(
            "tap answered request {} with the wrong response kind",
            response.request_id
        )));
    }
    if tracked.phase != Phase::AwaitingResponse {
        return Err(HubError::InvalidMessage(format!(
            "duplicate response for request {}",
            response.request_id
        )));
    }

    actions.push(HubAction::CancelTimer(TimerId::Request(
        response.request_id,
    )));

    // An accepted audio request is not finished: the tap now streams over UDP,
    // and the hub stays interested until it hears how that went.
    let keeps_streaming = matches!(response.variant, ResponseVariant::AudioRequestSuccess(_));

    actions.push(HubAction::CompleteRequest {
        request_id: response.request_id,
        outcome: RequestOutcome::Answered(response.variant),
    });

    if keeps_streaming {
        outstanding.insert(
            response.request_id,
            Tracked {
                kind: Kind::Audio,
                phase: Phase::Streaming,
            },
        );
    } else {
        outstanding.remove(&response.request_id);
    }

    Ok(())
}

/// Ping payload. Only needs to be unpredictable enough that a stale `Pong`
/// cannot satisfy a later `Ping`; it is not a security boundary.
fn rand_nonce(hello: &TapClientHello, outstanding: usize) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    hello.tap_id.hash(&mut h);
    outstanding.hash(&mut h);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
        .hash(&mut h);
    h.finish()
}
