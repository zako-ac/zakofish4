use crate::messages::{TapServerReject, TapToHubMessage};
use crate::model::{RequestId, StreamOutcome};
use crate::state::PendingRequest;

/// Everything that can happen to one tap connection.
///
/// The state machine is driven purely by these; it reads no clock and touches
/// no socket, so a timeout is an event like any other.
#[derive(Debug, Clone)]
pub enum HubEvent {
    MessageFromTap(TapToHubMessage),

    /// Credential check came back. Answers a [`crate::action::HubAction::ValidateCredential`].
    TapAccepted,
    TapRejected(TapServerReject),

    /// The hub wants this tap to serve a request.
    DispatchRequest(PendingRequest),

    /// The listener went away; tell the tap to stop.
    CancelRequest(RequestId),

    /// The hub wants this tap to prove it can synthesize.
    ///
    /// Separate from [`Self::DispatchRequest`] because a probe is not a request:
    /// nothing is streamed, nothing is cached, and no sink is waiting on it.
    DispatchProbe(u64),

    /// A timer started by [`crate::action::HubAction::StartTimer`] fired.
    TimerFired(TimerId),
}

/// Identifies a timer so the state machine can cancel the right one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimerId {
    /// A dispatched request has taken too long to answer.
    Request(RequestId),
    /// Time to send the next `Ping`.
    Heartbeat,
    /// A `Ping` went out and no `Pong` came back.
    HeartbeatDeadline,
    /// A tap connected but never sent `ClientHello`.
    Handshake,
    /// A probe went out and has not been answered.
    Probe(u64),
}

/// What the hub learned from a completed request, handed to the caller so it
/// can resolve whoever was waiting.
#[derive(Debug, Clone)]
pub enum RequestOutcome {
    Answered(crate::messages::ResponseVariant),
    /// The tap never answered within its deadline.
    TimedOut,
    /// The connection went away before the tap answered.
    Disconnected,
    /// The tap reported how the audio transfer itself ended, after having
    /// already answered the request.
    Streamed(StreamOutcome),
}
