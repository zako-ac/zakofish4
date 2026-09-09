use std::time::Duration;

use crate::event::{RequestOutcome, TimerId};
use crate::messages::{HubToTapMessage, TapClientHello};
use crate::model::RequestId;

/// Something the caller must do on the state machine's behalf.
///
/// Keeping timers in here rather than in the adapter is deliberate: if the
/// adapter owned them, it would also own when a request expires, and the state
/// machine would no longer be the single source of truth for what is
/// outstanding.
#[derive(Debug, Clone)]
pub enum HubAction {
    SendMessageToTap(HubToTapMessage),

    /// Check this hello against the tap registry, then feed back
    /// [`crate::event::HubEvent::TapAccepted`] or `TapRejected`.
    ValidateCredential(TapClientHello),

    StartTimer(TimerId, Duration),
    CancelTimer(TimerId),

    /// A request reached a terminal state; resolve whoever is waiting on it.
    CompleteRequest {
        request_id: RequestId,
        outcome: RequestOutcome,
    },

    /// Close the connection. The reason is for logging, not the wire — it is
    /// the data that shows whether this protocol actually disconnects less
    /// than the one it replaces, so it is worth recording precisely.
    Disconnect(DisconnectReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    /// A message arrived that the current state does not allow.
    ProtocolViolation,
    /// `ClientHello` never arrived.
    HandshakeTimeout,
    /// The tap stopped answering `Ping`.
    HeartbeatTimeout,
    /// Credentials rejected.
    Unauthorized,
    /// The tap speaks a version the hub cannot serve.
    UnsupportedVersion,
}

impl DisconnectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            DisconnectReason::ProtocolViolation => "protocol_violation",
            DisconnectReason::HandshakeTimeout => "handshake_timeout",
            DisconnectReason::HeartbeatTimeout => "heartbeat_timeout",
            DisconnectReason::Unauthorized => "unauthorized",
            DisconnectReason::UnsupportedVersion => "unsupported_version",
        }
    }
}
