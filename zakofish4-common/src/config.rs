use std::time::Duration;

/// Timings the state machine schedules against.
///
/// It holds these rather than the adapter so that "when does a request expire"
/// has exactly one answer, checkable without a socket.
#[derive(Debug, Clone)]
pub struct HubConfig {
    /// How long a freshly connected tap has to send `ClientHello` before the
    /// connection is dropped. Without this, a client that opens a socket and
    /// says nothing holds a task and a connection slot indefinitely.
    pub handshake_timeout: Duration,

    /// Gap between `Ping`s.
    pub heartbeat_interval: Duration,

    /// How long a `Pong` may take before the connection is considered dead.
    /// This is the check that makes a black-holed TCP connection visible.
    pub heartbeat_deadline: Duration,

    /// Highest protocol version this hub serves.
    pub protocol_version: u32,

    /// How long a tap has to answer a probe before it counts as unanswerable.
    ///
    /// Generous compared to the heartbeat: the tap is synthesizing a phrase,
    /// which on a cold cache is real work, and the check is periodic rather
    /// than on the request path. Marking a healthy tap unhealthy for being
    /// briefly busy costs every subsequent request.
    pub probe_timeout: Duration,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(15),
            heartbeat_deadline: Duration::from_secs(10),
            protocol_version: crate::messages::PROTOCOL_VERSION,
            probe_timeout: Duration::from_secs(20),
        }
    }
}
