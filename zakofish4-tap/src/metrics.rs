//! Request-path instrumentation, and the state `/health` decides on.
//!
//! Two readers share this state, which is why it lives beside the transport
//! rather than in the tap binary:
//!
//! - a `/metrics` scrape, which wants counters and timestamps, and
//! - `/health`, which wants the one thing a scrape can only show after the
//!   fact: *is a request being served right now, and has anything moved since
//!   it arrived?*
//!
//! The series names are the protofish3 SDK's, unchanged, so the dashboards and
//! queries written against a pf3 tap keep reading the same numbers from a pf4
//! one: `tap_requests_received_total{kind}`, `tap_requests_completed_total`,
//! `tap_in_flight`, `tap_last_request_at_ms`, `tap_last_reply_at_ms`.
//!
//! Three pf3 series describe things this transport does not have and are
//! deliberately not carried over: `tap_chan_open_total`,
//! `tap_chan_closed_total`, `tap_accept_chan_errors_total` and
//! `tap_handshake_total` all counted channels on one QUIC connection. The pf4
//! transport has one WebSocket per tap and one request id per transfer, so
//! `tap_connections_total` (WebSocket connections the hub accepted, including
//! reconnects) is the event those counters were standing in for.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The `kind` label a metadata request carries.
pub const REQUEST_KIND_META: &str = "meta";
/// The `kind` label an audio request carries.
pub const REQUEST_KIND_AUDIO: &str = "audio";

/// Which half of the request path a request arrived on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    /// `AudioMetadataRequest` — the cheap lookup every message makes first.
    Meta,
    /// `AudioRequest` — the one that streams.
    Audio,
}

impl RequestKind {
    /// The value this kind carries in the `kind` label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Meta => REQUEST_KIND_META,
            Self::Audio => REQUEST_KIND_AUDIO,
        }
    }
}

/// Wall-clock milliseconds since the unix epoch.
pub fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Everything `/health` needs, read in one go so the values agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TapSnapshot {
    /// Requests being served right now.
    pub in_flight: u64,
    /// When a request was last taken off the wire.
    pub last_request_at_ms: u64,
    /// When a response (or an audio frame) was last written to the sink.
    pub last_reply_at_ms: u64,
    /// The later of the two — the last moment the tap demonstrably did
    /// something about a request.
    pub last_activity_at_ms: u64,
    /// WebSocket connections the hub has accepted since the process started.
    pub connections: u64,
}

/// Request-path state for one tap process.
pub struct TapMetrics {
    requests_meta: AtomicU64,
    requests_audio: AtomicU64,
    completed: AtomicU64,
    in_flight: AtomicU64,
    last_request_at_ms: AtomicU64,
    last_reply_at_ms: AtomicU64,
    last_activity_at_ms: AtomicU64,
    connections: AtomicU64,
    deadline_exceeded: AtomicU64,
}

impl TapMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            requests_meta: AtomicU64::new(0),
            requests_audio: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            last_request_at_ms: AtomicU64::new(0),
            last_reply_at_ms: AtomicU64::new(0),
            last_activity_at_ms: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            deadline_exceeded: AtomicU64::new(0),
        })
    }

    /// Requests being served right now. The same number a drain waits on.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// The values `/health` decides on.
    pub fn snapshot(&self) -> TapSnapshot {
        TapSnapshot {
            in_flight: self.in_flight.load(Ordering::SeqCst),
            last_request_at_ms: self.last_request_at_ms.load(Ordering::SeqCst),
            last_reply_at_ms: self.last_reply_at_ms.load(Ordering::SeqCst),
            last_activity_at_ms: self.last_activity_at_ms.load(Ordering::SeqCst),
            connections: self.connections.load(Ordering::SeqCst),
        }
    }

    /// Counts a request in and hands back the guard that counts it out.
    ///
    /// The guard is dropped however the request ends — answered, refused,
    /// failed, or panicked with `catch_unwind` nowhere in sight — because a
    /// drop is the only exit every path shares.
    pub fn request_received(self: &Arc<Self>, kind: RequestKind) -> RequestGuard {
        let now = unix_millis();
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        match kind {
            RequestKind::Meta => self.requests_meta.fetch_add(1, Ordering::SeqCst),
            RequestKind::Audio => self.requests_audio.fetch_add(1, Ordering::SeqCst),
        };
        self.last_request_at_ms.store(now, Ordering::SeqCst);
        self.last_activity_at_ms.store(now, Ordering::SeqCst);
        RequestGuard {
            metrics: Arc::clone(self),
        }
    }

    /// Notes that a response — or one audio frame — went out.
    ///
    /// Called per frame as well as per response: a request that has been
    /// streaming for a minute is making progress, and `/health` must not call
    /// that a stall.
    pub fn reply_sent(&self) {
        let now = unix_millis();
        self.last_reply_at_ms.store(now, Ordering::SeqCst);
        self.last_activity_at_ms.store(now, Ordering::SeqCst);
    }

    /// The hub accepted a WebSocket from us.
    pub fn connection_accepted(&self) {
        self.connections.fetch_add(1, Ordering::SeqCst);
    }

    /// A request sat in flight with nothing moving past the deadline, so the
    /// process is about to end itself.
    ///
    /// Incremented immediately before the exit. A scrape only sees it if one
    /// lands in that window, which is why the log line is the durable record and
    /// this counter is the dashboard's — see [`crate::watchdog`].
    pub fn request_deadline_exceeded(&self) {
        self.deadline_exceeded.fetch_add(1, Ordering::SeqCst);
    }

    /// How many times this process ended itself on a request deadline.
    pub fn deadline_exceeded(&self) -> u64 {
        self.deadline_exceeded.load(Ordering::SeqCst)
    }

    /// Render the Prometheus text exposition format.
    ///
    /// Hand-written rather than through an OpenTelemetry exporter: this crate
    /// has no exporter in its dependency tree, and the taps are scraped by a
    /// `PodMonitor` on the same port `/health` answers on, so what has to be
    /// right is the text, not the SDK.
    pub fn render(&self) -> String {
        let s = self.snapshot();
        let mut out = String::with_capacity(1024);
        out.push_str("# HELP tap_requests_received_total Requests taken off the wire, by kind\n");
        out.push_str("# TYPE tap_requests_received_total counter\n");
        out.push_str(&format!(
            "tap_requests_received_total{{kind=\"{}\"}} {}\n",
            REQUEST_KIND_META,
            self.requests_meta.load(Ordering::SeqCst)
        ));
        out.push_str(&format!(
            "tap_requests_received_total{{kind=\"{}\"}} {}\n",
            REQUEST_KIND_AUDIO,
            self.requests_audio.load(Ordering::SeqCst)
        ));
        out.push_str(
            "# HELP tap_requests_completed_total Requests that finished, however they ended\n",
        );
        out.push_str("# TYPE tap_requests_completed_total counter\n");
        out.push_str(&format!(
            "tap_requests_completed_total {}\n",
            self.completed.load(Ordering::SeqCst)
        ));
        out.push_str("# HELP tap_in_flight Requests being served right now\n");
        out.push_str("# TYPE tap_in_flight gauge\n");
        out.push_str(&format!("tap_in_flight {}\n", s.in_flight));
        out.push_str("# HELP tap_last_request_at_ms Unix ms when a request was last received\n");
        out.push_str("# TYPE tap_last_request_at_ms gauge\n");
        out.push_str(&format!(
            "tap_last_request_at_ms {}\n",
            s.last_request_at_ms
        ));
        out.push_str(
            "# HELP tap_last_reply_at_ms Unix ms when a response or audio frame was last written\n",
        );
        out.push_str("# TYPE tap_last_reply_at_ms gauge\n");
        out.push_str(&format!("tap_last_reply_at_ms {}\n", s.last_reply_at_ms));
        out.push_str("# HELP tap_connections_total WebSocket connections the hub accepted, including reconnects\n");
        out.push_str("# TYPE tap_connections_total counter\n");
        out.push_str(&format!("tap_connections_total {}\n", s.connections));
        out.push_str(
            "# HELP tap_request_deadline_exceeded_total Requests that stopped making progress past the deadline and ended the process\n",
        );
        out.push_str("# TYPE tap_request_deadline_exceeded_total counter\n");
        out.push_str(&format!(
            "tap_request_deadline_exceeded_total {}\n",
            self.deadline_exceeded()
        ));
        out
    }
}

/// Holds one request in the in-flight count for as long as it is alive.
pub struct RequestGuard {
    metrics: Arc<TapMetrics>,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.metrics.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.metrics.completed.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
impl TapMetrics {
    /// Pretend nothing has been written for the last `by_ms` milliseconds.
    ///
    /// Lets `/health`'s stall branch be tested without sleeping for the
    /// threshold, which would make the test slower than the thing it checks.
    pub(crate) fn backdate_activity(&self, by_ms: u64) {
        let then = unix_millis().saturating_sub(by_ms);
        self.last_activity_at_ms.store(then, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_idle_tap_has_nothing_in_flight_and_no_timestamps() {
        let metrics = TapMetrics::new();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(snapshot.last_request_at_ms, 0);
        assert_eq!(snapshot.last_reply_at_ms, 0);
        assert_eq!(snapshot.connections, 0);
    }

    /// The count has to come back down however the request ends, or the drain
    /// waits for a request that is long gone and `/health` reports a stall that
    /// is not happening.
    #[test]
    fn the_guard_puts_the_in_flight_count_back_where_it_found_it() {
        let metrics = TapMetrics::new();

        let first = metrics.request_received(RequestKind::Meta);
        assert_eq!(metrics.in_flight(), 1);
        let second = metrics.request_received(RequestKind::Audio);
        assert_eq!(metrics.in_flight(), 2);

        drop(first);
        assert_eq!(metrics.in_flight(), 1);
        drop(second);
        assert_eq!(metrics.in_flight(), 0);
    }

    #[test]
    fn a_received_request_stamps_the_request_and_activity_time() {
        let metrics = TapMetrics::new();
        let before = unix_millis();
        let guard = metrics.request_received(RequestKind::Meta);

        let snapshot = metrics.snapshot();
        assert!(snapshot.last_request_at_ms >= before);
        assert_eq!(snapshot.last_activity_at_ms, snapshot.last_request_at_ms);
        assert_eq!(snapshot.last_reply_at_ms, 0);

        metrics.reply_sent();
        let snapshot = metrics.snapshot();
        assert!(snapshot.last_reply_at_ms >= snapshot.last_request_at_ms);
        assert_eq!(snapshot.last_activity_at_ms, snapshot.last_reply_at_ms);

        drop(guard);
    }

    #[test]
    fn request_kinds_carry_the_label_values_the_dashboards_use() {
        assert_eq!(RequestKind::Meta.as_str(), "meta");
        assert_eq!(RequestKind::Audio.as_str(), "audio");
    }

    /// The names on `/metrics` are a contract with dashboards written against
    /// the pf3 SDK, so they are asserted rather than left to a reviewer's eye.
    #[test]
    fn the_scrape_carries_every_series_the_dashboards_read() {
        let metrics = TapMetrics::new();
        let _guard = metrics.request_received(RequestKind::Audio);
        metrics.reply_sent();
        metrics.connection_accepted();

        let text = metrics.render();
        for expected in [
            "tap_requests_received_total{kind=\"meta\"} 0",
            "tap_requests_received_total{kind=\"audio\"} 1",
            "tap_requests_completed_total 0",
            "tap_in_flight 1",
            "tap_connections_total 1",
            "tap_last_reply_at_ms ",
        ] {
            assert!(text.contains(expected), "missing {expected:?} in {text}");
        }
    }
}
