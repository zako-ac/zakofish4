use std::sync::Arc;
use std::time::Duration;

use zakofish4_common::model::TapId;

use crate::error::SdkError;
use crate::handler::TapHandler;
use crate::metrics::TapMetrics;
use crate::runtime;

/// Entry point: `tap().hub(..).tap_id(..).api_token(..).run(handler)`.
pub fn tap() -> TapBuilder {
    TapBuilder::default()
}

/// How long a draining tap may keep serving the requests it has already
/// accepted before it exits, unless the builder overrides it.
///
/// Read from `ZAKO_DRAIN_TIMEOUT_SECS` because that is the name the taps
/// already carry in their shared ConfigMap, so the value in git stays the one
/// knob. It must stay comfortably under the pods'
/// `terminationGracePeriodSeconds`, or the kubelet kills the tap mid-drain.
fn default_drain_timeout() -> Duration {
    std::env::var("ZAKO_DRAIN_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(20))
}

/// How long a request may be in flight with nothing written back before
/// `/health` answers 503, unless the builder overrides it.
///
/// Read from `TAP_HEALTH_STALL_SECS`, the pf3 SDK's name for the same knob, so
/// a deployment that already sets it keeps the threshold it had.
fn default_health_stall() -> Duration {
    std::env::var("TAP_HEALTH_STALL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(20))
}

pub struct TapBuilder {
    hub_url: Option<String>,
    tap_id: Option<String>,
    friendly_name: Option<String>,
    api_token: Option<String>,
    selection_weight: f32,
    pacing_lead: Duration,
    reconnect_min: Duration,
    reconnect_max: Duration,
    idle_timeout: Duration,
    /// How long a drain may wait for accepted transfers to finish.
    drain_timeout: Duration,
    /// How long a request may sit in flight with nothing written back before
    /// `/health` reports 503.
    health_stall: Duration,
    #[cfg(feature = "healthcheck")]
    healthcheck_port: Option<u16>,
}

impl Default for TapBuilder {
    fn default() -> Self {
        Self {
            hub_url: None,
            tap_id: None,
            friendly_name: None,
            api_token: None,
            selection_weight: 1.0,
            // Enough audio buffered ahead that playback starts without waiting,
            // while still holding the tap near realtime afterwards.
            pacing_lead: Duration::from_secs(4),
            reconnect_min: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(30),
            // Four times the hub's default `heartbeat_interval` of 15s: long
            // enough that a slow-but-alive hub is never mistaken for a dead
            // one, short enough that a black-holed connection is noticed in a
            // minute rather than never.
            idle_timeout: Duration::from_secs(60),
            drain_timeout: default_drain_timeout(),
            health_stall: default_health_stall(),
            #[cfg(feature = "healthcheck")]
            healthcheck_port: None,
        }
    }
}

impl TapBuilder {
    /// Gateway URL, e.g. `wss://api.zako.ac/gateway`.
    ///
    /// A WebSocket over TLS on the ordinary HTTPS port, which is the point:
    /// it survives the NATs, proxies and firewalls that a long-lived QUIC
    /// connection did not.
    pub fn hub(mut self, url: impl Into<String>) -> Self {
        self.hub_url = Some(url.into());
        self
    }

    pub fn tap_id(mut self, id: impl Into<String>) -> Self {
        self.tap_id = Some(id.into());
        self
    }

    pub fn friendly_name(mut self, name: impl Into<String>) -> Self {
        self.friendly_name = Some(name.into());
        self
    }

    pub fn api_token(mut self, token: impl Into<String>) -> Self {
        self.api_token = Some(token.into());
        self
    }

    pub fn selection_weight(mut self, weight: f32) -> Self {
        self.selection_weight = weight;
        self
    }

    /// How far ahead of realtime this tap may run.
    ///
    /// Raise it for a faster start at the cost of a burstier upload; lower it
    /// on a constrained uplink. Zero sends strictly in realtime.
    pub fn pacing_lead(mut self, lead: Duration) -> Self {
        self.pacing_lead = lead;
        self
    }

    /// How long the connection may go without a frame from the hub before the
    /// tap treats it as gone and reconnects.
    ///
    /// The hub pings, so silence is a liveness signal in itself: a tap whose
    /// path to the hub is black-holed keeps a socket that still looks open and
    /// would otherwise never reconnect. Keep this well above the hub's
    /// `heartbeat_interval`.
    pub fn idle_timeout(mut self, idle: Duration) -> Self {
        self.idle_timeout = idle;
        self
    }

    /// How long a SIGTERM drain may keep serving the transfers already
    /// accepted before the process exits.
    ///
    /// Keep this under the pod's `terminationGracePeriodSeconds`. The default
    /// comes from `ZAKO_DRAIN_TIMEOUT_SECS`.
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// Serve `GET /health` and `GET /metrics` on this port, for an
    /// orchestrator's probes and a `PodMonitor`'s scrape.
    ///
    /// `/health` reports the request path, not the hub connection: the SDK
    /// reconnects with backoff, and restarting a tap that is briefly
    /// disconnected would only make the outage longer. It answers 503 when a
    /// request has been in flight for longer than `health_stall_after` with
    /// nothing written back.
    #[cfg(feature = "healthcheck")]
    pub fn healthcheck_port(mut self, port: u16) -> Self {
        self.healthcheck_port = Some(port);
        self
    }

    /// How long a request may sit in flight with nothing written back before
    /// `/health` answers 503. Default from `TAP_HEALTH_STALL_SECS` (20s).
    pub fn health_stall_after(mut self, stall: Duration) -> Self {
        self.health_stall = stall;
        self
    }

    /// Connect and serve until the hub rejects this tap, a drain signal
    /// arrives, or the process ends. Reconnection with backoff is handled
    /// internally.
    pub async fn run(self, handler: Arc<dyn TapHandler>) -> Result<(), SdkError> {
        let metrics = TapMetrics::new();
        let cfg = runtime::Config {
            hub_url: self
                .hub_url
                .ok_or(SdkError::Config("hub url is required"))?,
            tap_id: TapId(self.tap_id.ok_or(SdkError::Config("tap_id is required"))?),
            friendly_name: self.friendly_name.unwrap_or_default(),
            api_token: self
                .api_token
                .ok_or(SdkError::Config("api_token is required"))?,
            selection_weight: self.selection_weight,
            pacing_lead: self.pacing_lead,
            reconnect_min: self.reconnect_min,
            reconnect_max: self.reconnect_max,
            idle_timeout: self.idle_timeout,
            drain_timeout: self.drain_timeout,
            metrics: Arc::clone(&metrics),
        };

        #[cfg(feature = "healthcheck")]
        if let Some(port) = self.healthcheck_port {
            tokio::spawn(crate::healthcheck::serve(
                port,
                Arc::clone(&metrics),
                self.health_stall,
            ));
        }
        // Without the `healthcheck` feature nothing serves the state, but the
        // runtime still counts into it; keeping the binding explicit leaves one
        // place to look for the value.
        #[cfg(not(feature = "healthcheck"))]
        let _ = (&metrics, self.health_stall);

        runtime::run(cfg, handler).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two knobs come off the environment under the names the taps already
    /// carry in `taps-common-config`, so a deployment does not have to change
    /// to keep its old threshold. One test rather than two because both touch
    /// process-global variables, and two parallel tests would race on them.
    #[test]
    fn the_drain_and_stall_knobs_read_the_names_the_deployments_use() {
        // SAFETY: `set_var` is unsafe in edition 2024 because a concurrent
        // reader would race it; the asserts below are the only reads and they
        // are on this thread.
        unsafe {
            std::env::set_var("ZAKO_DRAIN_TIMEOUT_SECS", "7");
            std::env::set_var("TAP_HEALTH_STALL_SECS", "11");
        }
        let b = tap();
        assert_eq!(b.drain_timeout, Duration::from_secs(7));
        assert_eq!(b.health_stall, Duration::from_secs(11));

        unsafe {
            std::env::remove_var("ZAKO_DRAIN_TIMEOUT_SECS");
            std::env::remove_var("TAP_HEALTH_STALL_SECS");
        }
        let b = tap();
        assert_eq!(b.drain_timeout, Duration::from_secs(20));
        assert_eq!(b.health_stall, Duration::from_secs(20));
    }
}
