use std::sync::Arc;
use std::time::Duration;

use zakofish4_common::model::TapId;

use crate::error::SdkError;
use crate::handler::TapHandler;
use crate::runtime;

/// Entry point: `tap().hub(..).tap_id(..).api_token(..).run(handler)`.
pub fn tap() -> TapBuilder {
    TapBuilder::default()
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

    /// Connect and serve until the hub rejects this tap or the process ends.
    /// Reconnection with backoff is handled internally.
    pub async fn run(self, handler: Arc<dyn TapHandler>) -> Result<(), SdkError> {
        let cfg = runtime::Config {
            hub_url: self.hub_url.ok_or(SdkError::Config("hub url is required"))?,
            tap_id: TapId(self.tap_id.ok_or(SdkError::Config("tap_id is required"))?),
            friendly_name: self.friendly_name.unwrap_or_default(),
            api_token: self
                .api_token
                .ok_or(SdkError::Config("api_token is required"))?,
            selection_weight: self.selection_weight,
            pacing_lead: self.pacing_lead,
            reconnect_min: self.reconnect_min,
            reconnect_max: self.reconnect_max,
        };
        runtime::run(cfg, handler).await
    }
}
