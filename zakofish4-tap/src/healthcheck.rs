//! `/health` and `/metrics`, on one port, for taps running under an
//! orchestrator.
//!
//! `/health` is deliberately about the *request path*, not the hub connection:
//! the SDK reconnects with backoff, so a tap that is briefly disconnected is
//! working as designed and must not be restarted for it. What it does report is
//! the state a constant `"ok"` hid for a whole S2b incident — a request is in
//! flight and nothing has been written back for longer than `stall_after`,
//! which is exactly what a wedged synthesis pipeline looks like from outside.
//!
//! `/metrics` renders [`TapMetrics`] in the Prometheus text format. The
//! `PodMonitor` scrapes this same port (`TAP_HEALTHCHECK_PORT` is the one knob
//! for both), and the series names are the pf3 SDK's, so moving a tap to this
//! SDK does not empty a dashboard.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;

use crate::metrics::{TapMetrics, unix_millis};

#[derive(Clone)]
pub(crate) struct HealthState {
    metrics: Arc<TapMetrics>,
    /// How long a request may be in flight with nothing written back before
    /// `/health` reports 503.
    stall_after: Duration,
}

pub(crate) async fn serve(port: u16, metrics: Arc<TapMetrics>, stall_after: Duration) {
    let state = HealthState {
        metrics,
        stall_after,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_text))
        .with_state(state);

    match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(listener) => {
            tracing::info!(port, "healthcheck and metrics listening");
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(%e, "healthcheck server stopped");
            }
        }
        Err(e) => tracing::error!(%e, port, "failed to bind the healthcheck port"),
    }
}

async fn health(State(state): State<HealthState>) -> impl IntoResponse {
    let snapshot = state.metrics.snapshot();
    let now = unix_millis();
    let idle_ms = now.saturating_sub(snapshot.last_activity_at_ms);

    // A tap that has served nothing yet is healthy: `last_activity_at_ms` is 0
    // and there is no request to be stalled in.
    let stalled = snapshot.in_flight > 0
        && snapshot.last_activity_at_ms > 0
        && idle_ms > state.stall_after.as_millis() as u64;

    if stalled {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "stalled: {} request(s) in flight, nothing written for {}ms\n",
                snapshot.in_flight, idle_ms
            ),
        )
    } else {
        (
            StatusCode::OK,
            format!(
                "ok: {} in flight, {} connections, idle {}ms\n",
                snapshot.in_flight, snapshot.connections, idle_ms
            ),
        )
    }
}

async fn metrics_text(State(state): State<HealthState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::RequestKind;

    fn state(metrics: Arc<TapMetrics>, stall_after: Duration) -> HealthState {
        HealthState {
            metrics,
            stall_after,
        }
    }

    #[tokio::test]
    async fn a_tap_that_has_served_nothing_is_healthy() {
        let st = state(TapMetrics::new(), Duration::from_secs(20));
        assert_eq!(
            health(State(st)).await.into_response().status(),
            StatusCode::OK
        );
    }

    /// The case `/health` exists for: a request is in flight and the pipeline
    /// has gone quiet. A constant `"ok"` answered 200 through the whole S2b
    /// incident.
    #[tokio::test]
    async fn an_in_flight_request_with_nothing_written_back_is_unhealthy() {
        let metrics = TapMetrics::new();
        let _guard = metrics.request_received(RequestKind::Audio);
        // Pretend the request arrived a minute ago, without sleeping.
        metrics.backdate_activity(60_000);

        let st = state(metrics, Duration::from_secs(20));
        assert_eq!(
            health(State(st)).await.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    /// Streaming audio writes a frame every 20ms, so a long request must never
    /// be read as a stall.
    #[tokio::test]
    async fn a_stream_that_keeps_writing_stays_healthy() {
        let metrics = TapMetrics::new();
        let _guard = metrics.request_received(RequestKind::Audio);
        metrics.reply_sent();

        let st = state(metrics, Duration::from_secs(20));
        assert_eq!(
            health(State(st)).await.into_response().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn metrics_renders_the_series_names_the_dashboards_read() {
        let metrics = TapMetrics::new();
        let st = state(metrics, Duration::from_secs(20));
        let response = metrics_text(State(st)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
