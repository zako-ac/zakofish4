//! An HTTP liveness endpoint, for taps that run under an orchestrator.
//!
//! Deliberately says nothing about the hub connection: the SDK reconnects with
//! backoff, so a tap that is briefly disconnected is working as designed and
//! must not be restarted for it.

use axum::{Router, routing::get};

pub(crate) async fn serve(port: u16) {
    let app = Router::new().route("/health", get(|| async { "ok" }));
    match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(listener) => {
            tracing::info!(port, "healthcheck listening");
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(%e, "healthcheck server stopped");
            }
        }
        Err(e) => tracing::error!(%e, port, "failed to bind the healthcheck port"),
    }
}
