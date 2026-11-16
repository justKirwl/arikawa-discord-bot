//! HTTP listener: liveness/readiness probes now, WebSocket (`/ws`) in S3.3.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

#[derive(Debug, Clone)]
pub struct AppState {
    started_at: Instant,
    ready: Arc<AtomicBool>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Readiness flips to true once both listeners are bound.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

#[derive(Debug, Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
    uptime_ms: u64,
}

impl Health {
    fn new(state: &AppState, status: &'static str) -> Self {
        Self {
            status,
            version: env!("CARGO_PKG_VERSION"),
            uptime_ms: state.uptime().as_millis() as u64,
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz(State(state): State<AppState>) -> Json<Health> {
    Json(Health::new(&state, "ok"))
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.is_ready() {
        (StatusCode::OK, Json(Health::new(&state, "ok")))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health::new(&state, "starting")),
        )
    }
}

/// Serves the HTTP API on an already-bound listener.
pub async fn serve(
    listener: TcpListener,
    state: AppState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn healthz_is_ok_while_starting() {
        let response = router(AppState::new())
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_reports_starting_until_ready() {
        let state = AppState::new();

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("request should be handled");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        state.mark_ready();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("request should be handled");
        assert_eq!(response.status(), StatusCode::OK);
    }
}
