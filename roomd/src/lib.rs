//! roomd — realtime playback rooms for the cinema bot.
//!
//! The service owns room state, synchronization and WebSocket fan-out. It is
//! deliberately unaware of Discord: callers pass opaque `guild_id`,
//! `channel_id`, `user_id` and `media_id` values.

pub mod auth;
pub mod backpressure;
pub mod chat;
pub mod config;
pub mod events;
pub mod grpc;
pub mod http;
pub mod hub;
pub mod idle;
pub mod media_end;
pub mod participants;
pub mod pb;
pub mod ratelimit;
pub mod shutdown;
pub mod state;
pub mod telemetry;
pub mod ws;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::auth::JwtAuth;
use crate::config::Config;
use crate::http::AppState;
use crate::hub::RoomHub;
use crate::state::RoomStore;

/// How long in-flight connections get to finish after a shutdown signal.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// Binds both listeners, serves gRPC and HTTP, and drains on shutdown.
pub async fn run(config: Config) -> anyhow::Result<()> {
    let store = Arc::new(RoomStore::default());
    let hub = Arc::new(RoomHub::new());
    let jwt = match config.jwt_public_key.as_deref() {
        Some(public_key) => Some(Arc::new(
            JwtAuth::new(
                public_key,
                config.jwt_issuer.clone(),
                config.jwt_key_id.clone(),
            )
            .context("invalid ROOMD_JWT_PUBLIC_KEY")?,
        )),
        None => {
            warn!("ROOMD_JWT_PUBLIC_KEY is not set; /ws will reject every connection");
            None
        }
    };
    let state = AppState::with_services(store.clone(), jwt, hub.clone());

    let grpc_listener = TcpListener::bind(config.grpc_addr)
        .await
        .with_context(|| format!("failed to bind gRPC listener on {}", config.grpc_addr))?;
    let http_listener = TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("failed to bind HTTP listener on {}", config.http_addr))?;

    info!(
        grpc_addr = %grpc_listener.local_addr()?,
        http_addr = %http_listener.local_addr()?,
        "listeners bound"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    state.mark_ready();

    let idle_task = idle::spawn_idle_sweeper(
        store.clone(),
        hub.clone(),
        config.room_idle_timeout,
        shutdown_rx.clone(),
    );

    let mut grpc_task = tokio::spawn(grpc::serve(
        grpc_listener,
        store,
        hub,
        shutdown::wait(shutdown_rx.clone()),
    ));
    let mut http_task = tokio::spawn(http::serve(
        http_listener,
        state,
        shutdown::wait(shutdown_rx.clone()),
    ));

    tokio::select! {
        () = shutdown::signal() => info!("shutdown signal received, draining connections"),
        result = &mut grpc_task => anyhow::bail!("gRPC server stopped unexpectedly: {result:?}"),
        result = &mut http_task => anyhow::bail!("HTTP server stopped unexpectedly: {result:?}"),
    }

    let _ = shutdown_tx.send(true);

    let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
        let _ = grpc_task.await;
        let _ = http_task.await;
        let _ = idle_task.await;
    })
    .await;

    if drained.is_err() {
        warn!(
            grace_secs = SHUTDOWN_GRACE.as_secs(),
            "graceful shutdown timed out"
        );
    }

    info!("roomd stopped");
    Ok(())
}
