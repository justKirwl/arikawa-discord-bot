//! Shutdown signalling shared by both listeners.

use tokio::sync::watch;

/// Resolves once the process receives Ctrl+C or SIGTERM.
pub async fn signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(handle) => handle,
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
                return std::future::pending().await;
            }
        };

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "failed to listen for Ctrl+C");
                }
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for Ctrl+C");
        }
    }
}

/// Resolves once the shutdown watch flips to `true`.
pub async fn wait(mut receiver: watch::Receiver<bool>) {
    if *receiver.borrow() {
        return;
    }

    while receiver.changed().await.is_ok() {
        if *receiver.borrow() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wait_returns_immediately_when_already_shutting_down() {
        let (_sender, receiver) = watch::channel(true);

        tokio::time::timeout(std::time::Duration::from_secs(1), wait(receiver))
            .await
            .expect("wait must not block when shutdown is already requested");
    }

    #[tokio::test]
    async fn wait_returns_after_the_flag_flips() {
        let (sender, receiver) = watch::channel(false);

        let waiter = tokio::spawn(wait(receiver));
        sender.send(true).expect("receiver is alive");

        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("wait must resolve after shutdown")
            .expect("wait task must not panic");
    }
}
