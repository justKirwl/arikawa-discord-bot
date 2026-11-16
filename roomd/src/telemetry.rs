//! Tracing setup. Logs go to stdout, either human-readable or JSON.

use anyhow::Context;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::{Config, LogFormat};

pub fn init(config: &Config) -> anyhow::Result<()> {
    // `RUST_LOG` wins over the service-specific default.
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.log_filter))
        .with_context(|| format!("invalid log filter: {:?}", config.log_filter))?;

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);

    match config.log_format {
        LogFormat::Pretty => tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init(),
        LogFormat::Json => tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer.json())
            .try_init(),
    }
    .context("failed to install the tracing subscriber")
}
