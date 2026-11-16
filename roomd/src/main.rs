use roomd::config::Config;
use roomd::telemetry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `.env` is optional; real deployments use process environment variables.
    dotenvy::dotenv().ok();

    let config = Config::from_env()?;
    telemetry::init(&config)?;

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting roomd");
    roomd::run(config).await
}
