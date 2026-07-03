mod config;
mod transport;
mod updater;

use tracing::{info, error};
use tracing_subscriber;
use std::sync::Arc;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() {
    init_tracing();
    info!("=== IBC UPDATER STARTING ===");

    let cfg = match config::load_config() {
        Ok(c) => c,
        Err(e) => {
            error!("Config error: {}", e);
            std::process::exit(1);
        }
    };

    let (input_buf, output_buf) = transport::init(
        vec![cfg.source_addr.clone()],
        cfg.listen_addr.clone(),
    ).await;

    let updater: Arc<updater::Updater> = updater::Updater::new(cfg).await;

    tokio::spawn(async move {
        updater.run(input_buf, output_buf).await;
    });

    tokio::signal::ctrl_c().await.ok();
    info!("Updater shutdown");
}
