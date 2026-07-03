mod config;
mod transport;
mod filter;

use std::collections::HashSet;
use tracing::{info, error};
use tracing_subscriber;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() {
    init_tracing();
    info!("=== IBC FILTER STARTING ===");

    let cfg = match config::load_config() {
        Ok(c) => c,
        Err(e) => {
            error!("Config error: {}", e);
            std::process::exit(1);
        }
    };

    let allowed: HashSet<String> = cfg.allowed_prefixes
        .into_iter()
        .map(|p| p.to_lowercase())
        .collect();

    let (input_buf, output_buf) = transport::init(
        vec![cfg.source_addr],
        cfg.listen_addr,
    ).await;

    tokio::spawn(filter::run(input_buf, output_buf, allowed));

    tokio::signal::ctrl_c().await.ok();
    info!("Filter shutdown");
}
