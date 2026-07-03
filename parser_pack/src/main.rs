mod config;
mod transport;
mod parser;

use anyhow::Result;
use tracing::{info, error};
use tracing_subscriber;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("parser=info".parse().unwrap())
                .add_directive("error".parse().unwrap()),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!("=== aero-parser starting ===");

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

    tokio::spawn(parser::run(input_buf, output_buf));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("[SHUTDOWN] Parser stopped");
        }
    }

    Ok(())
}
