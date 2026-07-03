mod config;
mod transport;
mod batcher;

use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = config::load_config()?;

    let (input_buf, output_buf) = transport::init(
        cfg.source_addr.clone(), 
        cfg.listen_addr.clone()
    ).await;

    let batcher = batcher::Batcher::new(cfg, output_buf.clone());

    println!("[BATCHER] Running... Press Ctrl+C to stop.");

    tokio::select! {
        _ = batcher.run(input_buf) => {
            println!("[BATCHER] Logic stopped.");
        }
        _ = signal::ctrl_c() => {
            println!("[BATCHER] Shutdown signal received. Closing...");
        }
    }

    Ok(())
}