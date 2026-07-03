mod config;
mod connector;
mod broadcaster;
mod multiplexer;

use tokio::signal;
use tokio::sync::broadcast;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let config = config::load_config();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let connector = Arc::new(connector::Connector::new(
        config.chains.clone(),
        shutdown_tx.subscribe(),
    ));
    
    let multiplexer = Arc::new(multiplexer::Multiplexer::new(connector));
    
    let broadcaster = broadcaster::Broadcaster::new(
        multiplexer,
        shutdown_tx.subscribe(),
    );
    
    let config_clone = config.clone();
    tokio::spawn(async move {
        broadcaster.start(config_clone).await;
    });

    println!("🚀 Aero-Web Transport Hub is running...");

    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("Failed to setup SIGTERM handler");

    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Received CTRL-C, shutting down...");
        }
        _ = sigterm.recv() => {
            println!("Received SIGTERM, shutting down...");
        }
    }

    let _ = shutdown_tx.send(());

    // Graceful shutdown period
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    println!("Shutdown complete.");
}