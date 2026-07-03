mod config;
mod transport;
mod watcher;

use tokio::signal;

#[tokio::main]
async fn main() {
    let cfg = config::load_config();

    let (_input_buf, output_buf) = transport::init(
        vec![cfg.source_addr.clone()],
        cfg.listen_addr.clone(),
    ).await;

    let watcher_handle = tokio::spawn(watcher::run(cfg.networks, output_buf));

    tokio::select! {
        _ = watcher_handle => {
            println!("[WATCHER] All watchers exited");
        }
        _ = signal::ctrl_c() => {
            println!("\n[SHUTDOWN] Stopping process...");
        }
    }
}
