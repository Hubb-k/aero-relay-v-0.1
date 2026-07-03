mod config;
mod transport;
mod logger;

use tokio::signal;

#[tokio::main]
async fn main() {
    let cfg = config::load_config();

    let (input_buf, output_buf) = transport::init(cfg.source_addr, cfg.listen_addr).await;

    tokio::select! {
        _ = logger::run(input_buf, output_buf) => {},
        _ = signal::ctrl_c() => {
            println!("\n[SHUTDOWN] Stopping process...");
        }
    }
}
