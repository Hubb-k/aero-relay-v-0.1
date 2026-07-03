use tokio::net::TcpStream;
use tokio::io::AsyncBufReadExt;
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use crate::transport::BufferTask;
use chrono::Local;

pub async fn start_inbound(sources: Vec<String>, buffer: Arc<BufferTask>) {
    for addr in sources {
        let buf_clone = buffer.clone();
        tokio::spawn(async move {
            let port = addr.split(':').last().unwrap_or("0000").to_string();

            loop {
                if let Ok(stream) = TcpStream::connect(&addr).await {
                    let _ = stream.set_nodelay(true);
                    let mut reader = tokio::io::BufReader::new(stream);
                    let mut line = String::new();

                    println!("[INBOUND] Connected to {}", addr);

                    loop {
                        line.clear();
                        match reader.read_line(&mut line).await {
                            Ok(0) => break,
                            Err(_) => break,
                            Ok(_) => {
                                let now = Local::now().format("%H:%M:%S%.3f");
                                let full_line = format!("{}, {}, {}", port, now, line);
                                let bytes = if full_line.ends_with('\n') {
                                    full_line.into_bytes()
                                } else {
                                    format!("{}\n", full_line).into_bytes()
                                };
                                buf_clone.push(&bytes);
                            }
                        }
                    }
                    println!("[INBOUND] Disconnected from {}. Retrying...", addr);
                }
                sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
