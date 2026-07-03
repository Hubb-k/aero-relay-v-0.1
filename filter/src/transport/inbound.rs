use tokio::net::TcpStream;
use tokio::io::AsyncBufReadExt;
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use crate::transport::BufferTask;

pub async fn start_inbound(sources: Vec<String>, buffer: Arc<BufferTask>) {
    for addr in sources {
        let buf_clone = buffer.clone();
        tokio::spawn(async move {
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
                                if !line.is_empty() {
                                    let bytes = if line.ends_with('\n') {
                                        line.as_bytes().to_vec()
                                    } else {
                                        format!("{}
", line).into_bytes()
                                    };
                                    buf_clone.push(&bytes);
                                }
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
