use tokio::net::TcpListener;
use tokio::io::AsyncWriteExt;
use std::sync::Arc;
use crate::transport::BufferTask;

pub async fn run_outbound_worker(listen: String, buffer: Arc<BufferTask>) {
    let listener = TcpListener::bind(&listen).await.expect("Bind failed");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let buf_clone = Arc::clone(&buffer);

            tokio::spawn(async move {
                let _ = stream.set_nodelay(true);
                let mut rp = buf_clone.get_current_head();

                loop {
                    let (data, next_rp) = buf_clone.pull_from(rp);

                    if !data.is_empty() {
                        if stream.write_all(&data).await.is_err() {
                            break;
                        }
                        rp = next_rp;
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            });
        }
    }
}
