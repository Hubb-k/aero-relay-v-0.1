use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, broadcast};
use h2::client;
use http::Request;
use crate::config::ChainConfig;
use rand::Rng;

pub type H2Request = (
    Request<()>,
    oneshot::Sender<Result<(h2::client::ResponseFuture, h2::SendStream<bytes::Bytes>), h2::Error>>
);

pub struct Connector {
    pub txs: HashMap<String, mpsc::Sender<H2Request>>,
}

impl Connector {
    pub fn new(configs: Vec<ChainConfig>, shutdown_rx: broadcast::Receiver<()>) -> Self {
        let mut txs = HashMap::new();

        for c in configs {
            let (tx, mut rx) = mpsc::channel::<H2Request>(2048);
            let target_addr = c.grpc_url.replace("http://", "").replace("https://", "");
            let prefix = c.prefix.clone();
            let mut shutdown_rx_clone = shutdown_rx.resubscribe();

            tokio::spawn(async move {
                let mut backoff_secs = 2u64;
                'outer: loop {
                    println!("[{}] Connecting to {}...", &prefix, &target_addr);

                    match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(&target_addr)).await {
                        Ok(Ok(stream)) => {
                            stream.set_nodelay(true).ok();

                            let mut builder = client::Builder::new();
                            builder.initial_window_size(8 * 1024 * 1024);
                            builder.initial_connection_window_size(8 * 1024 * 1024);
                            builder.enable_push(false);

                            match tokio::time::timeout(Duration::from_secs(10), builder.handshake(stream)).await {
                                Ok(Ok((h2_client, connection))) => {
                                    println!("[{}] H2 Handshake OK.", &prefix);

                                    let prefix_for_conn = prefix.clone();
                                    let mut conn_handle = tokio::spawn(async move {
                                        if let Err(err) = connection.await {
                                            eprintln!("[{}] Connection error: {:?}", prefix_for_conn, err);
                                        }
                                    });

                                    'inner: loop {
                                        tokio::select! {
                                            Some((req, res_tx)) = rx.recv() => {
                                                let h2_clone = h2_client.clone();
                                                let prefix_clone = prefix.clone();
                                                tokio::spawn(async move {
                                                    match h2_clone.ready().await {
                                                        Ok(mut ready_client) => {
                                                            let res = ready_client.send_request(req, false);
                                                            let _ = res_tx.send(res);
                                                        }
                                                        Err(e) => {
                                                            eprintln!("[{}] H2 client not ready: {:?}", prefix_clone, e);
                                                            let _ = res_tx.send(Err(e));
                                                        }
                                                    }
                                                });
                                            }
                                            _ = &mut conn_handle => {
                                                println!("[{}] Connection ended, reconnecting...", &prefix);
                                                break 'inner;
                                            }
                                            _ = shutdown_rx_clone.recv() => {
                                                break 'outer;
                                            }
                                        }
                                    }
                                    backoff_secs = 2;
                                }
                                _ => eprintln!("[{}] H2 Handshake failed", &prefix),
                            }
                        }
                        _ => eprintln!("[{}] Node unreachable", &prefix),
                    }

                    let jitter = rand::thread_rng().gen_range(0..backoff_secs);
                    tokio::time::sleep(Duration::from_secs(backoff_secs + jitter)).await;
                    backoff_secs = (backoff_secs * 2).min(30);

                    if shutdown_rx_clone.try_recv().is_ok() { break 'outer; }
                }
            });
            txs.insert(c.prefix, tx);
        }
        Self { txs }
    }
}
