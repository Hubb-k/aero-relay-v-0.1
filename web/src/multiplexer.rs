use h2::server;
use tokio::net::TcpStream;
use std::sync::Arc;
use crate::connector::Connector;
use tokio::sync::oneshot;
use bytes::Bytes;
use http::Response;

pub struct Multiplexer {
    connector: Arc<Connector>,
}

impl Multiplexer {
    pub fn new(connector: Arc<Connector>) -> Self { Self { connector } }

    pub async fn handle_connection(&self, prefix: String, socket: TcpStream) {
        let connector = Arc::clone(&self.connector);
        tokio::spawn(async move {
            let mut builder = server::Builder::new();
            builder.initial_window_size(8 * 1024 * 1024);
            builder.initial_connection_window_size(8 * 1024 * 1024);

            if let Ok(mut h2_server) = builder.handshake(socket).await {
                while let Some(result) = h2_server.accept().await {
                    match result {
                        Ok((module_req, mut respond)) => {
                            let connector_clone = Arc::clone(&connector);
                            let prefix_inner = prefix.clone();

                            tokio::spawn(async move {
                                let (parts, mut module_recv_body) = module_req.into_parts();
                                let (res_tx, res_rx) = oneshot::channel();

                                if let Some(tx) = connector_clone.txs.get(&prefix_inner) {
                                    let mut node_req = http::Request::new(());
                                    *node_req.uri_mut() = parts.uri.clone();
                                    *node_req.method_mut() = parts.method.clone();
                                    *node_req.headers_mut() = parts.headers.clone();

                                    if tx.send((node_req, res_tx)).await.is_err() { return; }

                                    if let Ok(Ok((response_future, mut node_send_stream))) = res_rx.await {
                                        let pump_req_body = tokio::spawn(async move {
                                            while let Some(chunk) = module_recv_body.data().await {
                                                if let Ok(data) = chunk {
                                                    let len = data.len();
                                                    if node_send_stream.send_data(data, false).is_err() { break; }
                                                    let _ = module_recv_body.flow_control().release_capacity(len);
                                                }
                                            }
                                            if let Ok(Some(trailers)) = module_recv_body.trailers().await {
                                                let _ = node_send_stream.send_trailers(trailers);
                                            } else {
                                                let _ = node_send_stream.send_data(Bytes::new(), true);
                                            }
                                        });

                                        if let Ok(node_response) = response_future.await {
                                            let (n_parts, mut n_body) = node_response.into_parts();

                                            if let Ok(mut module_send_stream) = respond.send_response(Response::from_parts(n_parts, ()), false) {
                                                while let Some(chunk) = n_body.data().await {
                                                    if let Ok(data) = chunk {
                                                        let len = data.len();
                                                        if module_send_stream.send_data(data, false).is_err() { break; }
                                                        let _ = n_body.flow_control().release_capacity(len);
                                                    }
                                                }
                                                if let Ok(Some(trailers)) = n_body.trailers().await {
                                                    let _ = module_send_stream.send_trailers(trailers);
                                                } else {
                                                    let _ = module_send_stream.send_data(Bytes::new(), true);
                                                }
                                            }
                                        }
                                        let _ = pump_req_body.await;
                                    }
                                }
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        });
    }
}
