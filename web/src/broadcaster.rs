use tokio::net::{TcpSocket};
use std::sync::Arc;
use crate::multiplexer::Multiplexer;
use crate::config::AppConfig;
use tokio::sync::{Semaphore, broadcast};
use std::net::SocketAddr;

pub struct Broadcaster {
    multiplexer: Arc<Multiplexer>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl Broadcaster {
    pub fn new(m: Arc<Multiplexer>, shutdown_rx: broadcast::Receiver<()>) -> Self {
        Self { multiplexer: m, shutdown_rx }
    }

    pub async fn start(&self, config: AppConfig) {
        let interface = config.listen_interface.clone();

        for c in config.chains {
            let mpx = Arc::clone(&self.multiplexer);
            let mut shutdown_rx_clone = self.shutdown_rx.resubscribe();
            let prefix = c.prefix.clone();
            let addr_str = format!("{}:{}", interface, c.listen_port);
            let addr: SocketAddr = addr_str.parse().expect("Invalid address");

            let conn_limit = Arc::new(Semaphore::new(1024));

            tokio::spawn(async move {
                let socket = if addr.is_ipv4() {
                    TcpSocket::new_v4().unwrap()
                } else {
                    TcpSocket::new_v6().unwrap()
                };

                socket.set_reuseaddr(true).ok();
                #[cfg(unix)]
                socket.set_reuseport(true).ok();

                let listener = match socket.bind(addr) {
                    Ok(_) => match socket.listen(1024) {
                        Ok(l) => {
                            println!("[{}] Broadcaster online at {}", prefix, addr);
                            l
                        }
                        Err(e) => {
                            eprintln!("[{}] Listen error: {}", prefix, e);
                            return;
                        }
                    },
                    Err(e) => {
                        eprintln!("[{}] Bind error on {}: {}", prefix, addr, e);
                        return;
                    }
                };

                loop {
                    tokio::select! {
                        accept_res = listener.accept() => {
                            match accept_res {
                                Ok((socket, _remote_addr)) => {
                                    socket.set_nodelay(true).ok();

                                    let permit = match Arc::clone(&conn_limit).acquire_owned().await {
                                        Ok(p) => p,
                                        Err(_) => continue,
                                    };

                                    let mpx_clone = Arc::clone(&mpx);
                                    let prefix_clone = prefix.clone();

                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        mpx_clone.handle_connection(prefix_clone, socket).await;
                                    });
                                }
                                Err(e) => eprintln!("[{}] Accept error: {}", prefix, e),
                            }
                        }
                        _ = shutdown_rx_clone.recv() => {
                            println!("[{}] Received shutdown signal.", prefix);
                            break;
                        }
                    }
                }
                println!("[{}] Broadcaster shutdown complete.", prefix);
            });
        }
    }
}