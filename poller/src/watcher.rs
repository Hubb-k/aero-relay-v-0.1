use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::json;
use tokio::time::{sleep, Duration};
use std::sync::Arc;

use crate::transport::BufferTask;
use crate::config::Network;

pub async fn run(networks: Vec<Network>, output_buf: Arc<BufferTask>) {
    let mut handles = vec![];

    for net in networks {
        let name = net.name.clone();
        let ws_url = net.ws_url.clone();
        let buf = output_buf.clone();

        let handle = tokio::spawn(async move {
            loop {
                let ws_stream = match connect_async(&ws_url).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        eprintln!("[{}] Connect error: {}. Retrying in 5s...", name, e);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                let (mut sender, mut receiver) = ws_stream.split();

                let sub = json!({
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "id": 1,
                    "params": ["tm.event='Tx'"]
                }).to_string();

                if let Err(e) = sender.send(Message::Text(sub)).await {
                    eprintln!("[{}] Subscribe error: {}", name, e);
                    continue;
                }

                println!("[{}] Subscribed to {}", name, ws_url);

                while let Some(msg) = receiver.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if text.contains("send_packet")
                                || text.contains("write_acknowledgement")
                                || text.contains("incentivized_packet")
                            {
                                let line = format!("{} {} \n\n", name, text);
                                buf.push(line.as_bytes());
                            }
                        }
                        Ok(Message::Close(_)) => {
                            eprintln!("[{}] Connection closed by server", name);
                            break;
                        },
                        Err(e) => {
                            eprintln!("[{}] Stream error: {}", name, e);
                            break;
                        },
                        _ => {}
                    }
                }

                eprintln!("[{}] Connection lost. Reconnecting in 5s...", name);
                sleep(Duration::from_secs(5)).await;
            }
        });

        handles.push(handle);
    }

    futures_util::future::join_all(handles).await;
}
