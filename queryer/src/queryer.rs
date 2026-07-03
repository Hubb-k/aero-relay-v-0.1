use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use tendermint_rpc::{Client, HttpClient};

use ibc_proto::ibc::core::channel::v1::{
    Packet, MsgRecvPacket, MsgAcknowledgement,
};
use ibc_proto::ibc::core::commitment::v1::MerkleProof;
use ibc_proto::ics23::CommitmentProof;
use ibc_proto::ibc::core::client::v1::Height as IbcHeight;
use prost::Message;
use hex;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::transport::BufferTask;

pub struct Queryer {
    clients: HashMap<String, HttpClient>,
    revisions: HashMap<String, u64>,
}

impl Queryer {
    pub async fn new(cfg: Config) -> Arc<Self> {
        let mut clients = HashMap::new();
        let mut revisions = HashMap::new();
        for chain in cfg.chains {
            if let Ok(client) = HttpClient::new(chain.rpc_url.as_str()) {
                clients.insert(chain.chain_id.clone(), client);
                
                // Берем ревизию из конфига, если там пусто или мусор — ставим 1
                let rev = chain.revision.parse::<u64>().unwrap_or(1);
                revisions.insert(chain.chain_id.clone(), rev);
                println!("[QUERYER] Registered {} with revision {}", chain.chain_id, rev);
            }
        }
        Arc::new(Self { clients, revisions })
    }

    pub async fn run(self: Arc<Self>, input: Arc<BufferTask>, output: Arc<BufferTask>) {
        let mut workers = HashMap::new();

        for chain_id in self.clients.keys() {
            let (tx, mut rx) = mpsc::channel::<Value>(1000);
            let queryer = Arc::clone(&self);
            let out = Arc::clone(&output);
            let cid = chain_id.clone();

            tokio::spawn(async move {
                println!("[QUERYER] Worker for {} started", cid);
                while let Some(mut event) = rx.recv().await {
                    let ev_type = event["type"].as_str().unwrap_or("").to_string();
                    if ev_type != "Send" && ev_type != "Ack" { continue; }

                    let query_chain = event["payload"]["packet"]["origin_chain"]
                        .as_str()
                        .or(event["payload"]["origin_chain"].as_str())
                        .unwrap_or("")
                        .to_string();

                    if query_chain.is_empty() {
                        queryer.send_to_output(&out, &event);
                        continue;
                    }

                    let event_height = event["payload"]["proof_height"].as_u64()
                        .or(event["proof_height"].as_u64())
                        .unwrap_or(0);

                    let enriched = queryer.enrich_any(&mut event, &query_chain, &ev_type, event_height).await;

                    if let Some(payload) = event.get_mut("payload").and_then(|p| p.as_object_mut()) {
                        if enriched {
                            payload.insert("enriched".to_string(), json!(true));
                        } else {
                            payload.insert("enrichment_error".to_string(), json!("proof_not_found"));
                        }
                    }
                    queryer.send_to_output(&out, &event);
                }
            });
            workers.insert(chain_id.clone(), tx);
        }

        let mut rp = input.get_current_head();
        loop {
            let (chunk, next_rp) = input.pull_from(rp);
            rp = next_rp;

            if chunk.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let text = String::from_utf8_lossy(&chunk);
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }

                if let Ok(event) = serde_json::from_str::<Value>(trimmed) {
                    let src_chain = event["payload"]["packet"]["origin_chain"]
                        .as_str()
                        .or(event["payload"]["origin_chain"].as_str())
                        .unwrap_or("")
                        .to_string();

                    if let Some(worker_tx) = workers.get(&src_chain) {
                        let _ = worker_tx.send(event).await;
                    } else {
                        let _ = output.push(format!("{}\n", event).as_bytes());
                    }
                }
            }
        }
    }

    async fn enrich_any(&self, event: &mut Value, query_chain: &str, ev_type: &str, event_height: u64) -> bool {
        let payload = match event.get_mut("payload") {
            Some(p) => p,
            None => return false,
        };
        
        let p_info = if payload["packet"].is_object() { &payload["packet"] } else { &*payload };

        // ИСПРАВЛЕНО: Достаем ревизию из конфига по ID сети
        let rev_number = *self.revisions.get(query_chain).unwrap_or(&1);

        let seq = p_info["sequence"].as_u64().unwrap_or(0);
        
        let (port_query, chan_query) = if ev_type == "Send" {
            (p_info["source_port"].as_str().unwrap_or(""), p_info["source_channel"].as_str().unwrap_or(""))
        } else {
            (p_info["destination_port"].as_str().unwrap_or(""), p_info["destination_channel"].as_str().unwrap_or(""))
        };

        if seq == 0 || port_query.is_empty() || chan_query.is_empty() { return false; }

        let ack_bytes = if let Some(ack_hex) = p_info["acknowledgement"].as_str() {
            hex::decode(ack_hex.trim_start_matches("0x")).unwrap_or_default()
        } else {
            payload["acknowledgement"]
                .as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_u64()).map(|v| v as u8).collect::<Vec<u8>>())
                .unwrap_or_default()
        };

        let max_retries = 20;
        let retry_interval = Duration::from_millis(300);

        for attempt in 1..=max_retries {
            let result = match ev_type {
                "Send" => self.query_ibc_proof(query_chain, "commitments", port_query, chan_query, seq, rev_number, event_height, true).await,
                "Ack" => self.query_ibc_proof(query_chain, "acks", port_query, chan_query, seq, rev_number, event_height, false).await,
                _ => None,
            };

            if let Some((proof, height)) = result {
                let mut msg_bytes = Vec::new();
                let success = match ev_type {
                    "Send" => {
                        let msg = MsgRecvPacket {
                            packet: Some(self.map_json_to_packet(p_info)),
                            proof_commitment: proof,
                            proof_height: Some(height.clone()),
                            signer: String::new(),
                        };
                        msg.encode(&mut msg_bytes).is_ok()
                    },
                    "Ack" => {
                        let msg = MsgAcknowledgement {
                            packet: Some(self.map_json_to_packet(p_info)),
                            acknowledgement: ack_bytes.clone(),
                            proof_acked: proof,
                            proof_height: Some(height.clone()),
                            signer: String::new(),
                        };
                        msg.encode(&mut msg_bytes).is_ok()
                    },
                    _ => false,
                };

                if success {
                    if let Some(obj) = payload.as_object_mut() {
                        obj.insert("msg_bytes".to_string(), json!(STANDARD.encode(&msg_bytes)));
                        obj.insert("proof_height".to_string(), height_to_json(Some(height)));
                    }
                    return true;
                }
            }

            if attempt < max_retries {
                tokio::time::sleep(retry_interval).await;
            }
        }
        false
    }

    async fn query_ibc_proof(&self, chain: &str, store: &str, port: &str, chan: &str, seq: u64, rev: u64, event_height: u64, is_send: bool) -> Option<(Vec<u8>, IbcHeight)> {
        let client = self.clients.get(chain)?;
        
        
        let key_path = if is_send {
            format!("{}/ports/{}/channels/{}/sequences/{}", store, port, chan, seq)
        } else {
            format!("{}/ports/{}/channels/{}/acknowledgements/{}", store, port, chan, seq)
};
        
        let key = key_path.into_bytes();
        let abci_path = "/store/ibc/key".to_string();

        let q_height = if event_height > 0 {
            Some(tendermint::block::Height::try_from(event_height).ok()?)
        } else {
            None
        };

        let resp = client.abci_query(Some(abci_path), key, q_height, true).await.ok()?;
        
        // Для Ack допускаем пустое value (зависит от версии IBC), для Send оставляем проверку
        if resp.code.is_err() || resp.proof.is_none() || (is_send && resp.value.is_empty()) { return None; }

        let height = IbcHeight {
            revision_number: rev,
            revision_height: u64::from(resp.height) + 1,
        };

        let mut proofs = Vec::new();
        if let Some(proof_ops) = resp.proof {
            for op in proof_ops.ops {
                if let Ok(cp) = CommitmentProof::decode(op.data.as_slice()) {
                    proofs.push(cp);
                }
            }
        }

        if proofs.is_empty() { return None; }

        let merkle_proof = MerkleProof { proofs };
        let mut proof_bytes = Vec::new();
        merkle_proof.encode(&mut proof_bytes).ok()?;

        Some((proof_bytes, height))
    }

    fn map_json_to_packet(&self, p: &Value) -> Packet {
        Packet {
            sequence: p["sequence"].as_u64().unwrap_or(0),
            source_port: p["source_port"].as_str().unwrap_or_default().to_string(),
            source_channel: p["source_channel"].as_str().unwrap_or_default().to_string(),
            destination_port: p["destination_port"].as_str().unwrap_or_default().to_string(),
            destination_channel: p["destination_channel"].as_str().unwrap_or_default().to_string(),
            data: hex::decode(p["data_hex"].as_str().unwrap_or("").trim_start_matches("0x")).unwrap_or_default(),
            timeout_height: p["timeout_height"].as_object().and_then(|h| {
                let h_val = h["revision_height"].as_u64().unwrap_or(0);
                if h_val == 0 { None } else {
                    Some(IbcHeight {
                        revision_number: h["revision_number"].as_u64().unwrap_or(0),
                        revision_height: h_val,
                    })
                }
            }),
            timeout_timestamp: p["timeout_timestamp"].as_u64().unwrap_or(0),
        }
    }

    fn send_to_output(&self, out: &Arc<BufferTask>, event: &Value) {
        let mut line = event.to_string();
        line.push('\n');
        out.push(line.as_bytes());
    }
}

fn height_to_json(height: Option<IbcHeight>) -> Value {
    height.map_or(json!({}), |h| json!({ 
        "revision_number": h.revision_number, 
        "revision_height": h.revision_height 
    }))
}