use crate::config::{Config, Chain};
use crate::transport::BufferTask;
use std::{sync::Arc, collections::HashMap, str::FromStr, time::{Instant, Duration}};
use tokio::{sync::mpsc, time::sleep};
use serde_json::{Value, json};
use ibc_proto::cosmos::auth::v1beta1::{query_client::QueryClient, QueryAccountRequest, BaseAccount};
use ibc_proto::ibc::core::channel::v1::{MsgRecvPacket, MsgAcknowledgement};
use ibc_proto::ibc::core::client::v1::MsgUpdateClient;
use cosmrs::crypto::secp256k1::SigningKey;
use cosmrs::tx::{Body, Fee, SignerInfo, AuthInfo, SignDoc};
use cosmrs::{Any, Coin, Denom};
use prost::Message;
use base64::{Engine as _, engine::general_purpose};
use bip32::{Mnemonic, XPrv, Language};

fn find_value(v: &Value, key: &str) -> Option<Value> {
    if let Some(val) = v.get(key) {
        if !val.is_null() { return Some(val.clone()); }
    }
    if let Some(obj) = v.as_object() {
        for (_, child) in obj {
            if let Some(found) = find_value(child, key) { return Some(found); }
        }
    } else if let Some(array) = v.as_array() {
        for child in array {
            if let Some(found) = find_value(child, key) { return Some(found); }
        }
    }
    None
}

pub struct Batcher {
    workers: HashMap<String, mpsc::UnboundedSender<Value>>,
}

impl Batcher {
    pub fn new(cfg: Config, output: Arc<BufferTask>) -> Self {
        let mut workers = HashMap::new();
        let shared_cfg = Arc::new(cfg);
        for c in &shared_cfg.chains {
            let (tx, rx) = mpsc::unbounded_channel();
            let c_cfg = c.clone();
            let mnem = shared_cfg.relayer_mnemonic.clone();
            let memo = shared_cfg.relayer_memo.clone();
            let out_clone = output.clone();
            tokio::spawn(async move { run_worker(c_cfg, mnem, memo, rx, out_clone).await; });
            workers.insert(c.prefix.clone(), tx);
        }
        Self { workers }
    }

    pub async fn run(&self, input: Arc<BufferTask>) {
        let mut rp = input.get_current_head();
        loop {
            let (raw_data, next_rp) = input.pull_from(rp);
            rp = next_rp;
            if raw_data.is_empty() { sleep(Duration::from_millis(15)).await; continue; }
            for line in raw_data.split(|&b| b == b'\n') {
                if line.is_empty() { continue; }
                if let Ok(ev) = serde_json::from_slice::<Value>(line) {
                    let etype = ev["type"].as_str().unwrap_or("");
                    let target = if etype == "Ack" {
                        find_value(&ev, "src_chain").or(find_value(&ev, "origin_chain")).and_then(|v| v.as_str().map(|s| s.to_string()))
                    } else {
                        find_value(&ev, "dst_chain").and_then(|v| v.as_str().map(|s| s.to_string()))
                    };
                    if let Some(dst) = target {
                        if let Some(w) = self.workers.get(&dst) { let _ = w.send(ev); }
                    }
                }
            }
        }
    }
}

async fn run_worker(cfg: Chain, mnem_str: String, memo: String, mut rx: mpsc::UnboundedReceiver<Value>, _output: Arc<BufferTask>) {
    let mnemonic = Mnemonic::new(&mnem_str, Language::English).unwrap();
    let seed = mnemonic.to_seed("");
    let xprv = XPrv::derive_from_path(&seed, &"m/44'/118'/0'/0/0".parse().unwrap()).unwrap();
    let signer = Arc::new(SigningKey::from_slice(&xprv.private_key().to_bytes()).unwrap());
    let account_id_str = signer.public_key().account_id(&cfg.prefix).unwrap().to_string();
    let mut processed_cache: HashMap<String, Instant> = HashMap::new();
    let http_client = reqwest::Client::new();
    let forced_rev = cfg.chain_id.split('-').last().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);

    loop {
        let res: Result<(), anyhow::Error> = async {
            let mut au_c = QueryClient::connect(cfg.grpc_url.clone()).await?;
            let a_res = au_c.account(QueryAccountRequest { address: account_id_str.clone() }).await?.into_inner();
            let acc = BaseAccount::decode(a_res.account.ok_or(anyhow::anyhow!("Account not found"))?.value.as_slice())?;
            // Neutron-specific fix: account.sequence is sometimes incorrectly reported as 0
            let mut seq = if cfg.chain_id.contains("neutron") && acc.sequence == 0 { 1 } else { acc.sequence };
            let acc_num = acc.account_number;
            println!("[ONLINE] [{}] Seq: {}", cfg.chain_id, seq);

            let mut pending_batches: HashMap<String, (Option<String>, Option<String>, Value, Instant)> = HashMap::new();

            loop {
                while let Ok(ev) = rx.try_recv() {
                    let etype = ev["type"].as_str().unwrap_or("Unknown");
                    let src_id = find_value(&ev, "src_chain").and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_default();
                    let dst_id = find_value(&ev, "dst_chain").and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_default();
                    let target_client_key = if etype == "Ack" { format!("client_{}", src_id) } else { format!("client_{}", dst_id) };
                    let client_id = find_value(&ev, &target_client_key).and_then(|v| v.as_str().map(|s| s.to_string())).unwrap_or_default();
                    if client_id.is_empty() { continue; }
                    let seq_val = find_value(&ev, "sequence").and_then(|v| v.as_u64()).unwrap_or(0);

                    let batch_key = format!("{}:{}:{}", client_id, etype, seq_val);

                    if let Some(t) = processed_cache.get(&batch_key) {
                        if t.elapsed() < Duration::from_secs(86400) { continue; }
                    }

                    let raw_h = find_value(&ev, "client_update_header").and_then(|v| v.as_str().map(|s| s.to_string()));
                    let is_follow = raw_h.as_deref() == Some("FOLLOW_LEADER");

                    if let Some(h) = &raw_h {
                        if !is_follow {
                            if let Some(entry) = pending_batches.get_mut(&batch_key) {
                                if entry.0.is_none() {
                                    entry.0 = Some(h.clone());
                                    println!("[MERGE_UPDATE] [{}] Header merged for {}", cfg.chain_id, batch_key);
                                    let (h_opt, m_val, meta, _) = pending_batches.get(&batch_key).unwrap();
                                    let mut msgs = Vec::new();

                                    if let Some(h_str) = h_opt {
                                        if let Ok(h_bytes) = general_purpose::STANDARD.decode(h_str) {
                                            msgs.push(Any {
                                                type_url: "/ibc.core.client.v1.MsgUpdateClient".to_string(),
                                                value: MsgUpdateClient {
                                                    client_id: client_id.clone(),
                                                    client_message: Some(ibc_proto::google::protobuf::Any {
                                                        type_url: "/ibc.lightclients.tendermint.v1.Header".into(),
                                                        value: h_bytes
                                                    }),
                                                    signer: account_id_str.clone(),
                                                }.encode_to_vec(),
                                            });
                                        }
                                    }

                                    if let Some(m_str) = m_val {
                                        if let Some(am) = decode_msg(&meta, m_str, &account_id_str, forced_rev) {
                                            msgs.push(am);
                                        }
                                    }

                                    if !msgs.is_empty() {
                                        println!("[RESTART_SEND] [{}] seq {} type {} after merge", cfg.chain_id, seq_val, etype);
                                        send_via_rest(&http_client, &cfg, msgs, &signer, &mut seq, acc_num, &memo).await;
                                        processed_cache.insert(batch_key.clone(), Instant::now());
                                    }
                                }
                            } else {
                                pending_batches.insert(batch_key.clone(), (Some(h.clone()), None, ev.clone(), Instant::now()));
                            }
                            continue;
                        }
                    }

                    if let Some(m) = find_value(&ev, "msg_bytes").and_then(|v| v.as_str().map(|s| s.to_string())) {
                        let entry = pending_batches.entry(batch_key.clone()).or_insert((None, None, ev.clone(), Instant::now()));
                        entry.1 = Some(m);
                        entry.2 = ev.clone();

                        let ready = (entry.0.is_some() && entry.1.is_some()) || (is_follow && entry.1.is_some());

                        if ready {
                            let (h_opt, m_val, meta, _) = pending_batches.remove(&batch_key).unwrap();
                            let mut msgs = Vec::new();

                            if let Some(h_str) = h_opt {
                                if let Ok(h_bytes) = general_purpose::STANDARD.decode(h_str) {
                                    msgs.push(Any {
                                        type_url: "/ibc.core.client.v1.MsgUpdateClient".to_string(),
                                        value: MsgUpdateClient {
                                            client_id: client_id.clone(),
                                            client_message: Some(ibc_proto::google::protobuf::Any {
                                                type_url: "/ibc.lightclients.tendermint.v1.Header".into(),
                                                value: h_bytes
                                            }),
                                            signer: account_id_str.clone(),
                                        }.encode_to_vec(),
                                    });
                                }
                            }

                            if let Some(m_str) = m_val {
                                if let Some(am) = decode_msg(&meta, &m_str, &account_id_str, forced_rev) {
                                    msgs.push(am);
                                }
                            }

                            if !msgs.is_empty() {
                                println!("[DELAY] [{}] Waiting before relay seq {} type {}", cfg.chain_id, seq_val, etype);
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                send_via_rest(&http_client, &cfg, msgs, &signer, &mut seq, acc_num, &memo).await;
                                processed_cache.insert(batch_key, Instant::now());
                            }
                        }
                    }
                }

                pending_batches.retain(|_, entry| entry.3.elapsed() < Duration::from_secs(20));
                processed_cache.retain(|_, v| v.elapsed() < Duration::from_secs(86400));
                sleep(Duration::from_millis(15)).await;
            }
        }.await;
        if let Err(e) = res { println!("[ERR] [{}] {}", cfg.chain_id, e); sleep(Duration::from_secs(5)).await; }
    }
}

fn decode_msg(meta: &Value, m_val: &String, signer: &String, forced_rev: u64) -> Option<Any> {
    let m_bytes = general_purpose::STANDARD.decode(m_val).ok()?;
    match meta["type"].as_str().unwrap_or("") {
        "Send" => {
            let mut m = MsgRecvPacket::decode(m_bytes.as_slice()).ok()?;
            m.signer = signer.clone();
            if let Some(ref mut h) = m.proof_height { if h.revision_number == 0 { h.revision_number = forced_rev; } }
            Any::from_msg(&m).ok()
        },
        "Ack" => {
            let mut m = MsgAcknowledgement::decode(m_bytes.as_slice()).ok()?;
            m.signer = signer.clone();
            if let Some(ref mut h) = m.proof_height { if h.revision_number == 0 { h.revision_number = forced_rev; } }
            Any::from_msg(&m).ok()
        },
        _ => None,
    }
}

async fn send_via_rest(client: &reqwest::Client, cfg: &Chain, msgs: Vec<Any>, signer: &SigningKey, seq: &mut u64, acc_num: u64, memo: &str) {
    let mut gas_limit = 0u64;

    {
        let sim_body = Body::new(msgs.clone(), memo, 0u32);
        let sim_auth = AuthInfo {
            signer_infos: vec![SignerInfo::single_direct(Some(signer.public_key()), *seq)],
            fee: Fee::from_amount_and_gas(Coin { amount: 0u128, denom: Denom::from_str(&cfg.denom).unwrap() }, 0u64),
        };
        let sim_sign_doc = SignDoc::new(&sim_body, &sim_auth, &cfg.chain_id.parse().unwrap(), acc_num).unwrap();
        let sim_tx_bytes = sim_sign_doc.sign(signer).unwrap().to_bytes().unwrap();
        let sim_tx_b64 = general_purpose::STANDARD.encode(sim_tx_bytes);

        let sim_resp = client.post(&format!("{}/cosmos/tx/v1beta1/simulate", cfg.lcd_url))
            .json(&json!({ "tx_bytes": sim_tx_b64 }))
            .send().await;

        match sim_resp {
            Ok(resp) => {
                let res_json: Value = resp.json().await.unwrap_or(json!({}));
                let code = res_json["code"].as_u64().unwrap_or(0);

                if let Some(gas_used) = res_json["gas_info"]["gas_used"].as_str().and_then(|s| s.parse::<u64>().ok()) {
                    gas_limit = (gas_used as f64 * 1.1) as u64 + 999;
                    println!("[SIM_OK] [{}] Used: {} -> Limit: {}", cfg.chain_id, gas_used, gas_limit);
                } else if code == 2 {
                    let msg = res_json["message"].as_str().unwrap_or("");
                    if let Some(pos) = msg.find("gas used: '") {
                        let start = pos + 11;
                        if let Some(end) = msg[start..].find('\'') {
                            if let Ok(gas_used) = msg[start..start + end].parse::<u64>() {
                                gas_limit = (gas_used as f64 * 1.1) as u64 + 999;
                                println!("[SIM_CODE2] [{}] Code 2, parsed gasUsed: {} -> Limit: {}", cfg.chain_id, gas_used, gas_limit);
                            }
                        }
                    }
                    if gas_limit == 0 {
                        println!("[SIM_SKIP] [{}] Code 2, no gasUsed in response: {}", cfg.chain_id, res_json);
                        return;
                    }
                } else {
                    println!("[SIM_SKIP] [{}] Simulation failed: {}", cfg.chain_id, res_json);
                    return;
                }
            },
            Err(e) => {
                println!("[SIM_ERR] [{}] {}", cfg.chain_id, e);
                return;
            }
        }
    }

    if gas_limit == 0 { return; }

    let mut amount = (gas_limit as f64 * cfg.gas_price).ceil() as u128;
    if amount == 0 && cfg.gas_price > 0.0 { amount = 1; }
    let body = Body::new(msgs, memo, 0u32);

    for attempt in 1..=20 {
        let fee = Fee::from_amount_and_gas(
            Coin { amount, denom: Denom::from_str(&cfg.denom).unwrap() },
            gas_limit,
        );

        let auth = AuthInfo {
            signer_infos: vec![SignerInfo::single_direct(Some(signer.public_key()), *seq)],
            fee,
        };
        let sign_doc = SignDoc::new(&body, &auth, &cfg.chain_id.parse().unwrap(), acc_num).unwrap();
        let tx_bytes = sign_doc.sign(signer).unwrap().to_bytes().unwrap();
        let tx_b64 = general_purpose::STANDARD.encode(tx_bytes);

        match client.post(&format!("{}/cosmos/tx/v1beta1/txs", cfg.lcd_url))
            .json(&json!({"tx_bytes": tx_b64, "mode": "BROADCAST_MODE_SYNC"}))
            .send().await
        {
            Ok(resp) => {
                let res: Value = resp.json().await.unwrap_or(json!({}));
                let tx_res = &res["tx_response"];
                let b_code = tx_res["code"].as_u64().unwrap_or(0);
                let hash = tx_res["txhash"].as_str().unwrap_or("NOT_FOUND");

                if b_code == 0 && hash != "NOT_FOUND" {
                    println!("[SENT_OK] [{}] Hash: {} | Seq: {} | Fee: {} {}", cfg.chain_id, hash, *seq, amount, cfg.denom);
                    *seq += 1;
                    return;
                }

                if b_code == 22 {
                    println!("[BROADCAST_SKIP] [{}] Code 22 (redundant)", cfg.chain_id);
                    return;
                }

                if b_code == 11 {
                    let raw_log = tx_res["raw_log"].as_str().unwrap_or("");
                    if let Some(pos) = raw_log.find("gasUsed: ") {
                        let sub = &raw_log[pos + 9..];
                        if let Some(end) = sub.find(|c: char| !c.is_numeric()) {
                            if let Ok(gas_used) = sub[..end].parse::<u64>() {
                                gas_limit = (gas_used as f64 * 1.1).ceil() as u64 + 500;
                                amount = (gas_limit as f64 * cfg.gas_price).ceil() as u128;
                                if amount == 0 && cfg.gas_price > 0.0 { amount = 1; }
                                println!("[GAS_ADJUST] [{}] Out of gas. New limit: {} (used: {})", cfg.chain_id, gas_limit, gas_used);
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        }
                    }
                }

                let raw_log = tx_res["raw_log"].as_str().unwrap_or("");
                if b_code == 32 || raw_log.contains("sequence mismatch") {
                    if let Some(pos) = raw_log.find("expected ") {
                        let sub = &raw_log[pos+9..];
                        let end = sub.find(|c: char| !c.is_numeric()).unwrap_or(sub.len());
                        if let Ok(real) = sub[..end].parse::<u64>() {
                            println!("[SYNC] [{}] Seq: {} -> {}", cfg.chain_id, *seq, real);
                            *seq = real;
                            continue;
                        }
                    }
                }

                println!("[BROADCAST_RETRY] [{}] Attempt {}/20 | Code: {} | {}", cfg.chain_id, attempt, b_code, raw_log);
                if attempt < 20 {
                    tokio::time::sleep(Duration::from_millis(334)).await;
                    continue;
                }
            },
            Err(e) => {
                println!("[HTTP_ERR] [{}] {}", cfg.chain_id, e);
                if attempt < 20 {
                    tokio::time::sleep(Duration::from_millis(339)).await;
                    continue;
                }
            }
        }
    }

    println!("[BROADCAST_FAIL] [{}] All 20 broadcast attempts failed", cfg.chain_id);
}
