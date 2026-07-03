use crate::config::Config;
use crate::transport::BufferTask;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use anyhow::{Context, Result};
use tendermint_rpc::{HttpClient, Client};
use ibc_proto::ibc::core::client::v1::{QueryClientStateResponse, QueryClientStateRequest, MsgUpdateClient, Height as IbcHeight};
use ibc_proto::ibc::lightclients::tendermint::v1::{Header as TmHeader, ClientState as TmClientState};
use ibc_proto::google::protobuf::Any as IbcAny;
use tendermint::validator::Set as TmValidatorSet;
use prost::Message;
use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;

pub struct Updater {
    clients: HashMap<String, HttpClient>,
    state_clients: HashMap<String, HttpClient>,
    revisions: HashMap<String, u64>,
    pending: Arc<tokio::sync::Mutex<HashMap<(u64, String), Vec<tokio::sync::oneshot::Sender<String>>>>>,
}

impl Updater {
    pub async fn new(cfg: Config) -> Arc<Self> {
        let mut clients = HashMap::new();
        let mut state_clients = HashMap::new();
        let mut revisions = HashMap::new();
        for c in &cfg.chains {
            if let Ok(client) = HttpClient::new(c.url.as_str()) {
                clients.insert(c.chain_id.clone(), client);
            }
            if let Ok(s_client) = HttpClient::new(c.state_url.as_str()) {
                state_clients.insert(c.chain_id.clone(), s_client);
            }
            let rev = c.revision.parse::<u64>().unwrap_or(0);
            revisions.insert(c.chain_id.clone(), rev);
        }
        Arc::new(Self { 
            clients, 
            state_clients, 
            revisions, 
            pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())) 
        })
    }

    pub async fn run(self: Arc<Self>, input: Arc<BufferTask>, output: Arc<BufferTask>) {
        let mut rp = input.get_current_head();
        println!("[UPDATER] Listening for events...");

        loop {
            let (chunk, next_rp) = input.pull_from(rp);
            rp = next_rp;
            
            if chunk.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let text = String::from_utf8_lossy(&chunk);
            for line in text.lines() {
                let line = line.trim().to_string();
                if line.is_empty() { continue; }

                let up = Arc::clone(&self);
                let out = Arc::clone(&output);

                tokio::spawn(async move {
                    let v: Value = match serde_json::from_str(&line) {
                        Ok(val) => val,
                        Err(e) => {
                            eprintln!("[UPDATER-JSON-ERR] Could not parse line: {}", e);
                            return;
                        }
                    };

                    let event_type = find_val(&v, "type");
                    let src_id = find_val(&v, "src_chain");
                    let dst_id = find_val(&v, "dst_chain");

                    let (header_donor, state_owner, client_key) = if event_type == "Ack" {
                        (dst_id.clone(), src_id.clone(), format!("client_{}", src_id))
                    } else {
                        (src_id.clone(), dst_id.clone(), format!("client_{}", dst_id))
                    };

                    let client_id = find_val(&v, &client_key);
                    if header_donor.is_empty() || state_owner.is_empty() || client_id.is_empty() {
                        return;
                    }

                    let event_h: u64 = find_val(&v, "proof_height").parse().unwrap_or(0);
                    let key = (event_h, client_id.clone());

                    let mut lock = up.pending.lock().await;
                    if let Some(waiters) = lock.get_mut(&key) {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        waiters.push(tx);
                        drop(lock);

                        // Ждем пока лидер получит хедер и раздаст метку
                        if let Ok(header_b64) = rx.await {
                            let _ = up.finalize_and_send(v, header_b64, out);
                        }
                    } else {
                        lock.insert(key.clone(), Vec::new());
                        drop(lock);

                        match up.get_header_only(&v, &header_donor, &state_owner, &client_id).await {
                            Ok(header_b64) => {
                                let mut lock = up.pending.lock().await;
                                if let Some(waiters) = lock.remove(&key) {
                                    for tx in waiters {
                                        // Рассылаем метку остальным, чтобы они слали месседжи атомарно
                                        let _ = tx.send("FOLLOW_LEADER".to_string());
                                    }
                                }
                                let _ = up.finalize_and_send(v, header_b64, out);
                            },
                            Err(e) => {
                                up.pending.lock().await.remove(&key);
                                eprintln!("[UPDATER-FATAL] {} -> {}: {}", header_donor, state_owner, e);
                            }
                        }
                    }
                });
            }
        }
    }

    async fn get_header_only(&self, v_val: &Value, src_id: &String, dst_id: &String, client_id: &String) -> Result<String> {
        let src_rpc = self.clients.get(src_id).context("No src RPC")?.clone();
        let dst_state_rpc = self.state_clients.get(dst_id).context("No dst state RPC")?.clone();
        let src_revision = *self.revisions.get(src_id).context("No src revision")?;
        
        let event_h: u64 = find_val(v_val, "proof_height").parse().unwrap_or(0);
        let required_h = event_h + 1;

        let req = QueryClientStateRequest { client_id: client_id.clone() };
        let query_resp = dst_state_rpc.abci_query(
            Some("/ibc.core.client.v1.Query/ClientState".to_string()), 
            req.encode_to_vec(), 
            None, 
            false
        ).await?;

        let client_resp = QueryClientStateResponse::decode(query_resp.value.as_slice())?;
        let tm_client_state = TmClientState::decode(client_resp.client_state.context("No state")?.value.as_slice())?;
        let trusted_height = tm_client_state.latest_height.context("No trusted height")?;

        let mut attempts = 20;
        let mut latest_src_height = 0;
        while attempts > 0 {
            let status = src_rpc.status().await?;
            latest_src_height = status.sync_info.latest_block_height.value();
            if latest_src_height >= required_h {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            attempts -= 1;
        }

        if latest_src_height < required_h {
            return Err(anyhow::anyhow!("Height {} still below required {}", latest_src_height, required_h));
        }

        let up_h = IbcHeight { 
            revision_number: src_revision, 
            revision_height: latest_src_height 
        };

        let any_msg = build_update_client_any(&src_rpc, client_id, &up_h, &trusted_height).await?;
        let msg_update_client = MsgUpdateClient::decode(any_msg.value.as_slice())?;
        let header_any = msg_update_client.client_message.context("No header")?;
        Ok(general_purpose::STANDARD.encode(header_any.value))
    }

    fn finalize_and_send(&self, mut v: Value, header_b64: String, output: Arc<BufferTask>) -> Result<()> {
        v["client_update_header"] = Value::String(header_b64);
        let mut res_bytes = serde_json::to_vec(&v)?;
        res_bytes.push(b'\n');
        output.push(&res_bytes);
        Ok(())
    }
}

async fn build_update_client_any(rpc: &HttpClient, cid: &str, up_h: &IbcHeight, tr_h: &IbcHeight) -> Result<IbcAny> {
    let hp = tendermint::block::Height::try_from(up_h.revision_height)?;
    let ht = tendermint::block::Height::try_from(tr_h.revision_height)?;
    
    let commit = rpc.commit(hp).await?;
    let v_resp = rpc.validators(hp, tendermint_rpc::Paging::All).await?;
    let tv_resp = rpc.validators(ht, tendermint_rpc::Paging::All).await?;
    
    let mk_val = |v_list: Vec<tendermint::validator::Info>| {
        let proposer = v_list.first().cloned();
        let tm_set = TmValidatorSet::new(v_list, proposer);
        tm_set.into()
    };

    let header = TmHeader {
        signed_header: Some(commit.signed_header.into()),
        validator_set: Some(mk_val(v_resp.validators)),
        trusted_height: Some(tr_h.clone()),
        trusted_validators: Some(mk_val(tv_resp.validators)),
    };

    Ok(IbcAny {
        type_url: "/ibc.core.client.v1.MsgUpdateClient".to_string(),
        value: MsgUpdateClient {
            client_id: cid.to_string(),
            client_message: Some(IbcAny { 
                type_url: "/ibc.lightclients.tendermint.v1.Header".to_string(), 
                value: header.encode_to_vec() 
            }),
            signer: "".to_string(),
        }.encode_to_vec(),
    })
}

fn find_val(v: &Value, key: &str) -> String {
    if let Some(val) = v.get(key) {
        if let Some(s) = val.as_str() { return s.to_string(); }
        if let Some(n) = val.as_u64() { return n.to_string(); }
    }
    if let Some(obj) = v.as_object() {
        for (_, inner) in obj {
            let found = find_val(inner, key);
            if !found.is_empty() { return found; }
        }
    } else if let Some(arr) = v.as_array() {
        for inner in arr {
            let found = find_val(inner, key);
            if !found.is_empty() { return found; }
        }
    }
    "".to_string()
}