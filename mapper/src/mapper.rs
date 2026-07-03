use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::NamedTempFile;
use tonic::transport::{Channel, Endpoint};

use ibc_proto::ibc::core::channel::v1::{query_client::QueryClient as ChannelQueryClient, QueryChannelRequest};
use ibc_proto::ibc::core::connection::v1::{query_client::QueryClient as ConnectionQueryClient, QueryConnectionRequest};

use crate::config::Config;
use crate::transport::BufferTask;

const DEFAULT_CACHE_CAP: usize = 16384;
const MAPPING_FILE: &str = "clients.json";
const SAVE_INTERVAL_SEC: u64 = 600;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MappingEntry {
    client_a: (String, String),
    client_b: (String, String),
    channel_a: String,
    channel_b: String,
    last_updated: u64,
}

pub struct Mapper {
    channels: HashMap<String, Channel>,
    cache: RwLock<LruCache<String, MappingEntry>>,
    full_map: RwLock<HashMap<String, MappingEntry>>,
    dirty: RwLock<bool>,
    mapping_file: String,
}

impl Mapper {
    pub async fn new(cfg: Config) -> Arc<Self> {
        let mut channels = HashMap::new();
        for chain in &cfg.chains {
            let endpoint = Endpoint::from_shared(chain.grpc_url.clone())
                .expect("Invalid grpc_url")
                .tcp_keepalive(Some(Duration::from_secs(45)))
                .http2_keep_alive_interval(Duration::from_secs(15));

            if let Ok(ch) = endpoint.connect().await {
                channels.insert(chain.chain_id.clone(), ch);
            }
        }

        let mapper = Arc::new(Self {
            channels,
            cache: RwLock::new(LruCache::new(std::num::NonZeroUsize::new(DEFAULT_CACHE_CAP).unwrap())),
            full_map: RwLock::new(HashMap::new()),
            dirty: RwLock::new(false),
            mapping_file: MAPPING_FILE.to_string(),
        });

        mapper.load_from_file().await;

        let m_clone = mapper.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(SAVE_INTERVAL_SEC));
            loop {
                interval.tick().await;
                m_clone.sync_to_disk().await;
            }
        });

        let m_shutdown = mapper.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            m_shutdown.sync_to_disk().await;
            std::process::exit(0);
        });

        mapper
    }

    pub async fn run(&self, input: Arc<BufferTask>, output: Arc<BufferTask>) {
        let mut rp = input.get_current_head();

        loop {
            let (chunk, next_rp) = input.pull_from(rp);
            rp = next_rp;

            if chunk.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }

            let text = String::from_utf8_lossy(&chunk);

            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || !trimmed.contains("src_chain") {
                    output.push(format!("{}\n", trimmed).as_bytes());
                    continue;
                }

                let event: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(_) => {
                        output.push(format!("{}\n", trimmed).as_bytes());
                        continue;
                    }
                };

                let src_chain = event.get("src_chain").and_then(|v| v.as_str()).unwrap_or("");
                let dst_chain = event.get("dst_chain").and_then(|v| v.as_str()).unwrap_or("");
                let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");

                if src_chain.is_empty() || dst_chain.is_empty() {
                    output.push(format!("{}\n", trimmed).as_bytes());
                    continue;
                }

                let src_port = event.pointer("/payload/source_port").and_then(|v| v.as_str()).unwrap_or("transfer");
                let dst_port = event.pointer("/payload/destination_port").and_then(|v| v.as_str()).unwrap_or("transfer");
                let src_channel = event.pointer("/payload/source_channel").or_else(|| event.pointer("/payload/packet/source_channel")).and_then(|v| v.as_str()).unwrap_or("");
                let dst_channel = event.pointer("/payload/destination_channel").or_else(|| event.pointer("/payload/packet/destination_channel")).and_then(|v| v.as_str()).unwrap_or("");

                if src_channel.is_empty() || dst_channel.is_empty() {
                    output.push(format!("{}\n", trimmed).as_bytes());
                    continue;
                }

                let mapping = self.check_cache(src_chain, src_channel, dst_chain, dst_channel);

                if let Some(m) = mapping {
                    let target_chain = if event_type == "Ack" { src_chain } else { dst_chain };
                    let client_id = if m.client_a.0 == target_chain { &m.client_a.1 } else { &m.client_b.1 };

                    let mut out = trimmed.to_string();
                    if !client_id.is_empty() {
                        out.pop();
                        out.push_str(&format!(",\"client_{}\":\"{}\"}}",  target_chain, client_id));
                    }
                    out.push('\n');
                    output.push(out.as_bytes());
                } else {
                    let c1 = src_chain.to_string();
                    let p1 = src_port.to_string();
                    let ch1 = src_channel.to_string();
                    let c2 = dst_chain.to_string();
                    let p2 = dst_port.to_string();
                    let ch2 = dst_channel.to_string();

                    let this = unsafe { &*(self as *const Self) };
                    tokio::spawn(async move {
                        this.fetch_and_cache(&c1, &p1, &ch1, &c2, &p2, &ch2).await;
                    });

                    output.push(format!("{}\n", trimmed).as_bytes());
                }
            }
        }
    }

    fn check_cache(&self, c1: &str, ch1: &str, c2: &str, ch2: &str) -> Option<MappingEntry> {
        let (ca, cha, cb, chb) = if c1 < c2 { (c1, ch1, c2, ch2) } else { (c2, ch2, c1, ch1) };
        let key = format!("map_{}_{}_{}_{}", ca, cha, cb, chb);
        let mut cache = self.cache.write();
        cache.get(&key).cloned()
    }

    async fn fetch_and_cache(&self, c1: &str, p1: &str, ch1: &str, c2: &str, p2: &str, ch2: &str) {
        let (ca, pa, cha, cb, pb, chb) = if c1 < c2 { (c1, p1, ch1, c2, p2, ch2) } else { (c2, p2, ch2, c1, p1, ch1) };
        let key = format!("map_{}_{}_{}_{}", ca, cha, cb, chb);

        let (res_a, res_b) = tokio::join!(
            self.query_via_proxy(ca, pa, cha),
            self.query_via_proxy(cb, pb, chb)
        );

        let entry = MappingEntry {
            client_a: (ca.to_string(), res_a.unwrap_or_default()),
            client_b: (cb.to_string(), res_b.unwrap_or_default()),
            channel_a: cha.to_string(),
            channel_b: chb.to_string(),
            last_updated: unix_now(),
        };

        self.cache.write().put(key.clone(), entry.clone());
        self.full_map.write().insert(key, entry);
        *self.dirty.write() = true;
    }

    async fn query_via_proxy(&self, chain_id: &str, port_id: &str, channel_id: &str) -> Option<String> {
        let channel = self.channels.get(chain_id)?.clone();
        let mut ch_client = ChannelQueryClient::new(channel.clone());

        let resp = ch_client.channel(QueryChannelRequest {
            port_id: port_id.to_string(),
            channel_id: channel_id.to_string(),
        }).await.ok()?.into_inner();

        let conn_id = resp.channel?.connection_hops.first()?.clone();
        let mut conn_client = ConnectionQueryClient::new(channel);

        let conn_resp = conn_client.connection(QueryConnectionRequest { connection_id: conn_id }).await.ok()?.into_inner();
        Some(conn_resp.connection?.client_id)
    }

    async fn sync_to_disk(&self) {
        if !*self.dirty.read() { return; }

        let data = {
            let map = self.full_map.read();
            serde_json::to_string_pretty(&*map).unwrap_or_default()
        };

        if !data.is_empty() {
            let path = Path::new(&self.mapping_file);
            if let Ok(temp) = NamedTempFile::new_in(path.parent().unwrap_or(Path::new("."))) {
                if tokio::fs::write(temp.path(), &data).await.is_ok() {
                    let _ = temp.persist(path);
                    *self.dirty.write() = false;
                }
            }
        }
    }

    async fn load_from_file(&self) {
        let path = Path::new(&self.mapping_file);
        if let Ok(content) = tokio::fs::read_to_string(path).await {
            if let Ok(map) = serde_json::from_str::<HashMap<String, MappingEntry>>(&content) {
                let mut cache = self.cache.write();
                let mut full = self.full_map.write();
                for (k, v) in map {
                    cache.put(k.clone(), v.clone());
                    full.insert(k, v);
                }
            }
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
