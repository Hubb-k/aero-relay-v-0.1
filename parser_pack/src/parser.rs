use std::sync::{Arc, Mutex, LazyLock};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH, Instant, Duration};
use crate::transport::BufferTask;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::warn;
use base64::Engine as _;

// Глобальный кэш для защиты от дублей (на 24 часа)
static SEEN_CACHE: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
static CLEANUP_QUEUE: LazyLock<Mutex<Vec<(String, Instant)>>> = LazyLock::new(|| Mutex::new(Vec::new()));

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TimeoutHeight {
    pub revision_number: u64,
    pub revision_height: u64,
}

impl TimeoutHeight {
    pub fn from_str(s: &str) -> Self {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() >= 2 {
            let rev_num = parts[0].chars().filter(|c| c.is_ascii_digit()).collect::<String>();
            let rev_height = parts[parts.len() - 1].chars().filter(|c| c.is_ascii_digit()).collect::<String>();
            TimeoutHeight {
                revision_number: rev_num.parse().unwrap_or(0),
                revision_height: rev_height.parse().unwrap_or(0),
            }
        } else {
            let digits = s.chars().filter(|c| c.is_ascii_digit()).collect::<String>();
            Self {
                revision_number: 0,
                revision_height: digits.parse().unwrap_or(0),
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IbcPacket {
    pub origin_chain: String,
    pub sequence: u64,
    pub proof_height: u64,
    pub source_port: String,
    pub source_channel: String,
    pub destination_port: String,
    pub destination_channel: String,
    pub data_hex: String,
    pub acknowledgement: Option<String>,
    pub data_parsed: Value,
    pub timeout_height: TimeoutHeight,
    pub timeout_timestamp: u64,
    pub is_incentivized: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "payload")]
pub enum IbcTask {
    Send(IbcPacket),
    Ack(IbcPacket),
}

pub async fn run(input_buf: Arc<BufferTask>, output_buf: Arc<BufferTask>) {
    let mut rp = input_buf.get_current_head();

    loop {
        let (chunk, next_rp) = input_buf.pull_from(rp);
        rp = next_rp;

        if chunk.is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
            continue;
        }

        let text = String::from_utf8_lossy(&chunk);

        for line in text.lines() {
            let line_owned = line.to_string();
            let out = Arc::clone(&output_buf);

            // Асинхронная задача на каждую строку, чтобы не блокировать чтение
            tokio::spawn(async move {
                let trimmed = line_owned.trim();
                if trimmed.is_empty() { return; }

                let Some(pos) = trimmed.find('{') else { return };
                
                let prefix_part = &trimmed[..pos];
                let origin_chain = prefix_part
                    .split(',')
                    .last()
                    .map(|s| s.trim())
                    .unwrap_or("unknown")
                    .to_string();

                // ВАЖНО: Десериализатор-итератор выгребает ВСЕ JSON-объекты из одной строки
                let stream = serde_json::Deserializer::from_str(&trimmed[pos..]).into_iter::<Value>();

                for value_res in stream {
                    let Ok(v) = value_res else { break };

                    let block_height = v.pointer("/result/data/value/TxResult/height")
                        .and_then(|h| h.as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| h.as_u64()))
                        .unwrap_or(0);

                    if let Some(events) = find_events(&v) {
                        // Проверка наличия инсентивов в блоке/транзакции
                        let has_incentive = events.iter().any(|e| {
                            let etype = e.get("type").and_then(|t| t.as_str()).unwrap_or("");
                            matches!(etype, "pay_packet_fee" | "fee_pay" | "incentivized_packet" | "distribute_fee")
                        });

                        for ev in events {
                            let ev_type = ev.get("type").and_then(|t| t.as_str()).unwrap_or("");
                            let is_send = ev_type == "send_packet";
                            let is_ack = ev_type == "write_acknowledgement";

                            if is_send || is_ack {
                                let mut attr_map = HashMap::with_capacity(14);
                                if let Some(attrs) = ev.get("attributes").and_then(|a| a.as_array()) {
                                    for attr in attrs {
                                        let k = attr.get("key").and_then(|k| k.as_str()).unwrap_or("");
                                        let v = attr.get("value").and_then(|v| v.as_str()).unwrap_or("");
                                        if !k.is_empty() {
                                            attr_map.insert(format!("{}.{}", ev_type, k), v.to_string());
                                        }
                                    }
                                }

                                attr_map.insert("__origin_chain".into(), origin_chain.clone());
                                attr_map.insert("__block_height".into(), block_height.to_string());
                                attr_map.insert("__is_incentivized".into(), has_incentive.to_string());

                                if let Some(packet) = extract_packet(&attr_map, ev_type) {
                                    let p_type = if is_send { "s" } else { "a" };
                                    let key = format!("{}:{}:{}:{}", packet.origin_chain, packet.source_channel, packet.sequence, p_type);

                                    // Проверка на дубликат
                                    let is_new = {
                                        let mut seen = SEEN_CACHE.lock().unwrap();
                                        if seen.contains(&key) {
                                            false
                                        } else {
                                            seen.insert(key.clone());
                                            true
                                        }
                                    };

                                    if is_new {
                                        CLEANUP_QUEUE.lock().unwrap().push((key, Instant::now()));
                                        if is_send { check_timeout(&packet); }
                                        
                                        let task = if is_send { IbcTask::Send(packet) } else { IbcTask::Ack(packet) };

                                        if let Ok(mut serialized) = serde_json::to_vec(&task) {
                                            serialized.push(b'\n');
                                            out.push(&serialized);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
        try_cleanup();
    }
}

fn try_cleanup() {
    if let Ok(mut cleanup_q) = CLEANUP_QUEUE.try_lock() {
        if !cleanup_q.is_empty() && cleanup_q[0].1.elapsed() > Duration::from_secs(86400) {
            let mut seen = SEEN_CACHE.lock().unwrap();
            let now = Instant::now();
            cleanup_q.retain(|(k, time)| {
                if now.duration_since(*time) > Duration::from_secs(86400) {
                    seen.remove(k);
                    false
                } else {
                    true
                }
            });
        }
    }
}

fn find_events(v: &Value) -> Option<&Vec<Value>> {
    if let Some(evs) = v.get("events").and_then(|e| e.as_array()) { return Some(evs); }
    if let Some(obj) = v.as_object() {
        for val in obj.values() {
            if let Some(found) = find_events(val) { return Some(found); }
        }
    }
    None
}

fn check_timeout(p: &IbcPacket) {
    if p.timeout_timestamp > 0 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        if now > p.timeout_timestamp {
            warn!("Packet sequence {} EXPIRED! (Timeout: {}, Now: {})", p.sequence, p.timeout_timestamp, now);
        }
    }
}

fn to_bytes_simple(input: &str) -> Vec<u8> {
    let s = input.trim();
    if s.is_empty() { return vec![]; }
    let hex_str = s.trim_start_matches("0x");
    if let Ok(h) = hex::decode(hex_str) { 
        h 
    } else if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(s) { 
        b 
    } else { 
        s.as_bytes().to_vec() 
    }
}

fn extract_packet(kv: &HashMap<String, String>, prefix: &str) -> Option<IbcPacket> {
    let get = |suffix: &str| kv.get(&format!("{}.{}", prefix, suffix)).cloned().unwrap_or_default();
    
    let origin_chain = kv.get("__origin_chain").cloned().unwrap_or_else(|| "unknown".into());
    let proof_height = kv.get("__block_height").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    let is_incentivized = kv.get("__is_incentivized").map(|v| v == "true").unwrap_or(false);

    let seq_str = get("packet_sequence");
    if seq_str.is_empty() { return None; }

    let sequence = seq_str.parse::<u64>().unwrap_or_else(|_| {
        seq_str.chars().filter(|c| c.is_ascii_digit()).collect::<String>().parse().unwrap_or(0)
    });

    let ts_str = get("packet_timeout_timestamp");
    let timeout_timestamp = ts_str.parse::<u64>().unwrap_or_else(|_| {
        ts_str.chars().filter(|c| c.is_ascii_digit()).collect::<String>().parse().unwrap_or(0)
    });

    let p_data = {
        let d = get("packet_data");
        if !d.is_empty() { d } else { get("packet_data_hex") }
    };

    let p_ack = {
        let a = get("packet_ack");
        if !a.is_empty() { a } else { get("packet_ack_hex") }
    };

    let raw_input = if !p_data.is_empty() { p_data } else { p_ack.clone() };
    let ack_raw = if !p_ack.is_empty() { Some(p_ack) } else { None };

    if raw_input.is_empty() && ack_raw.is_none() { return None; }

    let data_bytes = to_bytes_simple(&raw_input);
    let data_hex = hex::encode(&data_bytes);
    let acknowledgement = ack_raw.map(|a| hex::encode(to_bytes_simple(&a)));

    let data_parsed = if let Ok(v) = serde_json::from_slice::<Value>(&data_bytes) {
        v
    } else if let Ok(utf8) = String::from_utf8(data_bytes.clone()) {
        Value::String(utf8)
    } else {
        json!({"raw_hex": data_hex.clone()})
    };

    Some(IbcPacket {
        origin_chain,
        sequence,
        proof_height,
        source_port: get("packet_src_port"),
        source_channel: get("packet_src_channel"),
        destination_port: get("packet_dst_port"),
        destination_channel: get("packet_dst_channel"),
        data_hex,
        acknowledgement,
        data_parsed,
        timeout_height: TimeoutHeight::from_str(&get("packet_timeout_height")),
        timeout_timestamp,
        is_incentivized,
    })
}