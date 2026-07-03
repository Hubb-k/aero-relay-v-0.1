use std::sync::Arc;
use std::collections::HashSet;
use crate::transport::BufferTask;
use serde_json::Value;

pub async fn run(input_buf: Arc<BufferTask>, output_buf: Arc<BufferTask>, allowed_prefixes: HashSet<String>) {
    let mut rp = input_buf.get_current_head();

    loop {
        let (chunk, next_rp) = input_buf.pull_from(rp);
        
        if chunk.is_empty() {
            tokio::task::yield_now().await;
            continue;
        }
        
        rp = next_rp;

        for line in chunk.split(|&b| b == b'\n') {
            let line = line.trim_ascii();
            if line.is_empty() || !line.ends_with(b"}") {
                continue;
            }

            let Ok(v) = serde_json::from_slice::<Value>(line) else { continue; };

            let Some(payload) = v.get("payload") else { continue; };
            let ctx = payload.get("packet").unwrap_or(payload);

            let is_inc = ctx.get("is_incentivized").and_then(|b| b.as_bool()).unwrap_or(false);
            if !is_inc { continue; }

            let data_ctx = ctx.get("data_parsed").unwrap_or(ctx);
            let sender = data_ctx.get("sender").and_then(|s| s.as_str()).unwrap_or("");
            let receiver = data_ctx.get("receiver").and_then(|r| r.as_str()).unwrap_or("");

            if sender.is_empty() || receiver.is_empty() { continue; }

            let s_prefix = sender.split_once('1').map(|(p, _)| p).unwrap_or("");
            let r_prefix = receiver.split_once('1').map(|(p, _)| p).unwrap_or("");

            if !allowed_prefixes.contains(s_prefix) || !allowed_prefixes.contains(r_prefix) {
                continue;
            }

            if let Some(timeout) = ctx.get("timeout_timestamp").and_then(|t| t.as_u64()) {
                let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
                if timeout > 0 && timeout <= now_nanos { continue; }
            }

            let mut out = line.to_vec();
            out.pop();

            let extra = format!(
                ",\"src_chain\":\"{}\",\"dst_chain\":\"{}\"}}\n", 
                s_prefix, r_prefix
            );
            out.extend_from_slice(extra.as_bytes());

            output_buf.push(&out);
        }
    }
}