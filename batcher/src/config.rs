use serde::Deserialize;
use std::fs;
use anyhow::{Context, Result};

#[derive(Deserialize, Debug, Clone)]
pub struct Chain {
    pub chain_id: String,
    pub prefix: String,
    pub denom: String,
    pub grpc_url: String,
    pub lcd_url: String,
    pub gas_price: f64,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub source_addr: Vec<String>,
    pub listen_addr: String,
    pub relayer_mnemonic: String,
    pub relayer_memo: String,
    pub chains: Vec<Chain>,
}

pub fn load_config() -> Result<Config> {
    let content = fs::read_to_string("config.toml")
        .context("config.toml not found")?;

    let config: Config = toml::from_str(&content)
        .context("Failed to parse config.toml")?;

    if config.chains.is_empty() {
        anyhow::bail!("No chains configured");
    }

    Ok(config)
}
