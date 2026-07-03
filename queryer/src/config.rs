use serde::Deserialize;
use std::fs;
use anyhow::{Context, Result};

#[derive(Deserialize, Debug, Clone)]
pub struct Chain {
    pub chain_id: String,
    pub rpc_url: String,
    pub revision: String,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub source_addr: String,
    pub listen_addr: String,
    pub chains: Vec<Chain>,
}

pub fn load_config() -> Result<Config> {
    let content = fs::read_to_string("config.toml")
        .or_else(|_| fs::read_to_string("Config.toml"))
        .context("Config file not found")?;

    let config: Config = toml::from_str(&content)
        .context("Failed to parse config")?;

    if config.chains.is_empty() {
        anyhow::bail!("No chains configured");
    }

    Ok(config)
}
