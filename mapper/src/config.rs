use serde::Deserialize;
use std::fs;
use anyhow::{Context, Result};

#[derive(Deserialize, Debug, Clone)]
pub struct Chain {
    pub chain_id: String,
    pub grpc_url: String,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub source_addr: String,
    pub listen_addr: String,
    pub chains: Vec<Chain>,
}

pub fn load_config() -> Result<Config> {
    let content = fs::read_to_string("Config.toml")
        .context("Config.toml not found")?;

    let config: Config = toml::from_str(&content)
        .context("Failed to parse Config.toml")?;

    if config.chains.is_empty() {
        anyhow::bail!("No chains configured");
    }

    Ok(config)
}
