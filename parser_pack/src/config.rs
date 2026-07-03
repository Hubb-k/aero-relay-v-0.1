use serde::Deserialize;
use std::fs;
use anyhow::{Context, Result};

#[derive(Deserialize, Debug)]
pub struct Config {
    pub source_addr: String,
    pub listen_addr: String,
}

pub fn load_config() -> Result<Config> {
    let content = fs::read_to_string("Config.toml")
        .context("Config.toml not found")?;

    let config: Config = toml::from_str(&content)
        .context("Failed to parse Config.toml")?;

    if config.source_addr.trim().is_empty() {
        anyhow::bail!("source_addr is empty");
    }
    if config.listen_addr.trim().is_empty() {
        anyhow::bail!("listen_addr is empty");
    }

    Ok(config)
}
