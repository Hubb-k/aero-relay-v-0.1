use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Clone)]
pub struct Network {
    pub name: String,
    pub ws_url: String,
}

#[derive(Deserialize)]
pub struct Config {
    pub source_addr: String,
    pub listen_addr: String,
    pub networks: Vec<Network>,
}

pub fn load_config() -> Config {
    let content = fs::read_to_string("Config.toml")
        .expect("Config.toml not found");
    toml::from_str(&content)
        .expect("Failed to parse Config.toml")
}
