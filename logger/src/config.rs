use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct Config {
    pub source_addr: Vec<String>,
    pub listen_addr: String,
}

pub fn load_config() -> Config {
    let content = fs::read_to_string("config.toml")
        .expect("Failed to read config.toml");
    toml::from_str(&content)
        .expect("Failed to parse TOML")
}
