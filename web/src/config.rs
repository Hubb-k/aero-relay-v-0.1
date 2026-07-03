use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub listen_interface: String,
    pub chains: Vec<ChainConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChainConfig {
    pub prefix: String,
    pub grpc_url: String,
    pub listen_port: u16,
}

pub fn load_config() -> AppConfig {
    let config_str = std::fs::read_to_string("config.toml").expect("Missing config.toml");
    toml::from_str(&config_str).expect("Invalid TOML")
}