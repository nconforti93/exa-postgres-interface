use std::fs;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    pub exasol: ExasolConfig,
    #[serde(default)]
    pub translation: TranslationConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen_host")]
    pub listen_host: String,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExasolConfig {
    pub dsn: String,
    #[serde(default = "default_true")]
    pub encryption: bool,
    #[serde(default)]
    pub certificate_fingerprint: String,
    #[serde(default = "default_true")]
    pub validate_certificate: bool,
    #[serde(default = "default_true")]
    pub pass_client_credentials: bool,
    #[serde(default)]
    pub schema: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranslationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub sql_preprocessor_script: String,
    #[serde(default)]
    pub session_init_sql: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_host: default_listen_host(),
            listen_port: default_listen_port(),
            log_level: default_log_level(),
        }
    }
}

impl Default for TranslationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sql_preprocessor_script: String::new(),
            session_init_sql: Vec::new(),
        }
    }
}

impl AppConfig {
    pub fn from_file(
        path: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        if config.exasol.dsn.trim().is_empty() {
            return Err("exasol.dsn is required".into());
        }
        Ok(config)
    }

    pub fn log_filter(&self) -> String {
        format!(
            "exa_postgres_interface={},pgwire=info",
            self.server.log_level
        )
    }
}

fn default_listen_host() -> String {
    "127.0.0.1".to_owned()
}

fn default_listen_port() -> u16 {
    15432
}

fn default_log_level() -> String {
    "info".to_owned()
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_config() {
        let raw = r#"
            [server]
            listen_host = "0.0.0.0"
            listen_port = 15432

            [exasol]
            dsn = "127.0.0.1:8563"
            validate_certificate = false

            [translation]
            enabled = true
            sql_preprocessor_script = "PG_DEMO.PG_SQL_PREPROCESSOR"
            session_init_sql = ["ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {script}"]
        "#;

        let config: AppConfig = toml::from_str(raw).unwrap();

        assert_eq!(config.server.listen_host, "0.0.0.0");
        assert_eq!(config.exasol.dsn, "127.0.0.1:8563");
        assert!(!config.exasol.validate_certificate);
        assert_eq!(config.translation.session_init_sql.len(), 1);
    }
}
