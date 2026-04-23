mod config;
mod exasol;
mod pg_server;
mod policy;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::info;

use crate::config::AppConfig;
use crate::pg_server::{ExasolPgWireFactory, ExasolPgWireHandler};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path = std::env::args()
        .skip_while(|arg| arg != "--config")
        .nth(1)
        .unwrap_or_else(|| "config/local.toml".to_owned());

    let config = Arc::new(AppConfig::from_file(&config_path)?);
    tracing_subscriber::fmt()
        .with_env_filter(config.log_filter())
        .init();

    let listen_addr: SocketAddr = format!(
        "{}:{}",
        config.server.listen_host, config.server.listen_port
    )
    .parse()?;

    let handler = Arc::new(ExasolPgWireHandler::new(config.clone()));
    let factory = Arc::new(ExasolPgWireFactory { handler });
    let listener = TcpListener::bind(listen_addr).await?;

    info!(
        listen = %listen_addr,
        exasol_dsn = %config.exasol.dsn,
        translation = config.translation.enabled,
        "exa-postgres-interface pgwire server listening"
    );

    loop {
        let (socket, peer) = listener.accept().await?;
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(error) = pgwire::tokio::process_socket(socket, None, factory).await {
                tracing::warn!(%peer, %error, "client connection ended with error");
            }
        });
    }
}
