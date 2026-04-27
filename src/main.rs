mod config;
mod exasol;
mod metadata;
mod pg_server;
mod policy;

use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;

use rustls_pemfile::{certs, private_key};
use rustls_pki_types::CertificateDer;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
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
    let tls_acceptor = setup_tls(&config)?;

    info!(
        listen = %listen_addr,
        exasol_dsn = %config.exasol.dsn,
        translation = config.translation.enabled,
        tls = tls_acceptor.is_some(),
        "exa-postgres-interface pgwire server listening"
    );

    loop {
        let (socket, peer) = listener.accept().await?;
        let factory = factory.clone();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            if let Err(error) = pgwire::tokio::process_socket(socket, tls_acceptor, factory).await {
                tracing::warn!(%peer, %error, "client connection ended with error");
            }
        });
    }
}

fn setup_tls(config: &AppConfig) -> Result<Option<TlsAcceptor>, IoError> {
    if config.server.tls_cert_path.trim().is_empty() {
        return Ok(None);
    }

    let certs = certs(&mut BufReader::new(File::open(
        &config.server.tls_cert_path,
    )?))
    .collect::<Result<Vec<CertificateDer>, IoError>>()?;

    let key = private_key(&mut BufReader::new(File::open(
        &config.server.tls_key_path,
    )?))?
    .ok_or_else(|| {
        IoError::new(
            ErrorKind::InvalidInput,
            "TLS key file contains no private key",
        )
    })?;

    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?;
    server_config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(Some(TlsAcceptor::from(Arc::new(server_config))))
}
