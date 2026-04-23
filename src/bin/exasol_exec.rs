#[path = "../config.rs"]
mod config;
#[path = "../exasol.rs"]
mod exasol;

use std::env;
use std::fs;

use config::ExasolConfig;
use exasol::{ExasolResult, ExasolSession};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut dsn = String::new();
    let mut user = String::new();
    let mut password = String::new();
    let mut schema = String::new();
    let mut certificate_fingerprint = String::new();
    let mut validate_certificate = true;
    let mut sql = None;
    let mut file = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dsn" => dsn = required_value(&mut args, "--dsn")?,
            "--user" => user = required_value(&mut args, "--user")?,
            "--password" => password = required_value(&mut args, "--password")?,
            "--schema" => schema = required_value(&mut args, "--schema")?,
            "--fingerprint" => {
                certificate_fingerprint = required_value(&mut args, "--fingerprint")?
            }
            "--no-verify" => validate_certificate = false,
            "--sql" => sql = Some(required_value(&mut args, "--sql")?),
            "--file" => file = Some(required_value(&mut args, "--file")?),
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    if dsn.is_empty() || user.is_empty() || password.is_empty() {
        return Err("required: --dsn --user --password".into());
    }

    let sql = match (sql, file) {
        (Some(sql), None) => sql,
        (None, Some(path)) => fs::read_to_string(path)?,
        (Some(_), Some(_)) => return Err("pass either --sql or --file, not both".into()),
        (None, None) => return Err("required: --sql or --file".into()),
    };

    let config = ExasolConfig {
        dsn,
        encryption: true,
        certificate_fingerprint,
        validate_certificate,
        pass_client_credentials: true,
        schema,
    };

    let mut session = ExasolSession::connect(&config, &user, &password)?;
    match session.execute(&sql)? {
        ExasolResult::RowCount(count) => {
            println!("row_count={count}");
        }
        ExasolResult::ResultSet { columns, rows } => {
            println!(
                "{}",
                columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
                    .join("\t")
            );
            for row in rows {
                println!(
                    "{}",
                    row.into_iter()
                        .map(|value| value.unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join("\t")
                );
            }
        }
    }

    Ok(())
}

fn required_value(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}").into())
}

fn print_help() {
    eprintln!(
        "Usage: cargo run --bin exasol_exec -- --dsn <host:port> --user <user> --password <pwd> [--schema <schema>] [--no-verify] [--fingerprint <sha256>] (--sql <sql> | --file <path>)"
    );
}
