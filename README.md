# exa-postgres-interface

Prototype PostgreSQL wire-protocol gateway for Exasol, implemented as a Rust
binary using the `pgwire` protocol library.

The server accepts PostgreSQL startup/password authentication, opens one Exasol
WebSocket session per client, activates a configured Exasol-side SQL
preprocessor, allows read-only DQL, and returns query results through PostgreSQL
wire messages. PostgreSQL SQL translation happens inside Exasol through the
configured Python preprocessor script, not in this gateway process.

## Current Scope

Implemented prototype scope:

* PostgreSQL protocol startup with cleartext password authentication.
* Simple Query and Extended Query protocol paths for row-returning read-only
  statements.
* Local acknowledgement of safe PostgreSQL client session commands such as
  startup `SET` statements.
* Local protocol handling for common client transaction wrappers.
* Explicit policy rejection for DML, DDL, and unsupported SQL classes.
* Direct Exasol WebSocket backend with TLS, certificate fingerprint pinning, and
  a development-only `validate_certificate = false` mode.
* Configurable Exasol session initialization for database-side SQL preprocessor activation.
* PostgreSQL metadata compatibility for the common JDBC and DbVisualizer browse
  paths: `pg_database`, `pg_namespace`, `pg_tables`, `information_schema`,
  `pg_user`, `pg_group`, `pg_stat_activity`, and `pg_locks`.
* Repeatable sample data SQL and an `exapump` setup helper.
* systemd unit and config examples.

Not implemented yet:

* General PostgreSQL system catalog emulation beyond the currently mapped
  JDBC/DbVisualizer query shapes.
* Binary prepared statement parameters.
* Real transaction semantic mapping to Exasol.
* Broader client compatibility coverage and automated integration tests.

## Build

```bash
cargo build --release
```

## Configure

Copy the example and adjust values for your environment. Do not commit secrets.

```bash
cp config/example.toml config/local.toml
```

The PostgreSQL client username and password are passed through to Exasol.

Exasol Personal often uses a self-signed TLS certificate. For secure operation,
pin the certificate fingerprint in `config/local.toml`:

```toml
[exasol]
dsn = "127.0.0.1:8563"
encryption = true
certificate_fingerprint = "SHA256_HEX_FINGERPRINT"
```

For a quick local-only prototype, certificate validation can be disabled:

```toml
[exasol]
dsn = "127.0.0.1:8563"
encryption = true
validate_certificate = false
```

This uses a `nocertcheck`-style TLS policy and should not be used for exposed or
production-like deployments.

For same-host testing, `server.listen_host = "127.0.0.1"` is sufficient. For a
client connecting from outside the EC2 instance, set:

```toml
[server]
listen_host = "0.0.0.0"
listen_port = 15432
```

The EC2 security group must also allow inbound TCP `15432` from the client IP.

## Run

```bash
cargo run -- --config config/local.toml
```

Then connect a PostgreSQL client to the configured listen host and port.

For a deployed binary, copy `target/release/exa-postgres-interface` to the
location referenced by the systemd unit.

## Sample Data

The setup helper uses the requested `nc-personal-2` exapump profile by default:

```bash
scripts/setup_sample_data.sh
```

The Exasol Personal endpoint used during development requires TLS, so the
selected `exapump` profile must have `--tls true` configured.

Override the profile when needed:

```bash
EXAPUMP_PROFILE=other-profile scripts/setup_sample_data.sh
```

## Test

```bash
cargo test
```

See [docs/postgres-metadata-compatibility.md](docs/postgres-metadata-compatibility.md)
for the current catalog mapping matrix.
