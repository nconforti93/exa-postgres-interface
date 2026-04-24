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

## Performance Considerations

This gateway adds measurable overhead compared with a direct Exasol JDBC
connection. The effect is workload-dependent:

* Small interactive queries with tiny result sets typically see a mostly fixed
  gateway tax. On the current benchmark host this was about `140-155 ms` per
  query, which made `SELECT 1` and similar probes look roughly `5x-7x` slower
  than direct JDBC.
* Large result transfers see an additional payload-dependent cost. In the
  current benchmark runs, returning `1M-10M` rows through the gateway added
  anywhere from about `11%` to `38%` wall-clock time depending on row count and
  column width.
* Long-running analytical queries that return very little data behave
  differently. For one-row aggregation workloads taking about `2.5s`, `5.6s`,
  and `20s` in Exasol, the observed gateway delta stayed within a few hundred
  milliseconds. In that regime, the gateway behaved much more like a small
  additive cost than a fixed percentage slowdown.

Practical guidance:

* Expect the gateway to be most visible for metadata browsing, BI tool
  exploration, and other short round trips.
* Expect bulk result export or wide result sets to pay an additional transfer
  cost beyond the small-query tax.
* Do not assume a query that takes `30s` in Exasol will become `37.5s` through
  the gateway if it only returns a few rows. On the current test system, heavy
  compute with tiny results stayed close to direct execution time.

These numbers are environment-specific, not protocol constants. Re-run the
benchmark in your target environment before treating them as acceptance
criteria. See
[docs/client-compatibility-test-framework.md](docs/client-compatibility-test-framework.md)
for the benchmark harness and current workload shapes.

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

`cargo run` now defaults to the main gateway binary. The repository also
contains a helper binary, `exasol_exec`, for applying and probing Exasol SQL
directly during development.

If you want to run a binary explicitly:

```bash
cargo run --bin exa-postgres-interface -- --config config/local.toml
cargo run --bin exasol_exec -- --help
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
