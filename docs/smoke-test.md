# Smoke Test

This document assumes the current architecture:

* the gateway runs as the Rust `exa-postgres-interface` binary;
* PostgreSQL dialect translation runs inside Exasol through
  `PG_DEMO.PG_SQL_PREPROCESSOR`;
* PostgreSQL catalog metadata is exposed by Exasol-side `PG_CATALOG` and
  `INFORMATION_SCHEMA` compatibility schemas.

## Prepare Sample Data

```bash
scripts/setup_sample_data.sh
```

The script uses `EXAPUMP_PROFILE=nc-personal-2` unless overridden.
That profile must be configured with TLS enabled because the Exasol Personal
endpoint rejects plaintext database connections.

## Install Exasol-Side Compatibility SQL

Install or refresh the PostgreSQL compatibility schemas:

```bash
python3 scripts/exasol_exec.py \
  --dsn EXASOL_HOST:8563 \
  --user sys \
  --password 'EXASOL_PASSWORD' \
  --file sql/postgres_catalog_compatibility.sql
```

Install or refresh the SQL preprocessor:

```bash
python3 scripts/exasol_exec.py \
  --dsn EXASOL_HOST:8563 \
  --user sys \
  --password 'EXASOL_PASSWORD' \
  --file sql/exasol_sql_preprocessor.sql
```

With `exapump`, the preprocessor install is equivalent to:

```bash
exapump sql --profile nc-personal-2 < sql/exasol_sql_preprocessor.sql
```

The gateway activates the installed preprocessor for each Exasol session using
the configured `translation.session_init_sql` statement.

## Start The Gateway Manually

```bash
cp config/example.toml config/local.toml
cargo run -- --config config/local.toml
```

Set `exasol.dsn` and the preprocessor activation SQL for the target Exasol
Personal instance before starting the service.

For Exasol running on the same host with a self-signed certificate, either pin
the server fingerprint:

```toml
[exasol]
dsn = "127.0.0.1:8563"
encryption = true
certificate_fingerprint = "SHA256_HEX_FINGERPRINT"
```

or use the local prototype escape hatch:

```toml
[exasol]
dsn = "127.0.0.1:8563"
encryption = true
validate_certificate = false
```

When running on the same EC2 instance as Exasol and connecting from a remote
desktop client, use this server binding:

```toml
[server]
listen_host = "0.0.0.0"
listen_port = 15432
# Optional: set both paths if PostgreSQL clients require SSL.
# tls_cert_path = "/etc/exa-postgres-interface/server.crt"
# tls_key_path = "/etc/exa-postgres-interface/server.key"
```

`127.0.0.1` only listens on the EC2 instance loopback interface and is not
reachable through the instance public IP.

## Start The Gateway With systemd

For a Linux deployment, build and install the release binary:

```bash
cargo build --release
sudo useradd --system --home /opt/exa-postgres-interface --shell /usr/sbin/nologin exa-postgres-interface
sudo mkdir -p /opt/exa-postgres-interface/bin /etc/exa-postgres-interface
sudo install -m 0755 target/release/exa-postgres-interface /opt/exa-postgres-interface/bin/exa-postgres-interface
sudo install -m 0640 -o root -g exa-postgres-interface config/example.toml /etc/exa-postgres-interface/config.toml
sudo install -m 0644 packaging/exa-postgres-interface.service /etc/systemd/system/exa-postgres-interface.service
```

Edit `/etc/exa-postgres-interface/config.toml`, then start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now exa-postgres-interface
systemctl status exa-postgres-interface
journalctl -u exa-postgres-interface -f
```

## Connect DbVisualizer

Use the PostgreSQL connector:

* Host: gateway host
* Port: configured `server.listen_port`
* Database: any value accepted by the client UI
* User/password: Exasol credentials

DbVisualizer and DBeaver schema-browsing paths currently rely on the
Exasol-side compatibility views and preprocessor rewrites. If a browser action
fails, inspect `SYS.EXA_DBA_AUDIT_SQL` for the session and replay the original
`/* preprocessing: ... */` SQL after enabling `PG_DEMO.PG_SQL_PREPROCESSOR`.

## Optional psql Smoke Test

If `psql` is available on the machine running the client test, use the same
Exasol credentials that DbVisualizer uses:

```bash
PGPASSWORD='EXASOL_PASSWORD' psql \
  --host 127.0.0.1 \
  --port 15432 \
  --username sys \
  --dbname exasol \
  --command 'SELECT 1;'
```

When testing from outside the EC2 instance, replace `127.0.0.1` with the
instance address and ensure `server.listen_host = "0.0.0.0"`.

## Optional JDBC Smoke Test

The repository includes a small PostgreSQL JDBC smoke client that uses
`PreparedStatement`, forcing the extended protocol path used by many Java tools.

Download the PostgreSQL JDBC driver, compile the smoke client, and run it
against a running gateway:

```bash
curl -L -o /tmp/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.8.jar
javac -cp /tmp/postgresql.jar tests/jdbc/PgJdbcSmoke.java
java -cp /tmp/postgresql.jar:tests/jdbc PgJdbcSmoke \
  'jdbc:postgresql://127.0.0.1:15432/exasol?preferQueryMode=extended' \
  sys \
  'EXASOL_PASSWORD'
```

Expected output:

```text
OK columns=1 rows=1
OK columns=1 rows=1
OK columns=3 rows=3
```

## Optional JDBC Metadata Smoke Test

The repository also includes a metadata-focused JDBC smoke client that exercises
`DatabaseMetaData.getCatalogs()`, `getSchemas()`, `getTables()`, and
`getColumns()` against the gateway.

```bash
javac -cp /tmp/postgresql.jar tests/jdbc/PgJdbcMetaSmoke.java
java -cp /tmp/postgresql.jar:tests/jdbc PgJdbcMetaSmoke \
  'jdbc:postgresql://127.0.0.1:15432/exasol?preferQueryMode=extended' \
  sys \
  'EXASOL_PASSWORD'
```

The expected shape is:

```text
-- catalogs cols=1
TABLE_CAT=exasol
-- schemas cols=2
TABLE_SCHEM=...
TABLE_CATALOG=exasol
-- tables cols=10
TABLE_CAT=exasol
TABLE_SCHEM=...
TABLE_NAME=...
-- columns cols=24
TABLE_CAT=exasol
TABLE_SCHEM=...
TABLE_NAME=...
COLUMN_NAME=...
```

## JDBC Compatibility Suite

The repository also includes a broader compatibility suite that sweeps all
public `DatabaseMetaData` methods and runs persona query corpora for
DbVisualizer, pgJDBC, Metabase, DBeaver, and analyst-oriented PostgreSQL
`SELECT` stress cases.

```bash
scripts/run_jdbc_compatibility_suite.sh \
  'jdbc:postgresql://127.0.0.1:15432/exasol?preferQueryMode=extended' \
  sys \
  'EXASOL_PASSWORD'
```

See [docs/client-compatibility-test-framework.md](docs/client-compatibility-test-framework.md)
for persona details and report semantics.

## Gateway Vs Direct Exasol Benchmark

To compare warm-query latency through the PostgreSQL gateway against a direct
Exasol JDBC connection, run:

```bash
scripts/run_gateway_vs_exasol_benchmark.sh \
  'jdbc:postgresql://127.0.0.1:15432/exasol?preferQueryMode=extended' \
  sys \
  'EXASOL_PASSWORD' \
  'jdbc:exa:127.0.0.1:8563' \
  sys \
  'EXASOL_PASSWORD'
```

The benchmark uses logically equivalent small and medium read-only query pairs
and reports latency statistics plus the gateway/direct ratio.

## Optional Metadata Query Smoke Test

Run representative metadata queries through `psql` or another PostgreSQL client
to verify the catalog path used by JDBC tools:

```sql
select * from pg_tables where schemaname != 'pg_catalog';
select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = 'exasol' and TABLE_SCHEMA = 'PG_DEMO' order by TABLE_NAME;
select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = 'exasol' and TABLE_SCHEMA = 'PG_DEMO' and TABLE_NAME = 'ORDERS' order by COLUMN_NAME;
select * from information_schema.table_constraints where table_schema = 'DEMO_FINANCE' and table_name = 'INVOICES';
select * from pg_user;
select * from pg_group;
select * from pg_stat_activity;
select * from pg_locks;
```

## Basic Query

```sql
SELECT 1;
```

Expected result:

```text
1
```

## Dialect Conversion Query

For direct `exapump` verification, activate the preprocessor in the same session:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = pg_demo.pg_sql_preprocessor;
```

```sql
SELECT
  order_id,
  order_ts::DATE AS order_date,
  amount::DECIMAL(18, 2) AS amount_eur
FROM pg_demo.orders
WHERE customer_name ILIKE 'acme%'
ORDER BY order_id
LIMIT 3;
```

Expected result shape:

```text
order_id | order_date | amount_eur
```

Expected rows are the first three Acme-prefixed sample orders ordered by
`order_id`.

Current `sqlglot` translation for the casts is not enough by itself because it
leaves PostgreSQL `ILIKE` unchanged for Exasol. The database-side preprocessor
script adds a targeted rewrite from `x ILIKE 'pattern'` to
`UPPER(x) LIKE UPPER('pattern')` for this demo scope. This translation is not
performed by the gateway application.
