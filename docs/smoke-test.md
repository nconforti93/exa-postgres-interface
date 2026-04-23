# Smoke Test

## Prepare Sample Data

```bash
scripts/setup_sample_data.sh
```

The script uses `EXAPUMP_PROFILE=nc-personal-2` unless overridden.
That profile must be configured with TLS enabled because the Exasol Personal
endpoint rejects plaintext database connections.

## Install SQL Preprocessor

```bash
exapump sql --profile nc-personal-2 < sql/exasol_sql_preprocessor.sql
```

The gateway activates the installed preprocessor for each Exasol session using
the configured `translation.session_init_sql` statement.

## Start The Gateway

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
```

`127.0.0.1` only listens on the EC2 instance loopback interface and is not
reachable through the instance public IP.

## Connect DbVisualizer

Use the PostgreSQL connector:

* Host: gateway host
* Port: configured `server.listen_port`
* Database: any value accepted by the client UI
* User/password: Exasol credentials

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
