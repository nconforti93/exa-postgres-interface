# Verification Report: Add Read-Only PostgreSQL Interface

## Summary

Reimplemented the prototype as a Rust binary using the `pgwire` crate instead
of the direct Python wire-protocol server. The active implementation now
provides:

* PostgreSQL startup with cleartext password authentication.
* One direct Exasol WebSocket session per PostgreSQL client connection.
* Exasol TLS support with strict validation, SHA-256 certificate fingerprint
  pinning, and a development-only `validate_certificate = false` mode.
* Configurable Exasol session initialization for the database-side SQL
  preprocessor.
* Simple Query and basic Extended Query support through `pgwire`.
* PostgreSQL-style row descriptions, data rows, command completions, errors, and
  ReadyForQuery handling.
* Extended Query execution sends row descriptions before result tuples so
  PostgreSQL clients do not receive `DataRow` messages without field metadata.
* Read-only statement policy separated from Exasol execution.
* Local compatibility handling for common PostgreSQL client session commands and
  transaction wrappers.
* Multi-statement Simple Query splitting, with execution stopped after the first
  error response.
* Explicit rejection for DML, DDL, prepared statement parameters, and
  multi-statement Extended Query Parse payloads.

SQL dialect translation remains inside Exasol through
`sql/exasol_sql_preprocessor.sql`; the gateway does not run `sqlglot` as an
external application.

## Changed Files

* `Cargo.toml`
* `Cargo.lock`
* `README.md`
* `.gitignore`
* `config/example.toml`
* `docs/smoke-test.md`
* `packaging/exa-postgres-interface.service`
* `src/config.rs`
* `src/exasol.rs`
* `src/main.rs`
* `src/pg_server.rs`
* `src/policy.rs`
* `specs/mission.md`
* `specs/_plans/add-read-only-postgres-interface/implementation-notes.md`
* `specs/_plans/add-read-only-postgres-interface/protocol/read-only-query-path/spec.md`

Removed the obsolete Python prototype package and Python unit tests so the
repository has one active implementation path.

## Tests and Commands Run

* `rustup component add rustfmt` installed the formatter component for the local
  Rust toolchain.
* `cargo fmt` passed.
* `cargo fmt --check` passed.
* `cargo test` passed: 10 tests.
* `cargo build --release` passed.
* `git diff --check` passed.
* `exapump sql --profile nc-personal-2 "SELECT 1"` passed and returned `1`.
* Downloaded PostgreSQL client packages without system install privileges,
  extracted `psql` under `/tmp/pgclient`, and used it for live smoke tests.
* Started the release gateway locally on `127.0.0.1:15433` with a temporary
  config targeting `3.66.165.192:8563`.
* `psql` through the gateway passed `SELECT 1` and returned `1`.
* `psql` through the gateway passed the PostgreSQL-flavored sample query using
  `::` casts and `ILIKE`, returning the expected three sample rows.

Earlier live database checks from this plan remain valid:

* `scripts/setup_sample_data.sh` passed with `nc-personal-2`.
* `exapump sql --profile nc-personal-2 < sql/exasol_sql_preprocessor.sql`
  passed, installing the database-side Python preprocessor.
* The canonical PostgreSQL-flavored demo query passed in Exasol after activating
  the database-side preprocessor in the same session.

## Spec Scenarios Covered

Covered by implementation and unit tests:

* Configuration loading and TLS policy defaults.
* Exasol DSN fingerprint and `nocertcheck` handling.
* Read/write/DDL/transaction statement classification.
* Local handling for safe PostgreSQL driver session commands.
* Local handling for common transaction wrappers.
* Simple Query batch splitting for PostgreSQL protocol compatibility.
* Exasol WebSocket `Pong("EXECUTING")` progress-frame handling.

Covered by build/manual verification:

* The Rust binary builds in release mode.
* The configured Exasol Personal profile `nc-personal-2` is reachable with TLS.
* The Exasol-side SQL preprocessor and sample data path were previously verified
  against the same profile.
* A real PostgreSQL client, `psql`, can connect through the gateway and execute
  the smoke and sample queries.

## Known Gaps and Follow-Up Work

* Manual DbVisualizer smoke testing through the Rust gateway still needs to be
  run with user-supplied Exasol credentials.
* PostgreSQL system catalog and metadata compatibility for browsing are not
  implemented.
* Prepared statement parameters are not implemented.
* Extended Query describe responses do not infer result schema before execution.
* Transaction wrappers are client compatibility acknowledgements, not
  Exasol-backed PostgreSQL transaction semantics.
* Column values are currently returned as PostgreSQL text fields for broad
  client compatibility; richer type mapping is future work.
