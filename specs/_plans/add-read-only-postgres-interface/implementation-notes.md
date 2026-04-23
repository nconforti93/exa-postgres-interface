# Implementation Notes

## Runtime Selection

The active prototype uses Rust 1.89 and the `pgwire` crate.

Reasons:

* The initial direct Python wire-protocol prototype was not stable enough for
  DbVisualizer's connection behavior.
* `pgwire` provides startup/authentication, Simple Query, Extended Query,
  response framing, connection state, and ReadyForQuery handling as protocol
  primitives instead of ad hoc socket message construction.
* Rust produces a standalone Linux binary that matches the preferred operational
  shape for a systemd-managed gateway.
* SQL dialect translation remains inside Exasol through the configured Python
  preprocessor script. The gateway does not run `sqlglot` as an external
  application.

Tradeoffs:

* PostgreSQL system catalog compatibility remains future work.
* Prepared statement parameters are rejected for now, although Extended Query
  parse/bind/execute without parameters reaches the same execution path.
* Common transaction wrappers are acknowledged locally for client compatibility,
  but real Exasol-backed transaction semantics are not implemented.

## Architecture

The implementation separates:

* Configuration loading in `src/config.rs`.
* Exasol WebSocket session handling, TLS policy, certificate fingerprint
  verification, login, execution, and result parsing in `src/exasol.rs`.
* PostgreSQL startup/authentication, Simple Query, Extended Query, response
  encoding, and per-client Exasol session state in `src/pg_server.rs`.
* Statement classification and local client compatibility commands in
  `src/policy.rs`.
* Process startup and listener lifecycle in `src/main.rs`.

The execution result model distinguishes row-returning query results from
command-completion results so future write support can add DML/DDL behavior
without replacing the protocol response path.

## PostgreSQL Protocol Reference

The implementation follows the PostgreSQL frontend/backend protocol message
flow for the currently supported paths:

* Startup requests cleartext password authentication, then completes with
  AuthenticationOk, ParameterStatus, BackendKeyData, and ReadyForQuery.
* Simple Query sends one or more response sequences and ends with ReadyForQuery.
* Empty simple queries return EmptyQueryResponse.
* Extended Query accepts Parse/Bind/Describe/Execute/Sync, rejects
  multi-statement Parse payloads, and rejects bound parameters until parameter
  translation is implemented.
