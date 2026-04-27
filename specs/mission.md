# Mission: exa-postgres-interface

Last updated: 2026-04-27

## Project Identity and Summary

`exa-postgres-interface` is a PostgreSQL wire-protocol gateway for Exasol. It is intended to run as an add-on Linux service between PostgreSQL-compatible client tools and an Exasol database, allowing tools that already support PostgreSQL to connect to Exasol even when they do not provide a native Exasol connector.

## Problem Statement

Exasol customers use a wide range of database clients, BI tools, data tools, and integration platforms. Only some of those tools have native Exasol connectors, while PostgreSQL connectivity is nearly universal. This project SHOULD let customers reuse PostgreSQL-compatible tooling to reach Exasol by placing a PostgreSQL-compatible protocol layer in front of Exasol.

The prototype MUST preserve Exasol as the actual database system. It SHALL NOT modify Exasol database behavior or require customers to replace Exasol internals.

## Target Users and Workflows

Primary users are Exasol customers who want to connect existing PostgreSQL-capable tools to Exasol.

Current target workflow:

* An Exasol customer installs and runs this application on a Linux machine.
* The Exasol-side PostgreSQL compatibility SQL is installed in the database.
* The application listens for PostgreSQL wire-protocol client connections.
* PostgreSQL-capable clients such as DbVisualizer, DBeaver, `psql`, and JDBC-based tools connect to the application as if it were connecting to PostgreSQL.
* The application authenticates to Exasol, initializes the Exasol session, and forwards or translates client activity so the user can reach Exasol.

Future target workflow:

* The application MAY run near Exasol as a sidecar component, but sidecar packaging and integration are out of scope for the prototype.

## Core Capabilities

The prototype SHOULD establish a useful PostgreSQL-compatible path from common PostgreSQL clients to Exasol:

* Accept PostgreSQL wire-protocol connections from standard PostgreSQL clients.
* Support username/password authentication first.
* Connect to Exasol using configured connection information and customer credentials.
* Automatically initialize each Exasol session with a Python preprocessor script.
* Use a Python preprocessor based on `sqlglot` to convert SQL from PostgreSQL dialect to Exasol dialect.
* Return query results, metadata, errors, and connection state in forms PostgreSQL clients can consume.
* Make unsupported protocol, SQL, authentication, or metadata behavior explicit.
* Expose broad PostgreSQL metadata compatibility through Exasol-side `PG_CATALOG` and `INFORMATION_SCHEMA` schemas.
* Run as a systemd-managed Linux service.

Longer-term capabilities SHOULD include broader PostgreSQL protocol compatibility and additional Exasol authentication types.

## Out of Scope

The prototype SHALL NOT include:

* Changes to Exasol database engine behavior.
* Server-side Exasol engine modifications beyond normal SQL objects, session setup, and use of a Python preprocessor script.
* Sidecar packaging or integration into Exasol deployment topology.
* A guarantee that arbitrary PostgreSQL applications work unchanged.
* Full coverage of every PostgreSQL SQL construct, system catalog, extension, or behavior.
* Production hardening beyond what is necessary to evaluate the service safely.

## Domain Glossary

* PostgreSQL wire protocol: The network protocol spoken by PostgreSQL clients and servers.
* PostgreSQL client: Any tool or application that can connect to a PostgreSQL server, including DbVisualizer.
* Protocol server: The application built by this repository; it accepts PostgreSQL wire-protocol connections.
* Exasol session: A database session opened by the protocol server against Exasol on behalf of a client connection.
* Python preprocessor: An Exasol-side preprocessor script that transforms incoming SQL before execution.
* Metadata compatibility layer: Exasol-side `PG_CATALOG` and `INFORMATION_SCHEMA` schemas containing PostgreSQL-shaped views and helper functions backed by Exasol system metadata where possible.
* SQL dialect translation: Conversion of PostgreSQL-flavored SQL to Exasol-flavored SQL, expected to use `sqlglot`.
* Add-on process: A separately installed application that runs between client tools and Exasol without modifying the database engine.

## Tech Stack

Current implementation stack:

* Rust binary built with Cargo.
* `pgwire` for PostgreSQL wire-protocol server behavior.
* Direct Exasol WebSocket protocol client for database sessions.
* Exasol-side Python preprocessor script using `sqlglot` for SQL dialect translation.
* TOML configuration for listen address, Exasol endpoint, TLS policy, logging, and session initialization.
* systemd unit template for Linux service operation.

Open decisions:

* Prepared statement parameter translation and type handling.
* Exasol-backed transaction semantics.
* GitHub release automation and binary distribution format.

## Build, Test, Lint, and Format Commands

Current commands:

```bash
cargo fmt
cargo test
cargo build --release
```

Integration verification also uses:

```bash
python3 scripts/exasol_exec.py --dsn EXASOL_HOST:8563 --user sys --password 'EXASOL_PASSWORD' --sql "SELECT 1"
```

## Project Structure

Current structure:

```text
.
|-- config/
|-- docs/
|-- packaging/
|-- scripts/
|-- specs/
|-- sql/
|-- src/
`-- tests/
```

Expected future structure SHOULD keep permanent Speq specs under `specs/<domain>/<feature>/spec.md` and staged work under `specs/_plans/<plan-name>/`.

## Architecture and Data Flow

Target prototype data flow:

1. A PostgreSQL client opens a connection to the protocol server.
2. The protocol server performs the required PostgreSQL wire-protocol startup and authentication exchange.
3. The protocol server opens an Exasol connection for the client session.
4. The protocol server initializes the Exasol session so PostgreSQL-dialect SQL is converted through the Python `sqlglot` preprocessor.
5. The client sends SQL or metadata requests through PostgreSQL protocol messages.
6. The protocol server routes requests to Exasol, maps responses back to PostgreSQL-compatible protocol messages, and returns them to the client.
7. The protocol server closes or cleans up Exasol session state when the client disconnects.

The implementation has moved beyond the first smoke test: it includes
Exasol-side PostgreSQL catalog compatibility, systemd deployment guidance,
DbVisualizer/DBeaver metadata fixes, JDBC compatibility tests, and performance
benchmark tooling.

## Constraints

* This project remains prototype-stage; implementation choices SHOULD optimize for learning, demonstrable connectivity, and clear compatibility boundaries before production completeness.
* PostgreSQL protocol compatibility is the desired direction, but compatibility boundaries MUST be documented as the prototype discovers unsupported behavior.
* Exasol remains the source of truth for database execution and behavior.
* PostgreSQL metadata compatibility SHOULD live in Exasol-side views/functions where practical, not in ad hoc gateway-side query matching.
* The protocol server SHALL NOT silently emulate or alter database semantics in ways that hide meaningful Exasol/PostgreSQL differences.
* Session initialization MUST make the SQL preprocessor behavior explicit and observable enough to debug.
* Specs SHOULD be written before substantial behavior is implemented.
* Permanent behavior specs SHALL use RFC 2119 language in observable scenarios.
* Integration behavior SHOULD be testable against Exasol Personal.

## External Dependencies

Likely dependency categories:

* PostgreSQL wire-protocol server library or implementation primitives.
* Exasol client library or driver.
* Exasol Python preprocessor script support.
* `sqlglot` for PostgreSQL-to-Exasol SQL dialect conversion.
* DbVisualizer for the initial manual smoke test.
* Exasol Personal for integration testing.
* Linux packaging and process supervision tools, once packaging is planned.
* GitHub Releases or an equivalent artifact pipeline for distributing a Linux binary.

## Assumptions and Open Decisions

Confirmed mission facts:

* The project is about implementing PostgreSQL wire-protocol compatibility for Exasol.
* The primary users are Exasol customers.
* The first visible success case is connecting DbVisualizer through this server to Exasol.
* Username/password authentication is the first authentication target.
* Broader Exasol authentication support is desirable later.
* The project is a prototype.

Resolved implementation decisions:

* The active implementation is a Rust binary using `pgwire`.
* SQL translation runs inside Exasol through `PG_DEMO.PG_SQL_PREPROCESSOR`.
* PostgreSQL catalog compatibility is implemented through Exasol-side
  `PG_CATALOG` and `INFORMATION_SCHEMA` schemas.
* Linux operation uses a systemd unit and TOML config.

Open decisions:

* How to package and publish release binaries.
* How much PostgreSQL write/transaction behavior, if any, should be supported.
* Which additional PostgreSQL clients should become formal compatibility
  targets.
