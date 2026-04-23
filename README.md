# exa-postgres-interface

Prototype PostgreSQL wire-protocol gateway for Exasol.

The first implementation accepts PostgreSQL startup/password authentication,
opens one Exasol session per client, activates a configured SQL preprocessor,
allows read-only DQL, and returns simple-query result sets in PostgreSQL wire
format. Unsupported protocol and SQL behavior is surfaced as PostgreSQL-style
errors instead of being silently emulated.

## Current Scope

Implemented prototype scope:

* PostgreSQL protocol startup with cleartext password authentication.
* Simple Query protocol for row-returning read-only statements.
* Explicit policy rejection for DML, DDL, transaction commands, and unsupported
  protocol messages.
* Exasol backend abstraction using `pyexasol`.
* Configurable Exasol session initialization for database-side SQL preprocessor activation.
* Repeatable sample data SQL and an `exapump` setup helper.
* systemd unit and config examples.

Not implemented yet:

* PostgreSQL extended query protocol.
* Transaction compatibility.
* PostgreSQL system catalog emulation.
* DbVisualizer metadata-query compatibility beyond the basic startup/query path.
* Live Exasol integration verification in this sandbox.

## Install

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

## Configure

Copy the example and adjust values for your environment. Do not commit secrets.

```bash
cp config/example.toml config/local.toml
```

The PostgreSQL client username and password are passed through to Exasol.

## Run

```bash
exa-postgres-interface --config config/local.toml
```

Then connect a PostgreSQL client to the configured listen host and port.

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
PYTHONPATH=src python -m unittest discover -s tests
```
