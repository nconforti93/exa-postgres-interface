# Verification Report: Add Read-Only PostgreSQL Interface

## Summary

Implemented the first prototype scaffold for the read-only PostgreSQL interface:

* PostgreSQL startup handling with cleartext password authentication.
* Client credential passthrough into an Exasol `pyexasol` session.
* Configurable SQL preprocessor session initialization.
* Simple Query protocol support for read-only DQL result sets.
* Explicit PostgreSQL-style errors for unsupported SQL and protocol behavior.
* Read-only statement policy separated from Exasol execution.
* Sample Exasol data SQL and an `exapump` setup helper.
* Runtime config example, systemd unit template, smoke-test docs, and tests.

## Changed Files

* `pyproject.toml`
* `README.md`
* `config/example.toml`
* `src/exa_postgres_interface/*`
* `sql/sample_data.sql`
* `sql/exasol_sql_preprocessor.sql`
* `scripts/setup_sample_data.sh`
* `packaging/exa-postgres-interface.service`
* `docs/smoke-test.md`
* `tests/*`
* `specs/_plans/add-read-only-postgres-interface/implementation-notes.md`

## Tests and Commands Run

* `exapump sql --profile nc-personal "SELECT 1"` failed before implementation
  verification because the sandbox could not resolve the configured Exasol host:
  `Temporary failure in name resolution`.
* `exapump sql --profile nc-personal-2 "SELECT 1"` reached the IP-based
  profile but was blocked by sandbox network policy:
  `Operation not permitted (os error 1)`.
* A requested outside-sandbox retry for
  `exapump sql --profile nc-personal-2 "SELECT 1"` was rejected by the harness:
  `approval policy is UnlessTrusted; reject command`.
* After full access was enabled, `exapump sql --profile nc-personal-2 "SELECT 1"`
  reached Exasol and failed with a real server response:
  `Only TLS connections are allowed. (SQL code: 08004)`.
* `exapump sql --profile nc-personal "SELECT 1"` produced the same TLS-required
  server response.
* After `nc-personal-2` was recreated with TLS enabled,
  `exapump sql --profile nc-personal-2 "SELECT 1"` passed and returned `1`.
* `scripts/setup_sample_data.sh` passed with `nc-personal-2`, creating
  `pg_demo.orders` and inserting four rows.
* `exapump sql --profile nc-personal-2 < sql/exasol_sql_preprocessor.sql`
  passed, installing the database-side Python preprocessor script.
* The canonical PostgreSQL-flavored demo query passed in Exasol after activating
  the database-side preprocessor in the same session:
  `ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = pg_demo.pg_sql_preprocessor`.
  Returned rows:
  `1,2026-01-02,125.50`, `2,2026-01-03,210.00`, and
  `4,2026-01-05,42.42`.
* Added local acknowledgement for safe PostgreSQL client session commands such
  as startup `SET` statements issued by PostgreSQL drivers and DbVisualizer.
* `PYTHONPATH=src python -m unittest discover -s tests` passed:
  21 tests ran successfully, including a TCP-level PostgreSQL startup,
  password-authentication, and simple-query protocol test with a fake backend.
* The canonical PostgreSQL demo query fails when sent directly to Exasol without
  the preprocessor, first on PostgreSQL `::` casts and then on `ILIKE` after
  `sqlglot` cast translation. This confirms the preprocessor must be activated
  inside the Exasol session rather than handled in the gateway application.

Automated unit tests are expected to run with:

```bash
PYTHONPATH=src python -m unittest discover -s tests
```

## Spec Scenarios Covered

Covered by implementation and unit tests:

* Startup and cleartext password authentication path.
* Client credentials are passed to the backend factory.
* `SELECT 1` and other read-only DQL can flow through the simple-query path.
* Row description, data row, command completion, and ready state messages are
  encoded.
* DML, DDL, transaction, session, and unknown statements are rejected by policy.
* Rejected statements do not mutate gateway state and later read statements can
  still execute.
* Configuration identifies Exasol endpoint and preprocessor initialization SQL.
* Sample data setup is repeatable and keeps credentials outside the repository.

## Known Gaps and Follow-Up Work

* DbVisualizer smoke testing through the gateway still needs to be run manually.
* End-to-end gateway-to-Exasol testing requires a temporary config and client
  credentials; the live database-side setup has been verified with `exapump`.
* The exact Exasol SQL preprocessor callback contract must be verified against
  the target Exasol Personal version before treating `sql/exasol_sql_preprocessor.sql`
  as final.
* PostgreSQL extended query protocol is not implemented.
* PostgreSQL metadata and system catalog compatibility for DbVisualizer browsing
  is not implemented.
* Transaction commands are explicitly rejected rather than mapped.
* Packaging is a Python console script plus systemd template, not a native
  static binary.
