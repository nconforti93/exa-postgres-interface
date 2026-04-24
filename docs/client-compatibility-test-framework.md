# Client Compatibility Test Framework

This repository now includes a JDBC-driven compatibility suite for the read-only
PostgreSQL gateway. The goal is not only to keep a smoke test green, but to
show which JDBC metadata calls and PostgreSQL-flavored `SELECT` queries work,
which fail, and which fail only for specific client personas.

## What It Covers

The framework has three layers:

* An exhaustive `DatabaseMetaData` sweep that attempts every public
  `java.sql.DatabaseMetaData` method with deterministic sample arguments and
  records pass/fail, SQLState, and result shape.
* A persona query corpus with must-pass baseline probes plus exploratory probes
  drawn from real PostgreSQL clients and analytical SQL patterns.
* A gateway-vs-direct Exasol latency benchmark for small and medium read-only
  query pairs.

Current personas:

* `baseline`: the gateway's existing must-pass smoke path.
* `dbvisualizer`: catalog and browse queries already documented in this repo.
* `pgjdbc`: helper queries used by PostgreSQL JDBC metadata code.
* `metabase`: metadata-sync and query-result-metadata patterns, especially
  `LIMIT 0` and `LIMIT 1` probes.
* `dbeaver`: direct `pg_catalog` schema browser queries.
* `analyst`: PostgreSQL-flavored read-only stress queries that may expose
  translation or semantic gaps.

## Upstream Research Inputs

The current corpus is based on these public sources:

* pgJDBC `PgDatabaseMetaData` source:
  [PgDatabaseMetaData.java](https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/PgDatabaseMetaData.java?plain=1)
* Metabase driver docs:
  [metabase.driver](https://metabase-dev-docs.github.io/metabase/metabase.driver.html)
* DBeaver PostgreSQL source:
  [PostgreDatabase.java](https://github.com/dbeaver/dbeaver/blob/devel/plugins/org.jkiss.dbeaver.ext.postgresql/src/org/jkiss/dbeaver/ext/postgresql/model/PostgreDatabase.java),
  [PostgreSchema.java](https://github.com/dbeaver/dbeaver/blob/devel/plugins/org.jkiss.dbeaver.ext.postgresql/src/org/jkiss/dbeaver/ext/postgresql/model/PostgreSchema.java)

Relevant behaviors observed from those sources:

* pgJDBC metadata touches `pg_settings`, `pg_type`, `pg_namespace`,
  `pg_class`, `pg_attribute`, `pg_proc`, `pg_index`, and related catalog
  helpers.
* Metabase relies heavily on JDBC metadata such as `.getTables` and
  `.getColumns`, and its query-result-metadata path commonly uses `LIMIT 0` or
  `LIMIT 1` wrappers.
* DBeaver performs direct schema-browser queries against `pg_namespace`,
  `pg_class`, `pg_attribute`, `pg_constraint`, and `pg_index`, often with
  parameterized schema or OID lookups.

## Run It

Start the gateway first, then run:

```bash
scripts/run_jdbc_compatibility_suite.sh \
  'jdbc:postgresql://127.0.0.1:15432/exasol?preferQueryMode=extended' \
  sys \
  'EXASOL_PASSWORD'
```

Useful options:

```bash
--catalog=exasol
--schema=PG_DEMO
--table=ORDERS
--column-pattern=%
--personas=baseline,dbvisualizer,metabase,dbeaver
--strict
--output=/tmp/exa-postgres-compat.txt
```

`--strict` only fails the process when must-pass baseline probes fail.
Exploratory failures are still reported, but they stay informational by
default.

To compare query latency against a direct Exasol JDBC connection:

```bash
scripts/run_gateway_vs_exasol_benchmark.sh \
  'jdbc:postgresql://127.0.0.1:15432/exasol?preferQueryMode=extended' \
  sys \
  'EXASOL_PASSWORD' \
  'jdbc:exa:127.0.0.1:8563' \
  sys \
  'EXASOL_PASSWORD'
```

Useful benchmark options:

```bash
--warmup=3
--iterations=10
--output=/tmp/exa-postgres-benchmark.txt
```

The benchmark measures query latency on already-open JDBC connections. It does
not measure connection-establishment time.

## Reading The Report

The suite prints three result kinds:

* `PASS`: the metadata call or SQL probe succeeded.
* `FAIL`: the call or probe failed. For SQL failures, the suite prints SQLState
  and the driver-visible error message.
* `SKIP`: argument mapping was not available for that metadata method.

The summary separates must-pass failures from exploratory failures so the team
can keep core smoke coverage stable while still learning about unsupported
PostgreSQL surface area.

The benchmark report prints, for each query pair:

* direct Exasol latency statistics;
* gateway latency statistics;
* gateway-over-direct ratio;
* a deterministic result digest comparison to guard against comparing unlike
  results.

## Likely Gap Areas

The suite is designed to confirm these, not assume them, but the current code
and upstream client behavior suggest that these PostgreSQL features are
especially important to watch:

* Rich metadata methods beyond `getCatalogs`, `getSchemas`, `getTables`, and
  `getColumns`.
* Queries that depend on PostgreSQL catalog helper functions such as
  `pg_get_expr`, `pg_get_constraintdef`, or other `pg_catalog` helpers used by
  DBeaver and pgJDBC.
* PostgreSQL-specific `SELECT` features such as `DISTINCT ON`, `FILTER`,
  arrays, JSON builders, and other constructs that may not translate cleanly to
  Exasol through the current SQL preprocessor path.
* Gateway latency overhead that is acceptable for small smoke queries but may be
  more visible on medium queries where protocol translation and metadata mapping
  dominate.

Those points are an inference from the current implementation plus the public
client sources above; the suite exists to replace that inference with concrete
results.
