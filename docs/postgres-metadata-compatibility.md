# PostgreSQL Metadata Compatibility

The current implementation centers metadata compatibility inside Exasol, not in
the pgwire gateway. Two Exasol schemas are created:

* `PG_CATALOG`
* `INFORMATION_SCHEMA`

These schemas expose PostgreSQL-shaped views backed by Exasol metadata system
tables. The gateway now stays focused on protocol, authentication, session
state, and a small number of PostgreSQL session primitives such as `SHOW`,
`version()`, `current_database()`, and `current_catalog()`.

Primary references:

* [Exasol metadata system tables](https://docs.exasol.com/db/latest/sql_references/system_tables/metadata_system_tables.htm)
* [PostgreSQL system catalogs](https://www.postgresql.org/docs/18/catalogs.html)
* [PostgreSQL information schema](https://www.postgresql.org/docs/18/information-schema.html)
* DbVisualizer PostgreSQL profile:
  `C:/Program Files/DbVisualizer/resources/profiles/postgresql8.xml`

## Compatibility Matrix

| PostgreSQL surface | Handling | Exasol source |
| --- | --- | --- |
| `pg_catalog.pg_database` | compatibility view with one logical catalog `exasol` | synthetic |
| `pg_catalog.pg_namespace` | compatibility view over Exasol schemas plus `PG_CATALOG` and `INFORMATION_SCHEMA` | `SYS.EXA_DBA_SCHEMAS` |
| `pg_catalog.pg_roles` / `pg_user` | compatibility view over Exasol users and roles | `SYS.EXA_DBA_USERS`, `SYS.EXA_DBA_ROLES` |
| `pg_catalog.pg_class` | compatibility view over Exasol tables and views | `SYS.EXA_DBA_TABLES`, `SYS.EXA_DBA_VIEWS`, `SYS.EXA_DBA_COLUMNS`, `SYS.EXA_DBA_INDICES` |
| `pg_catalog.pg_attribute` | compatibility view over Exasol columns | `SYS.EXA_DBA_COLUMNS` |
| `pg_catalog.pg_attrdef` | compatibility view over Exasol column defaults | `SYS.EXA_DBA_COLUMNS` |
| `pg_catalog.pg_description` | compatibility view over schema / object / column comments | `SYS.EXA_DBA_SCHEMAS`, `SYS.EXA_DBA_OBJECTS`, `SYS.EXA_DBA_COLUMNS` |
| `pg_catalog.pg_constraint` | compatibility view over Exasol constraints | `SYS.EXA_DBA_CONSTRAINTS`, `SYS.EXA_DBA_CONSTRAINT_COLUMNS` |
| `pg_catalog.pg_tables` / `pg_views` | compatibility views | `SYS.EXA_DBA_TABLES`, `SYS.EXA_DBA_VIEWS` |
| `pg_catalog.pg_settings` | compatibility view with common PostgreSQL session settings | synthetic |
| unsupported PostgreSQL catalog relations currently needed by clients | empty compatibility views with PostgreSQL-shaped columns | synthetic |
| `information_schema.schemata` | compatibility view | `PG_CATALOG.PG_NAMESPACE` |
| `information_schema.tables` / `views` | compatibility views | `SYS.EXA_DBA_TABLES`, `SYS.EXA_DBA_VIEWS` |
| `information_schema.columns` | compatibility view | `SYS.EXA_DBA_COLUMNS` |
| `information_schema.table_constraints` / `key_column_usage` / `referential_constraints` | compatibility views | `SYS.EXA_DBA_CONSTRAINTS`, `SYS.EXA_DBA_CONSTRAINT_COLUMNS` |
| `information_schema.triggers` | empty compatibility view | synthetic |

## Current Rules

* The PostgreSQL database concept is flattened to one logical catalog:
  `exasol`.
* Exasol schemas are exposed as PostgreSQL schemas.
* Unsupported PostgreSQL metadata that is not meaningful in Exasol returns an
  empty relation with the expected column layout instead of an object-not-found
  error.
* PostgreSQL helper functions and unqualified catalog relations are rewritten by
  the Exasol SQL preprocessor so clients can issue PostgreSQL-style metadata SQL
  directly.
* PostgreSQL type OIDs returned for column metadata are compatibility values
  derived from Exasol `COLUMN_TYPE`, not true PostgreSQL storage definitions.

## Known Limits

* PostgreSQL 18 documents far more `information_schema` views than are
  currently installed. The current Exasol compatibility layer covers only the
  core subset needed for common JDBC and DbVisualizer browsing paths:
  `schemata`, `tables`, `views`, `columns`, `table_constraints`,
  `key_column_usage`, `referential_constraints`, and `triggers`.
* The remaining PostgreSQL 18 `information_schema` views still need to be added
  as either mapped views or empty placeholders.
* Rich PostgreSQL type metadata such as arrays, domains, enums, collations,
  routines, privileges, and user-defined types are not fully represented yet.
