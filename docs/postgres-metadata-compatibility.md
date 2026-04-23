# PostgreSQL Metadata Compatibility

This gateway does not try to recreate PostgreSQL storage internals inside
Exasol. Instead it maps the PostgreSQL metadata surfaces that common clients
actually query to Exasol system views and returns synthetic PostgreSQL-shaped
rows where needed.

Primary references:

* [Exasol metadata system tables](https://docs.exasol.com/db/latest/sql_references/system_tables/metadata_system_tables.htm)
* [PostgreSQL system catalogs](https://www.postgresql.org/docs/current/catalogs.html)
* DbVisualizer PostgreSQL profile:
  `C:/Program Files/DbVisualizer/resources/profiles/postgresql.xml`
* DbVisualizer Exasol profile:
  `C:/Program Files/DbVisualizer/resources/profiles/exasol.xml`

## Compatibility Matrix

| PostgreSQL surface | Client usage | Gateway handling | Exasol source |
| --- | --- | --- | --- |
| `pg_catalog.pg_database` | database list, startup metadata | synthetic single database named `exasol` | none |
| `pg_catalog.pg_namespace` | JDBC `getSchemas()` and schema browsing | live schemas plus synthetic `pg_catalog` and `information_schema` | `SYS.EXA_SCHEMAS` |
| JDBC `getTables()` query over `pg_namespace` + `pg_class` | schema browser table discovery | mapped directly to PostgreSQL-shaped rows | `SYS.EXA_ALL_TABLES`, `SYS.EXA_ALL_VIEWS` |
| JDBC `getColumns()` query over `pg_attribute` + `pg_type` | column browser, metadata panels | mapped directly to PostgreSQL-shaped rows with synthetic type OIDs/modifiers | `SYS.EXA_ALL_COLUMNS` |
| `pg_tables` | DbVisualizer tree and object actions | mapped view of Exasol tables | `SYS.EXA_ALL_TABLES` |
| `information_schema.tables` | DbVisualizer `getTableNamesFor()` | mapped table and view names | `SYS.EXA_ALL_TABLES`, `SYS.EXA_ALL_VIEWS` |
| `information_schema.columns` | DbVisualizer `getColumnNamesFor()` | mapped column names | `SYS.EXA_ALL_COLUMNS` |
| `pg_user` | DbVisualizer DBA panes | synthetic single `sys` user row | none |
| `pg_group` | DbVisualizer DBA panes | empty result with PostgreSQL column names | none |
| `pg_stat_activity` | DbVisualizer DBA panes | empty result with PostgreSQL column names | none |
| `pg_locks` | DbVisualizer DBA panes | empty result with PostgreSQL column names | none |
| `pg_catalog.pg_roles` | driver role inspection | synthetic `sys` superuser role | none |
| `pg_catalog.pg_settings` | driver/session inspection | synthetic settings rows for common PostgreSQL settings | none |

## Current Rules

* The PostgreSQL database concept is flattened to one logical catalog:
  `exasol`.
* Exasol schemas are exposed as PostgreSQL schemas.
* Unsupported PostgreSQL metadata that is not meaningful in Exasol returns an
  empty table with the expected column layout instead of a server error.
* PostgreSQL type OIDs returned for column metadata are compatibility values
  derived from Exasol `COLUMN_TYPE`, not true PostgreSQL storage definitions.

## Known Limits

* Projection-aware emulation is still narrow. The gateway currently targets the
  exact JDBC and DbVisualizer query shapes that have been observed.
* PostgreSQL catalog joins outside the implemented metadata probes are not
  generally emulated.
* Rich PostgreSQL type metadata such as arrays, domains, enums, collations,
  and typmods beyond the common scalar types are not fully represented yet.
