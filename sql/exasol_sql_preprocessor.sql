-- Prototype SQL preprocessor script for Exasol-side PostgreSQL-to-Exasol translation.
--
-- Install this only in an Exasol environment whose Python script language
-- includes sqlglot. The exact SQL preprocessor callback contract can vary by
-- Exasol version and deployment; keep the activation SQL configurable in
-- config/example.toml until the target system is fixed.

CREATE SCHEMA IF NOT EXISTS pg_demo;

ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL;

CREATE OR REPLACE PYTHON3 PREPROCESSOR SCRIPT pg_demo.pg_sql_preprocessor AS
import re

ILIKE_RE = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\"|CAST\([^)]+\))\s+ILIKE\s+('(''|[^'])*'|[A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\")",
    re.IGNORECASE,
)

def rewrite_ilike(sql):
    return ILIKE_RE.sub(
        lambda match: "UPPER({}) LIKE UPPER({})".format(match.group(1), match.group(2)),
        sql,
    )

def adapter_call(sql_statement):
    try:
        import sqlglot
        translated = sqlglot.transpile(sql_statement, read="postgres", write="exasol")[0]
        return rewrite_ilike(translated)
    except Exception as exc:
        raise Exception("PostgreSQL-to-Exasol SQL translation failed: " + str(exc))
/
