#!/usr/bin/env python3
import pathlib


ROOT = pathlib.Path(__file__).resolve().parents[1]
PREPROCESSOR = ROOT / "sql" / "exasol_sql_preprocessor.sql"


def load_preprocessor_namespace():
    text = PREPROCESSOR.read_text(encoding="utf-8")
    marker = "CREATE OR REPLACE PYTHON3 PREPROCESSOR SCRIPT pg_demo.pg_sql_preprocessor AS\n"
    start = text.index(marker) + len(marker)
    end = text.rindex("\n/")
    namespace = {}
    exec(text[start:end], namespace)
    return namespace


def test_rewrites_postgres_qualified_operator_syntax():
    namespace = load_preprocessor_namespace()
    sql = """
SELECT a.attname
FROM PG_CATALOG.pg_attribute AS a
JOIN PG_CATALOG.pg_class AS c
  ON a.attrelid OPERATOR(PG_CATALOG.=) c.oid
WHERE a.attnum OPERATOR(PG_CATALOG.>) 0
"""
    translated = namespace["adapter_call"](sql)
    normalized = translated.upper()
    assert "OPERATOR(" not in translated.upper()
    assert "A.ATTRELID = C.OID" in normalized
    assert "A.ATTNUM > 0" in normalized


if __name__ == "__main__":
    test_rewrites_postgres_qualified_operator_syntax()
