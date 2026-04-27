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


def test_rewrites_metabase_table_privileges_query():
    namespace = load_preprocessor_namespace()
    sql = """
with table_privileges as (
 select
   NULL as role,
   t.schemaname as schema,
   t.objectname as table,
   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'update') as update,
   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'select') as select,
   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'insert') as insert,
   pg_catalog.has_table_privilege(     current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'delete') as delete
 from (
   select schemaname, tablename as objectname from pg_catalog.pg_tables
   union
   select schemaname, viewname as objectname from pg_catalog.pg_views
   union
   select schemaname, matviewname as objectname from pg_catalog.pg_matviews
 ) t
 where t.schemaname !~ '^pg_'
   and t.schemaname <> 'information_schema'
   and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'usage')
)
select t.*
from table_privileges t
"""
    translated = namespace["adapter_call"](sql)
    assert "HAS_ANY_COLUMN_PRIVILEGE" not in translated.upper()
    assert 'AS "select"' in translated
    assert "SYS.EXA_ALL_TABLES" in translated
    assert "SYS.EXA_ALL_VIEWS" in translated


if __name__ == "__main__":
    test_rewrites_postgres_qualified_operator_syntax()
    test_rewrites_metabase_table_privileges_query()
