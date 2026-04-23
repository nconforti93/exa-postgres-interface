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
CURRENT_DATABASE_RE = re.compile(r"(?i)\bcurrent_database\s*\(\s*\)")
CURRENT_CATALOG_RE = re.compile(r"(?i)\bcurrent_catalog(?:\s*\(\s*\))?")
CURRENT_SCHEMAS_FIRST_RE = re.compile(
    r"(?i)\(?\s*(?:pg_catalog\.)?current_schemas\s*\(\s*true\s*\)\s*\)?\s*\[\s*1\s*\]"
)
QUALIFIED_PG_CATALOG_RE = re.compile(r"(?i)\bpg_catalog\.")
QUALIFIED_INFO_SCHEMA_RE = re.compile(r"(?i)\binformation_schema\.")
OBJ_DESCRIPTION_RE = re.compile(
    r"(?is)(?:pg_catalog\.)?obj_description\s*\(\s*([^,]+?)\s*,\s*'(pg_namespace|pg_class)'\s*\)"
)
REGCLASS_LITERAL_RE = re.compile(
    r"(?i)'((?:pg_catalog\.)?[A-Za-z_][A-Za-z0-9_]*)'\s*::\s*regclass"
)
REGEX_NOT_MATCH_RE = re.compile(
    r"(?i)([A-Za-z_][A-Za-z0-9_.]*|\"[^\"]+\")\s*!~\s*('(?:''|[^'])*')"
)
REGEX_MATCH_RE = re.compile(
    r"(?i)([A-Za-z_][A-Za-z0-9_.]*|\"[^\"]+\")\s*~\s*('(?:''|[^'])*')"
)

CATALOG_RELATIONS = [
    "pg_attrdef",
    "pg_attribute",
    "pg_class",
    "pg_constraint",
    "pg_database",
    "pg_depend",
    "pg_description",
    "pg_foreign_server",
    "pg_foreign_table",
    "pg_group",
    "pg_index",
    "pg_locks",
    "pg_matviews",
    "pg_namespace",
    "pg_proc",
    "pg_rewrite",
    "pg_roles",
    "pg_rules",
    "pg_sequence",
    "pg_settings",
    "pg_stat_activity",
    "pg_tables",
    "pg_tablespace",
    "pg_trigger",
    "pg_type",
    "pg_user",
    "pg_views",
]

FUNCTION_REPLACEMENTS = {
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_userbyid\s*\("): "PG_CATALOG.PG_GET_USERBYID(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_expr\s*\("): "PG_CATALOG.PG_GET_EXPR(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_constraintdef\s*\("): "PG_CATALOG.PG_GET_CONSTRAINTDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_indexdef\s*\("): "PG_CATALOG.PG_GET_INDEXDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_stat_get_blocks_fetched\s*\("): "PG_CATALOG.PG_STAT_GET_BLOCKS_FETCHED(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_stat_get_blocks_hit\s*\("): "PG_CATALOG.PG_STAT_GET_BLOCKS_HIT(",
}
REGCLASS_OIDS = {
    "pg_class": "1259",
    "pg_database": "1262",
    "pg_namespace": "2615",
    "pg_proc": "1255",
    "pg_type": "1247",
}

JOIN_PREFIX_RE = re.compile(
    r"(?i)(\bfrom\s+|\bjoin\s+|\bleft\s+join\s+|\bright\s+join\s+|\binner\s+join\s+|\bfull\s+join\s+|\bcross\s+join\s+|,\s*)(%s)\b"
    % "|".join(CATALOG_RELATIONS)
)


def rewrite_catalog_relations(sql):
    def repl(match):
        prefix = match.group(1)
        relation = match.group(2)
        return "{}PG_CATALOG.{}".format(prefix, relation.upper())

    return JOIN_PREFIX_RE.sub(repl, sql)


def rewrite_object_description(sql):
    def repl(match):
        obj_expr = match.group(1).strip()
        obj_type = match.group(2).lower()
        classoid = "2615" if obj_type == "pg_namespace" else "1259"
        return (
            "(SELECT D.DESCRIPTION FROM PG_CATALOG.PG_DESCRIPTION D "
            "WHERE D.OBJOID = {obj_expr} AND D.CLASSOID = {classoid} AND D.OBJSUBID = 0)"
        ).format(obj_expr=obj_expr, classoid=classoid)

    return OBJ_DESCRIPTION_RE.sub(repl, sql)


def rewrite_regex_operators(sql):
    sql = REGEX_NOT_MATCH_RE.sub(
        lambda match: "REGEXP_INSTR({}, {}) = 0".format(match.group(1), match.group(2)),
        sql,
    )
    sql = REGEX_MATCH_RE.sub(
        lambda match: "REGEXP_INSTR({}, {}) > 0".format(match.group(1), match.group(2)),
        sql,
    )
    return sql


def rewrite_regclass_literals(sql):
    def repl(match):
        name = match.group(1).split(".")[-1].lower()
        return REGCLASS_OIDS.get(name, "0")

    return REGCLASS_LITERAL_RE.sub(repl, sql)


def rewrite_pg_catalog(sql):
    sql = rewrite_object_description(sql)
    sql = CURRENT_DATABASE_RE.sub("'exasol'", sql)
    sql = CURRENT_CATALOG_RE.sub("'exasol'", sql)
    sql = CURRENT_SCHEMAS_FIRST_RE.sub("'PG_CATALOG'", sql)
    sql = QUALIFIED_PG_CATALOG_RE.sub("PG_CATALOG.", sql)
    sql = QUALIFIED_INFO_SCHEMA_RE.sub("INFORMATION_SCHEMA.", sql)
    sql = rewrite_catalog_relations(sql)
    sql = rewrite_regex_operators(sql)
    sql = rewrite_regclass_literals(sql)
    for pattern, replacement in FUNCTION_REPLACEMENTS.items():
        sql = pattern.sub(replacement, sql)
    return sql

def rewrite_ilike(sql):
    return ILIKE_RE.sub(
        lambda match: "UPPER({}) LIKE UPPER({})".format(match.group(1), match.group(2)),
        sql,
    )

def adapter_call(sql_statement):
    try:
        import sqlglot
        rewritten = rewrite_pg_catalog(sql_statement)
        translated = sqlglot.transpile(rewritten, read="postgres", write="exasol")[0]
        return rewrite_ilike(translated)
    except Exception as exc:
        raise Exception("PostgreSQL-to-Exasol SQL translation failed: " + str(exc))
/
