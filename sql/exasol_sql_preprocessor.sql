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
PG_IDENTIFY_OBJECT_IDENTITY_RE = re.compile(
    r"(?is)\(\s*(?:pg_catalog\.)?pg_identify_object\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)\s*\)\s*\.\s*identity\b"
)
PG_GET_VIEWDEF_PRETTY_RE = re.compile(
    r"(?is)(?:pg_catalog\.)?pg_get_viewdef\s*\(\s*([^,]+?)\s*,\s*(?:true|false)\s*\)"
)
PG_GET_EXPR_PRETTY_RE = re.compile(
    r"(?is)(?:pg_catalog\.)?pg_get_expr\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*(?:true|false)\s*\)"
)
PG_GET_CONSTRAINTDEF_PRETTY_RE = re.compile(
    r"(?is)(?:pg_catalog\.)?pg_get_constraintdef\s*\(\s*([^,]+?)\s*,\s*(?:true|false)\s*\)"
)
SPECIAL_CATALOG_OBJECTS_RE = re.compile(
    r"(?i)\bPG_CATALOG\.(PG_FOREIGN_SERVER|PG_FOREIGN_DATA_WRAPPER)\b"
)
QUALIFIED_OPERATOR_RE = re.compile(
    r"(?i)\s+OPERATOR\s*\(\s*(?:PG_CATALOG|pg_catalog)\s*\.\s*(<>|!=|<=|>=|=|<|>)\s*\)\s*"
)

CATALOG_RELATIONS = [
    # GENERATED_CATALOG_RELATIONS_START
    "pg_aggregate",
    "pg_am",
    "pg_amop",
    "pg_amproc",
    "pg_available_extensions",
    "pg_attrdef",
    "pg_attribute",
    "pg_auth_members",
    "pg_authid",
    "pg_cast",
    "pg_class",
    "pg_collation",
    "pg_constraint",
    "pg_conversion",
    "pg_database",
    "pg_db_role_setting",
    "pg_default_acl",
    "pg_depend",
    "pg_description",
    "pg_enum",
    "pg_event_trigger",
    "pg_extension",
    "pg_foreign_data_wrapper",
    "pg_foreign_server",
    "pg_foreign_table",
    "pg_group",
    "pg_index",
    "pg_inherits",
    "pg_init_privs",
    "pg_language",
    "pg_largeobject",
    "pg_largeobject_metadata",
    "pg_locks",
    "pg_matviews",
    "pg_namespace",
    "pg_opclass",
    "pg_operator",
    "pg_opfamily",
    "pg_parameter_acl",
    "pg_partitioned_table",
    "pg_policy",
    "pg_proc",
    "pg_publication",
    "pg_publication_namespace",
    "pg_publication_rel",
    "pg_range",
    "pg_replication_origin",
    "pg_rewrite",
    "pg_roles",
    "pg_rules",
    "pg_sequences",
    "pg_seclabel",
    "pg_sequence",
    "pg_settings",
    "pg_shdepend",
    "pg_shdescription",
    "pg_shseclabel",
    "pg_stat_activity",
    "pg_statistic",
    "pg_statistic_ext",
    "pg_statistic_ext_data",
    "pg_subscription",
    "pg_subscription_rel",
    "pg_tables",
    "pg_tablespace",
    "pg_transform",
    "pg_trigger",
    "pg_ts_config",
    "pg_ts_config_map",
    "pg_ts_dict",
    "pg_ts_parser",
    "pg_ts_template",
    "pg_type",
    "pg_user",
    "pg_user_mapping",
    "pg_user_mappings",
    "pg_views",
    # GENERATED_CATALOG_RELATIONS_END
]

FUNCTION_REPLACEMENTS = {
    re.compile(r"(?i)(?<![\w.\"])\bformat_type\s*\("): "PG_CATALOG.FORMAT_TYPE(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_identify_object\s*\("): "PG_CATALOG.PG_IDENTIFY_OBJECT(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_functiondef\s*\("): "PG_CATALOG.PG_GET_FUNCTIONDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_userbyid\s*\("): "PG_CATALOG.PG_GET_USERBYID(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_expr\s*\("): "PG_CATALOG.PG_GET_EXPR(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_constraintdef\s*\("): "PG_CATALOG.PG_GET_CONSTRAINTDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_indexdef\s*\("): "PG_CATALOG.PG_GET_INDEXDEF(",
    re.compile(r"(?i)(?<![\w.\"])\boidvectortypes\s*\("): "PG_CATALOG.OIDVECTORTYPES(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_partkeydef\s*\("): "PG_CATALOG.PG_GET_PARTKEYDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_ruledef\s*\("): "PG_CATALOG.PG_GET_RULEDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_triggerdef\s*\("): "PG_CATALOG.PG_GET_TRIGGERDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_get_viewdef\s*\("): "PG_CATALOG.PG_GET_VIEWDEF(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_encoding_to_char\s*\("): "PG_CATALOG.PG_ENCODING_TO_CHAR(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_total_relation_size\s*\("): "PG_CATALOG.PG_TOTAL_RELATION_SIZE(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_relation_size\s*\("): "PG_CATALOG.PG_RELATION_SIZE(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_stat_get_numscans\s*\("): "PG_CATALOG.PG_STAT_GET_NUMSCANS(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_stat_get_blocks_fetched\s*\("): "PG_CATALOG.PG_STAT_GET_BLOCKS_FETCHED(",
    re.compile(r"(?i)(?<![\w.\"])\bpg_stat_get_blocks_hit\s*\("): "PG_CATALOG.PG_STAT_GET_BLOCKS_HIT(",
    re.compile(r"(?i)(?<![\w.\"])\bto_regclass\s*\("): "PG_CATALOG.TO_REGCLASS(",
    re.compile(r"(?i)(?<![\w.\"])\bshobj_description\s*\("): "PG_CATALOG.SHOBJ_DESCRIPTION(",
    re.compile(r"(?i)(?<![\w.\"])\bhas_schema_privilege\s*\("): "PG_CATALOG.HAS_SCHEMA_PRIVILEGE(",
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

def rewrite_known_metadata_query(sql):
    normalized = " ".join(sql.split()).lower()
    if (
        normalized.startswith("with table_privileges as (")
        and "has_any_column_privilege" in normalized
        and "has_table_privilege" in normalized
        and "from table_privileges" in normalized
    ):
        return """
SELECT
    CAST(NULL AS VARCHAR(128)) AS "role",
    object_schema AS "schema",
    object_name AS "table",
    TRUE AS "update",
    TRUE AS "select",
    TRUE AS "insert",
    TRUE AS "delete"
FROM (
    SELECT TABLE_SCHEMA AS object_schema, TABLE_NAME AS object_name
    FROM SYS.EXA_ALL_TABLES
    UNION
    SELECT VIEW_SCHEMA AS object_schema, VIEW_NAME AS object_name
    FROM SYS.EXA_ALL_VIEWS
) t
WHERE LOWER(object_schema) NOT LIKE 'pg\\_%'
  AND LOWER(object_schema) <> 'information_schema'
"""
    if (
        "from pg_constraint" in normalized
        and "lateral unnest" in normalized
        and "array_agg(col.attname" in normalized
    ):
        schema_filter = "1 = 1"
        table_filter = "1 = 1"
        schema_match = re.search(r"(?is)\bsch\.nspname\s+LIKE\s+('(?:''|[^'])*')", sql)
        table_match = re.search(r"(?is)\btbl\.relname\s+LIKE\s+('(?:''|[^'])*')", sql)
        if schema_match:
            schema_filter = "C.CONSTRAINT_SCHEMA LIKE {}".format(schema_match.group(1))
        if table_match:
            table_filter = "C.CONSTRAINT_TABLE LIKE {}".format(table_match.group(1))
        return """
WITH CONSTRAINT_COLUMNS AS (
    SELECT
        CONSTRAINT_SCHEMA,
        CONSTRAINT_TABLE,
        CONSTRAINT_NAME,
        GROUP_CONCAT(COLUMN_NAME ORDER BY COALESCE(ORDINAL_POSITION, 0) SEPARATOR ',') AS COLUMNS,
        MAX(REFERENCED_SCHEMA) AS FOREIGN_SCHEMA_NAME,
        MAX(REFERENCED_TABLE) AS FOREIGN_TABLE_NAME,
        GROUP_CONCAT(REFERENCED_COLUMN ORDER BY COALESCE(ORDINAL_POSITION, 0) SEPARATOR ',') AS FOREIGN_COLUMNS
    FROM SYS.EXA_DBA_CONSTRAINT_COLUMNS
    GROUP BY CONSTRAINT_SCHEMA, CONSTRAINT_TABLE, CONSTRAINT_NAME
)
SELECT
    C.CONSTRAINT_NAME AS constraint_name,
    CASE
        WHEN C.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'Primary Key'
        WHEN C.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'Foreign Key'
        WHEN C.CONSTRAINT_TYPE = 'NOT NULL' THEN 'Check'
        ELSE C.CONSTRAINT_TYPE
    END AS constraint_type,
    C.CONSTRAINT_SCHEMA AS "schema_name",
    C.CONSTRAINT_TABLE AS "table_name",
    CC.COLUMNS AS "columns",
    CC.FOREIGN_SCHEMA_NAME AS "foreign_schema_name",
    CC.FOREIGN_TABLE_NAME AS "foreign_table_name",
    CC.FOREIGN_COLUMNS AS "foreign_columns",
    CASE
        WHEN C.CONSTRAINT_TYPE = 'PRIMARY KEY'
            THEN 'PRIMARY KEY (' || COALESCE(CC.COLUMNS, '') || ')'
        WHEN C.CONSTRAINT_TYPE = 'FOREIGN KEY'
            THEN 'FOREIGN KEY (' || COALESCE(CC.COLUMNS, '') || ') REFERENCES '
                 || COALESCE(CC.FOREIGN_SCHEMA_NAME, '') || '.'
                 || COALESCE(CC.FOREIGN_TABLE_NAME, '') || '('
                 || COALESCE(CC.FOREIGN_COLUMNS, '') || ')'
        WHEN C.CONSTRAINT_TYPE = 'NOT NULL'
            THEN COALESCE(CC.COLUMNS, '') || ' IS NOT NULL'
        ELSE C.CONSTRAINT_TYPE
    END AS definition
FROM SYS.EXA_DBA_CONSTRAINTS C
LEFT JOIN CONSTRAINT_COLUMNS CC
  ON CC.CONSTRAINT_SCHEMA = C.CONSTRAINT_SCHEMA
 AND CC.CONSTRAINT_TABLE = C.CONSTRAINT_TABLE
 AND CC.CONSTRAINT_NAME = C.CONSTRAINT_NAME
WHERE {schema_filter}
  AND {table_filter}
ORDER BY "schema_name", "table_name"
""".format(schema_filter=schema_filter, table_filter=table_filter)

    if (
        "from pg_catalog.pg_trigger" in normalized
        and "information_schema.triggers" in normalized
        and "array_agg(" in normalized
    ):
        return """
SELECT
    trigger_name AS "Trigger Name",
    trigger_catalog AS "Trigger Catalog",
    trigger_schema AS "Trigger Schema",
    CAST(NULL AS VARCHAR(2000000)) AS "Event Manipulation",
    action_orientation AS "Action Orientation",
    action_condition AS "Action Condition",
    action_statement AS "Action Statement",
    CAST(NULL AS VARCHAR(2000000)) AS "Procedure Name",
    CAST(NULL AS DECIMAL(18,0)) AS "proc_oid",
    action_timing AS "Condition Timing",
    event_object_catalog AS "Event Object Catalog",
    event_object_schema AS "Event Object Schema",
    event_object_table AS "Event Object Table",
    action_reference_old_table AS "Action ref Old Table",
    action_reference_new_table AS "Action ref New Table",
    CAST(NULL AS VARCHAR(32)) AS "Status"
FROM information_schema.triggers
WHERE 1 = 0
"""
    return sql


def rewrite_catalog_relations(sql):
    def repl(match):
        prefix = match.group(1)
        relation = match.group(2)
        upper_relation = relation.upper()
        if relation.lower() in {"pg_foreign_server", "pg_foreign_data_wrapper"}:
            return '{}PG_CATALOG."{}"'.format(prefix, upper_relation)
        return "{}PG_CATALOG.{}".format(prefix, upper_relation)

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


def rewrite_qualified_operators(sql):
    return QUALIFIED_OPERATOR_RE.sub(lambda match: " {} ".format(match.group(1)), sql)


def rewrite_pg_catalog(sql):
    sql = rewrite_known_metadata_query(sql)
    sql = rewrite_qualified_operators(sql)
    sql = rewrite_object_description(sql)
    sql = PG_IDENTIFY_OBJECT_IDENTITY_RE.sub(
        lambda match: "PG_CATALOG.PG_IDENTIFY_OBJECT({}, {}, {})".format(
            match.group(1).strip(),
            match.group(2).strip(),
            match.group(3).strip(),
        ),
        sql,
    )
    sql = PG_GET_VIEWDEF_PRETTY_RE.sub(
        lambda match: "PG_CATALOG.PG_GET_VIEWDEF({})".format(match.group(1).strip()),
        sql,
    )
    sql = PG_GET_EXPR_PRETTY_RE.sub(
        lambda match: "PG_CATALOG.PG_GET_EXPR({}, {})".format(
            match.group(1).strip(),
            match.group(2).strip(),
        ),
        sql,
    )
    sql = PG_GET_CONSTRAINTDEF_PRETTY_RE.sub(
        lambda match: "PG_CATALOG.PG_GET_CONSTRAINTDEF({})".format(match.group(1).strip()),
        sql,
    )
    sql = CURRENT_DATABASE_RE.sub("'exasol'", sql)
    sql = CURRENT_CATALOG_RE.sub("'exasol'", sql)
    sql = CURRENT_SCHEMAS_FIRST_RE.sub("'PG_CATALOG'", sql)
    sql = QUALIFIED_PG_CATALOG_RE.sub("PG_CATALOG.", sql)
    sql = QUALIFIED_INFO_SCHEMA_RE.sub("INFORMATION_SCHEMA.", sql)
    sql = rewrite_catalog_relations(sql)
    sql = SPECIAL_CATALOG_OBJECTS_RE.sub(
        lambda match: 'PG_CATALOG."{}"'.format(match.group(1).upper()),
        sql,
    )
    sql = rewrite_regex_operators(sql)
    sql = rewrite_regclass_literals(sql)
    for pattern, replacement in FUNCTION_REPLACEMENTS.items():
        sql = pattern.sub(replacement, sql)
    return sql

def rewrite_sqlglot_edge_cases(sql):
    sql = sql.replace(
        'PG_CATALOG."PG_FOREIGN_SERVER" AS fs',
        'PG_CATALOG."PG_FOREIGN_SERVER" AS srv',
    )
    if 'PG_CATALOG."PG_FOREIGN_SERVER" AS srv' in sql:
        sql = re.sub(r"(?i)\bfs\.", "srv.", sql)
    sql = sql.replace(
        "ARRAY_AGG(CAST(event_manipulation AS LONG VARCHAR))",
        "LISTAGG(CAST(event_manipulation AS VARCHAR(2000000)), ', ') WITHIN GROUP (ORDER BY event_manipulation)",
    )
    sql = sql.replace(
        " WHERE p.prorettype <> CAST('PG_CATALOG.cstring' AS PG_CATALOG.regtype) AND (p.proargtypes[-1] IS NULL OR p.proargtypes[-1] <> CAST('PG_CATALOG.cstring' AS PG_CATALOG.regtype)) AND",
        " WHERE",
    )
    sql = sql.replace(
        " WHERE p.prorettype <> CAST('PG_CATALOG.cstring' AS PG_CATALOG.regtype) AND (p.proargtypes[-1] IS NULL OR p.proargtypes[-1] <> CAST('PG_CATALOG.cstring' AS PG_CATALOG.regtype))",
        " WHERE 1 = 1",
    )
    sql = sql.replace(
        "CASE p.proargtypes[-1] WHEN CAST('PG_CATALOG.\"any\"' AS PG_CATALOG.regtype) THEN CAST('(all types)' AS PG_CATALOG.text) ELSE PG_CATALOG.format_type(p.proargtypes[-1], NULL) END",
        "PG_CATALOG.OIDVECTORTYPES(p.proargtypes)",
    )
    sql = sql.replace(
        "(t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM PG_CATALOG.pg_class AS c WHERE c.oid = t.typrelid))",
        "(t.typrelid = 0)",
    )
    sql = sql.replace(
        "ON (C.TABLE_CATALOG, C.TABLE_SCHEMA, C.TABLE_NAME, 'TABLE', C.DTD_IDENTIFIER) = (E.OBJECT_CATALOG, E.OBJECT_SCHEMA, E.OBJECT_NAME, E.OBJECT_TYPE, E.DTD_IDENTIFIER)",
        "ON C.TABLE_CATALOG = E.OBJECT_CATALOG AND C.TABLE_SCHEMA = E.OBJECT_SCHEMA AND C.TABLE_NAME = E.OBJECT_NAME AND E.OBJECT_TYPE = 'TABLE' AND C.DTD_IDENTIFIER = E.DTD_IDENTIFIER",
    )
    sql = sql.replace(
        "ON (C.TABLE_CATALOG, C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, 'column_name') = (CO.TABLE_CATALOG, CO.TABLE_SCHEMA, CO.TABLE_NAME, CO.COLUMN_NAME, CO.OPTION_NAME)",
        "ON C.TABLE_CATALOG = CO.TABLE_CATALOG AND C.TABLE_SCHEMA = CO.TABLE_SCHEMA AND C.TABLE_NAME = CO.TABLE_NAME AND C.COLUMN_NAME = CO.COLUMN_NAME AND CO.OPTION_NAME = 'column_name'",
    )
    sql = sql.replace(
        "CO.OPTION_VALUE AS COLUMN_OPTION, C.ORDINAL_POSITION, C.IS_IDENTITY",
        "CO.OPTION_VALUE AS COLUMN_OPTION, C.ORDINAL_POSITION AS ORDINAL_POSITION_DUP, C.IS_IDENTITY",
    )
    return sql

def rewrite_ilike(sql):
    return ILIKE_RE.sub(
        lambda match: "UPPER({}) LIKE UPPER({})".format(match.group(1), match.group(2)),
        sql,
    )

def adapter_call(sql_statement):
    try:
        known_metadata_query = rewrite_known_metadata_query(sql_statement)
        if known_metadata_query != sql_statement:
            return rewrite_ilike(known_metadata_query)
        import sqlglot
        rewritten = rewrite_pg_catalog(sql_statement)
        translated = sqlglot.transpile(rewritten, read="postgres", write="exasol")[0]
        translated = rewrite_sqlglot_edge_cases(translated)
        return rewrite_ilike(translated)
    except Exception as exc:
        raise Exception("PostgreSQL-to-Exasol SQL translation failed: " + str(exc))
/
