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
SPECIAL_CATALOG_OBJECTS_RE = re.compile(
    r"(?i)\bPG_CATALOG\.(PG_FOREIGN_SERVER|PG_FOREIGN_DATA_WRAPPER)\b"
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


def rewrite_pg_catalog(sql):
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
