use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataPlan {
    PgNamespace,
    JdbcSchemas,
    JdbcTables {
        schema_pattern: String,
        table_pattern: String,
    },
    JdbcColumns {
        schema_pattern: String,
        table_pattern: String,
        column_pattern: String,
    },
    PgTables {
        schema_exclude: Option<String>,
        table_name: Option<String>,
    },
    InfoSchemaTableNames {
        catalog: String,
        schema: String,
    },
    InfoSchemaColumnNames {
        catalog: String,
        schema: String,
        table: String,
    },
    PgUser,
    PgGroup,
    PgStatActivity,
    PgLocks,
}

static JDBC_SCHEMAS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)^select\s+nspname\s+as\s+"TABLE_SCHEM".*from\s+pg_catalog\.pg_namespace"#)
        .unwrap()
});
static JDBC_TABLES_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?is)^select\s+current_database\(\)\s+as\s+"TABLE_CAT".*from\s+pg_catalog\.pg_namespace\s+n,\s+pg_catalog\.pg_class\s+c"#,
    )
    .unwrap()
});
static JDBC_COLUMNS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?is)^select\s+\*\s+from\s+\(select\s+current_database\(\)\s+as\s+current_database.*join\s+pg_catalog\.pg_attribute\s+a"#,
    )
    .unwrap()
});
static PG_NAMESPACE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)^select\s+\*\s+from\s+pg_catalog\.pg_namespace\b"#).unwrap()
});
static PG_TABLES_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)^select\s+\*\s+from\s+pg_tables\b"#).unwrap());
static INFO_SCHEMA_TABLES_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)^select\s+table_name\s+from\s+information_schema\.tables\b"#).unwrap()
});
static INFO_SCHEMA_COLUMNS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)^select\s+column_name\s+from\s+information_schema\.columns\b"#).unwrap()
});
static PG_USER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)^select\s+\*\s+from\s+pg_user\b"#).unwrap());
static PG_GROUP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)^select\s+\*\s+from\s+pg_group\b"#).unwrap());
static PG_STAT_ACTIVITY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)^select\s+\*\s+from\s+pg_stat_activity\b"#).unwrap());
static PG_LOCKS_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)^select\s+\*\s+from\s+pg_locks\b"#).unwrap());

static NAMESPACE_LIKE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\bn(?:\.nspname)?\s+like\s+'((?:[^']|'')*)'"#).unwrap());
static RELNAME_LIKE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\bc(?:\.relname)?\s+like\s+'((?:[^']|'')*)'"#).unwrap());
static ATTNAME_LIKE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\battname\s+like\s+'((?:[^']|'')*)'"#).unwrap());
static SCHEMANAME_NE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\bschemaname\s*!=\s*'((?:[^']|'')*)'"#).unwrap());
static TABLENAME_EQ_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\btablename\s*=\s*'((?:[^']|'')*)'"#).unwrap());
static TABLE_CATALOG_EQ_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\btable_catalog\s*=\s*'((?:[^']|'')*)'"#).unwrap());
static TABLE_SCHEMA_EQ_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\btable_schema\s*=\s*'((?:[^']|'')*)'"#).unwrap());
static TABLE_NAME_EQ_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\btable_name\s*=\s*'((?:[^']|'')*)'"#).unwrap());

pub fn detect(sql: &str) -> Option<MetadataPlan> {
    let trimmed = sql.trim();
    if JDBC_COLUMNS_RE.is_match(trimmed) {
        return Some(MetadataPlan::JdbcColumns {
            schema_pattern: capture_literal(&NAMESPACE_LIKE_RE, trimmed)
                .unwrap_or_else(|| "%".to_owned()),
            table_pattern: capture_literal(&RELNAME_LIKE_RE, trimmed)
                .unwrap_or_else(|| "%".to_owned()),
            column_pattern: capture_literal(&ATTNAME_LIKE_RE, trimmed)
                .unwrap_or_else(|| "%".to_owned()),
        });
    }
    if JDBC_TABLES_RE.is_match(trimmed) {
        return Some(MetadataPlan::JdbcTables {
            schema_pattern: capture_literal(&NAMESPACE_LIKE_RE, trimmed)
                .unwrap_or_else(|| "%".to_owned()),
            table_pattern: capture_literal(&RELNAME_LIKE_RE, trimmed)
                .unwrap_or_else(|| "%".to_owned()),
        });
    }
    if JDBC_SCHEMAS_RE.is_match(trimmed) {
        return Some(MetadataPlan::JdbcSchemas);
    }
    if PG_NAMESPACE_RE.is_match(trimmed) {
        return Some(MetadataPlan::PgNamespace);
    }
    if PG_TABLES_RE.is_match(trimmed) {
        return Some(MetadataPlan::PgTables {
            schema_exclude: capture_literal(&SCHEMANAME_NE_RE, trimmed),
            table_name: capture_literal(&TABLENAME_EQ_RE, trimmed),
        });
    }
    if INFO_SCHEMA_TABLES_RE.is_match(trimmed) {
        return Some(MetadataPlan::InfoSchemaTableNames {
            catalog: capture_literal(&TABLE_CATALOG_EQ_RE, trimmed)
                .unwrap_or_else(|| "exasol".to_owned()),
            schema: capture_literal(&TABLE_SCHEMA_EQ_RE, trimmed).unwrap_or_default(),
        });
    }
    if INFO_SCHEMA_COLUMNS_RE.is_match(trimmed) {
        return Some(MetadataPlan::InfoSchemaColumnNames {
            catalog: capture_literal(&TABLE_CATALOG_EQ_RE, trimmed)
                .unwrap_or_else(|| "exasol".to_owned()),
            schema: capture_literal(&TABLE_SCHEMA_EQ_RE, trimmed).unwrap_or_default(),
            table: capture_literal(&TABLE_NAME_EQ_RE, trimmed).unwrap_or_default(),
        });
    }
    if PG_USER_RE.is_match(trimmed) {
        return Some(MetadataPlan::PgUser);
    }
    if PG_GROUP_RE.is_match(trimmed) {
        return Some(MetadataPlan::PgGroup);
    }
    if PG_STAT_ACTIVITY_RE.is_match(trimmed) {
        return Some(MetadataPlan::PgStatActivity);
    }
    if PG_LOCKS_RE.is_match(trimmed) {
        return Some(MetadataPlan::PgLocks);
    }
    None
}

fn capture_literal(regex: &Regex, sql: &str) -> Option<String> {
    regex
        .captures(sql)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str().replace("''", "'"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTypeInfo {
    pub oid: i32,
    pub typmod: i32,
    pub typlen: i16,
    pub typtype: &'static str,
    pub typbasetype: i32,
}

static DECIMAL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)^DECIMAL\((\d+),\s*(\d+)\)$"#).unwrap());
static VARCHAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)^VARCHAR\((\d+)\)"#).unwrap());
static CHAR_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?i)^CHAR\((\d+)\)"#).unwrap());
static TIMESTAMP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)^TIMESTAMP(?:\((\d+)\))?"#).unwrap());

pub fn map_exasol_column_type(exasol_type: &str) -> PgTypeInfo {
    let upper = exasol_type.trim().to_ascii_uppercase();
    if let Some(cap) = DECIMAL_RE.captures(&upper) {
        let precision = cap
            .get(1)
            .and_then(|m| m.as_str().parse::<i32>().ok())
            .unwrap_or(36);
        let scale = cap
            .get(2)
            .and_then(|m| m.as_str().parse::<i32>().ok())
            .unwrap_or(0);
        return PgTypeInfo {
            oid: 1700,
            typmod: ((precision << 16) | scale) + 4,
            typlen: -1,
            typtype: "b",
            typbasetype: 0,
        };
    }
    if let Some(cap) = VARCHAR_RE.captures(&upper) {
        let len = cap
            .get(1)
            .and_then(|m| m.as_str().parse::<i32>().ok())
            .unwrap_or(2000);
        return PgTypeInfo {
            oid: 1043,
            typmod: len + 4,
            typlen: -1,
            typtype: "b",
            typbasetype: 0,
        };
    }
    if let Some(cap) = CHAR_RE.captures(&upper) {
        let len = cap
            .get(1)
            .and_then(|m| m.as_str().parse::<i32>().ok())
            .unwrap_or(1);
        return PgTypeInfo {
            oid: 1042,
            typmod: len + 4,
            typlen: -1,
            typtype: "b",
            typbasetype: 0,
        };
    }
    if let Some(cap) = TIMESTAMP_RE.captures(&upper) {
        let precision = cap
            .get(1)
            .and_then(|m| m.as_str().parse::<i32>().ok())
            .unwrap_or(6);
        let oid = if upper.contains("WITH LOCAL TIME ZONE") || upper.contains("WITH TIME ZONE") {
            1184
        } else {
            1114
        };
        return PgTypeInfo {
            oid,
            typmod: precision + 4,
            typlen: 8,
            typtype: "b",
            typbasetype: 0,
        };
    }

    match upper.as_str() {
        "BOOLEAN" => PgTypeInfo {
            oid: 16,
            typmod: -1,
            typlen: 1,
            typtype: "b",
            typbasetype: 0,
        },
        "DATE" => PgTypeInfo {
            oid: 1082,
            typmod: -1,
            typlen: 4,
            typtype: "b",
            typbasetype: 0,
        },
        "DOUBLE" | "DOUBLE PRECISION" => PgTypeInfo {
            oid: 701,
            typmod: -1,
            typlen: 8,
            typtype: "b",
            typbasetype: 0,
        },
        "REAL" => PgTypeInfo {
            oid: 700,
            typmod: -1,
            typlen: 4,
            typtype: "b",
            typbasetype: 0,
        },
        "INTERVAL DAY TO SECOND" | "INTERVAL YEAR TO MONTH" | "INTERVAL" => PgTypeInfo {
            oid: 1186,
            typmod: -1,
            typlen: 16,
            typtype: "b",
            typbasetype: 0,
        },
        _ => PgTypeInfo {
            oid: 25,
            typmod: -1,
            typlen: -1,
            typtype: "b",
            typbasetype: 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_jdbc_tables_query() {
        let sql = r#"SELECT current_database() AS "TABLE_CAT", n.nspname AS "TABLE_SCHEM", c.relname AS "TABLE_NAME" FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c WHERE c.relname LIKE '%'"#;
        assert_eq!(
            detect(sql),
            Some(MetadataPlan::JdbcTables {
                schema_pattern: "%".to_owned(),
                table_pattern: "%".to_owned(),
            })
        );
    }

    #[test]
    fn detects_info_schema_columns_query() {
        let sql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = 'exasol' and TABLE_SCHEMA = 'PG_DEMO' and TABLE_NAME = 'ORDERS' order by COLUMN_NAME";
        assert_eq!(
            detect(sql),
            Some(MetadataPlan::InfoSchemaColumnNames {
                catalog: "exasol".to_owned(),
                schema: "PG_DEMO".to_owned(),
                table: "ORDERS".to_owned(),
            })
        );
    }

    #[test]
    fn maps_decimal_type_to_numeric() {
        assert_eq!(
            map_exasol_column_type("DECIMAL(18,4)"),
            PgTypeInfo {
                oid: 1700,
                typmod: ((18 << 16) | 4) + 4,
                typlen: -1,
                typtype: "b",
                typbasetype: 0,
            }
        );
    }

    #[test]
    fn maps_varchar_type_to_text_metadata() {
        assert_eq!(
            map_exasol_column_type("VARCHAR(200) UTF8"),
            PgTypeInfo {
                oid: 1043,
                typmod: 204,
                typlen: -1,
                typtype: "b",
                typbasetype: 0,
            }
        );
    }
}
