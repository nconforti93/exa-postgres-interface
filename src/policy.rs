use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementPlan {
    Read,
    ClientSet,
    ClientTransactionStart,
    ClientTransactionEnd {
        command: &'static str,
    },
    ClientShow {
        name: String,
        value: String,
    },
    ClientSelect {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
    Empty,
    Reject {
        sqlstate: &'static str,
        message: String,
    },
}

static COMMENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/|--[^\n\r]*").unwrap());
static SET_ASSIGN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^SET\s+(SESSION\s+)?[A-Za-z_][A-Za-z0-9_.]*\s*(=|TO)\s+").unwrap()
});
static RESET_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^RESET\s+(ALL|[A-Za-z_][A-Za-z0-9_.]*)$").unwrap());
static SHOW_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^SHOW\s+([A-Za-z_][A-Za-z0-9_ .-]*)$").unwrap());

pub fn classify_statement(sql: &str) -> StatementPlan {
    let cleaned = normalize_sql(sql);
    if cleaned.is_empty() {
        return StatementPlan::Empty;
    }

    if is_safe_set(&cleaned) || RESET_RE.is_match(&cleaned) {
        return StatementPlan::ClientSet;
    }

    if let Some(show) = local_show(&cleaned) {
        return show;
    }

    if let Some(select) = local_select(&cleaned) {
        return select;
    }

    let keyword = first_keyword(&cleaned);
    match keyword.as_str() {
        "SELECT" | "WITH" | "VALUES" => StatementPlan::Read,
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "TRUNCATE" | "COPY" | "IMPORT" | "EXPORT" => {
            StatementPlan::Reject {
                sqlstate: "0A000",
                message: "write statements are outside the first prototype scope".to_owned(),
            }
        }
        "CREATE" | "ALTER" | "DROP" | "RENAME" | "COMMENT" | "GRANT" | "REVOKE" => {
            StatementPlan::Reject {
                sqlstate: "0A000",
                message: "DDL statements are outside the first prototype scope".to_owned(),
            }
        }
        "BEGIN" | "START" => StatementPlan::ClientTransactionStart,
        "COMMIT" => StatementPlan::ClientTransactionEnd { command: "COMMIT" },
        "ROLLBACK" => StatementPlan::ClientTransactionEnd {
            command: "ROLLBACK",
        },
        "SAVEPOINT" | "RELEASE" => StatementPlan::ClientSet,
        "SET" | "RESET" | "SHOW" => StatementPlan::Reject {
            sqlstate: "0A000",
            message: "unsupported PostgreSQL session command".to_owned(),
        },
        other => StatementPlan::Reject {
            sqlstate: "0A000",
            message: format!("unsupported SQL statement class: {other}"),
        },
    }
}

fn normalize_sql(sql: &str) -> String {
    COMMENT_RE
        .replace_all(sql, " ")
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_owned()
}

fn first_keyword(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_start_matches('(')
        .to_ascii_uppercase()
}

fn is_safe_set(sql: &str) -> bool {
    if Regex::new(r"(?i)^SET\s+SESSION\s+CHARACTERISTICS\s+AS\s+TRANSACTION\b")
        .unwrap()
        .is_match(sql)
    {
        return true;
    }
    if Regex::new(r"(?i)^SET\s+(SESSION\s+)?TRANSACTION\b")
        .unwrap()
        .is_match(sql)
    {
        return false;
    }
    SET_ASSIGN_RE.is_match(sql)
}

fn local_show(sql: &str) -> Option<StatementPlan> {
    let cap = SHOW_RE.captures(sql)?;
    let raw_name = cap.get(1)?.as_str().trim();
    let key = raw_name.replace([' ', '-'], "_").to_ascii_lowercase();
    let value = match key.as_str() {
        "datestyle" => "ISO, YMD",
        "timezone" | "time_zone" => "Etc/UTC",
        "transaction_isolation" | "transaction_isolation_level" => "read committed",
        "transaction_read_only" => "on",
        "standard_conforming_strings" => "on",
        "client_encoding" => "UTF8",
        "server_version" => "16.6-exasol-gateway",
        "application_name" => "",
        "search_path" => "public",
        _ => return None,
    };
    Some(StatementPlan::ClientShow {
        name: raw_name.to_owned(),
        value: value.to_owned(),
    })
}

fn local_select(sql: &str) -> Option<StatementPlan> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("pg_catalog.pg_database") || lower.contains(" pg_database") {
        return Some(catalog_pg_database(sql));
    }
    if lower.contains("pg_catalog.pg_namespace") || lower.contains(" pg_namespace") {
        return Some(catalog_pg_namespace(sql));
    }
    if lower.contains("pg_catalog.pg_roles") || lower.contains(" pg_roles") {
        return Some(catalog_pg_roles(sql));
    }
    if lower.contains("pg_catalog.pg_settings") || lower.contains(" pg_settings") {
        return Some(catalog_pg_settings(sql));
    }
    if lower == "select version()" {
        return Some(single_value(
            "version",
            "PostgreSQL 16.6 compatible Exasol gateway",
        ));
    }
    if lower == "select current_database()" {
        return Some(single_value("current_database", "exasol"));
    }
    if lower == "select current_catalog" || lower == "select current_catalog()" {
        return Some(single_value("current_catalog", "exasol"));
    }
    if lower == "select current_schema()" {
        return Some(single_value("current_schema", "public"));
    }
    if lower == "select current_user" || lower == "select user" {
        return Some(single_value("current_user", "sys"));
    }
    None
}

fn single_value(name: &str, value: &str) -> StatementPlan {
    StatementPlan::ClientSelect {
        columns: vec![name.to_owned()],
        rows: vec![vec![Some(value.to_owned())]],
    }
}

fn catalog_pg_database(sql: &str) -> StatementPlan {
    catalog_response(
        sql,
        &[
            ("oid", "1"),
            ("datname", "exasol"),
            ("datdba", "10"),
            ("encoding", "6"),
            ("datlocprovider", "c"),
            ("datistemplate", "f"),
            ("datallowconn", "t"),
            ("datconnlimit", "-1"),
            ("datfrozenxid", "0"),
            ("datminmxid", "0"),
            ("dattablespace", "1663"),
            ("datcollate", "C.UTF-8"),
            ("datctype", "C.UTF-8"),
            ("datlocale", ""),
            ("daticurules", ""),
            ("datcollversion", ""),
            ("datacl", ""),
        ],
    )
}

fn catalog_pg_namespace(sql: &str) -> StatementPlan {
    catalog_response_many(
        sql,
        &[
            vec![
                ("oid", "2200"),
                ("nspname", "public"),
                ("nspowner", "10"),
                ("nspacl", ""),
            ],
            vec![
                ("oid", "11"),
                ("nspname", "pg_catalog"),
                ("nspowner", "10"),
                ("nspacl", ""),
            ],
            vec![
                ("oid", "13207"),
                ("nspname", "information_schema"),
                ("nspowner", "10"),
                ("nspacl", ""),
            ],
        ],
    )
}

fn catalog_pg_roles(sql: &str) -> StatementPlan {
    catalog_response(
        sql,
        &[
            ("oid", "10"),
            ("rolname", "sys"),
            ("rolsuper", "t"),
            ("rolinherit", "t"),
            ("rolcreaterole", "t"),
            ("rolcreatedb", "t"),
            ("rolcanlogin", "t"),
            ("rolreplication", "f"),
            ("rolconnlimit", "-1"),
            ("rolpassword", ""),
            ("rolvaliduntil", ""),
            ("rolbypassrls", "t"),
            ("rolconfig", ""),
        ],
    )
}

fn catalog_pg_settings(sql: &str) -> StatementPlan {
    catalog_response_many(
        sql,
        &[
            vec![
                ("name", "server_version"),
                ("setting", "16.6-exasol-gateway"),
            ],
            vec![("name", "client_encoding"), ("setting", "UTF8")],
            vec![("name", "standard_conforming_strings"), ("setting", "on")],
            vec![("name", "TimeZone"), ("setting", "Etc/UTC")],
        ],
    )
}

fn catalog_response(sql: &str, row: &[(&str, &str)]) -> StatementPlan {
    catalog_response_many(sql, &[row.to_vec()])
}

fn catalog_response_many(sql: &str, source_rows: &[Vec<(&str, &str)>]) -> StatementPlan {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("count(") {
        return StatementPlan::ClientSelect {
            columns: vec![select_alias(sql).unwrap_or_else(|| "count".to_owned())],
            rows: vec![vec![Some(source_rows.len().to_string())]],
        };
    }

    let projections =
        catalog_projection(sql, source_rows.first().map(Vec::as_slice).unwrap_or(&[]));
    let columns = projections
        .iter()
        .map(|projection| projection.output_name.clone())
        .collect::<Vec<_>>();
    let rows = source_rows
        .iter()
        .map(|row| {
            projections
                .iter()
                .map(|projection| {
                    row.iter()
                        .find(|(key, _)| key.eq_ignore_ascii_case(&projection.source_name))
                        .map(|(_, value)| (*value).to_owned())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    StatementPlan::ClientSelect { columns, rows }
}

#[derive(Debug)]
struct CatalogProjection {
    source_name: String,
    output_name: String,
}

fn catalog_projection(sql: &str, default_row: &[(&str, &str)]) -> Vec<CatalogProjection> {
    let Some(select_list) = select_list(sql) else {
        return default_projection(default_row);
    };
    if select_list.trim() == "*" || select_list.trim().ends_with(".*") {
        return default_projection(default_row);
    }

    split_select_items(select_list)
        .into_iter()
        .map(|item| projection_from_item(&item))
        .collect()
}

fn default_projection(default_row: &[(&str, &str)]) -> Vec<CatalogProjection> {
    default_row
        .iter()
        .map(|(name, _)| CatalogProjection {
            source_name: (*name).to_owned(),
            output_name: (*name).to_owned(),
        })
        .collect()
}

fn select_list(sql: &str) -> Option<&str> {
    let lower = sql.to_ascii_lowercase();
    let select_start = lower.find("select")? + "select".len();
    let from_start = lower[select_start..].find(" from ")? + select_start;
    let mut list = sql[select_start..from_start].trim();
    if list.to_ascii_lowercase().starts_with("distinct ") {
        list = list[8..].trim();
    }
    Some(list)
}

fn select_alias(sql: &str) -> Option<String> {
    split_select_items(select_list(sql)?)
        .into_iter()
        .next()
        .map(|item| projection_from_item(&item).output_name)
}

fn split_select_items(select_list: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut in_single_quote = false;
    for (idx, ch) in select_list.char_indices() {
        match ch {
            '\'' => in_single_quote = !in_single_quote,
            '(' if !in_single_quote => depth += 1,
            ')' if !in_single_quote => depth -= 1,
            ',' if !in_single_quote && depth == 0 => {
                items.push(select_list[start..idx].trim().to_owned());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    let tail = select_list[start..].trim();
    if !tail.is_empty() {
        items.push(tail.to_owned());
    }
    items
}

fn projection_from_item(item: &str) -> CatalogProjection {
    let (expr, alias) = split_alias(item);
    let source_name = source_column_name(expr);
    CatalogProjection {
        source_name: source_name.clone(),
        output_name: alias.unwrap_or(source_name),
    }
}

fn split_alias(item: &str) -> (&str, Option<String>) {
    static AS_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?i)^(.*?)\s+AS\s+"?([A-Za-z_][A-Za-z0-9_]*)"?$"#).unwrap());
    if let Some(cap) = AS_RE.captures(item) {
        return (
            cap.get(1).map(|m| m.as_str()).unwrap_or(item).trim(),
            cap.get(2).map(|m| m.as_str().to_owned()),
        );
    }
    (item, None)
}

fn source_column_name(expr: &str) -> String {
    let expr = expr.trim().trim_matches('"');
    let last = expr
        .rsplit('.')
        .next()
        .unwrap_or(expr)
        .trim()
        .trim_matches('"')
        .to_ascii_lowercase();
    if last.contains("datname") {
        "datname".to_owned()
    } else if last.contains("nspname") {
        "nspname".to_owned()
    } else if last.contains("rolname") {
        "rolname".to_owned()
    } else if last.contains("current_database") {
        "datname".to_owned()
    } else if last.contains("current_catalog") {
        "datname".to_owned()
    } else {
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_read_and_write() {
        assert_eq!(classify_statement("SELECT 1"), StatementPlan::Read);
        assert!(matches!(
            classify_statement("DELETE FROM t"),
            StatementPlan::Reject { .. }
        ));
    }

    #[test]
    fn handles_driver_session_commands_locally() {
        assert_eq!(
            classify_statement("SET extra_float_digits = 3"),
            StatementPlan::ClientSet
        );
        assert_eq!(
            classify_statement("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"),
            StatementPlan::ClientSet
        );
        assert!(matches!(
            classify_statement("SHOW transaction isolation level"),
            StatementPlan::ClientShow { .. }
        ));
    }

    #[test]
    fn handles_driver_selects_locally() {
        assert!(matches!(
            classify_statement("SELECT version()"),
            StatementPlan::ClientSelect { .. }
        ));
    }

    #[test]
    fn handles_pg_database_catalog_query_locally() {
        let plan = classify_statement(
            "SELECT d.datname AS table_cat FROM pg_catalog.pg_database d ORDER BY d.datname",
        );
        assert_eq!(
            plan,
            StatementPlan::ClientSelect {
                columns: vec!["table_cat".to_owned()],
                rows: vec![vec![Some("exasol".to_owned())]],
            }
        );
    }

    #[test]
    fn handles_pg_namespace_catalog_query_locally() {
        let plan =
            classify_statement("SELECT n.nspname AS table_schem FROM pg_catalog.pg_namespace n");
        assert!(matches!(
            plan,
            StatementPlan::ClientSelect { columns, rows }
                if columns == vec!["table_schem".to_owned()] && rows.iter().any(|row| row == &vec![Some("public".to_owned())])
        ));
    }

    #[test]
    fn handles_transaction_wrappers_locally() {
        assert_eq!(
            classify_statement("BEGIN"),
            StatementPlan::ClientTransactionStart
        );
        assert_eq!(
            classify_statement("COMMIT"),
            StatementPlan::ClientTransactionEnd { command: "COMMIT" }
        );
    }
}
