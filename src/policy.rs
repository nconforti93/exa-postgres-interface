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
    if lower == "select version()" {
        return Some(single_value(
            "version",
            "PostgreSQL 16.6 compatible Exasol gateway",
        ));
    }
    if lower == "select current_database()" {
        return Some(single_value("current_database", "exasol"));
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
