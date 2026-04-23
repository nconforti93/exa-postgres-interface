from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re


class StatementCategory(str, Enum):
    READ = "read"
    WRITE = "write"
    DDL = "ddl"
    TRANSACTION = "transaction"
    SESSION = "session"
    CLIENT_SESSION = "client_session"
    EMPTY = "empty"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class StatementDecision:
    category: StatementCategory
    allowed: bool
    reason: str = ""


READ_KEYWORDS = {"SELECT", "WITH", "VALUES"}
WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "COPY", "IMPORT", "EXPORT"}
DDL_KEYWORDS = {"CREATE", "ALTER", "DROP", "RENAME", "COMMENT", "GRANT", "REVOKE"}
TRANSACTION_KEYWORDS = {"BEGIN", "START", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"}
SESSION_KEYWORDS = {"SET", "RESET", "SHOW"}
CLIENT_SESSION_SHOW_NAMES = {
    "APPLICATION_NAME",
    "CLIENT_ENCODING",
    "CLIENT_MIN_MESSAGES",
    "DATESTYLE",
    "EXTRA_FLOAT_DIGITS",
    "INTERVALSTYLE",
    "SEARCH_PATH",
    "STANDARD_CONFORMING_STRINGS",
    "TIMEZONE",
}


def strip_sql_comments(sql: str) -> str:
    out: list[str] = []
    i = 0
    in_string = False
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""
        if ch == "'":
            out.append(ch)
            if in_string and nxt == "'":
                out.append(nxt)
                i += 2
                continue
            in_string = not in_string
            i += 1
            continue
        if not in_string and ch == "-" and nxt == "-":
            i = sql.find("\n", i)
            if i == -1:
                break
            continue
        if not in_string and ch == "/" and nxt == "*":
            end = sql.find("*/", i + 2)
            i = len(sql) if end == -1 else end + 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def first_keyword(sql: str) -> str:
    cleaned = strip_sql_comments(sql).strip().lstrip("(").strip()
    match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", cleaned)
    return match.group(1).upper() if match else ""


def normalized_sql(sql: str) -> str:
    return strip_sql_comments(sql).strip().rstrip(";").strip()


def classify_statement(sql: str) -> StatementDecision:
    cleaned = normalized_sql(sql)
    keyword = first_keyword(cleaned)
    if not keyword:
        return StatementDecision(StatementCategory.EMPTY, True)
    if is_client_session_command(cleaned):
        return StatementDecision(StatementCategory.CLIENT_SESSION, True)
    if keyword in READ_KEYWORDS:
        return StatementDecision(StatementCategory.READ, True)
    if keyword in WRITE_KEYWORDS:
        return StatementDecision(
            StatementCategory.WRITE,
            False,
            "write statements are outside the first prototype scope",
        )
    if keyword in DDL_KEYWORDS:
        return StatementDecision(
            StatementCategory.DDL,
            False,
            "DDL statements are outside the first prototype scope",
        )
    if keyword in TRANSACTION_KEYWORDS:
        return StatementDecision(
            StatementCategory.TRANSACTION,
            False,
            "transaction compatibility is not implemented",
        )
    if keyword in SESSION_KEYWORDS:
        return StatementDecision(
            StatementCategory.SESSION,
            False,
            "session commands are not implemented by the prototype",
        )
    return StatementDecision(
        StatementCategory.UNSUPPORTED,
        False,
        f"unsupported SQL statement class: {keyword}",
    )


def is_client_session_command(sql: str) -> bool:
    if re.match(r"^SET\s+SESSION\s+CHARACTERISTICS\s+AS\s+TRANSACTION\b", sql, re.IGNORECASE):
        return True
    if re.match(r"^SET\s+(SESSION\s+)?(?!TRANSACTION\b)[A-Za-z_][A-Za-z0-9_.]*\s*=", sql, re.IGNORECASE):
        return True
    if re.match(r"^SET\s+(SESSION\s+)?[A-Za-z_][A-Za-z0-9_.]*\s+TO\s+", sql, re.IGNORECASE):
        return True
    if re.match(r"^RESET\s+(ALL|[A-Za-z_][A-Za-z0-9_.]*)$", sql, re.IGNORECASE):
        return True

    show_match = re.match(r"^SHOW\s+([A-Za-z_][A-Za-z0-9_.]*)$", sql, re.IGNORECASE)
    if show_match:
        return show_match.group(1).replace(".", "_").upper() in CLIENT_SESSION_SHOW_NAMES
    return False
