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


def classify_statement(sql: str) -> StatementDecision:
    keyword = first_keyword(sql)
    if not keyword:
        return StatementDecision(StatementCategory.EMPTY, True)
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
