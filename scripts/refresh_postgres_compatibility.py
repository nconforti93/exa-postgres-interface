#!/usr/bin/env python3

import argparse
import re
import ssl
from pathlib import Path

import bs4
import pyexasol
import requests

ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "postgres_catalog_compatibility.sql"
PREPROCESSOR_PATH = ROOT / "sql" / "exasol_sql_preprocessor.sql"

PG_INFO_TOC_URL = "https://www.postgresql.org/docs/18/information-schema.html"
PG_CATALOG_TOC_URL = "https://www.postgresql.org/docs/18/catalogs.html"
DOC_BASE = "https://www.postgresql.org/docs/18/"

SQL_MARKER_START = "-- GENERATED_PLACEHOLDER_VIEWS_START"
SQL_MARKER_END = "-- GENERATED_PLACEHOLDER_VIEWS_END"
PREPROCESSOR_MARKER_START = "    # GENERATED_CATALOG_RELATIONS_START"
PREPROCESSOR_MARKER_END = "    # GENERATED_CATALOG_RELATIONS_END"


def main() -> None:
    args = parse_args()
    sql_text = SQL_PATH.read_text()
    sql_prefix, _, sql_suffix = partition_marked_block(
        sql_text, SQL_MARKER_START, SQL_MARKER_END
    )

    info_objects = scrape_toc(PG_INFO_TOC_URL, section_prefix="35", min_section=3)
    catalog_objects = scrape_toc(PG_CATALOG_TOC_URL, section_prefix="52", min_section=2)
    object_docs = {
        ("PG_CATALOG", name): href for name, href in catalog_objects
    } | {
        ("INFORMATION_SCHEMA", name): href for name, href in info_objects
    }

    manual_views = extract_manual_view_bodies(sql_prefix)
    if args.dsn:
        base_columns = fetch_base_columns(
            args.dsn,
            args.user,
            args.password,
            args.schema,
            args.validate_certificate,
            manual_views,
        )
        sql_prefix = rewrite_manual_views(sql_prefix, manual_views, base_columns, object_docs)
        sql_text = sql_prefix + sql_text[sql_text.index(SQL_MARKER_START):]
        sql_prefix, _, sql_suffix = partition_marked_block(
            sql_text, SQL_MARKER_START, SQL_MARKER_END
        )

    implemented = existing_views(sql_prefix)

    generated_views: list[str] = []
    for object_name, href in catalog_objects:
        key = ("PG_CATALOG", object_name)
        if key not in implemented:
            generated_views.append(
                generate_empty_view_sql("PG_CATALOG", object_name, scrape_columns(href))
            )
    for object_name, href in info_objects:
        key = ("INFORMATION_SCHEMA", object_name)
        if key not in implemented:
            generated_views.append(
                generate_empty_view_sql(
                    "INFORMATION_SCHEMA", object_name, scrape_columns(href)
                )
            )

    generated_block = (
        f"{SQL_MARKER_START}\n"
        "-- Generated from PostgreSQL 18 catalog and information schema documentation.\n\n"
        + "\n\n".join(generated_views)
        + "\n"
        + SQL_MARKER_END
    )
    new_sql_text = sql_prefix + generated_block + sql_suffix
    SQL_PATH.write_text(new_sql_text)

    catalog_relations = sorted(
        {
            object_name.lower()
            for schema_name, object_name in existing_views(new_sql_text)
            if schema_name == "PG_CATALOG"
        }
    )
    preprocessor_text = PREPROCESSOR_PATH.read_text()
    pre_prefix, _, pre_suffix = partition_marked_block(
        preprocessor_text,
        PREPROCESSOR_MARKER_START,
        PREPROCESSOR_MARKER_END,
    )
    relation_lines = "\n".join(f'    "{name}",' for name in catalog_relations)
    generated_preprocessor_block = (
        f"{PREPROCESSOR_MARKER_START}\n"
        f"{relation_lines}\n"
        f"{PREPROCESSOR_MARKER_END}"
    )
    PREPROCESSOR_PATH.write_text(pre_prefix + generated_preprocessor_block + pre_suffix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh PostgreSQL compatibility placeholders and, optionally, "
            "rewrite implemented views so they expose the full documented column surface."
        )
    )
    parser.add_argument("--dsn")
    parser.add_argument("--user", default="")
    parser.add_argument("--password", default="")
    parser.add_argument("--schema", default="")
    parser.add_argument("--validate-certificate", action="store_true")
    return parser.parse_args()


def partition_marked_block(text: str, start_marker: str, end_marker: str) -> tuple[str, str, str]:
    start = text.index(start_marker)
    end = text.index(end_marker) + len(end_marker)
    return text[:start], text[start:end], text[end:]


def existing_views(sql_text: str) -> set[tuple[str, str]]:
    views: set[tuple[str, str]] = set()
    pattern = re.compile(
        r'CREATE OR REPLACE VIEW\s+([A-Z_]+)\.(?:"([A-Z0-9_]+)"|([A-Z0-9_]+))',
        re.IGNORECASE,
    )
    for match in pattern.finditer(sql_text):
        object_name = match.group(2) or match.group(3)
        views.add((match.group(1).upper(), normalize_identifier(object_name)))
    return views


def extract_manual_view_bodies(sql_text: str) -> dict[tuple[str, str], tuple[str, str]]:
    bodies: dict[tuple[str, str], tuple[str, str]] = {}
    for statement in split_exasol_sql(sql_text):
        match = re.match(
            r'CREATE OR REPLACE VIEW\s+([A-Z_]+)\.(?:"([A-Z0-9_]+)"|([A-Z0-9_]+))\s+AS\s*(.*)$',
            statement,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            continue
        schema_name = match.group(1).upper()
        object_name = normalize_identifier(match.group(2) or match.group(3))
        bodies[(schema_name, object_name)] = (
            unwrap_wrapped_view_body(match.group(4).strip()),
            statement,
        )
    return bodies


def split_exasol_sql(sql_text: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    in_function = False

    for raw_line in sql_text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()

        if not stripped and not buffer:
            continue

        buffer.append(raw_line)

        upper = stripped.upper()
        if upper.startswith("CREATE") and (
            " FUNCTION " in f" {upper} " or " SCRIPT " in f" {upper} "
        ):
            in_function = True

        if in_function and stripped == "/":
            statements.append("\n".join(buffer[:-1]).strip())
            buffer = []
            in_function = False
        elif not in_function and stripped.endswith(";"):
            statements.append("\n".join(buffer).strip().rstrip(";"))
            buffer = []

    tail = "\n".join(buffer).strip()
    if tail:
        statements.append(tail.rstrip(";"))

    return [statement for statement in statements if statement]


def fetch_base_columns(
    dsn: str,
    user: str,
    password: str,
    schema: str,
    validate_certificate: bool,
    manual_views: dict[tuple[str, str], tuple[str, str]],
) -> dict[tuple[str, str], list[str]]:
    sslopt = None
    if not validate_certificate:
        sslopt = {"cert_reqs": ssl.CERT_NONE}

    conn = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        encryption=True,
        websocket_sslopt=sslopt,
        client_name="exa-postgres-interface-refresh",
    )
    try:
        results: dict[tuple[str, str], list[str]] = {}
        probe_schema = "PG_COMPAT_PROBE"
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {probe_schema}")
        for (schema_name, object_name), (base_body, _) in manual_views.items():
            probe_view = f'{probe_schema}."{schema_name}_{object_name}_PROBE"'
            conn.execute(f"CREATE OR REPLACE VIEW {probe_view} AS\n{base_body}")
            stmt = conn.execute(
                f"DESCRIBE FULL {probe_view}"
            )
            results[(schema_name, object_name)] = [
                row[0] for row in stmt.fetchall()
            ]
            conn.execute(f"DROP VIEW {probe_view}")
        return results
    finally:
        conn.close()


def rewrite_manual_views(
    sql_text: str,
    manual_views: dict[tuple[str, str], tuple[str, str]],
    live_columns: dict[tuple[str, str], list[str]],
    object_docs: dict[tuple[str, str], str],
) -> str:
    rewritten = sql_text
    for key, (body, original_statement) in manual_views.items():
        href = object_docs.get(key)
        if not href:
            continue
        doc_columns = scrape_columns(href)
        current_columns = {
            normalize_identifier(column_name) for column_name in live_columns.get(key, [])
        }
        replacement = build_wrapped_view_sql(key[0], key[1], body, doc_columns, current_columns)
        pattern = re.escape(original_statement) + r";+"
        rewritten, count = re.subn(pattern, replacement + ";", rewritten, count=1)
        if count == 0:
            if original_statement not in rewritten:
                raise RuntimeError(f"Could not rewrite manual view {key[0]}.{key[1]}")
            rewritten = rewritten.replace(original_statement, replacement, 1)
    return rewritten


def build_wrapped_view_sql(
    schema_name: str,
    object_name: str,
    base_body: str,
    doc_columns: list[tuple[str, str]],
    current_columns: set[str],
) -> str:
    select_lines: list[str] = []
    for index, (column_name, pg_type) in enumerate(doc_columns):
        comma = "," if index < len(doc_columns) - 1 else ""
        if column_name in current_columns:
            select_lines.append(f'    BASE."{column_name}" AS "{column_name}"{comma}')
        else:
            select_lines.append(
                f'    CAST(NULL AS {map_type(pg_type)}) AS "{column_name}"{comma}'
            )

    indented_body = "\n".join(f"    {line}" if line else "" for line in base_body.splitlines())
    return "\n".join(
        [
            f"CREATE OR REPLACE VIEW {schema_name}.{object_name} AS",
            "WITH BASE AS (",
            indented_body,
            ")",
            "SELECT",
            *select_lines,
            "FROM BASE",
        ]
    )


def unwrap_wrapped_view_body(view_body: str) -> str:
    match = re.match(
        r'^WITH\s+BASE\s+AS\s*\(\n(.*)\n\)\nSELECT\n.*\nFROM\s+BASE;?$',
        view_body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return view_body

    inner_body = match.group(1)
    lines = inner_body.splitlines()
    if all(not line or line.startswith("    ") for line in lines):
        return "\n".join(line[4:] if line.startswith("    ") else line for line in lines)
    return inner_body


def scrape_toc(url: str, section_prefix: str, min_section: int) -> list[tuple[str, str]]:
    soup = fetch_soup(url)
    objects: list[tuple[str, str]] = []
    for anchor in soup.select("div.toc a"):
        text = normalize_spaces(anchor.get_text(" ", strip=True))
        match = re.match(rf"^{section_prefix}\.(\d+)\.\s+(.+)$", text)
        if not match:
            continue
        section_number = int(match.group(1))
        if section_number < min_section:
            continue
        object_name = normalize_identifier(match.group(2))
        href = anchor.get("href")
        if href:
            objects.append((object_name, href))
    return objects


def scrape_columns(relative_href: str) -> list[tuple[str, str]]:
    soup = fetch_soup(DOC_BASE + relative_href)
    for table in soup.find_all("table"):
        headers = " ".join(th.get_text(" ", strip=True) for th in table.find_all("th"))
        if "Column" not in headers or "Type" not in headers:
            continue
        columns: list[tuple[str, str]] = []
        for row in table.find_all("tr")[1:]:
            cell = row.find("td")
            if not cell:
                continue
            name_el = cell.select_one("code.structfield")
            type_el = cell.select_one("code.type")
            if not name_el or not type_el:
                continue
            columns.append(
                (
                    normalize_identifier(name_el.get_text(" ", strip=True)),
                    normalize_spaces(type_el.get_text(" ", strip=True)),
                )
            )
        if columns:
            return columns
    raise RuntimeError(f"Could not extract columns from {relative_href}")


def generate_empty_view_sql(schema_name: str, object_name: str, columns: list[tuple[str, str]]) -> str:
    select_lines = []
    for index, (column_name, pg_type) in enumerate(columns):
        comma = "," if index < len(columns) - 1 else ""
        select_lines.append(
            f'    CAST(NULL AS {map_type(pg_type)}) AS "{column_name}"{comma}'
        )
    return "\n".join(
        [
            f'CREATE OR REPLACE VIEW {schema_name}."{object_name}" AS',
            "SELECT",
            *select_lines,
            "FROM (SELECT 1 AS DUMMY)",
            "WHERE 1 = 0;",
        ]
    )


def map_type(pg_type: str) -> str:
    base_type = normalize_spaces(pg_type).lower()
    if base_type in {"yes_or_no"}:
        return "VARCHAR(3)"
    if any(token in base_type for token in ["bool", "boolean"]):
        return "BOOLEAN"
    if any(token in base_type for token in ["float", "double", "real"]):
        return "DOUBLE"
    if any(token in base_type for token in ["timestamp", "time_stamp", "date"]):
        return "TIMESTAMP"
    if base_type.startswith("_") or base_type.endswith("[]"):
        return "VARCHAR(2000000)"
    if any(
        token in base_type
        for token in [
            "oid",
            "xid",
            "cid",
            "int2",
            "int4",
            "int8",
            "smallint",
            "integer",
            "bigint",
            "cardinal_number",
            "numeric",
        ]
    ):
        return "DECIMAL(36,0)"
    return "VARCHAR(2000000)"


def fetch_soup(url: str) -> bs4.BeautifulSoup:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return bs4.BeautifulSoup(response.text, "html.parser")


def normalize_spaces(value: str) -> str:
    return " ".join(value.replace("\u200b", "").split())


def normalize_identifier(value: str) -> str:
    return normalize_spaces(value).replace(" ", "_").upper()


if __name__ == "__main__":
    main()
