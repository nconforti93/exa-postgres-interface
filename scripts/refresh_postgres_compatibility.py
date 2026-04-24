#!/usr/bin/env python3

import re
from pathlib import Path

import bs4
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
    sql_text = SQL_PATH.read_text()
    sql_prefix, _, sql_suffix = partition_marked_block(
        sql_text, SQL_MARKER_START, SQL_MARKER_END
    )

    implemented = existing_views(sql_prefix)
    info_objects = scrape_toc(PG_INFO_TOC_URL, section_prefix="35", min_section=3)
    catalog_objects = scrape_toc(PG_CATALOG_TOC_URL, section_prefix="52", min_section=2)

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
