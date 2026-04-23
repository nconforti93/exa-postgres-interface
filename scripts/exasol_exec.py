#!/usr/bin/env python3

import argparse
import csv
import ssl
import sys
from pathlib import Path

import pyexasol


def main() -> int:
    args = parse_args()
    sql_text = args.sql if args.sql is not None else Path(args.file).read_text()
    statements = split_exasol_sql(sql_text)
    if not statements:
        raise SystemExit("no SQL statements found")

    sslopt = None
    if not args.validate_certificate:
        sslopt = {"cert_reqs": ssl.CERT_NONE}

    conn = pyexasol.connect(
        dsn=args.dsn,
        user=args.user,
        password=args.password,
        schema=args.schema,
        encryption=True,
        websocket_sslopt=sslopt,
        client_name="exa-postgres-interface-dev",
    )

    try:
        for idx, statement in enumerate(statements, start=1):
            print(f"[{idx}/{len(statements)}] {preview(statement)}", file=sys.stderr)
            stmt = conn.execute(statement)
            if stmt.columns():
                writer = csv.writer(sys.stdout, lineterminator="\n")
                writer.writerow(stmt.columns())
                writer.writerows(stmt.fetchall())
            else:
                print(f"row_count={stmt.rowcount()}")
    finally:
        conn.close()

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute SQL against Exasol with simple support for slash-terminated function bodies."
    )
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--schema", default="")
    parser.add_argument("--validate-certificate", action="store_true")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sql")
    source.add_argument("--file")
    return parser.parse_args()


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


def preview(sql: str) -> str:
    one_line = " ".join(sql.split())
    return one_line if len(one_line) <= 80 else one_line[:77] + "..."


if __name__ == "__main__":
    raise SystemExit(main())
