from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import struct
from typing import Iterable


PG_TYPE_TEXT = 25
PG_TYPE_INT8 = 20
PG_TYPE_INT4 = 23
PG_TYPE_NUMERIC = 1700
PG_TYPE_FLOAT8 = 701
PG_TYPE_BOOL = 16
PG_TYPE_DATE = 1082
PG_TYPE_TIMESTAMP = 1114


@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int = PG_TYPE_TEXT
    type_size: int = -1
    type_modifier: int = -1


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[Column, ...]
    rows: tuple[tuple[object, ...], ...]


@dataclass(frozen=True)
class CommandResult:
    tag: str


def int16(value: int) -> bytes:
    return struct.pack("!h", value)


def uint16(value: int) -> bytes:
    return struct.pack("!H", value)


def int32(value: int) -> bytes:
    return struct.pack("!i", value)


def cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def message(kind: bytes, payload: bytes = b"") -> bytes:
    return kind + int32(len(payload) + 4) + payload


def authentication_cleartext_password() -> bytes:
    return message(b"R", int32(3))


def authentication_ok() -> bytes:
    return message(b"R", int32(0))


def parameter_status(name: str, value: str) -> bytes:
    return message(b"S", cstring(name) + cstring(value))


def backend_key_data(process_id: int, secret_key: int) -> bytes:
    return message(b"K", int32(process_id) + int32(secret_key))


def ready_for_query(status: bytes = b"I") -> bytes:
    return message(b"Z", status)


def empty_query_response() -> bytes:
    return message(b"I")


def command_complete(tag: str) -> bytes:
    return message(b"C", cstring(tag))


def row_description(columns: Iterable[Column]) -> bytes:
    cols = tuple(columns)
    payload = int16(len(cols))
    for col in cols:
        payload += (
            cstring(col.name)
            + int32(0)
            + int16(0)
            + int32(col.type_oid)
            + int16(col.type_size)
            + int32(col.type_modifier)
            + int16(0)
        )
    return message(b"T", payload)


def data_row(values: Iterable[object]) -> bytes:
    vals = tuple(values)
    payload = int16(len(vals))
    for value in vals:
        if value is None:
            payload += int32(-1)
            continue
        encoded = format_value(value).encode("utf-8")
        payload += int32(len(encoded)) + encoded
    return message(b"D", payload)


def error_response(message_text: str, sqlstate: str = "0A000", severity: str = "ERROR") -> bytes:
    payload = b"".join(
        (
            b"S" + cstring(severity),
            b"C" + cstring(sqlstate),
            b"M" + cstring(message_text),
            b"\x00",
        )
    )
    return message(b"E", payload)


def notice_response(message_text: str) -> bytes:
    payload = b"S" + cstring("WARNING") + b"M" + cstring(message_text) + b"\x00"
    return message(b"N", payload)


def format_value(value: object) -> str:
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def infer_type_oid(value: object) -> int:
    if isinstance(value, bool):
        return PG_TYPE_BOOL
    if isinstance(value, int):
        return PG_TYPE_INT8 if abs(value) > 2_147_483_647 else PG_TYPE_INT4
    if isinstance(value, float):
        return PG_TYPE_FLOAT8
    if isinstance(value, Decimal):
        return PG_TYPE_NUMERIC
    if isinstance(value, datetime):
        return PG_TYPE_TIMESTAMP
    if isinstance(value, date):
        return PG_TYPE_DATE
    return PG_TYPE_TEXT
