from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Protocol

from .config import AppConfig
from .messages import Column, QueryResult, infer_type_oid

LOG = logging.getLogger(__name__)


class BackendError(RuntimeError):
    def __init__(self, message: str, sqlstate: str = "XX000") -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


class BackendSession(Protocol):
    def execute_read(self, sql: str) -> QueryResult:
        ...

    def close(self) -> None:
        ...


class BackendFactory(Protocol):
    def connect(self, username: str, password: str) -> BackendSession:
        ...


@dataclass(frozen=True)
class PyexasolBackendFactory:
    config: AppConfig

    def connect(self, username: str, password: str) -> BackendSession:
        try:
            import pyexasol  # type: ignore[import-not-found]
        except ImportError as exc:
            raise BackendError("pyexasol is not installed; install project dependencies") from exc

        if not self.config.exasol.pass_client_credentials:
            raise BackendError("only client credential passthrough is implemented", "28000")

        dsn = _dsn_with_certificate_policy(
            self.config.exasol.dsn,
            self.config.exasol.certificate_fingerprint,
            self.config.exasol.validate_certificate,
        )
        kwargs: dict[str, object] = {
            "dsn": dsn,
            "user": username,
            "password": password,
            "encryption": self.config.exasol.encryption,
        }
        if self.config.exasol.schema:
            kwargs["schema"] = self.config.exasol.schema

        try:
            conn = pyexasol.connect(
                **kwargs,
            )
        except Exception as exc:
            raise BackendError(f"Exasol authentication or connection failed: {exc}", "28000") from exc

        session = PyexasolBackendSession(conn, self.config)
        session.initialize()
        return session


class PyexasolBackendSession:
    def __init__(self, conn: Any, config: AppConfig) -> None:
        self.conn = conn
        self.config = config

    def initialize(self) -> None:
        translation = self.config.translation
        if not translation.enabled:
            LOG.info("SQL translation disabled for Exasol session")
            return
        if not translation.session_init_sql:
            raise BackendError("SQL translation enabled but no session_init_sql configured", "58000")

        for template in translation.session_init_sql:
            sql = template.format(script=translation.sql_preprocessor_script)
            LOG.info("initializing Exasol SQL preprocessor with configured session SQL")
            try:
                self.conn.execute(sql)
            except Exception as exc:
                raise BackendError(f"failed to initialize SQL translation: {exc}", "58000") from exc

    def execute_read(self, sql: str) -> QueryResult:
        try:
            result = self.conn.execute(sql)
            rows = tuple(tuple(row) for row in result.fetchall())
        except Exception as exc:
            raise BackendError(f"Exasol execution failed: {exc}", "XX000") from exc

        columns = _columns_from_result(result, rows)
        return QueryResult(columns=columns, rows=rows)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            LOG.debug("error while closing Exasol connection", exc_info=True)


def _columns_from_result(result: Any, rows: tuple[tuple[object, ...], ...]) -> tuple[Column, ...]:
    names = _column_names(result)
    if not names and rows:
        names = [f"column_{i + 1}" for i in range(len(rows[0]))]
    columns: list[Column] = []
    for idx, name in enumerate(names):
        sample = _first_non_null(rows, idx)
        columns.append(Column(name=str(name), type_oid=infer_type_oid(sample)))
    return tuple(columns)


def _column_names(result: Any) -> list[str]:
    candidates = []
    if hasattr(result, "columns"):
        attr = result.columns
        candidates.append(attr() if callable(attr) else attr)
    if hasattr(result, "description"):
        candidates.append(result.description)

    for candidate in candidates:
        if not candidate:
            continue
        names: list[str] = []
        for item in candidate:
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, (tuple, list)) and item:
                names.append(str(item[0]))
            elif hasattr(item, "name"):
                names.append(str(item.name))
        if names:
            return names
    return []


def _first_non_null(rows: tuple[tuple[object, ...], ...], idx: int) -> object:
    for row in rows:
        if idx < len(row) and row[idx] is not None:
            return row[idx]
    return None


def _dsn_with_certificate_policy(dsn: str, fingerprint: str, validate_certificate: bool) -> str:
    if fingerprint:
        return _append_dsn_fingerprint(dsn, fingerprint)
    if not validate_certificate:
        return _append_dsn_fingerprint(dsn, "nocertcheck")
    return dsn


def _append_dsn_fingerprint(dsn: str, fingerprint: str) -> str:
    parts = []
    for part in dsn.split(","):
        part = part.strip()
        if not part:
            continue
        host_part, sep, port = part.rpartition(":")
        if not sep:
            host_part = part
            port = ""
        if "/" not in host_part:
            host_part = f"{host_part}/{fingerprint}"
        parts.append(f"{host_part}:{port}" if port else host_part)
    return ",".join(parts)
