from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class ServerConfig:
    listen_host: str = "127.0.0.1"
    listen_port: int = 15432
    log_level: str = "INFO"


@dataclass(frozen=True)
class ExasolConfig:
    dsn: str
    encryption: bool = True
    pass_client_credentials: bool = True
    schema: str = ""


@dataclass(frozen=True)
class TranslationConfig:
    enabled: bool = True
    sql_preprocessor_script: str = ""
    session_init_sql: tuple[str, ...] = field(default_factory=tuple)

    @property
    def required(self) -> bool:
        return self.enabled and bool(self.sql_preprocessor_script or self.session_init_sql)


@dataclass(frozen=True)
class AppConfig:
    server: ServerConfig
    exasol: ExasolConfig
    translation: TranslationConfig

    @classmethod
    def from_file(cls, path: str | Path) -> "AppConfig":
        raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_mapping(raw)

    @classmethod
    def from_mapping(cls, raw: dict) -> "AppConfig":
        server_raw = raw.get("server", {})
        exasol_raw = raw.get("exasol", {})
        translation_raw = raw.get("translation", {})

        dsn = str(exasol_raw.get("dsn", "")).strip()
        if not dsn:
            raise ValueError("exasol.dsn is required")

        server = ServerConfig(
            listen_host=str(server_raw.get("listen_host", "127.0.0.1")),
            listen_port=int(server_raw.get("listen_port", 15432)),
            log_level=str(server_raw.get("log_level", "INFO")),
        )
        exasol = ExasolConfig(
            dsn=dsn,
            encryption=bool(exasol_raw.get("encryption", True)),
            pass_client_credentials=bool(exasol_raw.get("pass_client_credentials", True)),
            schema=str(exasol_raw.get("schema", "")),
        )
        init_sql = translation_raw.get("session_init_sql", ())
        if isinstance(init_sql, str):
            init_sql = (init_sql,)
        translation = TranslationConfig(
            enabled=bool(translation_raw.get("enabled", True)),
            sql_preprocessor_script=str(translation_raw.get("sql_preprocessor_script", "")),
            session_init_sql=tuple(str(item) for item in init_sql),
        )
        return cls(server=server, exasol=exasol, translation=translation)
