from __future__ import annotations

import asyncio
import logging
import os
import random
import struct

from .backend import BackendError, BackendFactory, BackendSession, PyexasolBackendFactory
from .config import AppConfig
from .gateway import GatewayError, StatementGateway
from .messages import (
    CommandResult,
    QueryResult,
    authentication_cleartext_password,
    authentication_ok,
    backend_key_data,
    command_complete,
    data_row,
    empty_query_response,
    error_response,
    parameter_status,
    ready_for_query,
    row_description,
)

LOG = logging.getLogger(__name__)

SSL_REQUEST_CODE = 80877103
CANCEL_REQUEST_CODE = 80877102
PROTOCOL_VERSION_3 = 196608


class ClientProtocolError(RuntimeError):
    pass


async def serve(config: AppConfig, backend_factory: BackendFactory | None = None) -> None:
    factory = backend_factory or PyexasolBackendFactory(config)
    server = await asyncio.start_server(
        lambda r, w: ClientSession(r, w, factory).run(),
        config.server.listen_host,
        config.server.listen_port,
    )
    sockets = ", ".join(str(sock.getsockname()) for sock in server.sockets or ())
    LOG.info(
        "exa-postgres-interface listening on %s with Exasol dsn=%s translation=%s",
        sockets,
        config.exasol.dsn,
        "enabled" if config.translation.enabled else "disabled",
    )
    async with server:
        await server.serve_forever()


class ClientSession:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        backend_factory: BackendFactory,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.backend_factory = backend_factory
        self.backend: BackendSession | None = None

    async def run(self) -> None:
        peer = self.writer.get_extra_info("peername")
        LOG.info("client connected: %s", peer)
        try:
            params = await self._startup()
            await self._authenticate(params)
            await self._query_loop()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            LOG.info("client disconnected: %s", peer)
        except ClientProtocolError as exc:
            LOG.info("client session ended: %s", exc)
        except Exception:
            LOG.exception("unhandled client-session error")
            self._write(error_response("internal server error", "XX000"))
            await self.writer.drain()
        finally:
            if self.backend is not None:
                self.backend.close()
            self.writer.close()
            await self.writer.wait_closed()

    async def _startup(self) -> dict[str, str]:
        while True:
            payload = await self._read_untagged_message()
            if len(payload) < 4:
                raise ValueError("startup packet is too short")
            code = struct.unpack("!I", payload[:4])[0]
            if code == SSL_REQUEST_CODE:
                self.writer.write(b"N")
                await self.writer.drain()
                continue
            if code == CANCEL_REQUEST_CODE:
                raise ValueError("cancel requests are not implemented")
            if code != PROTOCOL_VERSION_3:
                self._write(error_response(f"unsupported PostgreSQL protocol version: {code}"))
                await self.writer.drain()
                raise ClientProtocolError(f"unsupported protocol version {code}")
            return _parse_startup_params(payload[4:])

    async def _authenticate(self, params: dict[str, str]) -> None:
        username = params.get("user", "")
        if not username:
            self._write(error_response("startup parameter 'user' is required", "28000"))
            await self.writer.drain()
            raise ClientProtocolError("missing startup user")

        self._write(authentication_cleartext_password())
        kind, payload = await self._read_tagged_message()
        if kind != b"p":
            self._write(error_response("password message is required", "28000"))
            await self.writer.drain()
            raise ClientProtocolError("missing password message")
        password = payload.rstrip(b"\x00").decode("utf-8")

        try:
            self.backend = self.backend_factory.connect(username, password)
        except BackendError as exc:
            self._write(error_response(str(exc), exc.sqlstate))
            await self.writer.drain()
            raise ClientProtocolError(str(exc)) from exc

        self._write(authentication_ok())
        self._write(parameter_status("server_version", "15.0-exasol-gateway"))
        self._write(parameter_status("server_encoding", "UTF8"))
        self._write(parameter_status("client_encoding", "UTF8"))
        self._write(parameter_status("DateStyle", "ISO, MDY"))
        self._write(parameter_status("integer_datetimes", "on"))
        self._write(backend_key_data(os.getpid(), random.randint(1, 2_147_483_647)))
        self._write(ready_for_query())
        await self.writer.drain()

    async def _query_loop(self) -> None:
        if self.backend is None:
            raise RuntimeError("backend is not connected")
        gateway = StatementGateway(self.backend)
        while not self.reader.at_eof():
            kind, payload = await self._read_tagged_message()
            if kind == b"X":
                return
            if kind != b"Q":
                LOG.warning("unsupported PostgreSQL protocol message: %r", kind)
                self._write(error_response(f"unsupported PostgreSQL protocol message: {kind!r}"))
                self._write(ready_for_query())
                await self.writer.drain()
                continue
            sql = payload.rstrip(b"\x00").decode("utf-8")
            await self._handle_query(gateway, sql)

    async def _handle_query(self, gateway: StatementGateway, sql: str) -> None:
        try:
            result = gateway.execute(sql)
            if isinstance(result, CommandResult):
                if result.tag:
                    self._write(command_complete(result.tag))
                else:
                    self._write(empty_query_response())
            elif isinstance(result, QueryResult):
                self._write(row_description(result.columns))
                for row in result.rows:
                    self._write(data_row(row))
                self._write(command_complete(f"SELECT {len(result.rows)}"))
            self._write(ready_for_query())
            await self.writer.drain()
        except GatewayError as exc:
            self._write(error_response(str(exc), exc.sqlstate))
            self._write(ready_for_query())
            await self.writer.drain()

    async def _read_untagged_message(self) -> bytes:
        length_data = await self.reader.readexactly(4)
        length = struct.unpack("!I", length_data)[0]
        if length < 4:
            raise ValueError("invalid message length")
        return await self.reader.readexactly(length - 4)

    async def _read_tagged_message(self) -> tuple[bytes, bytes]:
        kind = await self.reader.readexactly(1)
        return kind, await self._read_untagged_message()

    def _write(self, data: bytes) -> None:
        self.writer.write(data)


def _parse_startup_params(payload: bytes) -> dict[str, str]:
    parts = payload.split(b"\x00")
    params: dict[str, str] = {}
    for key, value in zip(parts[0::2], parts[1::2], strict=False):
        if not key:
            break
        params[key.decode("utf-8")] = value.decode("utf-8")
    return params
