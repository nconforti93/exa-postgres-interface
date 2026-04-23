from __future__ import annotations

import asyncio
import struct
import unittest

from exa_postgres_interface.messages import Column, QueryResult
from exa_postgres_interface.server import ClientSession


def _untagged(payload: bytes) -> bytes:
    return struct.pack("!I", len(payload) + 4) + payload


def _tagged(kind: bytes, payload: bytes = b"") -> bytes:
    return kind + _untagged(payload)


def _startup(user: str = "sys") -> bytes:
    payload = (
        struct.pack("!I", 196608)
        + b"user\x00"
        + user.encode("utf-8")
        + b"\x00database\x00exa\x00\x00"
    )
    return _untagged(payload)


async def _read_message(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    kind = await reader.readexactly(1)
    length = struct.unpack("!I", await reader.readexactly(4))[0]
    payload = await reader.readexactly(length - 4)
    return kind, payload


async def _read_until_ready(reader: asyncio.StreamReader) -> list[tuple[bytes, bytes]]:
    messages: list[tuple[bytes, bytes]] = []
    while True:
        message = await _read_message(reader)
        messages.append(message)
        if message[0] == b"Z":
            return messages


class FakeBackend:
    def __init__(self) -> None:
        self.closed = False
        self.executed: list[str] = []

    def execute_read(self, sql: str) -> QueryResult:
        self.executed.append(sql)
        return QueryResult(columns=(Column("?column?"),), rows=((1,),))

    def close(self) -> None:
        self.closed = True


class FakeBackendFactory:
    def __init__(self) -> None:
        self.username = ""
        self.password = ""
        self.backend = FakeBackend()

    def connect(self, username: str, password: str) -> FakeBackend:
        self.username = username
        self.password = password
        return self.backend


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_auth_and_simple_query(self) -> None:
        factory = FakeBackendFactory()
        server = await asyncio.start_server(
            lambda r, w: ClientSession(r, w, factory).run(),
            "127.0.0.1",
            0,
        )
        host, port = server.sockets[0].getsockname()[:2]

        async with server:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(_startup("alice"))
            await writer.drain()

            kind, payload = await _read_message(reader)
            self.assertEqual(kind, b"R")
            self.assertEqual(struct.unpack("!I", payload)[0], 3)

            writer.write(_tagged(b"p", b"secret\x00"))
            await writer.drain()
            auth_messages = await _read_until_ready(reader)
            self.assertEqual(auth_messages[0][0], b"R")
            self.assertEqual(factory.username, "alice")
            self.assertEqual(factory.password, "secret")

            writer.write(_tagged(b"Q", b"SELECT 1\x00"))
            await writer.drain()
            query_messages = await _read_until_ready(reader)
            self.assertEqual([m[0] for m in query_messages], [b"T", b"D", b"C", b"Z"])
            self.assertEqual(factory.backend.executed, ["SELECT 1"])

            writer.write(b"X" + struct.pack("!I", 4))
            await writer.drain()
            writer.close()
            await writer.wait_closed()


if __name__ == "__main__":
    unittest.main()
