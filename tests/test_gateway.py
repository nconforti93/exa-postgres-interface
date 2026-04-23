from __future__ import annotations

import unittest

from exa_postgres_interface.gateway import GatewayError, StatementGateway
from exa_postgres_interface.messages import Column, CommandResult, QueryResult


class FakeBackend:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute_read(self, sql: str) -> QueryResult:
        self.sql.append(sql)
        return QueryResult(columns=(Column("?column?"),), rows=((1,),))

    def close(self) -> None:
        pass


class GatewayTests(unittest.TestCase):
    def test_executes_read_query(self) -> None:
        backend = FakeBackend()
        result = StatementGateway(backend).execute("SELECT 1")
        self.assertIsInstance(result, QueryResult)
        self.assertEqual(backend.sql, ["SELECT 1"])

    def test_rejects_write_without_poisoning_gateway(self) -> None:
        backend = FakeBackend()
        gateway = StatementGateway(backend)

        with self.assertRaises(GatewayError):
            gateway.execute("DELETE FROM t")

        result = gateway.execute("SELECT 1")
        self.assertIsInstance(result, QueryResult)

    def test_acknowledges_safe_session_command_without_backend_execution(self) -> None:
        backend = FakeBackend()
        result = StatementGateway(backend).execute("SET extra_float_digits = 3")

        self.assertIsInstance(result, CommandResult)
        self.assertEqual(result.tag, "SET")
        self.assertEqual(backend.sql, [])


if __name__ == "__main__":
    unittest.main()
