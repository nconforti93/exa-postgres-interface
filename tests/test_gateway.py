from __future__ import annotations

import unittest

from exa_postgres_interface.gateway import GatewayError, StatementGateway
from exa_postgres_interface.messages import Column, QueryResult


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


if __name__ == "__main__":
    unittest.main()
