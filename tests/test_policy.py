from __future__ import annotations

import unittest

from exa_postgres_interface.policy import StatementCategory, classify_statement, first_keyword


class PolicyTests(unittest.TestCase):
    def test_allows_read_only_dql(self) -> None:
        for sql in ("SELECT 1", "WITH q AS (SELECT 1) SELECT * FROM q", "VALUES (1)"):
            decision = classify_statement(sql)
            self.assertTrue(decision.allowed)
            self.assertEqual(decision.category, StatementCategory.READ)

    def test_rejects_write_and_ddl(self) -> None:
        for sql, category in (
            ("INSERT INTO t VALUES (1)", StatementCategory.WRITE),
            ("UPDATE t SET a = 1", StatementCategory.WRITE),
            ("CREATE TABLE t(a INT)", StatementCategory.DDL),
        ):
            decision = classify_statement(sql)
            self.assertFalse(decision.allowed)
            self.assertEqual(decision.category, category)

    def test_rejects_transaction_commands(self) -> None:
        decision = classify_statement("BEGIN")
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.category, StatementCategory.TRANSACTION)

    def test_allows_safe_postgresql_client_session_commands(self) -> None:
        for sql in (
            "SET extra_float_digits = 3",
            "SET application_name TO 'DbVisualizer'",
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
            "RESET extra_float_digits",
            "SHOW DateStyle",
        ):
            decision = classify_statement(sql)
            self.assertTrue(decision.allowed)
            self.assertEqual(decision.category, StatementCategory.CLIENT_SESSION)

    def test_does_not_treat_set_transaction_as_client_local(self) -> None:
        decision = classify_statement("SET TRANSACTION READ ONLY")
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.category, StatementCategory.SESSION)

    def test_ignores_comments_before_keyword(self) -> None:
        self.assertEqual(first_keyword("-- hello\n/* block */ SELECT 1"), "SELECT")

    def test_preserves_comment_markers_inside_strings(self) -> None:
        decision = classify_statement("SELECT '-- not a comment'")
        self.assertTrue(decision.allowed)


if __name__ == "__main__":
    unittest.main()
