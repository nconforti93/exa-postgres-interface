from __future__ import annotations

import unittest

from exa_postgres_interface.backend import _dsn_with_certificate_policy


class BackendConfigTests(unittest.TestCase):
    def test_appends_fingerprint_before_port(self) -> None:
        self.assertEqual(
            _dsn_with_certificate_policy("127.0.0.1:8563", "ABCDEF", True),
            "127.0.0.1/ABCDEF:8563",
        )

    def test_appends_nocertcheck_when_validation_disabled(self) -> None:
        self.assertEqual(
            _dsn_with_certificate_policy("127.0.0.1:8563", "", False),
            "127.0.0.1/nocertcheck:8563",
        )

    def test_leaves_dsn_unchanged_when_strict_validation_enabled(self) -> None:
        self.assertEqual(
            _dsn_with_certificate_policy("db.example.com:8563", "", True),
            "db.example.com:8563",
        )

    def test_preserves_existing_dsn_fingerprint(self) -> None:
        self.assertEqual(
            _dsn_with_certificate_policy("127.0.0.1/OLD:8563", "NEW", True),
            "127.0.0.1/OLD:8563",
        )


if __name__ == "__main__":
    unittest.main()
