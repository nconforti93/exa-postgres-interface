from __future__ import annotations

import unittest

from exa_postgres_interface.config import AppConfig


class ConfigTests(unittest.TestCase):
    def test_requires_exasol_dsn(self) -> None:
        with self.assertRaises(ValueError):
            AppConfig.from_mapping({})

    def test_loads_nested_config(self) -> None:
        config = AppConfig.from_mapping(
            {
                "server": {"listen_host": "0.0.0.0", "listen_port": 15432},
                "exasol": {"dsn": "db.example.com:8563"},
                "translation": {
                    "enabled": True,
                    "sql_preprocessor_script": "S.P",
                    "session_init_sql": "ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = '{script}'",
                },
            }
        )

        self.assertEqual(config.server.listen_host, "0.0.0.0")
        self.assertEqual(config.exasol.dsn, "db.example.com:8563")
        self.assertEqual(config.translation.session_init_sql, ("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = '{script}'",))


if __name__ == "__main__":
    unittest.main()
