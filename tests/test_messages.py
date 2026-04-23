from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import unittest

from exa_postgres_interface.messages import (
    Column,
    data_row,
    error_response,
    format_value,
    row_description,
)


class MessageTests(unittest.TestCase):
    def test_formats_values_as_text_protocol(self) -> None:
        self.assertEqual(format_value(True), "t")
        self.assertEqual(format_value(False), "f")
        self.assertEqual(format_value(date(2026, 1, 2)), "2026-01-02")
        self.assertEqual(format_value(datetime(2026, 1, 2, 3, 4, 5)), "2026-01-02 03:04:05")
        self.assertEqual(format_value(Decimal("12.3400")), "12.3400")

    def test_row_description_is_tagged_message(self) -> None:
        payload = row_description((Column("x"),))
        self.assertEqual(payload[:1], b"T")

    def test_data_row_encodes_null(self) -> None:
        payload = data_row((1, None))
        self.assertEqual(payload[:1], b"D")
        self.assertIn(b"\xff\xff\xff\xff", payload)

    def test_error_response_contains_sqlstate_and_message(self) -> None:
        payload = error_response("not supported", "0A000")
        self.assertEqual(payload[:1], b"E")
        self.assertIn(b"0A000", payload)
        self.assertIn(b"not supported", payload)


if __name__ == "__main__":
    unittest.main()
