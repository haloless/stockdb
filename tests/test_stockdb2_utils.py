"""Unit tests for stockdb2 utility helpers."""

import unittest

from stockdb2.dbutils import (
    clean_symbol,
    infer_date_from_filename,
    rolling_stats,
    to_amount_100m,
)


class TestStockdb2Utils(unittest.TestCase):
    """Validate conversion helpers and rolling math."""

    def test_infer_date_from_filename(self):
        self.assertEqual(infer_date_from_filename("自选股20250911.xls"), "2025-09-11")

    def test_clean_symbol(self):
        self.assertEqual(clean_symbol('="603359"'), "603359")
        self.assertEqual(clean_symbol("603359"), "603359")
        self.assertIsNone(clean_symbol("--"))

    def test_to_amount_100m(self):
        self.assertAlmostEqual(to_amount_100m("1.5亿"), 1.5)
        self.assertAlmostEqual(to_amount_100m("2500万"), 0.25)
        self.assertAlmostEqual(to_amount_100m("0.5"), 0.5)

    def test_rolling_stats(self):
        values = [1.0, 2.0, 3.0, 4.0]
        result = rolling_stats(values, 2)
        self.assertAlmostEqual(result[0]["avg"], 1.0)
        self.assertAlmostEqual(result[1]["avg"], 1.5)
        self.assertAlmostEqual(result[2]["sum"], 5.0)
        self.assertAlmostEqual(result[3]["sum"], 7.0)


if __name__ == "__main__":
    unittest.main()
