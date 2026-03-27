"""Integration tests for stockdb2 Flask APIs."""

import io
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, cast

import dbapp2
from stockdb2 import dbconn


class TestStockdb2Api(unittest.TestCase):
    """Test upload and query API behavior."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        dbconn.DB_PATH = Path(cls.tmpdir.name) / "test_stock2.db"
        dbconn.init_db()
        dbapp2.app.config["TESTING"] = True
        cls.client = dbapp2.app.test_client()

    @classmethod
    def tearDownClass(cls):
        cls.tmpdir.cleanup()

    def test_upload_and_query(self):
        payload = (
            "代码\t名称\t换手%\t现量\t涨速%\t主力净额\t主力占比%\t均涨幅%\t卖价\t涨幅%\t攻击波%\t流通市值\t流通股(亿)\t细分行业\t现价\t连涨天\t内外比\t量比\n"
            "=\"600001\"\t测试A\t1.23\t100\t0.2\t1.20亿\t12\t1\t--\t2.5\t3.2\t100亿\t20\t软件服务\t10\t2\t1\t1.2\n"
        )

        data = {
            "files": (io.BytesIO(payload.encode("gb2312")), "自选股20250911.xls")
        }

        upload_resp = self.client.post(
            "/api2/upload",
            data=data,
            content_type="multipart/form-data",
        )
        self.assertIn(upload_resp.status_code, (200, 207))

        stats_resp = self.client.get("/api2/stats?group_by=stock")
        self.assertEqual(stats_resp.status_code, 200)
        stats_data = stats_resp.get_json()
        self.assertIsNotNone(stats_data)
        stats_data = cast(Dict[str, object], stats_data)
        stats_count = cast(int, stats_data["count"])
        self.assertGreaterEqual(stats_count, 1)

        ts_resp = self.client.get("/api2/timeseries?symbols=600001&window=2")
        self.assertEqual(ts_resp.status_code, 200)
        ts_data = ts_resp.get_json()
        self.assertIsNotNone(ts_data)
        ts_data = cast(Dict[str, object], ts_data)
        ts_count = cast(int, ts_data["count"])
        ts_rows = cast(List[Dict[str, object]], ts_data["rows"])
        self.assertEqual(ts_count, 1)
        self.assertEqual(str(ts_rows[0]["symbol"]), "600001")

    def test_timeseries_cumulative_values(self):
        payload_1 = (
            "代码\t名称\t换手%\t现量\t涨速%\t主力净额\t主力占比%\t均涨幅%\t卖价\t涨幅%\t攻击波%\t流通市值\t流通股(亿)\t细分行业\t现价\t连涨天\t内外比\t量比\n"
            "=\"600009\"\t测试累计\t1.23\t100\t0.2\t10\t10\t1\t--\t2.5\t3.2\t100亿\t20\t软件服务\t10\t2\t1\t1.2\n"
        )
        payload_2 = (
            "代码\t名称\t换手%\t现量\t涨速%\t主力净额\t主力占比%\t均涨幅%\t卖价\t涨幅%\t攻击波%\t流通市值\t流通股(亿)\t细分行业\t现价\t连涨天\t内外比\t量比\n"
            "=\"600009\"\t测试累计\t1.23\t100\t0.2\t-2\t10\t1\t--\t2.5\t3.2\t100亿\t20\t软件服务\t10\t2\t1\t1.2\n"
        )
        payload_3 = (
            "代码\t名称\t换手%\t现量\t涨速%\t主力净额\t主力占比%\t均涨幅%\t卖价\t涨幅%\t攻击波%\t流通市值\t流通股(亿)\t细分行业\t现价\t连涨天\t内外比\t量比\n"
            "=\"600009\"\t测试累计\t1.23\t100\t0.2\t5\t10\t1\t--\t2.5\t3.2\t100亿\t20\t软件服务\t10\t2\t1\t1.2\n"
        )

        self.client.post(
            "/api2/upload",
            data={"files": (io.BytesIO(payload_1.encode("gb2312")), "自选股20250901.xls")},
            content_type="multipart/form-data",
        )
        self.client.post(
            "/api2/upload",
            data={"files": (io.BytesIO(payload_2.encode("gb2312")), "自选股20250902.xls")},
            content_type="multipart/form-data",
        )
        self.client.post(
            "/api2/upload",
            data={"files": (io.BytesIO(payload_3.encode("gb2312")), "自选股20250903.xls")},
            content_type="multipart/form-data",
        )

        ts_resp = self.client.get("/api2/timeseries?symbols=600009&metrics=net_inflow_100m")
        self.assertEqual(ts_resp.status_code, 200)
        ts_data = ts_resp.get_json()
        self.assertIsNotNone(ts_data)
        ts_data = cast(Dict[str, object], ts_data)
        rows = cast(List[Dict[str, object]], ts_data["rows"])

        self.assertEqual(len(rows), 3)
        self.assertAlmostEqual(float(rows[0]["net_inflow_100m_cum"]), 10.0)
        self.assertAlmostEqual(float(rows[1]["net_inflow_100m_cum"]), 8.0)
        self.assertAlmostEqual(float(rows[2]["net_inflow_100m_cum"]), 13.0)


if __name__ == "__main__":
    unittest.main()
