import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "source"


def module():
    with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **kw: object())}):
        spec = importlib.util.spec_from_file_location("bridge_contract_fixture", SOURCE / "lambda_function.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.mod = module()
        self.docs = {}
        self.output = {}
        self.mod._get = lambda key, **kw: self.docs[key]
        self.mod._pub = lambda key, doc: self.output.update({key: doc})

    def test_soma_latest_and_components_reconcile_without_inflation_addition(self):
        latest = {"asOfDate": "2026-09-16", "total": "28", "bills": "1", "notesbonds": "2", "mbs": "3", "tips": "4", "frn": "5", "cmbs": "6", "agencies": "7", "tipsInflationCompensation": "9"}
        self.docs["data/warm/nyfed-markets/soma_summary.json.gz"] = {"payload": {"soma": {"summary": [{"asOfDate": "2020-01-01"}, latest]}}}
        self.assertEqual(self.mod._soma("2026-09-18"), 9)
        row = self.output["data/soma-holdings.json"]
        self.assertEqual(row["total"]["field"], "total")
        self.assertEqual(row["total"]["as_of"], "2026-09-16")
        self.assertEqual(row["total"]["unit"], "USD")
        self.assertEqual(row["reconciliation"]["status"], "reconciled")
        self.assertEqual(row["reconciliation"]["component_sum_usd"], 28)

    def test_bls_annual_average_cannot_replace_monthly_index(self):
        self.docs["data/warm/usgov/bls/CUUR0000SA0.json.gz"] = {"data": [
            {"year": "2026", "period": "M13", "value": "900"},
            {"year": "2025", "period": "M12", "value": "300"},
            {"year": "2026", "period": "M08", "value": "330"}]}
        self.mod._bls("2026-09-18")
        row = self.output["data/bls-macro.json"]["cpi_headline"]
        self.assertEqual(row["value"], 330)
        self.assertEqual(row["as_of"], "2026-M08")
        self.assertEqual(row["source"]["series_id"], "CUUR0000SA0")
        self.assertEqual(row["seasonal_adjustment"], "NSA")
        self.assertIn("Index", row["unit"])

    def test_treasury_lost_dimensions_cannot_be_arbitrary_scalar(self):
        self.docs["data/warm/treasury/rates_of_exchange.json.gz"] = {"observations": [{"date": "2026-06-30", "value": 1.2}, {"date": "2026-06-30", "value": 140}]}
        self.mod._treasury("2026-09-18")
        row = self.output["data/treasury-fiscal.json"]["rates_of_exchange"]
        self.assertIsNone(row["value"])
        self.assertEqual(row["latest_period_rows"], 2)
        self.assertIn("dimensions", row["reason"])

    def test_bea_uses_table_line_period_and_preserves_definition(self):
        self.docs["data/warm/usgov/bea/nipa-t10101.json.gz"] = {"rows": [
            {"LineNumber": "1", "TimePeriod": "2026Q2", "DataValue": "2.1"},
            {"LineNumber": "2", "TimePeriod": "2026Q3", "DataValue": "99"}]}
        self.mod._bea("2026-09-18")
        row = self.output["data/bea-gdp.json"]["real_gdp_qq_pct"]
        self.assertEqual(row["value"], 2.1)
        self.assertEqual(row["as_of"], "2026Q2")
        self.assertEqual(row["source"]["series_id"], "NIPA:T10101:L1")
        self.assertEqual(row["source"]["kind"], "bea")

    def test_new_processing_time_cannot_make_2019_cpi_fresh(self):
        now = datetime(2026, 9, 18, tzinfo=timezone.utc)
        old = self.mod.measurement_freshness({"as_of": "2019-M12", "value": 256.974}, now, 75)
        self.assertEqual(old["status"], "stale")
        current = self.mod.measurement_freshness({"as_of": "2026-M08", "value": 330}, now, 75)
        self.assertEqual(current["status"], "fresh")
        self.assertEqual(current["age_days"], 18)
        self.assertEqual(self.mod.measurement_freshness({"as_of": "2026-M09", "value": 330}, now, 75)["status"], "unavailable")
        quarter = self.mod.measurement_freshness({"as_of": "2026Q2", "value": 1.5}, now, 200)
        self.assertEqual(quarter["status"], "fresh")
