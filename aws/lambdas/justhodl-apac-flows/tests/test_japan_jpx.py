"""jpx_weekly_rows against the real 2026-11-06 weekly workbook (fixture from ops 6793), japan_jpx carrying stored
weeks forward, the reviewed-layout guard. Offline: no AWS, no network."""
from pathlib import Path
from unittest.mock import patch
import importlib.util
import sys
import types
import unittest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
FIXTURE = HERE / "fixtures" / "stock_1_w_20261102_20261106.xlsx"


def load():
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda *a, **k: types.SimpleNamespace(get_object=lambda **k: (_ for _ in ()).throw(Exception("offline")), put_object=lambda **k: None)
    stubs = {"boto3": fake_boto3}
    for name in ("fmp_bankfirst", "managed_secret"):
        m = types.ModuleType(name)
        m.install = lambda *a, **k: None
        m.managed_secret = lambda *a, **k: ""
        stubs[name] = m
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("apac_flows_under_test", HERE.parent / "source" / "lambda_function.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


class Tests(unittest.TestCase):
    def test_weekly_workbook_every_group_net_in_jpy_billions(self):
        mod = load()
        parsed = mod.jpx_weekly_rows(FIXTURE.read_bytes(), "20261106")
        self.assertEqual(parsed["week_ending"], "2026-11-06")
        prime = parsed["markets"]["prime"]
        # TSE Prime, Value basis (thousand yen): Foreigners institutions -80,715,840 + individuals -1,045,847
        self.assertEqual(prime["foreigners_institutions"], -80.716)
        self.assertEqual(prime["foreigners_individuals"], -1.046)
        self.assertEqual(prime["foreigners"], -81.762)
        self.assertEqual(prime["individuals"], round(-268.755 + -174.765, 3))
        self.assertEqual(prime["investment_trusts"], 102.431)
        self.assertEqual(prime["trust_banks"], 225.566)
        self.assertEqual(prime["proprietary"], round(217.970 + -141.074, 3))
        self.assertEqual(set(parsed["markets"]), {"prime", "standard", "growth", "tokyo_nagoya"})
        self.assertIsNotNone(parsed["markets"]["tokyo_nagoya"]["foreigners_institutions"])
        self.assertEqual(len(prime), len(mod.JPX_GROUPS) + 4)

    def test_layout_guard_and_carry_forward(self):
        mod = load()
        with self.assertRaises(ValueError):
            mod.jpx_weekly_rows(b"PK\x03\x04 not a workbook", "20261106")
        listing = b'<a href="/english/markets/statistics-equities/investor-type/x-att/stock_1_w_20261102_20261106.xlsx">w</a><a href="/english/markets/statistics-equities/investor-type/y-att/stock_1_w_20261027_20261030.xlsx">w</a>'
        fetched = []
        def http(url, headers=None, timeout=30):
            fetched.append(url)
            return listing if url == mod.JPX_LISTING else FIXTURE.read_bytes()
        stored = {"2026-10-30": {"markets": {"prime": {"foreigners": 1.0}}, "file": "stock_1_w_20261027_20261030.xlsx"}}
        with patch.object(mod, "_http_bytes", side_effect=http):
            out = mod.japan_jpx(previous=stored)
        self.assertEqual(out["status"], "LIVE")
        self.assertEqual(out["week_ending"], "2026-11-06")
        self.assertEqual(out["foreign_net"], -81.762)
        self.assertEqual(sorted(out["weeks"]), ["2026-10-30", "2026-11-06"])
        self.assertEqual(out["weeks"]["2026-10-30"]["markets"]["prime"]["foreigners"], 1.0)  # stored week kept, not re-fetched
        self.assertEqual(len([u for u in fetched if u.endswith(".xlsx")]), 1)
        with patch.object(mod, "_http_bytes", side_effect=lambda *a, **k: b"<html>no links</html>"):
            out = mod.japan_jpx(previous=stored)
        self.assertEqual(out["status"], "PENDING")
        self.assertEqual(out["weeks"], stored)


if __name__ == "__main__":
    unittest.main()
