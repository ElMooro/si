"""Exercise actual consumer functions against mixed, stale and typed packets."""
import ast
import io
import json
from pathlib import Path
from datetime import datetime, timezone
import unittest
ROOT = Path(__file__).resolve().parents[1]


def functions(engine, names, env):
    path = ROOT / "aws/lambdas" / engine / "source/lambda_function.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(selected) == len(names)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), env)
    return env


class PlumbingFiscalTests(unittest.TestCase):
    def run_joins(self, packet):
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None): return cls(2026, 9, 18, tzinfo=timezone.utc)
        env = {"_s3_json": lambda key: packet if key.endswith("latest-summary.json") else None,
               "datetime": Clock, "timezone": timezone}
        return functions("justhodl-plumbing-aggregator", ["l0_s3_joins"], env)["l0_s3_joins"]()

    def test_numeric_counts_and_untyped_tga_are_never_measurements(self):
        self.assertEqual(self.run_joins({"datasets": {"tga_operating_cash": {"n_obs": 999, "current": 2}}}), [])

    def test_only_fresh_closing_balance_uses_observation_date_and_unit(self):
        head = {"field": "open_today_bal", "unit": "USD_millions", "as_of": "2026-09-17", "value": 972675,
                "dimensions": {"account_type": "Treasury General Account (TGA) Closing Balance"}}
        row = {"headline": head, "freshness": {"status": "fresh"}}
        packet = {"contract": "treasury-fiscal-warehouse.v2", "as_of": "2026-09-18",
                  "datasets": {"tga_operating_cash": row}}
        out = self.run_joins(packet)
        self.assertEqual(out[0]["value"], 972675); self.assertEqual(out[0]["date"], "2026-09-17")
        self.assertEqual(out[0]["weight_in_layer"], 0); self.assertEqual(out[0]["unit"], "USD_millions")
        head["as_of"] = "2020-01-01"; self.assertEqual(self.run_joins(packet), [])
        head["as_of"] = "2026-09-17"
        row["freshness"]["status"] = "stale"; self.assertEqual(self.run_joins(packet), [])


class SymdirFiscalTests(unittest.TestCase):
    def test_legacy_cache_cannot_bypass_new_resolver(self):
        stamp = datetime.now(timezone.utc)
        class Store:
            def get_object(self, **kw):
                return {"Body": io.BytesIO(b'{"n":2,"obs":[["2026-01-01",9]]}'), "LastModified": stamp}
        env = {"s3": Store(), "BUCKET": "b", "_cache_key": lambda sid: sid, "_now": lambda: stamp, "json": json}
        fn = functions("justhodl-symdir", ["_cache_get"], env)["_cache_get"]
        self.assertIsNone(fn("treasury:rates_of_exchange", 900)[0])
        self.assertEqual(fn("fred:DFF", 900)[0]["n"], 2)

    def test_ambiguous_treasury_does_not_fuzzily_resolve_another_instrument(self):
        def fail(*args, **kw): raise ValueError("select explicit currency")
        def forbidden(*args): raise AssertionError("fuzzy fallback must not run")
        fn = functions("justhodl-symdir", ["fetch_series"], {"_fetch_series": fail, "closest_ids": forbidden})["fetch_series"]
        with self.assertRaisesRegex(ValueError, "explicit currency"): fn("treasury:rates_of_exchange")

    def test_typed_series_keeps_row_evidence_and_legacy_scalar_is_withheld(self):
        from treasury_fiscal_model import build
        from test_treasury_fiscal_model import page, debt, NOW
        warehouse = build("debt_to_penny", None, [page("debt_to_penny", [debt()])], NOW)
        env = {"_get_json": lambda key: warehouse, "_result": lambda *a, **kw: {"obs": a[2]}}
        fn = functions("justhodl-symdir", ["r_treasury"], env)["r_treasury"]
        out = fn("treasury:debt_to_penny", "debt_to_penny", None)
        self.assertEqual(out["measurement_evidence"][0]["value_decimal"], "40093343468150.50")
        self.assertFalse(out["sizing_eligible"])
        warehouse.clear(); warehouse["observations"] = [{"date": "2026-09-17", "value": 999}]
        with self.assertRaises(ValueError): fn("treasury:debt_to_penny", "debt_to_penny", None)
