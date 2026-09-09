"""Offline writer isolation, consumer schema, and enrichment race canaries."""
import ast
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def source(engine):
    return ROOT / "aws/lambdas" / ("justhodl-" + engine) / "source/lambda_function.py"


class Store:
    def __init__(self, doc=None):
        self.doc = doc or {}
        self.writes = []
        self.race = False
        self.always_race = False

    def etag(self):
        return hashlib.sha256(json.dumps(self.doc, sort_keys=True).encode()).hexdigest()

    def get_object(self, **kw):
        return {"Body": io.BytesIO(json.dumps(self.doc).encode()), "ETag": self.etag()}

    def put_object(self, **kw):
        if self.race or self.always_race:
            self.race = False
            self.doc.update({"price": 234, "khalid_index": {"score": 90}, "generated_at": "new-base"})
            exc = RuntimeError("synthetic conflict")
            exc.response = {"Error": {"Code": "PreconditionFailed"}}
            raise exc
        if kw.get("IfMatch"):
            assert kw["IfMatch"] == self.etag()
        self.writes.append(kw)
        self.doc = json.loads(kw["Body"])


def load(engine, store):
    with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **k: store),
            "managed_secret": types.SimpleNamespace(managed_secret=lambda *a, **k: "synthetic-key")}):
        return runpy.run_path(str(source(engine)))


def functions(engine, names):
    tree = ast.parse(source(engine).read_text())
    scope = {}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source(engine)), "exec"), scope)
    return scope


class OutputOwnershipTests(unittest.TestCase):
    def test_scanner_full_handler_owns_new_key_even_with_legacy_environment(self):
        store = Store()
        with patch.dict(os.environ, {"S3_KEY": "data/options-flow.json"}):
            scope = load("options-flow-scanner", store)
        handler = scope["lambda_handler"]
        env = handler.__globals__
        env["get_universe"] = lambda: ["NVDA"]
        env["get_finra_short_history"] = lambda **k: {}
        row = {"symbol": "NVDA", "score": 72, "tier": "TIER_A_BULLISH_FLOW", "flags": ["CPR_SURGING"],
               "metrics": {"spot": 123, "avg_cpr_recent_5d": 2.5, "cpr_change_pct": 15,
                           "call_vol_surge": 3, "short_metrics": {"short_pct_change": -4}}}
        env["evaluate_ticker"] = lambda *a: deepcopy(row)
        with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
            result = handler({}, None)
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual([w["Key"] for w in store.writes], ["data/options-flow-scanner.json"])
        self.assertEqual(store.doc["all_qualifying"], [row])
        self.assertEqual(store.doc["summary"]["top_25_overall"][0]["cpr_recent"], 2.5)

    def test_macro_flow_actual_publication_block_never_overwrites_scanner(self):
        tree = ast.parse(source("options-flow").read_text())
        block = next(n for n in ast.walk(tree) if isinstance(n, ast.Try)
                     and any(isinstance(x, ast.Import) and any(a.asname == "_b" for a in x.names) for x in n.body))
        store = Store()
        payload = {"success": True, "data": {"put_call": {"options_flow": [{"ticker": "NVDA"}]}}}
        with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **k: store)}), redirect_stdout(io.StringIO()):
            exec(compile(ast.Module(body=[block], type_ignores=[]), "macro-publication", "exec"), {"payload": payload, "json": json})
        self.assertEqual([w["Key"] for w in store.writes], ["flow-data.json"])
        self.assertEqual(store.doc, payload)

    def test_ecb_stress_calculation_keeps_owned_data_without_base_rmw(self):
        tree = ast.parse(source("ecb-derived").read_text())
        handler = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "lambda_handler")
        block = next(n for n in handler.body if isinstance(n, ast.Try)
                     and any(isinstance(x, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "ufsc" for t in x.targets) for x in n.body))
        out = {"indicators": {"usd_funding_stress_composite": {"score_0_100": 80},
            "ciss_acceleration": {"ciss_level": 0.25}, "bank_pass_through_premium": {"premium_pct": 2},
            "bank_funding_stress": {"mlf_eur_mn": 2000}}}
        env = {"out": out}  # Any old S3 read/write would fail this block.
        exec(compile(ast.Module(body=[block], type_ignores=[]), "ecb-stress", "exec"), env)
        esi = out["indicators"]["eurodollar_stress_index"]
        self.assertEqual(esi["esi_0_100"], 70.5)
        self.assertEqual(esi["tier"], "CRITICAL")
        self.assertNotIn('Key="data/ecb-detail.json", Body=', source("ecb-derived").read_text())

    def test_premium_consumers_use_nested_macro_rows_and_correct_ratio_direction(self):
        payload = {"data": {"put_call": {"options_flow": [
            {"ticker": "NVDA", "call_volume": 400, "put_volume": 100, "call_premium": 200000,
             "put_premium": 50000, "pc_ratio": .25, "sentiment": "BULLISH"},
            {"ticker": "BAD", "call_volume": 20, "put_volume": 0, "call_premium": 0, "put_premium": 0}]}}}
        extract = functions("catalyst-skew-premove", {"extract_options_skew"})["extract_options_skew"]
        self.assertEqual(extract(payload), {"NVDA": {"call_put_ratio": 4, "call_premium": 200000, "put_premium": 50000}})
        stealth = functions("stealth-accumulation", {"extract_options_flow_tickers"})["extract_options_flow_tickers"]
        row = stealth(payload)["NVDA"]
        self.assertEqual(row["call_put_ratio"], 4)
        self.assertEqual(row["call_premium_usd"], 200000)

    def test_master_ranker_maps_actual_scanner_score_and_tier_without_fabricated_premium(self):
        tree = ast.parse(source("master-ranker").read_text())
        block = next(n for n in ast.walk(tree) if isinstance(n, ast.If)
                     and ast.unparse(n.test) == "feeds['options_flow']")
        env = {"feeds": {"options_flow": {"all_qualifying": [{"symbol": "NVDA", "score": 0, "tier": "NEUTRAL"}]}}, "idx": {}}
        exec(compile(ast.Module(body=[block], type_ignores=[]), "ranker-flow", "exec"), env)
        self.assertEqual(env["idx"]["NVDA"]["options_flow"], {"score": 0, "flag": "NEUTRAL", "premium": None})

    def test_options_confluence_distinguishes_scanner_flow_from_volatility_compression(self):
        tree = ast.parse(source("options-confluence").read_text())
        block = next(n for n in ast.walk(tree) if isinstance(n, ast.For)
                     and isinstance(n.target, ast.Tuple) and ast.unparse(n.target) == "(fk, key)")
        calls = []
        docs = {"data/options-flow-scanner.json": {"all_qualifying": [{"symbol": "NVDA", "tier": "TIER_A_BULLISH_FLOW"}]},
                "data/volatility-squeeze.json": {"all_qualifying": [{"symbol": "SPY", "tier": "A"}]}}
        env = {"gated": lambda name: (True, None), "_read": docs.get, "_tk": lambda row: row["symbol"],
               "add": lambda *a, **k: calls.append((a, k))}
        exec(compile(ast.Module(body=[block], type_ignores=[]), "confluence-flow", "exec"), env)
        self.assertEqual(calls[0][0][:2], ("NVDA", "options-flow-scanner"))
        self.assertNotIn("coiled", calls[0][1])
        self.assertTrue(calls[1][1]["coiled"])

    def test_macro_majors_consumer_reads_actual_premium_flow_sentiment(self):
        tree = ast.parse(source("massive-signals").read_text())
        nodes = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id in {"flow_rows", "majors"} for t in n.targets)]
        env = {"of": {"data": {"put_call": {"options_flow": [{"ticker": "NVDA", "sentiment": "BULLISH"}]}}}}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "majors-flow", "exec"), env)
        self.assertEqual(env["majors"], {"NVDA": "BULLISH"})

    def test_crypto_full_handler_rebases_on_new_v10_without_losing_prices(self):
        store = Store({"version": "V10", "generated_at": "old-base", "price": 123, "khalid_index": {"score": 10}})
        scope = load("crypto-enricher", store)
        handler = scope["lambda_handler"]
        env = handler.__globals__
        for name in ("get_defi_tvl", "get_eth_gas", "get_funding_rates", "get_leverage_sentiment"):
            env[name] = lambda: {}
        store.race = True
        with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
            result = handler({}, None)
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(store.doc["price"], 234)
        self.assertEqual(store.doc["generated_at"], "new-base")
        self.assertEqual(store.doc["market_intelligence"]["ml_regime"], "RISK-ON")
        self.assertTrue(store.writes[0]["IfMatch"])
        self.assertEqual(store.doc["enrichment_provenance"]["base_producer"], "justhodl-daily-report-v3")
        self.assertEqual(len(store.writes), 1)

    def test_crypto_rejects_wrong_base_and_exhausted_races(self):
        store = Store({"version": "V8"})
        save = load("crypto-enricher", store)["save_enrichment"]
        with self.assertRaisesRegex(ValueError, "report_base_schema_not_v10"):
            save({})
        self.assertFalse(store.writes)
        store.doc = {"version": "V10"}
        store.always_race = True
        with self.assertRaisesRegex(RuntimeError, "report_changed_during_enrichment"):
            save({})
        self.assertFalse(store.writes)

    def test_source_keys_and_dedicated_pages_match_each_schema(self):
        self.assertIn("Key='data/bloomberg-report.json'", source("bloomberg-v8").read_text())
        self.assertNotIn("Key='data/report.json'", source("bloomberg-v8").read_text())
        self.assertIn("Key='data/report.json'", source("daily-report-v3").read_text())
        for page in ("options-scanner.html", "options.html", "intel/index.html", "web/intel/index.html"):
            text = (ROOT / page).read_text()
            self.assertIn("options-flow-scanner.json", text)
            self.assertNotIn("'options-flow.json'", text)
            self.assertNotIn('data/options-flow.json"', text)
        page = (ROOT / "ecb-detail.html").read_text()
        self.assertIn('J("ecb-derived.json")', page)
        self.assertIn("esi.esi_0_100", page)
        self.assertIn("esi.tier", page)
        for p in (ROOT / "aws/lambdas").glob("*/source/*.py"):
            self.assertNotIn("data/options-flow.json", p.read_text(), str(p))


if __name__ == "__main__":
    unittest.main()
