"""Synthetic private-prose canaries at real producer publication boundaries.

Only local source is executed. AWS clients, LLM calls and private publication are
stubbed. Marker text is synthetic, never a real note or credential.
"""
import ast
import io
import json
import re
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
MARKER = "SYNTHETIC_PRIVATE_NOTE_7f00"
NOW = "2026-09-09T00:00:00Z"


class S3:
    def __init__(self, docs=None):
        self.docs = docs or {}
        self.writes = {}

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(json.dumps(self.docs.get(kwargs["Key"], {})).encode())}

    def put_object(self, **kwargs):
        self.writes[kwargs["Key"]] = json.loads(kwargs["Body"])
        return {}


def source(engine):
    return ROOT / "aws/lambdas" / ("justhodl-" + engine) / "source/lambda_function.py"


def function(engine, name, env):
    tree = ast.parse(source(engine).read_text())
    selected = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)
    selected.decorator_list = []
    exec(compile(ast.Module(body=[selected], type_ignores=[]), str(source(engine)), "exec"), env)
    return env[name]


class PublicBoundaryTests(unittest.TestCase):
    def load(self, engine, s3):
        private = []
        with patch.dict(sys.modules, {
            "boto3": types.SimpleNamespace(client=lambda *a, **kw: s3),
            "anthropic_shim": types.ModuleType("anthropic_shim"),
            "private_artifact": types.SimpleNamespace(publish_private=lambda kind, doc: private.append((kind, json.loads(json.dumps(doc))))),
        }):
            scope = runpy.run_path(str(source(engine)))
        return scope, private

    def assert_public(self, doc):
        self.assertNotIn(MARKER.lower(), json.dumps(doc).lower())

    def test_compiler_keeps_claim_routing_and_note_ids_without_claim_prose(self):
        s3 = S3({"data/brain.json": {"notes": [{"id": "note_1", "cat": "macro", "text": "Watch the SOFR repo market above 7 during " + MARKER + "."}]}})
        scope, _ = self.load("brain-compiler", s3)
        scope["lambda_handler"]()
        out = s3.writes["data/brain-compiler.json"]
        self.assertGreater(out["summary"]["n_claims"], 0)
        self.assertEqual(out["claims"][0]["note_id"], "note_1")
        self.assertTrue(out["claims"][0]["claim_text_private"])
        self.assert_public(out)
        self.assertTrue(out["build_queue"][0]["note_ids"])

    def test_workbench_joins_private_notes_but_publishes_only_references(self):
        s3 = S3({"data/brain.json": {"notes": [{"id": "note_2", "text": "[TV:NASDAQ:NVDA] " + MARKER}]},
                 "data/tv-watchlists.json": {"lists": [{"id": "w1", "name": "Semiconductors", "symbols": ["NASDAQ:NVDA"]}]}})
        scope, _ = self.load("tv-workbench", s3)
        scope["lambda_handler"]({}, None)
        out = s3.writes["data/tv-workbench.json"]
        self.assertEqual(out["symbols"]["NASDAQ:NVDA"]["n_notes"], 1)
        self.assertEqual(out["symbols"]["NASDAQ:NVDA"]["notes"][0]["note_id"], "note_2")
        self.assert_public(out)

    def test_canary_playbook_retains_references_without_text(self):
        brain = {"notes": [{"id": "note_3", "pinned": True, "cat": "macro", "text": "Watch canaries and the liquidity crunch during " + MARKER}]}
        out = function("canary-warroom", "brain_playbook", {"re": re, "gj": lambda key: brain})()
        self.assertEqual(out[0]["note_id"], "note_3")
        self.assertTrue(out[0]["text_private"])
        self.assert_public(out)

    def test_public_llm_projection_excludes_all_model_prose_not_regex_redaction(self):
        for engine, key in [("my-brief", "brief"), ("devils-advocate", "cases")]:
            with self.subTest(engine=engine):
                s3 = S3()
                scope, private = self.load(engine, s3)
                doc = {"engine": engine, "generated_at": NOW, "brief": MARKER,
                       "cases": [{"ticker": "NVDA", "risk_level": "high", "bear_case": MARKER, "violates_your_rule": MARKER},
                                 {"ticker": MARKER, "risk_level": MARKER, "bear_case": MARKER}]}
                scope["publish"](doc, {"NVDA"}) if engine == "devils-advocate" else scope["publish"](doc)
                self.assertIn(MARKER, json.dumps(s3.writes["data/" + engine + ".json"]))
                self.assertIn(MARKER, json.dumps(private[0][1]))
                safe = s3.writes["data/" + engine + "-public.json"]
                self.assert_public(safe)
                self.assertTrue(safe["private_text"])
                if engine == "devils-advocate":
                    self.assertEqual(safe["cases"][0]["risk_level"], "high")
                    self.assertTrue(safe["cases"][0]["rule_violation"])
                    self.assertEqual(len(safe["cases"]), 1)

    def test_engine_conflicts_consumes_private_review_without_reemission(self):
        s3 = S3({"data/best-setups.json": {"top_setups": [{"ticker": "NVDA", "conviction": 90, "verdict": "WATCH"}]},
                 "data/devils-advocate.json": {"by_ticker": {"NVDA": {"bear_case": MARKER, "violates_your_rule": MARKER, "risk_level": "high"}}}})
        scope, _ = self.load("engine-conflicts", s3)
        scope["lambda_handler"]()
        out = s3.writes["data/engine-conflicts.json"]
        self.assertEqual(out["n_conflicts"], 1)
        self.assertTrue(out["conflicts"][0]["private_review_ref"]["rule_violation"])
        self.assert_public(out)

    def test_position_sizer_uses_private_posture_but_emits_only_enum(self):
        s3 = S3({"data/brain.json": {"directive": {"risk_posture": "aggressive because " + MARKER}},
                 "data/best-setups.json": {"top_setups": [{"ticker": "NVDA", "conviction": 90}]}})
        scope, _ = self.load("position-sizer", s3)
        scope["lambda_handler"]()
        out = s3.writes["data/position-sizing.json"]
        self.assertEqual(out["posture_mult"], 1.3)
        self.assertEqual(out["risk_posture"], "aggressive")
        self.assertTrue(out["sized_positions"])
        self.assert_public(out)

    def test_best_setups_theme_and_sector_alignment_explanations_do_not_quote_policy(self):
        for tilts, themes in [({"technology": "overweight because " + MARKER}, []), ({}, ["technology " + MARKER])]:
            fn = function("best-setups", "brain_match", {"brain_tilts": tilts, "brain_themes": themes})
            out = fn("Technology")
            self.assertTrue(out)
            self.assert_public(out)

    def test_master_allocator_retains_intensity_without_original_posture(self):
        s3 = S3({"data/brain.json": {"directive": {"risk_posture": "defensive at the macro because " + MARKER}}})
        scope, _ = self.load("master-allocator", s3)
        out = scope["gather_signals"]()
        self.assertEqual(out["brain_posture"]["value"], 0.6)
        self.assertEqual(out["brain_posture"]["regime"], "DEFENSIVE")
        self.assert_public(out)

    def test_domain_public_output_removes_stale_vault_note_snippet(self):
        s3 = S3({"data/brain.json": {"notes": [{"id": "note_1", "text": MARKER}]},
                 "data/tradingview.json": {"symbols": [{"symbol": "VIX", "category": "vol", "status": "LIVE", "value": 20, "note_snippet": MARKER}]}})
        scope, _ = self.load("domain-barometers", s3)
        env = scope["lambda_handler"].__globals__
        env.update({"classify": lambda brain, rows: ({"VIX": "RISK"}, {"VIX": "T1"}, {"VIX": "private note reference"}, {}, {"VIX": ["note_1"]}, {"VIX": "HIGH"}, {}, 1),
                    "load_prev_values": lambda: ({}, None), "update_ledger": lambda *a: {}, "predict": lambda *a: {},
                    "build_barometers": lambda *a: {d: {"score_0_100": 50} for d in ("MACRO", "LIQUIDITY", "RISK")}})
        scope["lambda_handler"]({}, None)
        out = s3.writes["data/domain-barometers.json"]
        self.assertEqual(out["symbols"][0]["brain_note_ids"], ["note_1"])
        self.assertTrue(out["symbols"][0]["note_text_private"])
        self.assert_public(out)

    def test_provider_search_never_indexes_cached_private_note_snippets(self):
        fn = function("provider-catalog", "_tradingview_live_search_rows", {})
        out = fn({"symbols": [{"symbol": "NVDA", "status": "LIVE", "note_snippet": MARKER, "category": "stock", "source": "market"}]})
        self.assertEqual(out[0]["id"], "tradingview-vault-live:NVDA")
        self.assert_public(out)


if __name__ == "__main__":
    unittest.main()
