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
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
import public_brain_projection
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
    env.update({"vault_search_rows": public_brain_projection.vault_search_rows, "project_notes_block": public_brain_projection.public_notes_block})
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
            "series_source": types.SimpleNamespace(fetch=lambda *a: {"2025-01-01": -0.2, "2026-08-01": 0.2}),
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

    def test_notes_intel_keeps_full_iam_and_owner_copy_and_publishes_safe_siblings(self):
        s3 = S3({"data/tradingview-notes.json": {"notes": [
            {"id": "stock-note", "symbol": "NVDA", "text": "buy above 100 during " + MARKER, "created": 1788912000},
            {"id": "macro-note", "symbol": "UNTAGGED", "text": "liquidity crisis " + MARKER, "created": 1788912000}]}})
        scope, private = self.load("notes-intel", s3)
        with patch.dict(sys.modules, {"llm_router": types.SimpleNamespace(complete=lambda *a, **kw: json.dumps({"view": MARKER, "stance": "BULLISH", "triggers": [MARKER], "invalidation": MARKER}))}):
            scope["lambda_handler"]({}, None)
        self.assertEqual({kind for kind, _ in private}, {"notes-index", "notes-themes"})
        for kind in ("notes-index", "notes-themes"):
            self.assertIn(MARKER, json.dumps(s3.writes["data/" + kind + ".json"]))
            self.assert_public(s3.writes["data/" + kind + "-public.json"])
        row = s3.writes["data/notes-index-public.json"]["index"]["NVDA"]
        self.assertEqual(row["note_ids"], ["stock-note"])
        self.assertEqual(row["levels"], [{"relation": "above", "value": 100.0}])
        self.assertNotIn("llm_view", row)

    def test_playbook_keeps_full_private_rules_but_publishes_reference_parameters(self):
        s3 = S3({"data/tradingview-notes.json": {"notes": [{"id": "rule_1", "text": "Yield curve inversion leads crashes by 30 months " + MARKER}]}})
        scope, private = self.load("playbook-engine", s3)
        scope["lambda_handler"]({}, None)
        self.assertIn(MARKER, json.dumps(s3.writes["data/playbook-rules.json"]))
        self.assertEqual(private[0][0], "playbook-rules")
        public = s3.writes["data/playbook-rules-public.json"]
        self.assertEqual(public["rules"][0]["id"], "rule_1")
        self.assertEqual(public["rules"][0]["params"]["n"], 30)
        self.assert_public(public)

    def test_equity_research_projection_scrubs_legacy_cache_and_current_note_references(self):
        project = function("equity-research", "public_notes_block", {})
        legacy = {"n_notes": 3, "stance": "BULLISH", "stance_score": 1.2, "latest_note": MARKER,
                  "llm_view": {"view": MARKER}, "note_ids": ["n1"], "unknown_prose": MARKER}
        public = project(legacy)
        self.assertEqual(public["n_notes"], 3)
        self.assertEqual(public["note_ids"], ["n1"])
        self.assertTrue(public["note_text_private"])
        self.assert_public(public)
        current = function("equity-research", "khalid_notes_block", {"_NOTES_IDX": {"v": {"NVDA": {"n_notes": 3, "latest": MARKER, "note_ids": ["n1"]}}}})("NVDA")
        self.assertEqual(current["note_ids"], ["n1"])
        self.assert_public(current)

    def test_equity_legacy_cache_is_rewritten_without_note_text_for_public_and_internal_hits(self):
        tree = ast.parse(source("equity-research").read_text())
        cache_branch = next(n for n in ast.walk(tree) if isinstance(n, ast.If)
                            and isinstance(n.test, ast.UnaryOp) and isinstance(n.test.op, ast.Not)
                            and isinstance(n.test.operand, ast.Name) and n.test.operand.id == "force_refresh")
        wrapper = ast.parse("def cache_only():\n    pass\n").body[0]
        wrapper.body = [cache_branch]
        for internal in (False, True):
            doc = {"schema_version": "fixture", "generated_at": NOW,
                   "khalid_notes": {"n_notes": 3, "latest_note": MARKER, "note_ids": ["n1"]}}
            s3 = S3({"equity-research/NVDA.json": doc})
            env = {"s3": s3, "force_refresh": False, "cache_key": "equity-research/NVDA.json", "S3_BUCKET": "bucket",
                   "ticker": "NVDA", "is_internal_async": internal, "json": json, "time": time,
                   "_iso_to_epoch": lambda _: time.time() - 100, "CACHE_TTL": 3600, "SCHEMA_CURRENT": "fixture", "_http_ok": lambda d: d}
            function("equity-research", "public_notes_block", env)
            exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])), "cache-boundary", "exec"), env)
            result = env["cache_only"]()
            self.assert_public(result)
            self.assert_public(s3.writes["equity-research/NVDA.json"])
            self.assertEqual(s3.writes["equity-research/NVDA.json"]["khalid_notes"]["note_ids"], ["n1"])

    def test_provider_search_never_indexes_cached_private_note_snippets(self):
        fn = function("provider-catalog", "_tradingview_live_search_rows", {})
        out = fn({"symbols": [{"symbol": "NVDA", "status": "LIVE", "note_snippet": MARKER, "category": "stock", "source": "market"}]})
        self.assertEqual(out[0]["id"], "tradingview-vault-live:NVDA")
        self.assert_public(out)


if __name__ == "__main__":
    unittest.main()
