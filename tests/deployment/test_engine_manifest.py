"""audit 2026-09-08 (section C-5): gen_engine_manifest binds the real Key of each write (AST), never a window."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _mod():
    spec = importlib.util.spec_from_file_location("gem", ROOT / "scripts" / "gen_engine_manifest.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_constant_prefix_fstring_outputs_are_declared():
    m = _mod()
    code = '''
import boto3
s3 = boto3.client("s3")
OUTPUT_PREFIX = "etf-flows/"
def lambda_handler(e, c):
    s3.put_object(Bucket="b", Key=f"{OUTPUT_PREFIX}daily.json", Body=b"{}")
    s3.put_object(Bucket="b", Key=OUTPUT_PREFIX + "composite.json", Body=b"{}")
    s3.put_object(Bucket="b", Key="data/activity-nowcast/snapshots/%s.json" % "x", Body=b"{}")
'''
    w, r, ok = m.ast_keys(code)
    assert ok and w == ["data/activity-nowcast/snapshots/x.json", "etf-flows/composite.json", "etf-flows/daily.json"], w
    dynamic, _, parsed = m.ast_keys(code.replace('% "x"', '% observed_date'))
    assert parsed and dynamic == ["data/activity-nowcast/snapshots/*.json", "etf-flows/composite.json", "etf-flows/daily.json"], dynamic


def test_a_read_after_a_write_is_not_an_output_and_wrappers_bind_their_key_argument():
    m = _mod()
    code = '''
import json, boto3
s3 = boto3.client("s3")
OUT_KEY = "data/allocator.json"
def put_json(key, obj):
    s3.put_object(Bucket="b", Key=key, Body=json.dumps(obj).encode())
def lambda_handler(e, c):
    bus = json.loads(s3.get_object(Bucket="b", Key="data/indicator-bus.json")["Body"].read())
    put_json(OUT_KEY, {"ok": True})
    put_json("air/hkia-cargo-levels.json", {"ok": True})
'''
    w, r, ok = m.ast_keys(code)
    assert ok and w == ["air/hkia-cargo-levels.json", "data/allocator.json"], w
    assert r == ["data/indicator-bus.json"], r
    # the legacy window scanner counted the bus read as an output; the AST path must not
    assert "data/indicator-bus.json" not in w


def test_parse_failure_remains_explicit_without_unsafe_regex_fallback():
    m = _mod()
    w, r, ok = m.ast_keys("def broken(:\n  pass")
    assert ok is False and w == [] and r == []
