"""justhodl-outcome-checker -- audit 2026-09-08 INST-12 tests (fake S3 + fake Yahoo, no AWS)."""
from __future__ import annotations

import gzip
import importlib.util
import io
import json
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
sys.path.insert(0, str(HERE.parents[2] / "shared"))


class _S3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise Exception("NoSuchKey")
        return {"Body": types.SimpleNamespace(read=lambda: self.objects[Key])}


def _load(objects, yahoo_closes=None):
    fake = types.ModuleType("boto3")
    s3 = _S3(objects)
    fake.client = lambda svc, *a, **k: s3
    fake.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda n: None)
    sys.modules["boto3"] = fake
    dyn = types.ModuleType("boto3.dynamodb"); cond = types.ModuleType("boto3.dynamodb.conditions"); cond.Attr = lambda *a, **k: None
    dyn.conditions = cond; sys.modules["boto3.dynamodb"] = dyn; sys.modules["boto3.dynamodb.conditions"] = cond
    sl = types.ModuleType("_sentry_lite"); sl.track_errors = lambda f: f; sys.modules["_sentry_lite"] = sl
    ms = types.ModuleType("managed_secret"); ms.managed_secret = lambda *a, **k: ""; sys.modules["managed_secret"] = ms
    import urllib.request

    def fake_urlopen(req, timeout=10):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "finance.yahoo.com" in url and yahoo_closes:
            body = json.dumps({"chart": {"result": [{"timestamp": [t for t, _ in yahoo_closes], "indicators": {"quote": [{"close": [c for _, c in yahoo_closes]}]}}]}}).encode()
            return io.BytesIO(body)
        raise Exception("no network in test: " + url)
    urllib.request.urlopen = fake_urlopen
    spec = importlib.util.spec_from_file_location("oc_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _gz(results):
    return gzip.compress(json.dumps({"results": results}).encode())


def test_marks_come_from_the_house_warehouse_first_with_provenance_and_session_alignment():
    objs = {"data/warm/polygon-full/grouped/2026/2026-09-04.json.gz": _gz([{"T": "NVDA", "c": 120.5}, {"T": "SPY", "c": 650.0}])}
    mod = _load(objs)
    m = mod.get_mark_at("NVDA", "2026-09-06")           # Saturday -> falls back to Friday's session
    assert m and m["price"] == 120.5 and m["as_of"] == "2026-09-04" and m["provider"].startswith("s3:polygon-grouped"), m
    b = mod.get_mark_at("SPY", "2026-09-06")
    assert b["as_of"] == m["as_of"], "asset and benchmark share the session"
    assert mod.get_price_at("NVDA", "2026-09-06") == 120.5


def test_falls_back_to_yahoo_when_the_warehouse_has_no_session_and_none_when_nothing_prices():
    import datetime as _dt
    t = int(_dt.datetime(2026, 9, 4, 20, tzinfo=_dt.timezone.utc).timestamp())
    mod = _load({}, yahoo_closes=[(t - 86400 * 3, 99.0), (t, 101.0)])
    m = mod.get_mark_at("XYZ", "2026-09-05")
    assert m and m["price"] == 101.0 and m["provider"].startswith("yahoo"), m
    mod2 = _load({})
    assert mod2.get_mark_at("NOPE", "2026-09-05") is None


def test_pending_policy_contract_no_zero_grades_and_shared_mark_function():
    src = SRC.read_text()
    assert "get_price(ticker)" not in src.split("def process_signals")[-1] if "def process_signals" in src else True
    assert '"UNSCOREABLE"' in src and "MAX_PENDING_ATTEMPTS" in src and '"_pending"' in src
    assert 'float(excess) if excess else 0.0' not in src, "missing marks must never finalise as a 0.0 excess return"
    assert "get_mark_at(benchmark, _as_of)" in src and "get_mark_at(ticker, _as_of)" in src
    assert '"graded_at_session"' in src and '"marks"' in src


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn()
        print("ok", name)
    print("outcome-checker tests passed: %d" % len(tests))
