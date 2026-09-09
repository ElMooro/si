"""justhodl-fleet-freshness-monitor -- audit 2026-09-08 INST-13 tests (fake S3, no AWS)."""
from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
sys.path.insert(0, str(HERE.parents[2] / "shared"))


class _S3:
    def __init__(self, objects):
        self.objects = dict(objects)   # key -> bytes
        self.reads = []
        self.writes = []

    def get_object(self, Bucket, Key, Range=None):
        self.reads.append(Key)
        if Key not in self.objects:
            raise Exception("NoSuchKey " + Key)
        b = self.objects[Key]
        if Range:
            lo, hi = Range.replace("bytes=", "").split("-")
            b = b[int(lo):int(hi) + 1]
        return {"Body": types.SimpleNamespace(read=lambda: b), "LastModified": datetime.now(timezone.utc)}

    def put_object(self, **kw):
        self.writes.append(kw["Key"])
        self.objects[kw["Key"]] = kw["Body"]

    def get_paginator(self, name):
        objs = self.objects

        class P:
            def paginate(self, Bucket, Prefix, Delimiter=None):
                items = [{"Key": k, "Size": len(v), "LastModified": datetime.now(timezone.utc)} for k, v in sorted(objs.items())
                         if k.startswith(Prefix) and (not Delimiter or Delimiter not in k[len(Prefix):])]
                for i in range(0, max(1, len(items)), 2):
                    yield {"Contents": items[i:i + 2]}
        return P()


def _load(objects, max_keys=None):
    fake = types.ModuleType("boto3")
    s3 = _S3(objects)
    fake.client = lambda svc, *a, **k: s3 if svc == "s3" else types.SimpleNamespace(publish=lambda **k: None)
    sys.modules["boto3"] = fake
    bc = types.ModuleType("botocore"); bce = types.ModuleType("botocore.exceptions"); bce.ClientError = Exception
    bc.exceptions = bce; sys.modules["botocore"] = bc; sys.modules["botocore.exceptions"] = bce
    import os
    os.environ["MAX_KEYS_PER_RULE"] = str(max_keys or 10000)
    spec = importlib.util.spec_from_file_location("fm_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, s3


def _iso(hours_ago):
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()


def test_zero_byte_and_invalid_json_are_not_fresh():
    mod, s3 = _load({"data/a.json": b"", "data/b.json": b"{not json", "data/c.json": b"{}"})
    assert mod.validate_body("data/a.json", 0, 26)["content_status"] == "EMPTY"
    assert mod.validate_body("data/b.json", 9, 26)["content_status"] == "INVALID"
    assert mod.validate_body("data/c.json", 2, 26)["content_status"] == "EMPTY"


def test_fresh_wrapper_with_old_source_timestamp_is_source_stale_and_future_is_invalid():
    mod, s3 = _load({"data/x.json": json.dumps({"generated_at": _iso(200), "rows": [1]}).encode(),
                     "data/y.json": json.dumps({"generated_at": _iso(-6), "rows": [1]}).encode(),
                     "data/z.json": json.dumps({"as_of": _iso(1), "rows": [1]}).encode()})
    x = mod.validate_body("data/x.json", 60, 26)
    assert x["content_status"] == "SOURCE_STALE" and x["source_age_h"] > 190, x
    y = mod.validate_body("data/y.json", 60, 26)
    assert y["content_status"] == "INVALID" and "future" in y["reason"], y
    z = mod.validate_body("data/z.json", 60, 26)
    assert z["content_status"] == "OK" and z["source_ts_field"] == "as_of", z


def test_large_object_uses_a_head_range_read_for_the_timestamp():
    big = ('{"generated_at": "%s", "payload": "%s"}' % (_iso(300), "x" * 100)).encode()
    mod, s3 = _load({"data/big.json": big})
    mod.VALIDATE_MAX_BYTES = 10
    r = mod.validate_body("data/big.json", len(big), 26)
    assert r["content_status"] == "SOURCE_STALE" and "head-only" in r.get("note", ""), r


def test_enumeration_reports_truncation_instead_of_silently_stopping():
    mod, s3 = _load({"data/%d.json" % i: b"{\"a\":1}" for i in range(7)}, max_keys=4)
    keys, truncated = mod.list_keys_under_rule({"prefix": "data/"})
    assert truncated is True and len(keys) == 4
    mod2, _ = _load({"data/%d.json" % i: b"{\"a\":1}" for i in range(3)}, max_keys=400)
    keys, truncated = mod2.list_keys_under_rule({"prefix": "data/"})
    assert truncated is False and len(keys) == 3


def test_data_rule_lists_depth_one_by_default_and_recursive_on_request():
    objs = {"data/a.json": b"{\"a\":1}", "data/warm/deep/b.json": b"{\"a\":1}"}
    mod, _ = _load(dict(objs), max_keys=400)
    keys, _t = mod.list_keys_under_rule({"prefix": "data/"})
    assert [k["Key"] for k in keys] == ["data/a.json"], "the warehouse must not be walked by default"
    keys, _t = mod.list_keys_under_rule({"prefix": "data/", "recursive": True})
    assert len(keys) == 2


def test_scoped_keys_are_depth_one_feeds_and_overrides():
    mod, _ = _load({})
    manifest = {"key_overrides": {"data/warm/special/feed.json": {"max_age_h": 48}}}
    assert mod.scoped_key("data/katlin.json", manifest)
    assert not mod.scoped_key("data/warm/katlin/shares/x.json", manifest)
    assert mod.scoped_key("data/warm/special/feed.json", manifest)


def test_real_evaluator_propagates_unknown_and_nested_empty():
    mod,_=_load({"data/a.json":b'{"generated_at":"garbage","rows":[1]}',"data/nested/empty.json":b''})
    def obj(key,size): return {"Key":key,"Size":size,"LastModified":datetime.now(timezone.utc)}
    rule={"default_max_age_h":26}
    assert mod.evaluate_key(obj("data/a.json",40),rule,{})["status"]=="UNKNOWN"
    assert mod.evaluate_key(obj("data/unreadable.json",40),rule,{})["status"]=="UNKNOWN"
    assert mod.evaluate_key(obj("data/nested/empty.json",0),rule,{})["status"]=="EMPTY"
    assert mod.validate_body("data/a.json",40,26,schema={"required_fields":["score"]})["content_status"]=="INVALID"


def test_actual_handler_validates_expected_nested_feeds_and_reports_missing():
    manifest={"rules":[{"prefix":"data/","default_max_age_h":26}]}
    engine_manifest={"engines":[{"engine":"fixture","keys":["portfolio/deep.json","portfolio/missing.json"]}]}
    objects={"data/_freshness-manifest.json":json.dumps(manifest).encode(),
             "data/engine-manifest.json":json.dumps(engine_manifest).encode(),
             "portfolio/deep.json":b''}
    mod,s3=_load(objects)
    class Absent(Exception): response={"Error":{"Code":"404"}}
    def head(**kw):
        if kw["Key"] not in s3.objects: raise Absent()
        return {"ContentLength":len(s3.objects[kw["Key"]]),"LastModified":datetime.now(timezone.utc)}
    s3.head_object=head
    mod.send_telegram=lambda *_:False; mod.publish_sns=lambda *_:False
    mod.lambda_handler()
    payload=json.loads(s3.objects["data/_freshness-monitor.json"])
    assert payload["status"]=="DEGRADED",payload
    assert payload["n_missing"]==1 and payload["n_invalid_or_empty"]>=1,payload
    assert any(r["key"]=="portfolio/deep.json" for r in payload["invalid_or_empty"])
    assert payload["coverage"]["expected_keys_checked"]==2


def test_actual_handler_missing_registry_publishes_unknown():
    mod,s3=_load({"data/_freshness-manifest.json":json.dumps({"rules":[{"prefix":"data/"}]}).encode()})
    mod.send_telegram=lambda *_:False; mod.publish_sns=lambda *_:False
    result=mod.lambda_handler()
    assert result["statusCode"]==503
    state=json.loads(s3.objects["data/_freshness-monitor.json"])
    assert state["status"]=="UNKNOWN" and state["full_expected_coverage"] is False


def _handler_fixture(count=1):
    manifest = {"rules": [{"prefix": "data/", "default_max_age_h": 26, "note": "SYNTHETIC_PRIVATE_DIAGNOSTIC"}]}
    expected = {"engines": [{"engine": "fixture", "keys": ["data/feed-%03d.json" % i for i in range(count)]}]}
    return {"data/_freshness-manifest.json": json.dumps(manifest).encode(),
            "data/engine-manifest.json": json.dumps(expected).encode(),
            **{"data/feed-%03d.json" % i: json.dumps({"generated_at": _iso(200), "private_canary": "SYNTHETIC_PRIVATE_DIAGNOSTIC"}).encode() for i in range(count)}}


def test_actual_handler_quiet_refresh_preserves_all_rows_and_never_notifies_or_updates_history():
    mod, s3 = _load(_handler_fixture(125))
    def denied(*a, **k): raise AssertionError("quiet refresh must not notify")
    mod.send_telegram = denied; mod.publish_sns = denied
    result = mod.lambda_handler({"mode": "quiet_refresh"})
    state = json.loads(s3.objects["data/_freshness-monitor.json"])
    assert state["status"] == "DEGRADED"
    assert state["n_source_stale"] == len(state["source_stale"]) == 125
    assert len(state["source_stale_top_50"]) == 50 and state["summary_limits"]["complete_results_field"] == "results"
    assert state["n_keys_tracked"] == len(state["results"]) == state["coverage"]["results_returned"]
    assert len(state["stale"]) >= 125 and state["coverage"]["results_complete"] is True
    assert s3.writes == ["data/_freshness-monitor.json"]
    assert "data/_freshness-alert-history.json" not in s3.reads
    assert state["notifications_suppressed"] is True and state["n_alerts_raised"] == 0
    assert "SYNTHETIC_PRIVATE_DIAGNOSTIC" not in json.dumps(state)
    checker_path = HERE.parents[2] / "ops/checks/audit_20260909_accounting.py"
    spec = importlib.util.spec_from_file_location("freshness_release_checker", checker_path)
    checker = importlib.util.module_from_spec(spec); spec.loader.exec_module(checker)
    assert checker.inspect_payload("data/_freshness-monitor.json", state) == []


def test_actual_handler_complete_unknown_invalid_and_missing_groups_exceed_legacy_caps():
    objects = _handler_fixture(180)
    for i in range(110):
        objects["data/feed-%03d.json" % i] = b'{"rows":[1]}'
    for i in range(110, 180):
        objects["data/feed-%03d.json" % i] = b''
    expected = json.loads(objects["data/engine-manifest.json"])
    expected["engines"][0]["keys"].extend("public/missing-%03d.json" % i for i in range(60))
    objects["data/engine-manifest.json"] = json.dumps(expected).encode()
    mod, s3 = _load(objects)
    class Missing(Exception):
        response = {"Error": {"Code": "404"}}
    s3.head_object = lambda **kw: (_ for _ in ()).throw(Missing())
    mod.lambda_handler({"mode": "quiet_refresh"})
    state = json.loads(s3.objects["data/_freshness-monitor.json"])
    assert state["n_unknown"] == len(state["unknown"]) >= 110
    assert state["n_invalid_or_empty"] == len(state["invalid_or_empty"]) == 70
    assert state["n_missing"] == len(state["missing"]) == 60
    assert state["n_keys_tracked"] == len(state["results"]) == 241
    assert state["status"] == "DEGRADED" and state["full_expected_coverage"] is False


def test_actual_handler_validate_only_computes_without_any_writes_and_preserves_normal_alerts():
    mod, s3 = _load(_handler_fixture())
    def denied(*a, **k): raise AssertionError("validation must not notify")
    mod.send_telegram = denied; mod.publish_sns = denied
    result = mod.lambda_handler({"mode": "validate_only"})
    assert result["ok"] is True and result["validation_only"] is True
    assert result["schema_version"] == "audit-freshness-1.0" and result["artifact_size_bytes"] > 0
    assert result["report_status"] == "DEGRADED" and s3.writes == []
    assert "data/feed-000.json" in s3.reads
    calls = []
    mod.send_telegram = lambda *a: calls.append("telegram") or True
    mod.publish_sns = lambda *a: calls.append("sns") or True
    mod.lambda_handler()
    assert "telegram" in calls and "sns" in calls
    assert "data/_freshness-seen-keys.json" in s3.writes and "data/_freshness-alert-history.json" in s3.writes


def test_actual_handler_private_key_guards_precede_any_body_or_expected_head_read():
    objects = _handler_fixture()
    private_keys = ["data/brain.json", "portfolio/snapshot.json", "data/vol-regime-private.json", "backtest/ledger/versions/private.json"]
    for key in private_keys: objects[key] = b'{"generated_at":"2000-01-01T00:00:00Z","canary":"SYNTHETIC_PRIVATE_DIAGNOSTIC"}'
    expected = json.loads(objects["data/engine-manifest.json"])
    expected["engines"][0]["keys"].extend(private_keys)
    objects["data/engine-manifest.json"] = json.dumps(expected).encode()
    mod, s3 = _load(objects)
    s3.head_object = lambda **kw: (_ for _ in ()).throw(AssertionError("private HEAD not permitted"))
    mod.lambda_handler({"mode": "quiet_refresh"})
    state = json.loads(s3.objects["data/_freshness-monitor.json"])
    assert not set(private_keys) & set(s3.reads)
    assert all(row["key"] not in private_keys for row in state["results"])
    assert state["coverage"]["expected_private_sources_excluded"] == len(private_keys)
    for key in private_keys:
        assert mod.validate_body(key, 100, 26)["reason_code"] == "PRIVATE_SOURCE_EXCLUDED"
    assert not set(private_keys) & set(s3.reads)


def test_actual_handler_exception_canary_and_incomplete_scan_fail_safely():
    import contextlib, io
    canary = "SYNTHETIC_PRIVATE_DIAGNOSTIC"
    mod, s3 = _load(_handler_fixture())
    original = s3.get_object
    def fail_body(**kw):
        if kw["Key"] == "data/feed-000.json": raise RuntimeError(canary)
        return original(**kw)
    s3.get_object = fail_body
    logs = io.StringIO()
    with contextlib.redirect_stdout(logs):
        mod.lambda_handler({"mode": "quiet_refresh"})
    state = json.loads(s3.objects["data/_freshness-monitor.json"])
    assert canary not in json.dumps(state) + logs.getvalue()
    row = next(row for row in state["results"] if row["key"] == "data/feed-000.json")
    assert row["status"] == "UNKNOWN" and row["reason_code"] == "CONTENT_READ_FAILED"
    mod.list_keys_under_rule = lambda *a: (_ for _ in ()).throw(RuntimeError(canary))
    mod.lambda_handler({"mode": "quiet_refresh"})
    state = json.loads(s3.objects["data/_freshness-monitor.json"])
    assert state["status"] == "UNKNOWN" and state["reason_code"] == "FEED_ENUMERATION_FAILED"
    assert state["coverage"]["enumeration_complete"] is False and canary not in json.dumps(state)
    s3.writes.clear()
    result = mod.lambda_handler({"mode": "validate_only"})
    assert result["ok"] is False and result["status"] == "BLOCKED" and not s3.writes


def test_actual_handler_http_spoof_denied_before_all_reads_and_writes():
    mod, s3 = _load(_handler_fixture())
    response = mod.lambda_handler({"headers": {}, "mode": "quiet_refresh", "source": "aws.events"})
    assert response["statusCode"] == 405 and not s3.reads and not s3.writes


def test_projection_whitelists_diagnostics_and_withholds_unmarked_legacy():
    from public_brain_projection import PUBLIC_FRESHNESS_REPORT, sanitize_public
    canary = "SYNTHETIC_PRIVATE_DIAGNOSTIC"
    old = sanitize_public("data/_freshness-monitor.json", {"reason": canary, "source_stale_top_50": [{"reason": canary}]})
    assert old["status"] == "UNKNOWN" and old["coverage"]["results_complete"] is False and canary not in json.dumps(old)
    assert sanitize_public("data/_freshness-monitor.json", old) == old
    doc = {"publication": PUBLIC_FRESHNESS_REPORT, "schema_version": "fleet-freshness-monitor.v3", "status": "DEGRADED", "reason": canary,
           "results": [{"key": "data/a.json", "status": "UNKNOWN", "reason": canary, "reason_code": "CONTENT_READ_FAILED", "error": canary, "zero": 0},
                       {"key": "portfolio/snapshot.json", "status": "FRESH", "reason": canary}],
           "manifest_rules": [{"prefix": "data/", "note": canary}], "extra": canary}
    safe = sanitize_public("data/_freshness-monitor.json", doc)
    assert canary not in json.dumps(safe) and len(safe["results"]) == 1
    assert safe["results"][0]["reason"] == "Content inspection unavailable."
    assert sanitize_public("data/_freshness-monitor.json", safe) == safe


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn()
        print("ok", name)
    print("freshness-monitor tests passed: %d" % len(tests))
