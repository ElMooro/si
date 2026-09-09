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

    def get_object(self, Bucket, Key, Range=None):
        if Key not in self.objects:
            raise Exception("NoSuchKey " + Key)
        b = self.objects[Key]
        if Range:
            lo, hi = Range.replace("bytes=", "").split("-")
            b = b[int(lo):int(hi) + 1]
        return {"Body": types.SimpleNamespace(read=lambda: b), "LastModified": datetime.now(timezone.utc)}

    def put_object(self, **kw):
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
    if max_keys:
        os.environ["MAX_KEYS_PER_RULE"] = str(max_keys)
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


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn()
        print("ok", name)
    print("freshness-monitor tests passed: %d" % len(tests))
