"""ops 6528: justhodl-ai's month-to-date SageMaker spend is cached 6 h, so Cost Explorer ($0.01 a call) is read
~4x a day instead of on every hourly inventory and 10-minute pipeline tick (~168 calls/day before).

Loads the real cost_guard.py with boto3 stubbed; never contacts AWS.  Runs in the deployment preflight.
"""
from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / "aws/lambdas/justhodl-ai/source/cost_guard.py"


class _S3:
    def __init__(self):
        self.objs = {}

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objs:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objs[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objs[(Bucket, Key)] = Body


def _load():
    saved = sys.modules.get("boto3")
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *a, **k: _S3())
    try:
        spec = importlib.util.spec_from_file_location("cost_guard_under_test", SOURCE)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        if saved is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = saved


def _ce(calls, amount="2.50"):
    return types.SimpleNamespace(get_cost_and_usage=lambda **k: calls.append(k) or {"ResultsByTime": [
        {"TimePeriod": {"Start": "2026-10-01"}, "Total": {"UnblendedCost": {"Amount": amount}}}]})


def test_second_read_inside_six_hours_never_reaches_cost_explorer():
    cg, s3, calls = _load(), _S3(), []
    first = cg.mtd_sagemaker_cost(_ce(calls), s3=s3, bucket="private")
    second = cg.mtd_sagemaker_cost(_ce(calls), s3=s3, bucket="private")
    assert first["usd_mtd"] == 2.5 and second["usd_mtd"] == 2.5, (first, second)
    assert second.get("cached") is True and len(calls) == 1, (second, calls)


def test_cache_expires_after_six_hours_and_on_a_new_month():
    cg, s3, calls = _load(), _S3(), []
    cg.mtd_sagemaker_cost(_ce(calls), s3=s3, bucket="private")
    doc = json.loads(s3.objs[("private", cg.MTD_CACHE_KEY)])
    doc["fetched_at"] = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
    s3.objs[("private", cg.MTD_CACHE_KEY)] = json.dumps(doc).encode()
    assert not cg.mtd_sagemaker_cost(_ce(calls), s3=s3, bucket="private").get("cached") and len(calls) == 2
    doc = json.loads(s3.objs[("private", cg.MTD_CACHE_KEY)])
    doc["month"] = "1999-01"
    s3.objs[("private", cg.MTD_CACHE_KEY)] = json.dumps(doc).encode()
    assert not cg.mtd_sagemaker_cost(_ce(calls), s3=s3, bucket="private").get("cached") and len(calls) == 3


def test_errors_are_never_cached():
    cg, s3 = _load(), _S3()
    broken = types.SimpleNamespace(get_cost_and_usage=lambda **k: (_ for _ in ()).throw(RuntimeError("ce down")))
    out = cg.mtd_sagemaker_cost(broken, s3=s3, bucket="private")
    assert out["usd_mtd"] is None and not s3.objs, (out, s3.objs)


def test_no_bucket_falls_back_to_a_direct_read():
    import os
    saved = os.environ.pop("AI_PRIVATE_BUCKET", None)  # the preflight runs every test in one process
    try:
        cg, calls = _load(), []
        out = cg.mtd_sagemaker_cost(_ce(calls), s3=None, bucket=None)
        assert out["usd_mtd"] == 2.5 and len(calls) == 1
    finally:
        if saved is not None:
            os.environ["AI_PRIVATE_BUCKET"] = saved
