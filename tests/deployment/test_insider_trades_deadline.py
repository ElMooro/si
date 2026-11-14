"""justhodl-insider-trades stops fetching at a wall-clock deadline (ops 6809, daily health run 2026-11-14).

Every 21:10 UTC run since Nov 12 died `Status: timeout` at 300 s (3 invocations, 3 errors a day; data/insider-trades.json
62 h old on Nov 14): the atom phase ended at ~60 s with fetch_errors=59 of 100, then backfill_from_daily_index fetched up
to 350 EDGAR submissions one by one with a 15 s socket timeout each and no wall clock - the AUTONOMY.md 'slow-drip
livelock' class. Now `soft_deadline(started, context)` = started + min(840, remaining - 75) bounds both fetch phases
(the atom pool gets 60 % of it, the backfill the rest) and the cursor keeps the day's progress where the backfill stopped.

Pinned here, offline (no network, no AWS): the deadline arithmetic, that the backfill loop stops at the deadline with
the cursor advanced only as far as it got, and that it is unchanged when the deadline is far away.
"""
import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aws/lambdas/justhodl-insider-trades/source/lambda_function.py"


def _load():
    if "boto3" not in sys.modules:
        try:
            import boto3  # noqa: F401
        except ImportError:
            fake_boto3 = types.ModuleType("boto3")
            fake_boto3.client = lambda *a, **k: None
            sys.modules["boto3"] = fake_boto3
    spec = importlib.util.spec_from_file_location("insider_deadline_test", SOURCE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["insider_deadline_test"] = mod
    spec.loader.exec_module(mod)
    return mod


class _Ctx:
    def __init__(self, remaining_ms):
        self._r = remaining_ms

    def get_remaining_time_in_millis(self):
        return self._r


class _S3:
    """get_object answers the cursor; put_object records the cursor write."""

    def __init__(self, cursor=None):
        self.cursor = cursor
        self.written = None

    def get_object(self, Bucket, Key):
        import io
        import json
        if self.cursor is None:
            raise KeyError(Key)
        return {"Body": io.BytesIO(json.dumps(self.cursor).encode()), "LastModified": None}

    def put_object(self, Bucket, Key, Body, **kw):
        import json
        self.written = json.loads(Body)


def test_soft_deadline_arithmetic():
    mod = _load()
    # a 300 s function at t=0 with 299 s left: 224 s of fetching, 75 s reserved for merge + write
    assert abs(mod.soft_deadline(1000.0, _Ctx(299_000)) - (1000.0 + 224.0)) < 1e-6
    # a 900 s function: capped at 840 s
    assert abs(mod.soft_deadline(0.0, _Ctx(900_000)) - 825.0) < 1e-6
    assert abs(mod.soft_deadline(0.0, _Ctx(2_000_000)) - 840.0) < 1e-6
    # almost no time left: never below 30 s (the write still gets its reserve from the function's own wall)
    assert abs(mod.soft_deadline(0.0, _Ctx(40_000)) - 30.0) < 1e-6
    # no context at all (local run): the 300 s default
    assert abs(mod.soft_deadline(0.0, None) - 225.0) < 1e-6


def _fake_index(mod, n_filings):
    """form.idx with n Form 4 lines; every submission fetch answers an empty body (no ownershipDocument -> skipped)."""
    lines = ["Form Type   Company Name   CIK   Date Filed   File Name", "-" * 60]
    for i in range(n_filings):
        lines.append("4           ACME INC       1234  20261113     edgar/data/1234/0001234-26-%06d.txt" % i)
    idx = "\n".join(lines).encode("latin-1")
    calls = {"idx": 0, "sub": 0}

    def fetch(url, accept="x"):
        if url.endswith(".idx"):
            calls["idx"] += 1
            return idx
        calls["sub"] += 1
        return b"<SEC-DOCUMENT>no xml here</SEC-DOCUMENT>"
    mod._fetch = fetch
    return calls


def test_backfill_stops_at_deadline_and_keeps_cursor():
    mod = _load()
    import time as _t
    calls = _fake_index(mod, 40)
    s3 = _S3(cursor=None)
    # a deadline already in the past: the outer loop stops before any index is fetched
    txns, cov = mod.backfill_from_daily_index(s3, deadline=_t.time() - 1)
    assert txns == [] and cov["stopped_at_deadline"] is True and calls["sub"] == 0
    assert cov["filings_scanned_this_run"] == 0
    # a deadline that passes mid-day: the cursor advances exactly as far as the loop got, the day is not complete
    calls = _fake_index(mod, 40)
    s3 = _S3(cursor=None)
    real_time = _t.time
    tick = {"n": 0}

    def clock():
        tick["n"] += 1
        return real_time() + (1000.0 if tick["n"] > 12 else 0.0)   # the 13th clock read is past the deadline
    mod.time = types.SimpleNamespace(time=clock, sleep=lambda s: None)
    txns, cov = mod.backfill_from_daily_index(s3, deadline=real_time() + 500.0)
    assert cov["stopped_at_deadline"] is True
    assert 0 < calls["sub"] < 40, calls
    day = next(iter(s3.written["done"].values()))
    assert day["complete"] is False and day["next"] == calls["sub"] and day["total"] == 40
    assert cov["filings_scanned_this_run"] == calls["sub"]


def test_backfill_unchanged_with_a_far_deadline():
    mod = _load()
    import time as _t
    calls = _fake_index(mod, 5)
    s3 = _S3(cursor=None)
    mod.time = types.SimpleNamespace(time=_t.time, sleep=lambda s: None)
    txns, cov = mod.backfill_from_daily_index(s3, deadline=_t.time() + 3600.0)
    assert cov["stopped_at_deadline"] is False
    assert all(v["complete"] for v in s3.written["done"].values())
    # no deadline at all behaves the same (callers that pass none)
    calls = _fake_index(mod, 5)
    s3 = _S3(cursor=None)
    txns, cov = mod.backfill_from_daily_index(s3)
    assert cov["stopped_at_deadline"] is False and calls["sub"] > 0


if __name__ == "__main__":
    test_soft_deadline_arithmetic()
    test_backfill_stops_at_deadline_and_keeps_cursor()
    test_backfill_unchanged_with_a_far_deadline()
    print("ok")
