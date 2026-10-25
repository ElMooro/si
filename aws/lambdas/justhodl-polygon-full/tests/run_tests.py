"""justhodl-polygon-full offline tests: in-memory S3 + a fake Polygon grouped endpoint; no network, no AWS.

Run: python3 aws/lambdas/justhodl-polygon-full/tests/run_tests.py
"""
import gzip
import io
import json
import os
import sys
import types
import urllib.request
from datetime import date, datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC, SHARED]
os.environ["POLYGON_API_KEY"] = "test-key-not-real"
os.environ["PG_BUDGET_S"] = "60"


class S3Stub:
    def __init__(self):
        self.store, self.meta = {}, {}

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body
        self.meta[Key] = kw.get("Metadata") or {}

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys]}


class FakePolygon:
    """grouped daily: rows for every weekday except the declared holidays; counts calls per session date."""

    def __init__(self, holidays):
        self.holidays = set(holidays)
        self.calls = []

    def __call__(self, req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        assert "test-key-not-real" in url
        ds = url.split("/market/stocks/")[1].split("?")[0]
        self.calls.append(ds)
        if ds in self.holidays or date.fromisoformat(ds).weekday() >= 5:
            body = json.dumps({"status": "OK", "queryCount": 0, "resultsCount": 0, "adjusted": True}).encode()
        else:
            body = json.dumps({"status": "OK", "queryCount": 2, "resultsCount": 2, "adjusted": True,
                               "results": [{"T": "AAPL", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100, "vw": 1.2, "t": 1, "n": 3},
                                           {"T": "SPY", "o": 10, "h": 20, "l": 5, "c": 15, "v": 1000, "vw": 12, "t": 1, "n": 30}]}).encode()
        resp = io.BytesIO(body)
        resp.__enter__ = lambda s=resp: s
        resp.__exit__ = lambda s=resp, *a: None
        return resp


def boot(fake):
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    urllib.request.urlopen = fake
    sys.modules["chain_guard"] = types.SimpleNamespace(begin=lambda e: None, chain_invoke=lambda *a, **k: None)
    for m in list(sys.modules):
        if m == "lambda_function":
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    return lf, s3


def test_holes_are_repaired_and_holidays_recorded_once():
    # the bank: window from Mon 2026-10-05; sessions banked for every weekday through Fri 10-23 EXCEPT Tue 10-13 and
    # Fri 10-16 (holes - the real ones ops 6654 found); Mon 10-12 is declared a holiday by the fake exchange (resultsCount 0)
    fake = FakePolygon(holidays={"2026-10-12"})
    lf, s3 = boot(fake)
    d = date(2026, 10, 5)
    while d <= date(2026, 10, 23):
        if d.weekday() < 5 and d.isoformat() not in ("2026-10-13", "2026-10-16", "2026-10-12"):
            s3.store[lf.ROOT + "grouped/%d/%s.json.gz" % (d.year, d.isoformat())] = gzip.compress(b'{"resultsCount":1}')
        d += timedelta(days=1)
    s3.store[lf.STATE_KEY] = json.dumps({"version": "1.0.0", "phase": "LIVE", "cursor": "2026-10-26", "sessions": 12, "bytes": 1, "skips": 3,
                                         "forbidden_advances": 0, "failures": {}, "window_start": "2026-10-05"}).encode()
    lf._now = lambda: datetime(2026, 10, 26, 2, 29, tzinfo=timezone.utc)      # the 02:29 UTC run: edge = 10-25 (Sunday) -> cursor stays
    out = lf.lambda_handler({"no_chain": True}, None)
    assert out["ok"] and out["phase"] == "LIVE", out
    h = out["holes"]
    assert sorted(h["closed"]) == ["2026-10-13", "2026-10-16"], h
    assert h["holidays"] == ["2026-10-12"], h
    assert h["open"] == 0, h
    for ds in ("2026-10-13", "2026-10-16"):
        key = lf.ROOT + "grouped/2026/%s.json.gz" % ds
        assert key in s3.store and s3.meta[key].get("repaired") == "hole", ds
        assert json.loads(gzip.decompress(s3.store[key]))["resultsCount"] == 2
    st = json.loads(s3.store[lf.STATE_KEY])
    assert st["holidays"] == ["2026-10-12"] and st["sessions"] == 14 and st["holes_closed_total"] == 2, st
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    assert man["version"] == "1.1.0" and man["holes_open"] == 0 and man["holidays"] == ["2026-10-12"], man
    # second run: nothing missing, the holiday is never asked again, no calls at all
    fake.calls.clear()
    out2 = lf.lambda_handler({"no_chain": True}, None)
    assert out2["holes"]["checked"] == 0 and fake.calls == [], (out2["holes"], fake.calls)


def test_hole_repair_stops_at_its_budget_and_never_blocks_the_edge():
    fake = FakePolygon(holidays=set())
    lf, s3 = boot(fake)
    s3.store[lf.STATE_KEY] = json.dumps({"version": "1.0.0", "phase": "LIVE", "cursor": "2026-10-26", "sessions": 0, "bytes": 0, "skips": 0,
                                         "forbidden_advances": 0, "failures": {}, "window_start": "2026-01-05"}).encode()
    lf._now = lambda: datetime(2026, 10, 26, 2, 29, tzinfo=timezone.utc)
    lf.HOLE_BUDGET_S = 0                                                     # no time for repairs this run
    out = lf.lambda_handler({"no_chain": True}, None)
    assert out["ok"] and out["holes"]["checked"] > 150 and out["holes"]["closed"] == [] and out["holes"]["open"] == out["holes"]["checked"], out["holes"]
    assert fake.calls == [], fake.calls


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
