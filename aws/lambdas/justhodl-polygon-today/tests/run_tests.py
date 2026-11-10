"""Offline tests for justhodl-polygon-today: the forming-bar filter, the session stamp, the handler's skip rules and its
artifact (stubbed S3, stubbed snapshot, fixed clock). No network, no AWS."""
from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import gzip
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path[:0] = [SRC]
BEGUN = []
sys.modules["chain_guard"] = SimpleNamespace(begin=lambda event, context=None: BEGUN.append(event) or 0)
sys.modules["managed_secret"] = SimpleNamespace(managed_secret=lambda env_names, ssm_names=(), region="us-east-1": "fixture-key")


class S3Stub:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body

    def doc(self, key):
        raw = self.objects[key]
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)


_boto = SimpleNamespace(client=lambda *a, **k: S3Stub())
sys.modules["boto3"] = _boto
spec = importlib.util.spec_from_file_location("lf", os.path.join(SRC, "lambda_function.py"))
lf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lf)
NY = ZoneInfo("America/New_York")


def ms(dt):
    return int(dt.timestamp() * 1000)


def snapshot_doc(now_ny):
    """A snapshot like Polygon's at 09:52 ET on 2026-11-10 (ops 6768): day blocks for the session, a few stale names."""
    upd = ms(now_ny) - 15 * 60 * 1000
    yesterday = ms(datetime(2026, 11, 9, 16, 0, tzinfo=NY))
    return {"status": "OK", "count": 6, "tickers": [
        {"ticker": "SPY", "updated": upd * 1_000_000, "day": {"o": 747.94, "h": 749.16, "l": 747.05, "c": 749.115, "v": 3594789.03, "vw": 748.4}, "prevDay": {"c": 752.69}},
        {"ticker": "AAPL", "updated": upd * 1_000_000, "day": {"o": 351.35, "h": 353.87, "l": 350, "c": 350.67, "v": 2670219, "vw": 351.1}, "prevDay": {"c": 350.69}},
        {"ticker": "STALE", "updated": yesterday * 1_000_000, "day": {"o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100}, "prevDay": {"c": 1.4}},       # last touched yesterday
        {"ticker": "NOTRD", "updated": upd * 1_000_000, "day": {"o": 0, "h": 0, "l": 0, "c": 0, "v": 0}, "prevDay": {"c": 9.9}},                   # no print today
        {"ticker": "PRE", "updated": ms(datetime(2026, 11, 10, 8, 55, tzinfo=NY)) * 1_000_000, "day": {"o": 5, "h": 5, "l": 5, "c": 5, "v": 10}}, # pre-market only
        {"ticker": "bad", "updated": "x", "day": {"c": 1}},
    ]}


def test_forming_rows_keeps_only_this_session():
    now_ny = datetime(2026, 11, 10, 9, 52, tzinfo=NY)
    rows, newest, n_stale = lf.forming_rows(snapshot_doc(now_ny)["tickers"], now_ny.astimezone(timezone.utc))
    assert set(rows) == {"SPY", "AAPL"}, rows
    assert rows["SPY"][:6] == [747.94, 749.16, 747.05, 749.115, 3594789.0, 748.4] and rows["SPY"][6] == ms(now_ny) - 15 * 60 * 1000
    assert newest == ms(now_ny) - 15 * 60 * 1000 and n_stale == 2, (newest, n_stale)


def test_session_open_stamp_matches_the_bank_convention():
    assert lf.session_open_ts(datetime(2026, 11, 10).date()) == 1794321000            # 09:30 EST
    assert lf.session_open_ts(datetime(2026, 7, 1).date()) == int(datetime(2026, 7, 1, 9, 30, tzinfo=NY).timestamp())   # 09:30 EDT


def test_handler_writes_the_overlay_and_the_manifest():
    now_ny = datetime(2026, 11, 10, 9, 52, tzinfo=NY)
    lf.S3 = S3Stub()
    lf.snapshot = lambda key: (snapshot_doc(now_ny), "https://api.massive.com")
    out = lf.lambda_handler({"trigger": "test"}, None, now=now_ny.astimezone(timezone.utc))
    assert out["wrote"] is True and out["n"] == 2 and out["session"] == "2026-11-10" and out["sample_spy"][3] == 749.115, out
    body = lf.S3.doc(lf.OUT_KEY)
    assert body["session"] == "2026-11-10" and body["t"] == 1794321000 and body["delay_minutes"] == 15 and body["n"] == 2 and body["fields"][:4] == ["o", "h", "l", "c"]
    assert body["rows"]["AAPL"][3] == 350.67 and body["as_of"].startswith("2026-11-10T14:37")
    man = lf.S3.doc(lf.MANIFEST_KEY)
    assert man["n"] == 2 and man["n_tickers"] == 6 and man["n_stale"] == 2 and "SPY" in man["sample"] and man["key"] == lf.OUT_KEY
    assert BEGUN and BEGUN[-1] == {"trigger": "test"}


def test_handler_skips_outside_the_session_and_without_forming_bars():
    lf.S3 = S3Stub()
    calls = []
    lf.snapshot = lambda key: calls.append(key) or ({"status": "OK", "tickers": []}, "h")
    pre = lf.lambda_handler({}, None, now=datetime(2026, 11, 10, 9, 10, tzinfo=NY).astimezone(timezone.utc))
    assert pre["wrote"] is False and "before 09:30" in pre["why"] and not calls and not lf.S3.objects
    sat = lf.lambda_handler({}, None, now=datetime(2026, 11, 14, 11, 0, tzinfo=NY).astimezone(timezone.utc))
    assert sat["wrote"] is False and not calls
    empty = lf.lambda_handler({}, None, now=datetime(2026, 11, 10, 9, 40, tzinfo=NY).astimezone(timezone.utc))
    assert empty["wrote"] is False and "no forming day bar" in empty["why"] and calls == ["fixture-key"] and not lf.S3.objects


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
    print("justhodl-polygon-today tests: all green")
