"""justhodl-polygon-eod offline tests: in-memory S3 holding polygon-full grouped sessions; no network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-polygon-eod/tests/run_tests.py
"""
import gzip
import io
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC, SHARED]
os.environ["POLYGON_EOD_BUDGET_S"] = "120"
os.environ["POLYGON_EOD_WORKERS"] = "4"


class S3Stub:
    def __init__(self):
        self.store, self.meta, self.puts = {}, {}, 0

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body
        self.meta[Key] = kw.get("Metadata") or {}
        self.puts += 1

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        return {"Contents": [{"Key": k} for k in sorted(k for k in self.store if k.startswith(Prefix))]}


def ms(d):
    return int(datetime(d.year, d.month, d.day, 4, tzinfo=timezone.utc).timestamp() * 1000)   # Polygon stamps 00:00 ET = 04:00 UTC


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    for m in list(sys.modules):
        if m == "lambda_function":
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    return lf, s3


def seed(lf, s3, days, tickers=("AAPL", "SPY", "BRK.B")):
    for d in days:
        rows = [{"T": t, "o": 1.0 + i, "h": 2.0 + i, "l": 0.5, "c": 1.5 + i, "v": 100 * (i + 1), "vw": 1.2, "t": ms(d), "n": 3} for i, t in enumerate(tickers)]
        s3.store[lf.IN_ROOT + "%d/%s.json.gz" % (d.year, d.isoformat())] = gzip.compress(json.dumps({"resultsCount": len(rows), "results": rows}).encode())


def weekdays(start, end):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def gz(s3, key):
    return json.loads(gzip.decompress(s3.store[key]))


def test_fold_builds_per_ticker_files_in_bounded_batches_and_reports_gaps():
    lf, s3 = boot()
    days = [d for d in weekdays(date(2026, 10, 5), date(2026, 10, 23)) if d.isoformat() not in ("2026-10-13", "2026-10-16")]   # the real holes
    seed(lf, s3, days)
    s3.store[lf.IN_STATE] = json.dumps({"window_start": "2026-10-05", "holidays": ["2026-10-12"]}).encode()
    out = lf.lambda_handler({"sessions_per_run": 5}, None)
    assert out["ok"] and out["phase"] == "BACKFILL" and out["sessions_this_run"] == 5 and out["folded"] == 5 and out["pending"] == 8, out
    aapl = gz(s3, lf.ROOT + "AAPL.json.gz")
    assert aapl["ticker"] == "AAPL" and aapl["n"] == 5 and aapl["first"] == "2026-10-05" and aapl["last"] == "2026-10-09", aapl
    assert aapl["fields"] == ["t", "o", "h", "l", "c", "v", "vw", "n"] and aapl["rows"][0][1:] == [1.0, 2.0, 0.5, 1.5, 100, 1.2, 3]
    assert lf.ROOT + "BRK.B.json.gz" in s3.store
    man = gz(s3, lf.MANIFEST_KEY) if lf.MANIFEST_KEY.endswith(".gz") else json.loads(s3.store[lf.MANIFEST_KEY])
    assert man["gaps"] == ["2026-10-13", "2026-10-16"] and man["holidays_recorded"] == ["2026-10-12"], man      # 10-12 is a holiday, not a gap
    assert man["sessions_available"] == 13 and man["sessions_folded"] == 5 and man["tickers"] == 3
    # second and third runs fold the rest, oldest first, merging into the same files; then STEADY
    out2 = lf.lambda_handler({"sessions_per_run": 5}, None)
    out3 = lf.lambda_handler({"sessions_per_run": 5}, None)
    assert out2["folded"] == 10 and out3["folded"] == 13 and out3["phase"] == "STEADY", (out2, out3)
    aapl = gz(s3, lf.ROOT + "AAPL.json.gz")
    assert aapl["n"] == 13 and aapl["last"] == "2026-10-23" and [r[0] for r in aapl["rows"]] == sorted(r[0] for r in aapl["rows"])
    # a repaired hole appears later in polygon-full: the next run folds just that session into every file (write-if-changed)
    seed(lf, s3, [date(2026, 10, 13)])
    puts = s3.puts
    out4 = lf.lambda_handler({}, None)
    assert out4["sessions_this_run"] == 1 and out4["tickers_written"] == 3 and out4["phase"] == "STEADY", out4
    aapl = gz(s3, lf.ROOT + "AAPL.json.gz")
    assert aapl["n"] == 14 and any(datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc).date() == date(2026, 10, 13) for r in aapl["rows"])
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    assert man["gaps"] == ["2026-10-16"], man["gaps"]
    assert s3.puts - puts == 3 + 3, (s3.puts, puts)                     # 3 tickers + lease, state, manifest
    # nothing new: no ticker file touched
    puts = s3.puts
    out5 = lf.lambda_handler({}, None)
    assert out5["sessions_this_run"] == 0 and out5["tickers_written"] == 0 and s3.puts - puts == 3, (out5, s3.puts - puts)


def test_lease_and_same_timestamp_rows_replace_not_duplicate():
    lf, s3 = boot()
    seed(lf, s3, [date(2026, 10, 5)])
    s3.store[lf.STATE_KEY] = json.dumps({"version": "1.0.0", "runs": 0, "folded": [], "tickers": [], "phase": "BACKFILL", "lease_until": 9e12}).encode()
    assert lf.lambda_handler({}, None).get("skipped") == "lease_held"
    out = lf.lambda_handler({"force": True}, None)
    assert out["ok"] and out["folded"] == 1
    # the same session re-folded (forced) replaces rows by timestamp instead of duplicating them
    s3.store[lf.STATE_KEY] = json.dumps({"version": "1.0.0", "runs": 1, "folded": [], "tickers": ["AAPL", "SPY", "BRK.B"], "phase": "BACKFILL"}).encode()
    out2 = lf.lambda_handler({}, None)
    assert out2["folded"] == 1 and gz(s3, lf.ROOT + "AAPL.json.gz")["n"] == 1


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
