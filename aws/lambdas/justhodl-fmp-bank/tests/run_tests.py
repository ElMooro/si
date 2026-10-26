"""justhodl-fmp-bank offline tests: a fake FMP server + in-memory S3, no network, no AWS.

Run: python3 aws/lambdas/justhodl-fmp-bank/tests/run_tests.py
"""
import gzip
import io
import json
import os
import sys
import types
import urllib.error
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC, SHARED]
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["FMP_BANK_BUDGET_S"] = "600"
os.environ["FMP_BANK_PACE_S"] = "0"
os.environ["FMP_EOD_FLOOR"] = "2026-10-15"
os.environ["FMP_YEAR_FLOOR"] = "2025"


class S3Stub:
    def __init__(self):
        self.store = {}
        self.puts = 0

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body.read() if hasattr(Body, "read") else Body
        self.puts += 1


class FakeFMP:
    """Serves CSV for known endpoints; 404 for unknown; configurable 403/429."""

    def __init__(self):
        self.calls = []
        self.serve_403 = False
        self.rate_limit_once = set()
        self.bandwidth_limited = False
        self.unknown = {"peers-bulk"}                      # pretend the plan does not serve this one

    def __call__(self, req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        self.calls.append(url)
        path = urllib.parse.urlparse(url).path.rsplit("/", 1)[-1]
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        assert q.get("apikey") == "test-key-not-real"
        if self.serve_403:
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(b"{}"))
        if path in self.unknown:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b"{}"))
        if path in self.rate_limit_once:
            self.rate_limit_once.discard(path)
            raise urllib.error.HTTPError(url, 429, "Too Many", {}, io.BytesIO(b"{}"))
        if self.bandwidth_limited:
            raise urllib.error.HTTPError(url, 429, "Too Many", {}, io.BytesIO(b'{"Error Message": "Bandwidth Limit Reach . Please upgrade your plan"}'))
        if path == "profile-bulk" and q.get("part") != "0":
            body = b"[]"                                   # parts beyond 0 are empty
        elif path == "etf-holder-bulk" and q.get("part") != "0":
            body = b"[]"
        elif path == "eod-bulk":
            if q["date"] == "2026-10-16":
                body = b""                                 # a holiday-like empty day
            else:
                body = ("symbol,date,open,low,high,close,adjClose,volume\nAAPL,%s,1,1,1,1,1,100\nMSFT,%s,2,2,2,2,2,200\n" % (q["date"], q["date"])).encode()
        elif path.endswith("-bulk"):
            body = ("symbol,date,period,x\nAAPL,2025-09-30,%s,1\nMSFT,2025-09-30,%s,2\n" % (q.get("period", "annual"), q.get("period", "annual"))).encode()
        else:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b"{}"))
        resp = io.BytesIO(body)
        resp.__enter__ = lambda s=resp: s
        resp.__exit__ = lambda s=resp, *a: None
        return resp


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    import urllib.request as ur
    fake = FakeFMP()
    ur.urlopen = fake
    for m in list(sys.modules):
        if m == "lambda_function":
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    return lf, s3, fake


def test_full_cycle():
    lf, s3, fake = boot()
    out = lf.lambda_handler({}, None)
    assert out["key_status"] == "ok", out
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    cat = json.loads(s3.store[lf.CATALOG_KEY])
    assert "peers-bulk" in cat["not_served"], cat["not_served"]
    assert cat["endpoints"]["peers-bulk"]["last_verdict"] == "http404"
    assert "rating-bulk" in cat["served"]
    latest = s3.store[lf.ROOT + "rating-bulk/latest.csv.gz"]
    assert gzip.decompress(latest).startswith(b"symbol,date")
    assert lf.ROOT + "profile-bulk/part-0/latest.csv.gz" in s3.store
    assert lf.ROOT + "income-statement-bulk/2026/Q1.csv.gz" in s3.store
    assert lf.ROOT + "income-statement-bulk/2025/annual.csv.gz" in s3.store
    assert lf.ROOT + "earnings-surprises-bulk/2026/year.csv.gz" in s3.store
    eod_keys = [k for k in s3.store if "/eod-bulk/" in k]
    assert any(k.endswith("2026-10-15.csv.gz") for k in eod_keys), eod_keys
    assert not any("2026-10-16" in k or "2026-10-17" in k or "2026-10-18" in k for k in eod_keys), eod_keys   # empty day, weekend
    assert man["objects"] == len([k for k in s3.store if k.endswith(".csv.gz")]), (man["objects"], len(s3.store))
    assert man["bytes"] == sum(len(v) for k, v in s3.store.items() if k.endswith(".csv.gz"))
    assert man["pages"] == man["objects"] and man["pages_bytes"] == man["bytes"]
    assert man["phase"] == "STEADY", man
    assert out["dated_pending"] == 0
    return lf, s3, fake


def test_unchanged_snapshot_not_rewritten():
    lf, s3, fake = test_full_cycle()
    puts = s3.puts
    objects = json.loads(s3.store[lf.MANIFEST_KEY])["objects"]
    lf.lambda_handler({"only": ["snapshot"]}, None)
    cat = json.loads(s3.store[lf.CATALOG_KEY])
    assert cat["endpoints"]["rating-bulk"]["changed"] is False
    assert json.loads(s3.store[lf.MANIFEST_KEY])["objects"] == objects
    assert s3.puts - puts <= 4, "only state (lease + final), manifest, catalog should be rewritten"


def test_forbidden_key_stops_and_is_visible():
    lf, s3, fake = boot()
    fake.serve_403 = True
    out = lf.lambda_handler({}, None)
    assert out["key_status"] == "unauthorized", out
    assert not [k for k in s3.store if k.endswith(".csv.gz")]
    assert json.loads(s3.store[lf.MANIFEST_KEY])["key_status"] == "unauthorized"


def test_429_retried():
    lf, s3, fake = boot()
    fake.rate_limit_once.add("rating-bulk")
    out = lf.lambda_handler({"only": ["snapshot"]}, None)
    assert out["run"]["retries"] >= 1
    assert lf.ROOT + "rating-bulk/latest.csv.gz" in s3.store


def test_bandwidth_limit_parks_the_bank_and_bytes_are_counted():
    """v1.3.0: FMP's 429 'Bandwidth Limit Reach' (the plan's 150 GB / 30 days) stops the run after five refusals and parks
    the bank for the day; a normal run counts the bytes it received against its per-run budget."""
    lf, s3, fake = boot()
    out = lf.lambda_handler({"only": ["snapshot"]}, None)
    assert out["run"]["bytes_received"] > 0 and out["run"]["bytes_budget"] == lf.Fetch.BYTES_PER_RUN
    fake.bandwidth_limited = True
    out2 = lf.lambda_handler({"only": ["snapshot"], "force": True}, None)
    st = json.loads(s3.store[lf.STATE_KEY])
    assert st.get("bandwidth_hold_until", 0) > __import__("time").time() + 3600 and "Bandwidth" in st["bandwidth_note"], st.get("bandwidth_note")
    assert out2["run"]["http"].get("http429_bandwidth", 0) >= 5 and not out2["run"]["http"].get("ok"), out2["run"]["http"]
    assert out2["run"]["calls"] < 40, out2["run"]["calls"]
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    assert man["bandwidth_hold_until"] == st["bandwidth_hold_until"]
    assert lf.lambda_handler({}, None).get("skipped") == "bandwidth_hold"
    fake.bandwidth_limited = False
    out3 = lf.lambda_handler({"only": ["snapshot"], "force": True}, None)
    assert "bandwidth_hold_until" not in json.loads(s3.store[lf.STATE_KEY]) and out3["key_status"] == "ok"


def test_lease_and_budget():
    lf, s3, fake = boot()
    s3.store[lf.STATE_KEY] = json.dumps({"lease_until": 9e12}).encode()
    assert lf.lambda_handler({}, None).get("skipped") == "lease_held"
    lf, s3, fake = boot()
    lf.BUDGET_S = 30                                       # below the guard thresholds -> every step stops cleanly
    out = lf.lambda_handler({}, None)
    assert out["phase"] == "BACKFILL" and len(out["run"]["steps"]) == 1 and not out["run"]["steps"][0]["complete"], out


def test_fetch_rejects_json_error_bodies_and_counts_verdicts():
    """v1.1.0: rows are counted while streaming through gzip; a JSON body is 'empty' (never banked as CSV); every verdict counted."""
    lf, s3, fake = boot()
    fake.unknown.discard("peers-bulk")
    fx = lf.Fetch("test-key-not-real")
    import urllib.request as ur
    real = ur.urlopen

    def serve(body):
        def _u(req, timeout=0):
            r = io.BytesIO(body)
            r.__enter__ = lambda s=r: s
            r.__exit__ = lambda s=r, *a: None
            return r
        ur.urlopen = _u
    serve(b'{"Error Message": "x"}')
    assert fx.get("rating-bulk", {})[0] == "empty"
    serve(b"a,b\n1,2\n3,4\n")
    v, path, n, rows, sha = fx.get("rating-bulk", {})
    assert (v, n, rows) == ("ok", 12, 2), (v, n, rows)
    assert gzip.open(path, "rb").read() == b"a,b\n1,2\n3,4\n"
    serve(b"a,b\n1,2\n3,4")                                  # no trailing newline
    assert fx.get("rating-bulk", {})[3] == 2
    assert fx.http == {"empty": 1, "ok": 2}, fx.http
    ur.urlopen = real


def test_parted_bulk_end_is_not_a_refusal():
    lf, s3, fake = boot()
    out = lf.lambda_handler({"only": ["snapshot"]}, None)
    cat = json.loads(s3.store[lf.CATALOG_KEY])
    assert "profile-bulk" in cat["served"] and cat["endpoints"]["profile-bulk"].get("parts") == 1, cat["endpoints"]["profile-bulk"]
    assert not any(k.startswith("profile-bulk?part=") for k in cat["not_served"]), cat["not_served"]
    assert out["run"]["http"]["ok"] >= 9, out["run"]["http"]


def test_429_keeps_the_year_pending_and_cools_down():
    lf, s3, fake = boot()
    lf.Fetch.COOLDOWN_S = 0.2
    fake.rate_limit_once.update({"income-statement-bulk", "cash-flow-statement-bulk", "rating-bulk"})
    out = lf.lambda_handler({"only": ["snapshot", "dated"]}, None)
    assert out["run"]["http"].get("http429") == 3, out["run"]["http"]
    st = json.loads(s3.store[lf.STATE_KEY])
    # each 429'd call was retried after the cooldown and then succeeded -> banked, never recorded as done-with-429
    assert all(d["verdict"] in ("ok", "empty", "http404", "http400") for d in st["dated_done"].values()), {k: d["verdict"] for k, d in st["dated_done"].items() if d["verdict"] not in ("ok", "empty")}
    assert lf.ROOT + "income-statement-bulk/2026/annual.csv.gz" in s3.store
    # a transient verdict recorded as done by an older version is re-queued on the next run
    st["dated_done"]["ratios-bulk/2025/Q1"] = {"verdict": "http429", "day": "2026-10-24"}
    s3.store[lf.STATE_KEY] = json.dumps(st).encode()
    jobs = lf._dated_jobs(st, 2026)
    assert ("ratios-bulk", 2025, "Q1", "ratios-bulk/2025/Q1") in jobs


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print("ok", t.__name__)
    print("%d tests passed" % len(tests))
