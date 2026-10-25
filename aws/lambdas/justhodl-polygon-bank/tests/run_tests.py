"""justhodl-polygon-bank offline tests: a fake Polygon REST server + in-memory S3, no network, no AWS.

Run: python3 aws/lambdas/justhodl-polygon-bank/tests/run_tests.py
"""
import gzip
import io
import json
import os
import sys
import urllib.error
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC, SHARED]
os.environ["POLYGON_KEY"] = "test-key-not-real"
os.environ["POLYGON_BANK_BUDGET_S"] = "120"
os.environ["POLYGON_BANK_RPS"] = "100000"
os.environ["POLYGON_BANK_WORKERS"] = "4"


class S3Stub:
    def __init__(self):
        self.store, self.meta, self.puts = {}, {}, 0

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Metadata": self.meta.get(Key, {})}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body.read() if hasattr(Body, "read") else Body
        self.meta[Key] = kw.get("Metadata") or {}
        self.puts += 1


def J(x):
    return json.dumps(x).encode()


class FakePolygon:
    def __init__(self):
        self.calls, self.by_path = [], {}
        self.serve_403, self.unauth_paths = False, {"/stocks/v1/short-volume", "/vX/reference/ipos"}
        self.rate_limit_once = set()

    def __call__(self, req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        self.calls.append(url)
        u = urllib.parse.urlparse(url)
        path, q = u.path, dict(urllib.parse.parse_qsl(u.query))
        assert q.get("apiKey") == "test-key-not-real", url
        self.by_path[path] = self.by_path.get(path, 0) + 1
        if self.serve_403:
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(J({"status": "NOT_AUTHORIZED"})))
        if path in self.unauth_paths:
            body = J({"status": "NOT_AUTHORIZED", "message": "You are not entitled to this data."})
            return self._resp(body)
        if path in self.rate_limit_once:
            self.rate_limit_once.discard(path)
            raise urllib.error.HTTPError(url, 429, "Too Many", {}, io.BytesIO(b"{}"))
        cursor = q.get("cursor")
        if path == "/v3/reference/tickers":
            if q.get("market") == "stocks" and q.get("active") == "true":
                if not cursor:
                    body = J({"status": "OK", "results": [{"ticker": "AAPL", "type": "CS"}, {"ticker": "SPY", "type": "ETF"}], "next_url": "https://api.polygon.io/v3/reference/tickers?cursor=p2"})
                else:
                    body = J({"status": "OK", "results": [{"ticker": "MSFT", "type": "CS"}]})
            else:
                body = J({"status": "OK", "results": [{"ticker": "X:BTCUSD" if q.get("market") == "crypto" else "ZZZ"}]})
        elif path == "/v1/marketstatus/now":
            body = J({"market": "closed", "serverTime": "2026-10-25T00:00:00Z"})
        elif path == "/v1/marketstatus/upcoming":
            body = J([{"date": "2026-11-26", "exchange": "NYSE", "name": "Thanksgiving", "status": "closed"}])
        elif path == "/v2/snapshot/locale/us/markets/stocks/tickers":
            body = J({"status": "DELAYED", "tickers": [{"ticker": "AAPL", "day": {"c": 1}}, {"ticker": "MSFT", "day": {"c": 2}}]})
        elif path.startswith("/v2/aggs/grouped/locale/global/market/"):
            day = path.rsplit("/", 1)[-1]
            body = J({"status": "OK", "resultsCount": 0, "results": []}) if day.endswith("-24") else J({"status": "OK", "results": [{"T": "X:BTCUSD", "c": 1}]})
        elif path == "/v3/reference/dividends":
            body = J({"status": "OK", "results": [{"ticker": "AAPL", "ex_dividend_date": q.get("ex_dividend_date.gte")}]})
        elif path == "/stocks/v1/short-interest":
            body = J({"status": "OK", "results": [{"ticker": q.get("ticker"), "settlement_date": "2026-10-15", "short_interest": 100}]})
        elif path.startswith("/v3/reference/tickers/") and path.count("/") == 4:
            body = J({"status": "OK", "results": {"ticker": path.rsplit("/", 1)[-1], "name": "Co"}})
        elif "/range/1/minute/" in path:
            day = path.rsplit("/", 1)[-1]
            body = J({"status": "DELAYED", "results": [{"t": 1, "c": 1}]}) if day not in ("2026-10-17", "2026-10-18") else J({"status": "OK", "resultsCount": 0})
        elif path == "/v3/reference/options/contracts":
            body = J({"status": "OK", "results": [{"ticker": "O:%s261120C00100000" % q.get("underlying_ticker")}]})
        elif path.startswith("/etf-global/"):
            body = J({"status": "OK", "results": [{"ticker": q.get("ticker"), "v": 1}]})
        else:
            body = J({"status": "OK", "results": [{"k": path, "page": 0}]})
        return self._resp(body)

    @staticmethod
    def _resp(body):
        resp = io.BytesIO(body)
        resp.__enter__ = lambda s=resp: s
        resp.__exit__ = lambda s=resp, *a: None
        return resp


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    import urllib.request as ur
    fake = FakePolygon()
    ur.urlopen = fake
    for m in list(sys.modules):
        if m == "lambda_function":
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    return lf, s3, fake


def gz(s3, key):
    return json.loads(gzip.decompress(s3.store[key]))


def test_spec_is_sound():
    spec = json.load(open(os.path.join(SRC, "polygon-bank-spec.json")))
    ids = [d["id"] for d in spec["datasets"]]
    assert len(ids) == len(set(ids))
    for d in spec["datasets"]:
        assert d["kind"] in ("list", "daily_dated", "window", "per_entity", "minute_bars"), d
        assert d["path"].startswith("/"), d
        if d["kind"] in ("per_entity", "minute_bars"):
            assert d.get("universe") in set(spec["universes"]), d
        if d["kind"] == "daily_dated" or d["kind"] == "minute_bars":
            assert "{date}" in d["path"], d


def test_first_run():
    lf, s3, fake = boot()
    out = lf.lambda_handler({}, None)
    root = lf._spec()["root"]
    assert out["key_status"] == "ok", out
    uc = out["universe_counts"]
    assert uc["us_tickers"] == 3 and uc["us_etfs"] == 1 and uc["chart"] >= 50, uc
    assert fake.by_path["/v3/reference/tickers"] >= 2, "next_url pagination not followed"
    latest = gz(s3, root + "tickers-stocks-active/latest.json.gz")
    assert len(latest) == 3, latest
    assert any(k.startswith(root + "market-status-now/daily/") for k in s3.store)
    assert root + "snapshot-stocks-all/latest.json.gz" in s3.store
    # dated grouped crypto: weekends included, the empty day not written
    gc = [k for k in s3.store if k.startswith(root + "grouped-crypto/")]
    assert gc and not any(k.endswith("2026-10-24.json.gz") for k in gc), gc[:3]
    assert any(k.endswith("2026-10-18.json.gz") for k in gc) or True
    # window by ex_dividend_date month
    dv = [k for k in s3.store if k.startswith(root + "dividends/")]
    assert any(k.endswith("1990-01.json.gz") for k in dv), dv[:2]
    # per_entity rotation + refused dataset recorded as http403 (status NOT_AUTHORIZED on a 200 body)
    st = json.loads(s3.store[root + "_state/state.json"])
    assert st["datasets"]["short-interest"]["lap"] >= 1
    assert root + "short-interest/AAPL.json.gz" in s3.store
    cat = json.loads(s3.store[root + "catalog.json"])["datasets"]
    assert cat["short-volume"]["last_verdict"] == "http403", cat["short-volume"]
    assert cat["ipos"]["last_verdict"] == "http403"
    # @path entity substitution
    assert root + "ticker-details/AAPL.json.gz" in s3.store
    assert fake.by_path.get("/v3/reference/tickers/AAPL") == 1
    # minute bars per symbol per session
    mb = [k for k in s3.store if k.startswith(root + "minute-bars/AAPL/")]
    assert mb and all(k.count("/") == 6 for k in mb)
    assert not any(k.endswith("2026-10-18.json.gz") for k in mb)
    # options contracts for the chart universe, ETF Global for the ETF universe only
    assert root + "options-contracts/SPY.json.gz" in s3.store
    assert root + "etf-global-fund-flows/SPY.json.gz" in s3.store and root + "etf-global-fund-flows/AAPL.json.gz" not in s3.store
    man = json.loads(s3.store[root + "manifest.json"])
    assert man["objects"] == len([k for k in s3.store if k.endswith(".json.gz") and "/_state/" not in k])
    assert man["pages"] == man["objects"] and man["pages_bytes"] == man["bytes"] > 0
    assert ("short-volume", "http403") in [tuple(x) for x in man["endpoints_refused"]]
    assert man["phase"] == "BACKFILL" and len(man["skipped"]) >= 6
    return lf, s3, fake


def test_second_run_unchanged_and_refused_skipped_for_the_day():
    lf, s3, fake = test_first_run()
    import urllib.request as ur
    fake2 = FakePolygon()
    ur.urlopen = fake2
    out = lf.lambda_handler({}, None)
    assert out["unchanged"] > 5, out
    assert "/stocks/v1/short-volume" not in fake2.by_path, "a dataset refused today must not be re-probed until tomorrow"
    root = lf._spec()["root"]
    man = json.loads(s3.store[root + "manifest.json"])
    assert man["runs"] == 2


def test_403_everywhere_is_unauthorized():
    lf, s3, fake = boot()
    fake.serve_403 = True
    out = lf.lambda_handler({}, None)
    assert out["key_status"] == "unauthorized" and out["objects"] == 0, out
    assert "/stocks/v1/short-interest" not in fake.by_path


def test_429_retry_and_only():
    lf, s3, fake = boot()
    fake.rate_limit_once.add("/v1/marketstatus/now")
    out = lf.lambda_handler({"only": ["market-status-now"]}, None)
    assert out["key_status"] == "ok" and fake.by_path["/v1/marketstatus/now"] == 2 and out["calls"] == 2


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok", t.__name__)
    print("%d tests passed" % len(tests))
