"""justhodl-provider-bank offline tests: fake TE / CMC / NewsAPI / NDL / CryptoQuant servers + in-memory S3, no network.

Run: python3 aws/lambdas/justhodl-provider-bank/tests/run_tests.py
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
for k in ("TE_API_KEY", "CMC_KEY", "NEWSAPI_KEY", "NASDAQ_API_KEY", "CRYPTOQUANT_API_KEY"):
    os.environ[k] = "k-" + k.lower()
os.environ["PROVIDER_BANK_BUDGET_S"] = "120"
os.environ["PROVIDER_BANK_WORKERS"] = "4"
os.environ["PROVIDER_BANK_RPS_SCALE"] = "100000"


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


class FakeNet:
    def __init__(self):
        self.calls = []
        self.by_host_path = {}
        self.plan_refused = {"/v1/global-metrics/quotes/historical", "/v2/cryptocurrency/ohlcv/historical", "/v2/cryptocurrency/quotes/historical"}

    def __call__(self, req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        headers = {k.lower(): v for k, v in (req.headers.items() if hasattr(req, "headers") else [])}
        self.calls.append(url)
        u = urllib.parse.urlparse(url)
        host, path, q = u.netloc, u.path, dict(urllib.parse.parse_qsl(u.query))
        self.by_host_path[(host, path)] = self.by_host_path.get((host, path), 0) + 1
        # auth per provider
        if host == "api.tradingeconomics.com":
            assert "c=k-te_api_key" in u.query and "%3A" not in u.query and q.get("f") == "json", url
            if path == "/country":
                body = J([{"Country": "United States"}, {"Country": "Germany"}])
            elif path == "/indicators":
                body = J([{"Category": "GDP"}, {"Category": "Inflation Rate"}, {"Category": "Interest Rate"}])
            elif path.startswith("/markets/historical/"):
                body = J([{"Symbol": urllib.parse.unquote(path.rsplit("/", 1)[-1]), "Close": 1}])
            elif path.startswith("/markets/"):
                body = J([{"Symbol": "US10Y:GOV", "Last": 4.1}, {"Symbol": "SPX:IND", "Last": 5000}])
            elif path == "/calendar":
                if q.get("d1", "2026") < "2026":
                    raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(b"[]"))   # history not in this plan
                body = J([{"Event": "CPI", "Date": q.get("d1")}])
            elif path.startswith("/historical/country/"):
                assert "/indicator/" in path and "," in urllib.parse.unquote(path), path
                body = J([{"Country": "United States", "Category": "GDP", "Value": 1}])
            else:
                body = J([{"ok": path}])
        elif host == "pro-api.coinmarketcap.com":
            assert headers.get("x-cmc_pro_api_key") == "k-cmc_key", headers
            if path in self.plan_refused:
                body = J({"status": {"error_code": 1006, "error_message": "Your API Key subscription plan doesn't support this endpoint."}, "data": {}})
            elif path == "/v1/cryptocurrency/map":
                start = int(q.get("start", 1))
                body = J({"status": {"error_code": 0}, "data": [{"id": i, "symbol": "C%d" % i} for i in range(start, start + (5000 if start == 1 else 10))]})
            elif path == "/v1/cryptocurrency/categories":
                body = J({"status": {"error_code": 0}, "data": [{"id": "cat1"}, {"id": "cat2"}]})
            elif path == "/v2/cryptocurrency/info":
                ids = q.get("id", "").split(",")
                assert len(ids) <= 100
                body = J({"status": {"error_code": 0}, "data": {i: {"id": int(i)} for i in ids}})
            else:
                body = J({"status": {"error_code": 0}, "data": [{"x": 1}]})
        elif host == "newsapi.org":
            assert headers.get("x-api-key") == "k-newsapi_key", headers
            if path == "/v2/sources":
                body = J({"status": "ok", "sources": [{"id": "bloomberg"}]})
            elif path == "/v2/everything" and "Federal" in q.get("q", ""):
                raise urllib.error.HTTPError(url, 429, "Too Many", {}, io.BytesIO(J({"status": "error", "code": "rateLimited"})))
            else:
                body = J({"status": "ok", "totalResults": 1, "articles": [{"title": "t", "country": q.get("country"), "q": q.get("q")}]})
        elif host == "data.nasdaq.com":
            assert q.get("api_key") == "k-nasdaq_api_key", url
            if path == "/api/v3/databases.json":
                body = J({"databases": [{"database_code": "LBMA"}, {"database_code": "OPEC"}], "meta": {"current_page": 1, "total_pages": 1}})
            elif path == "/api/v3/datasets.json":
                db = q.get("database_code")
                if db in ("LBMA", "OPEC"):
                    body = J({"datasets": [{"database_code": db, "dataset_code": "GOLD" if db == "LBMA" else "ORB"}], "meta": {"total_pages": 1}})
                else:
                    raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(J({"quandl_error": {"code": "QEPx05"}})))
            elif path.startswith("/api/v3/datasets/"):
                if "/FRED/" in path:
                    raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(J({"quandl_error": {"code": "QEPx05"}})))
                body = J({"dataset": {"dataset_code": path.split("/")[-1].replace(".json", ""), "data": [["2026-10-24", 1.0]]}})
            else:
                body = J({"ok": 1})
        elif host == "api.cryptoquant.com":
            assert headers.get("authorization") == "Bearer k-cryptoquant_api_key", headers
            if path == "/v1/btc/status/entity-list":
                body = J({"status": {"code": 200}, "result": {"data": [{"exchange" if q.get("type") == "exchange" else "miner": "binance" if q.get("type") == "exchange" else "f2pool"}, {"exchange" if q.get("type") == "exchange" else "miner": "coinbase_advanced" if q.get("type") == "exchange" else "antpool"}]}})
            elif q.get("window") == "hour":
                raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(J({"status": {"code": 403}})))   # hourly not in plan
            else:
                body = J({"status": {"code": 200}, "result": {"window": q.get("window"), "data": [{"date": "2026-10-24", "value": 1, "exchange": q.get("exchange")}]}})
        else:
            raise AssertionError("unexpected host " + host)
        resp = io.BytesIO(body)
        resp.__enter__ = lambda s=resp: s
        resp.__exit__ = lambda s=resp, *a: None
        return resp


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    import urllib.request as ur
    fake = FakeNet()
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
    spec = json.load(open(os.path.join(SRC, "provider-bank-spec.json")))
    ids = [d["id"] for d in spec["datasets"]]
    assert len(ids) == len(set(ids))
    for d in spec["datasets"]:
        assert d["provider"] in spec["providers"], d
        assert d["kind"] in ("list", "window", "per_entity", "latest_pages"), d
        if d["kind"] == "per_entity":
            assert d.get("universe") in spec["universes"], d
            assert "{entity}" in d["path"] or d.get("entity_param"), d
        if d.get("chunk_universe"):
            assert d["chunk_universe"] in spec["universes"] and "{chunk}" in d["path"], d
    for name, cfg in spec["providers"].items():
        assert cfg.get("daily_call_cap", 0) > 0, name


def test_first_run_all_five_providers():
    lf, s3, fake = boot()
    out = lf.lambda_handler({}, None)
    root = lf._spec()["root"]
    pv = out["providers"]
    assert all(pv[p]["key"] for p in pv), pv
    # TE: lists + chunked historical grid + plan-refused history windows recorded
    assert root + "te/countries/latest.json.gz" in s3.store
    hist = [k for k in s3.store if k.startswith(root + "te/historical-country/")]
    assert hist, "historical grid not banked"
    cat = json.loads(s3.store[root + "catalog.json"])["datasets"]
    assert cat["te/calendar"]["last_verdict"] == "http403", cat["te/calendar"]
    assert cat["te/calendar-forward"]["last_verdict"] == "ok"
    assert fake.by_host_path.get(("api.tradingeconomics.com", "/markets/historical/US10Y:GOV")) or fake.by_host_path.get(("api.tradingeconomics.com", "/markets/historical/US10Y%3AGOV"))
    # CMC: offset paging on map, chunked info ids (<=100 per call), plan refusals as api_error:1006
    assert fake.by_host_path[("pro-api.coinmarketcap.com", "/v1/cryptocurrency/map")] == 2
    assert len(gz(s3, root + "cmc/map/latest.json.gz")) == 5010
    assert cat["cmc/global-metrics-historical"]["last_verdict"] == "api_error:1006", cat["cmc/global-metrics-historical"]
    info = [k for k in s3.store if k.startswith(root + "cmc/info/")]
    assert info, "info chunks not banked"
    assert out["universe_counts"]["cmc_ids"] == 51          # 5010 ids / 100 per chunk
    assert pv["cmc"]["calls_today"] <= 110
    # NewsAPI: per-country dated files, a rateLimited envelope recorded as api_error:rateLimited
    assert any(k.startswith(root + "newsapi/top-headlines-business/us/") for k in s3.store)
    assert cat["newsapi/everything-macro"]["verdicts"].get("http429") == 1, cat["newsapi/everything-macro"]
    assert pv["newsapi"]["calls"] == 11 and pv["newsapi"]["http"].get("http429") == 1, pv["newsapi"]   # sources + 4 + 3 headlines, then the first macro query 429s and the day ends
    # NDL: databases, datasets per entitled db, a 403 db recorded, then every dataset code accumulated and pulled
    assert root + "ndl/databases/latest.json.gz" in s3.store
    assert cat["ndl/datasets-by-db"]["verdicts"].get("http403"), cat["ndl/datasets-by-db"]
    for k in [k for k in s3.store if "cq/exchange-split" in k][:1]:
        pass
    assert all("limit=1000" in c or "limit" not in c for c in fake.calls if "cryptoquant" in c), "CQ limit must be 1000"
    assert root + "ndl/dataset/LBMA_GOLD.json.gz" in s3.store, [k for k in s3.store if "ndl/dataset/" in k]
    assert fake.by_host_path.get(("data.nasdaq.com", "/api/v3/datasets/LBMA/GOLD.json")) == 1
    # CQ: entity lists -> per-exchange split files; hourly refused and recorded
    assert root + "cq/exchange-split/binance/btc_exchange-flows_netflow.json.gz" in s3.store, [k for k in s3.store if "exchange-split" in k][:3]
    assert cat["cq/hourly"]["last_verdict"] == "http403"
    man = json.loads(s3.store[root + "manifest.json"])
    assert man["objects"] == len([k for k in s3.store if k.endswith(".json.gz") and "/_state/" not in k])
    assert set(man["by_provider"]) == {"te", "cmc", "newsapi", "ndl", "cq"}
    assert man["by_provider"]["cmc"]["refused"], man["by_provider"]["cmc"]
    return lf, s3, fake


def test_second_run_write_if_changed_and_cap_persist():
    lf, s3, fake = test_first_run_all_five_providers()
    import urllib.request as ur
    fake2 = FakeNet()
    ur.urlopen = fake2
    out = lf.lambda_handler({}, None)
    assert out["unchanged"] > 5, out
    assert out["providers"]["cmc"]["calls_today"] > out["providers"]["cmc"]["calls"], "the daily cap counter must persist across runs"
    assert ("pro-api.coinmarketcap.com", "/v1/global-metrics/quotes/historical") not in fake2.by_host_path, "refused today must not be re-probed today"


def test_missing_key_is_recorded_not_fatal():
    lf, s3, fake = boot()
    os.environ["NEWSAPI_KEY"] = ""
    try:
        out = lf.lambda_handler({"providers": ["newsapi", "te"]}, None)
    finally:
        os.environ["NEWSAPI_KEY"] = "k-newsapi_key"
    assert out["providers"]["newsapi"]["key"] is False
    root = lf._spec()["root"]
    cat = json.loads(s3.store[root + "catalog.json"])["datasets"]
    assert cat["newsapi/sources"]["last_verdict"] == "no_key"
    assert cat["te/countries"]["last_verdict"] == "ok"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok", t.__name__)
    print("%d tests passed" % len(tests))
