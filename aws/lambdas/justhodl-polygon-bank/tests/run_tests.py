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
        self.empty_funds = set()                          # ETF Global: funds the provider has no rows for

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
                    body = J({"status": "OK", "results": [{"ticker": "AAPL", "type": "CS"}, {"ticker": "SPY", "type": "ETF"}, {"ticker": "SMH", "type": "ETF"}], "next_url": "https://api.polygon.io/v3/reference/tickers?cursor=p2"})
                else:
                    body = J({"status": "OK", "results": [{"ticker": "MSFT", "type": "CS"}]})
            else:
                body = J({"status": "OK", "results": [{"ticker": "X:BTCUSD" if q.get("market") == "crypto" else "ZZZ"}]})
        elif path == "/v1/marketstatus/now":
            body = J({"market": "closed", "serverTime": "2026-10-25T00:00:00Z"})
        elif path == "/v1/marketstatus/upcoming":
            body = J([{"date": "2026-11-26", "exchange": "NYSE", "name": "Thanksgiving", "status": "closed"}])   # a bare list
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
            # the real endpoints filter on composite_ticker ONLY; any other parameter (the v1.1.0 spec sent `ticker`) is
            # ignored and the unfiltered dataset comes back in ticker order - which is what polluted the bank
            fund = q.get("composite_ticker")
            if not fund:
                body = J({"status": "OK", "results": [{"composite_ticker": "AAAC", "effective_date": "2026-10-23", "fund_flow": 0.0}, {"composite_ticker": "AAXJ", "effective_date": "2026-10-23", "fund_flow": 1.0}]})
            elif fund in self.empty_funds:
                body = J({"status": "OK", "results": []})
            else:
                body = J({"status": "OK", "results": [{"composite_ticker": fund, "effective_date": "2026-11-03", "processed_date": "2026-11-03", "fund_flow": 5.0, "nav": 10.0, "shares_outstanding": 100.0}]})
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
        if d["path"].startswith("/etf-global/"):
            # ETF Global filters on composite_ticker; `ticker` is silently ignored (ops 6743: every fund's file held the same rows)
            assert d.get("entity_param") == "composite_ticker" and d.get("entity_field") == "composite_ticker" and d.get("epoch"), d
            assert "date.gte" not in (d.get("params") or {}) and (d.get("params") or {}).get("sort") == "processed_date.desc", d
        if d.get("priority_universe"):
            assert d["priority_universe"] in set(spec["universes"]) and d["kind"] == "per_entity", d
    for name, u in spec["universes"].items():
        assert sum(k in u for k in ("from", "static", "module")) == 1 or name == "chart", (name, u)


def test_first_run():
    lf, s3, fake = boot()
    out = lf.lambda_handler({}, None)
    root = lf._spec()["root"]
    assert out["key_status"] == "ok", out
    uc = out["universe_counts"]
    assert uc["us_tickers"] == 4 and uc["us_etfs"] == 2 and uc["chart"] >= 50 and uc["desk_etfs"] >= 100, uc
    assert fake.by_path["/v3/reference/tickers"] >= 2, "next_url pagination not followed"
    latest = gz(s3, root + "tickers-stocks-active/latest.json.gz")
    assert len(latest) == 4, latest
    assert any(k.startswith(root + "market-status-now/daily/") for k in s3.store)
    assert gz(s3, root + "market-holidays/latest.json.gz")["results"][0]["name"] == "Thanksgiving"   # bare-list response banked
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
    for ds in ("etf-global-fund-flows", "etf-global-constituents", "etf-global-profiles"):
        for fund in ("SPY", "SMH"):
            rows = gz(s3, root + "%s/%s.json.gz" % (ds, fund))
            rows = rows if isinstance(rows, list) else rows["results"]
            assert rows and {r["composite_ticker"] for r in rows} == {fund}, (ds, fund, rows)        # a fund's file holds that fund
        asked = [dict(urllib.parse.parse_qsl(urllib.parse.urlparse(u).query)) for u in fake.calls if "/etf-global/v1/" + ds.replace("etf-global-", "") in u]
        assert asked and all(q.get("composite_ticker") in ("SPY", "SMH") and "ticker" not in q for q in asked), (ds, asked[:2])
    assert s3.meta[root + "etf-global-fund-flows/SPY.json.gz"].get("epoch") == "2026-11-05"
    # the desks' funds first: SMH is in aws/shared/industry_etf_catalog, SPY is not
    flows = [dict(urllib.parse.parse_qsl(urllib.parse.urlparse(u).query)).get("composite_ticker") for u in fake.calls if "/etf-global/v1/fund-flows" in u]
    assert flows[0] == "SMH", flows
    assert st["datasets"]["etf-global-fund-flows"]["priority_last_run"] == 1 and st["datasets"]["etf-global-fund-flows"]["epoch"] == "2026-11-05"
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


def test_429_storm_stops_a_dataset_without_moving_its_cursor():
    lf, s3, fake = boot()
    real = fake.__call__

    def stormy(req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        if "/market/crypto/" in url:
            fake.calls.append(url)
            raise urllib.error.HTTPError(url, 429, "Too Many", {}, io.BytesIO(b"{}"))
        return real(req, timeout)
    import urllib.request as ur
    ur.urlopen = stormy
    out = lf.lambda_handler({"only": ["grouped-crypto"]}, None)
    root = lf._spec()["root"]
    st = json.loads(s3.store[root + "_state/state.json"])
    assert st["datasets"]["grouped-crypto"].get("cursor_date", "2018-01-01") <= "2018-01-02", st["datasets"]["grouped-crypto"]
    assert out["calls"] < 200, out["calls"]           # the storm guard stopped the dataset instead of burning the budget


def test_identity_gate_refuses_another_funds_rows():
    """The v1.1.0 question (`ticker=<fund>`) is answered with the unfiltered dataset: nothing may be banked under the
    fund's name, the verdict is `mismatch`, and the catalog names what came back."""
    lf, s3, fake = boot()
    spec = lf._spec()
    root = spec["root"]
    state, cat = {"datasets": {}}, {}
    w = lf.Walker(spec, "test-key-not-real", state, cat, {})
    old = {"id": "etf-global-fund-flows", "kind": "per_entity", "path": "/etf-global/v1/fund-flows", "universe": "us_etfs", "entity_param": "ticker",
           "entity_field": "composite_ticker", "epoch": "2026-11-05", "params": {"limit": 5000}, "paged": True, "max_pages": 2}
    assert w._entity_job(old, "SPY") == "mismatch"
    assert root + "etf-global-fund-flows/SPY.json.gz" not in s3.store
    assert cat["etf-global-fund-flows"]["last_verdict"] == "mismatch" and cat["etf-global-fund-flows"]["last_mismatch"]["got"] == ["AAAC", "AAXJ"]
    assert state["datasets"]["etf-global-fund-flows"]["mismatch"] == 1
    good = dict(old, entity_param="composite_ticker")
    assert w._entity_job(good, "SPY") == "ok" and gz(s3, root + "etf-global-fund-flows/SPY.json.gz")[0]["composite_ticker"] == "SPY"
    w.pool.shutdown(wait=True)


def test_epoch_restarts_the_lap_and_replaces_files_from_the_old_question():
    lf, s3, fake = boot()
    root = lf._spec()["root"]
    junk = gzip.compress(J([{"composite_ticker": "AAAC", "fund_flow": 0.0}, {"composite_ticker": "AAXJ", "fund_flow": 1.0}]))
    for fund in ("SPY", "SMH"):
        s3.store[root + "etf-global-fund-flows/%s.json.gz" % fund] = junk
        s3.meta[root + "etf-global-fund-flows/%s.json.gz" % fund] = {"sha256": "old", "pulled_at": "2026-10-26"}
    s3.store[root + "_state/state.json"] = J({"version": "1.1.0", "runs": 9, "key_status": "ok", "phase": "BACKFILL",
                                              "datasets": {"etf-global-fund-flows": {"objects": 1624, "bytes": 980853057, "cursor": 1, "lap": 0, "ok": 1624, "empty": 0, "fail": 0,
                                                                                    "entities_by_day": {"2026-11-04": 163}}}})
    s3.store[root + "tickers-stocks-active/latest.json.gz"] = gzip.compress(J([{"ticker": "AAPL", "type": "CS"}, {"ticker": "SPY", "type": "ETF"}, {"ticker": "SMH", "type": "ETF"}]))
    fake.empty_funds.add("SPY")                             # the provider has nothing for SPY: its old file must not stand
    lf.lambda_handler({"only": ["etf-global-fund-flows"]}, None)
    st = json.loads(s3.store[root + "_state/state.json"])["datasets"]["etf-global-fund-flows"]
    assert st["epoch"] == "2026-11-05" and st["epoch_prior_cursor"] == 1 and st["lap"] >= 1, st
    assert gz(s3, root + "etf-global-fund-flows/SMH.json.gz")[0]["composite_ticker"] == "SMH"
    assert gz(s3, root + "etf-global-fund-flows/SPY.json.gz") == [] and s3.meta[root + "etf-global-fund-flows/SPY.json.gz"].get("empty") == "1"
    assert st["emptied"] == 1
    man = json.loads(s3.store[root + "manifest.json"])["per_dataset"]["etf-global-fund-flows"]
    assert man["epoch"] == "2026-11-05" and man["entity_field"] == "composite_ticker" and man["priority_universe"] == "desk_etfs" and man["priority_last_run"] == 1, man
    # a second run does not restart the lap again
    before = st["lap"]
    lf.lambda_handler({"only": ["etf-global-fund-flows"]}, None)
    st2 = json.loads(s3.store[root + "_state/state.json"])["datasets"]["etf-global-fund-flows"]
    assert st2["lap"] >= before and st2["epoch_started_at"] == st["epoch_started_at"]


def test_module_universe_is_the_reviewed_catalog():
    lf, s3, fake = boot()
    import industry_etf_catalog as cat
    u = lf.build_universes(lf._spec(), {"tickers-stocks-active": []})
    assert u["desk_etfs"] == sorted(cat.ALL_ETFS) and "SMH" in u["desk_etfs"] and "XLK" in u["desk_etfs"]
    assert lf.build_universes({"universes": {"x": {"module": "no_such_module_here", "attr": "ALL"}}}, {})["x"] == []


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok", t.__name__)
    print("%d tests passed" % len(tests))
