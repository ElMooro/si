"""justhodl-fmp-walker offline tests: a fake FMP /stable server + in-memory S3, no network, no AWS.

Run: python3 aws/lambdas/justhodl-fmp-walker/tests/run_tests.py
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
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["FMP_WALKER_BUDGET_S"] = "120"
os.environ["FMP_WALKER_RPS"] = "100000"
os.environ["FMP_WALKER_WORKERS"] = "4"


class S3Stub:
    def __init__(self):
        self.store = {}
        self.meta = {}
        self.puts = 0

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {"Metadata": self.meta.get(Key, {}), "ContentLength": len(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body.read() if hasattr(Body, "read") else Body
        self.meta[Key] = kw.get("Metadata") or {}
        self.puts += 1


def J(x):
    return json.dumps(x).encode()


class FakeFMP:
    """JSON per endpoint; 404 for unknown; configurable 403/429; counts calls per endpoint."""

    def __init__(self):
        self.calls = []
        self.by_endpoint = {}
        self.serve_403 = False
        self.rate_limit_once = set()
        self.unknown = {"senate-trading-rss-feed", "esg-benchmark"}

    def __call__(self, req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        self.calls.append(url)
        u = urllib.parse.urlparse(url)
        ep = u.path.split("/stable/", 1)[1]
        q = dict(urllib.parse.parse_qsl(u.query))
        assert q.get("apikey") == "test-key-not-real"
        self.by_endpoint[ep] = self.by_endpoint.get(ep, 0) + 1
        if self.serve_403:
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(b"{}"))
        if ep in self.unknown:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b"{}"))
        if ep in self.rate_limit_once:
            self.rate_limit_once.discard(ep)
            raise urllib.error.HTTPError(url, 429, "Too Many", {}, io.BytesIO(b"{}"))
        page = int(q.get("page", 0) or 0)
        if ep == "stock-list":                               # /stable/stock-list carries NO exchange (live fact, 2026-10-25)
            body = J([{"symbol": "AAPL", "companyName": "Apple"}, {"symbol": "MSFT", "companyName": "Microsoft"},
                      {"symbol": "SAP.DE", "companyName": "SAP"}, {"symbol": "ZZZQ", "companyName": "Dead Co"}])
        elif ep == "etf-list":
            body = J([{"symbol": "SPY", "name": "SPDR"}, {"symbol": "EXS1.DE", "name": "iShares DAX"}])
        elif ep == "company-screener":                        # the US universe fallback when profile-bulk is not readable
            ex = q.get("exchange")
            body = J({"NASDAQ": [{"symbol": "AAPL", "exchange": "NASDAQ", "isEtf": False}, {"symbol": "MSFT", "exchange": "NASDAQ", "isEtf": False}],
                      "NYSE": [], "AMEX": [{"symbol": "SPY", "exchange": "AMEX", "isEtf": True}]}.get(ex, []))
        elif ep == "actively-trading-list":
            body = J([{"symbol": "AAPL"}, {"symbol": "MSFT"}, {"symbol": "SAP.DE"}, {"symbol": "SPY"}])
        elif ep in ("available-sectors",):
            body = J([{"sector": "Technology"}, {"sector": "Energy"}])
        elif ep in ("available-industries",):
            body = J([{"industry": "Software"}])
        elif ep in ("available-exchanges",):
            body = J([{"exchange": "NASDAQ", "name": "NASDAQ"}, {"exchange": "XETRA", "name": "Xetra"}])
        elif ep == "commitment-of-traders-list":
            body = J([{"symbol": "ES", "name": "S&P 500"}])
        elif ep == "earnings-transcript-list":
            body = J([{"symbol": "AAPL", "companyName": "Apple", "noOfTranscripts": 40}])
        elif ep == "cik-list":
            body = J([{"cik": "%010d" % i} for i in range(10000)] if page < 2 else [{"cik": "1"}])
        elif ep == "senate-profile":
            body = J([{"senateId": "S001", "firstName": "A"}] if page == 0 else [])
        elif ep == "institutional-ownership/latest":
            body = J([{"cik": "0001067983", "name": "BERKSHIRE"}, {"cik": "0001166559", "name": "GATES"}] if page == 0 else [])
        elif ep == "insider-trading/latest":
            body = J([{"symbol": "AAPL", "filingDate": q.get("date"), "transactionType": "S-Sale"}] if page == 0 else [])
        elif ep == "earnings-calendar":
            body = J([{"symbol": "AAPL", "date": q.get("from")}] if page == 0 else [])
        elif ep == "economic-calendar":
            body = J([{"event": "CPI", "date": q.get("from")}])
        elif ep == "news/stock-latest":
            body = J([{"title": "n%d-%d" % (page, i), "publishedDate": "2026-10-25"} for i in range(250)] if page < 2 else [])
        elif ep == "earning-call-transcript-dates":
            body = J([{"quarter": 3, "fiscalYear": 2026, "date": "2026-07-31"}, {"quarter": 2, "fiscalYear": 2026, "date": "2026-05-01"}])
        elif ep == "earning-call-transcript":
            body = J([{"symbol": q["symbol"], "year": int(q["year"]), "quarter": int(q["quarter"]), "content": "..."}])
        elif ep == "historical-chart/1min":
            body = J([{"date": "%s 15:59:00" % q["from"], "open": 1, "close": 1, "volume": 10}] if q["from"] not in ("2026-10-17", "2026-10-18") else [])
        elif ep == "institutional-ownership/symbol-positions-summary":
            body = J([{"symbol": q["symbol"], "year": int(q["year"]), "quarter": int(q["quarter"]), "investorsHolding": 1}])
        elif ep == "historical-sector-performance":
            body = J([{"date": "2026-10-24", "sector": q.get("sector"), "exchange": q.get("exchange"), "averageChange": 0.1}])
        elif ep == "exchange-market-hours":
            body = J([{"exchange": q.get("exchange"), "isMarketOpen": False}])
        elif ep == "biggest-gainers":
            body = J([{"symbol": "ZZZQ", "changesPercentage": 50}])
        elif ep == "sector-performance-snapshot":
            body = J([{"date": q.get("date"), "sector": "Technology", "averageChange": 0.5}])
        elif ep in ("stock-peers",):
            body = J([])                                      # a legit empty answer
        elif ep.endswith("-list") or ep in ("treasury-rates", "market-risk-premium", "symbol-change", "all-exchange-market-hours",
                                            "standard-industrial-classification-list", "insider-trading-transaction-type", "available-countries",
                                            "most-actives", "biggest-losers", "pre-post-market", "quotes/commodity", "quotes/forex", "quotes/crypto",
                                            "batch-index-quotes", "batch-etf-quotes", "batch-mutualfund-quotes", "financial-statement-symbol-list",
                                            "sp500-constituent", "nasdaq-constituent", "dowjones-constituent", "historical-sp500-constituent",
                                            "historical-nasdaq-constituent", "historical-dowjones-constituent", "tradable-list"):
            body = J([{"symbol": "X", "v": 1}])
        else:
            sym = q.get("symbol") or q.get("cik") or q.get("name") or q.get("exchange") or q.get("industry") or q.get("senateId") or q.get("formType") or "row"
            body = J([{"symbol": sym, "page": page, "v": 1}] if page == 0 else [])
        resp = io.BytesIO(body)
        resp.__enter__ = lambda s=resp: s
        resp.__exit__ = lambda s=resp, *a: None
        return resp


def boot(store=None):
    import boto3
    s3 = store or S3Stub()
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


def gz(s3, key):
    return json.loads(gzip.decompress(s3.store[key]))


def test_spec_is_sound():
    spec = json.load(open(os.path.join(SRC, "fmp-walker-spec.json")))
    ids = [d["id"] for d in spec["datasets"]]
    assert len(ids) == len(set(ids)), "duplicate dataset ids"
    kinds = {"list", "latest_pages", "daily_dated", "window", "per_entity", "per_entity_period"}
    for d in spec["datasets"]:
        assert d["kind"] in kinds, d
        assert d["endpoint"] and "/stable" not in d["endpoint"], d
        if d["kind"] in ("per_entity", "per_entity_period"):
            assert d.get("universe") and d.get("entity_param"), d
    cat = json.load(open(os.path.join(HERE, "stable-endpoints.json"))) if os.path.exists(os.path.join(HERE, "stable-endpoints.json")) else None
    if cat:
        for d in spec["datasets"]:
            ep = d["endpoint"].replace("historical-chart/1min", "historical-chart/{interval}")
            assert ep in cat, "endpoint not in the fmp_data catalog: %s" % ep
    # every universe referenced exists (declared, derived or static)
    declared = set(spec["universes"]) | {"years_2010", "quarters_2013", "cryptos_top"}
    for d in spec["datasets"]:
        if d.get("universe"):
            assert d["universe"] in declared, d["universe"]


def test_first_run_banks_lists_feeds_windows_and_rotates_entities():
    lf, s3, fake = boot()
    out = lf.lambda_handler({}, None)
    assert out["key_status"] == "ok", out
    root = lf._spec()["root"]
    # lists: latest + dated copy on first (changed) pull
    assert root + "stock-list/latest.json.gz" in s3.store
    assert any(k.startswith(root + "stock-list/daily/") for k in s3.store)
    # universes: tierA = US + actively trading (ZZZQ is not active, SAP.DE not US), + US ETF
    uc = out["universe_counts"]
    assert uc["tierA"] == 3, uc                       # AAPL MSFT SPY
    assert uc["all"] == 6, uc
    assert uc["etfs_us"] == 1 and uc["holder_ciks"] == 2 and uc["senate_ids"] == 1, uc
    st0 = json.loads(s3.store[root + "_state/state.json"])
    assert st0["universe_source"] == "profile-bulk", st0["universe_source"]
    assert "AAPL" in lf.CORE and uc["chart"] >= 40
    # latest_pages feed -> dated file, pages followed until short page
    news = [k for k in s3.store if k.startswith(root + "news-stock-latest/")]
    assert news and len(gz(s3, news[0])) == 500, news
    # window dataset -> month files from the floor, cursor advanced
    ec = [k for k in s3.store if k.startswith(root + "earnings-calendar/")]
    assert any(k.endswith("2000-01.json.gz") for k in ec), ec[:3]
    st = json.loads(s3.store[root + "_state/state.json"])
    assert st["datasets"]["earnings-calendar"]["cursor_month"] >= "2000-02"
    # daily_dated -> year/date files, weekends skipped
    sp = [k for k in s3.store if k.startswith(root + "sector-performance-snapshot/")]
    assert sp and all("/2026/" in k for k in sp), sp[:3]
    # per_entity: cursor moved, lap counted when the universe is exhausted
    pt = st["datasets"]["price-target"]
    assert pt["universe_size"] == 3 and pt["lap"] >= 1, pt
    assert root + "price-target/AAPL.json.gz" in s3.store
    # per_exchange history: one object per (sector, exchange)
    assert root + "historical-sector-performance/Technology/NASDAQ.json.gz" in s3.store
    # per_entity_period from trailing quarters: files per quarter
    pos = [k for k in s3.store if k.startswith(root + "institutional-ownership-symbol-positions-summary/AAPL/")]
    assert len(pos) == 12, len(pos)
    # transcripts from the dates dataset
    assert root + "earning-call-transcript/AAPL/2026Q3.json.gz" in s3.store
    # intraday: per-day keys under symbol/month, empty days not written
    bars = [k for k in s3.store if k.startswith(root + "historical-chart-1min/AAPL/")]
    assert bars and all(k.count("/") == 6 for k in bars), bars[:2]
    assert not any(k.endswith("2026-10-18.json.gz") for k in bars)
    # big paged reference feeds: one object per page, nothing merged
    cik = [k for k in s3.store if k.startswith(root + "cik-list/")]
    assert cik and all("/p0" in k for k in cik), cik[:3]
    # refusals recorded by http code, never fatal; a legit empty answer is 'empty'
    cat = json.loads(s3.store[root + "catalog.json"])["datasets"]
    assert cat["esg-benchmark"]["last_verdict"] == "http404", cat["esg-benchmark"]
    assert cat["stock-peers"]["last_verdict"] == "empty"
    man = json.loads(s3.store[root + "manifest.json"])
    assert man["objects"] == len([k for k in s3.store if k.endswith(".json.gz") and "/_state/" not in k]) , (man["objects"])
    assert man["pages"] == man["objects"] and man["pages_bytes"] == man["bytes"] > 0
    assert man["phase"] == "BACKFILL" and man["datasets_declared"] > 150
    assert ("esg-benchmark", "http404") in [tuple(x) for x in man["endpoints_refused"]]
    assert man["per_dataset"]["price-target"]["rotation_days"] is not None
    assert len(man["skipped"]) >= 10
    return lf, s3, fake, out


def test_daily_price_lane_runs_first_complete_and_shares_the_prefix():
    """v1.2.0: historical-price-eod-full-daily (rotation_days 1) covers its whole price_daily universe every run, before
    the rotating long tail, and lands under the long tail's prefix (store_as) so fmp_bank.walker() reads one place."""
    lf, s3, fake, out = test_first_run_banks_lists_feeds_windows_and_rotates_entities()
    root = lf._spec()["root"]
    st = json.loads(s3.store[root + "_state/state.json"])
    uc = out["universe_counts"]
    # the universe: chart (core 40 + banked US chart names) | tierA | indexes | USD forex | top cryptos | commodities
    assert uc["price_daily"] >= uc["chart"] and uc["price_daily"] >= uc["tierA"], uc
    d = st["datasets"]["historical-price-eod-full-daily"]
    assert d["universe_size"] == uc["price_daily"] and d["lap"] >= 1 and d["entities_last_run"] >= uc["price_daily"], d
    # every daily-lane symbol has its history banked under the shared prefix; none under the lane's own id
    for sym in ("AAPL", "MSFT", "SPY", "X"):
        assert root + "historical-price-eod-full/%s.json.gz" % sym in s3.store, sym
    assert not any(k.startswith(root + "historical-price-eod-full-daily/") for k in s3.store)
    # the lane ran before the rotating per-entity datasets
    steps = [x["step"] for x in json.loads(s3.store[root + "_state/state.json"])["last_run"]["steps"]]
    assert steps.index("per_entity_daily") < steps.index("per_entity"), steps
    assert fake.by_endpoint["historical-price-eod/full"] >= uc["price_daily"]


def test_second_run_is_write_if_changed_and_resumes():
    lf, s3, fake, out1 = test_first_run_banks_lists_feeds_windows_and_rotates_entities()
    puts1 = s3.puts
    import urllib.request as ur
    fake2 = FakeFMP()
    ur.urlopen = fake2
    out2 = lf.lambda_handler({}, None)
    root = lf._spec()["root"]
    assert out2["unchanged"] > 50, out2                        # identical bodies were not re-put
    man = json.loads(s3.store[root + "manifest.json"])
    assert man["runs"] == 2
    # dated_always lists still write a dated copy every run; latest unchanged
    assert fake2.by_endpoint.get("stock-list") == 1
    st = json.loads(s3.store[root + "_state/state.json"])
    assert st["datasets"]["price-target"]["lap"] >= 2
    assert s3.puts - puts1 < puts1, (s3.puts, puts1)


def test_403_records_unauthorized_and_stops():
    lf, s3, fake = boot()
    fake.serve_403 = True
    out = lf.lambda_handler({}, None)
    assert out["key_status"] == "unauthorized", out
    assert out["objects"] == 0
    root = lf._spec()["root"]
    cat = json.loads(s3.store[root + "catalog.json"])["datasets"]
    assert cat["stock-list"]["last_verdict"] == "http403"
    assert fake.by_endpoint.get("price-target") is None, "per-entity work must not start on an unauthorized key"


def test_429_is_retried_and_slows_the_bucket():
    lf, s3, fake = boot()
    fake.rate_limit_once.add("stock-list")
    out = lf.lambda_handler({"only": ["stock-list", "etf-list"]}, None)
    assert out["key_status"] == "ok"
    assert fake.by_endpoint["stock-list"] == 2
    assert out["calls"] == 3


def test_only_and_skip_kinds_and_lease():
    lf, s3, fake = boot()
    out = lf.lambda_handler({"only": ["treasury-rates"]}, None)
    assert out["calls"] == 1 and fake.by_endpoint == {"treasury-rates": 1}, fake.by_endpoint
    out = lf.lambda_handler({"skip_kinds": ["per_entity", "per_entity_period", "window", "daily_dated", "latest_pages"]}, None)
    assert all("/" not in ep or ep.startswith("quotes/") or ep in ("all-exchange-market-hours",) or ep.count("/") == 0 for ep in fake.by_endpoint), fake.by_endpoint
    root = lf._spec()["root"]
    st = json.loads(s3.store[root + "_state/state.json"])
    st["lease_until"] = 9e12
    s3.store[root + "_state/state.json"] = json.dumps(st).encode()
    assert lf.lambda_handler({}, None).get("skipped") == "lease_held"
    assert "universe_counts" in lf.lambda_handler({"force": True}, None)


def test_run_ends_inside_its_budget_under_a_slow_server():
    """v1.1.0: a run must return within budget + a small margin whatever the server does (the first live run never wrote state)."""
    import time as _t
    lf, s3, fake = boot()
    lf.BUDGET_S = 12
    lf.RPS = 40.0
    slow = fake.__call__

    def slow_call(req, timeout=0):
        _t.sleep(0.02)
        return slow(req, timeout)
    import urllib.request as ur
    ur.urlopen = slow_call
    t0 = _t.time()
    out = lf.lambda_handler({"rps": 40}, None)
    took = _t.time() - t0
    assert took < lf.BUDGET_S + 8, took
    assert out["calls"] > 0 and "universe_counts" in out
    root = lf._spec()["root"]
    st = json.loads(s3.store[root + "_state/state.json"])
    assert st["lease_until"] == 0 and st.get("last_run", {}).get("seconds")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok", t.__name__)
    print("%d tests passed" % len(tests))
