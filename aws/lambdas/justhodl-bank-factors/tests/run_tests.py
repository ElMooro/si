"""justhodl-bank-factors offline tests: an in-memory S3 seeded with small bank fixtures (FMP bulk CSVs, walker JSON,
Polygon short interest, the grouped tape).  No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-bank-factors/tests/run_tests.py
"""
import csv
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
os.environ["FACTOR_BOOK_BUDGET_S"] = "120"
os.environ["FACTOR_BOOK_WORKERS"] = "4"
os.environ["FACTOR_BOOK_RETURNS_SESSIONS"] = "30"
os.environ["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] = "3008"
os.environ["FACTOR_BOOK_DB"] = os.path.join(os.environ.get("TMPDIR", "/tmp"), "factor-book-test.sqlite")


class S3Stub:
    def __init__(self):
        self.store, self.meta, self.puts = {}, {}, 0

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"Body": io.BytesIO(self.store[Key]), "Metadata": self.meta.get(Key) or {}}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"ContentLength": len(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.meta[Key] = kw.get("Metadata") or {}
        self.puts += 1

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        return {"Contents": [{"Key": k} for k in sorted(k for k in self.store if k.startswith(Prefix))]}


def csv_gz(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return gzip.compress(buf.getvalue().encode())


def jgz(obj):
    return gzip.compress(json.dumps(obj).encode())


def ms(d):
    return int(datetime(d.year, d.month, d.day, 4, tzinfo=timezone.utc).timestamp() * 1000)


def boot():
    import boto3
    s3 = S3Stub()
    boto3.client = lambda *a, **k: s3
    import fmp_bank
    fmp_bank._S3 = s3
    fmp_bank._cache.clear()
    fmp_bank._TABLES.clear()
    fmp_bank._MISSING.clear()
    for m in list(sys.modules):
        if m in ("lambda_function", "bank_enrich"):
            del sys.modules[m]
    import lambda_function as lf
    lf.s3 = s3
    lf._tls.c = s3
    return lf, s3


SYMS = ["AAPL", "MSFT", "XOM", "JPM", "TINY", "BRK-B", "SPY", "OTCX", "SAP", "ABR-PD"]      # SAP: Frankfurt -> out; SPY: ETF; OTCX: OTC; ABR-PD: a preferred


def seed(lf, s3, today):
    R = lf.FMP_ROOT
    prof = []
    for i, s in enumerate(SYMS):
        prof.append({"symbol": s, "price": 100 + 10 * i, "beta": 1.0 + i / 10, "volAvg": 1000000 * (i + 1), "mktCap": [3e12, 2.5e12, 4e11, 5e11, 1.5e8, 9e11, 5e11, 2e7, 2e11, 5e8][i],
                     "lastDiv": 1.0, "range": "1-2", "changes": 0.5, "companyName": (s + " Inc") if s != "ABR-PD" else "Arbor Realty Trust 6.25% Series D Preferred", "currency": "USD", "cik": "0000%d" % (320193 + i), "isin": "US%d" % i, "cusip": str(i) * 9,
                     "exchange": "Nasdaq", "exchangeShortName": ["NASDAQ", "NASDAQ", "NYSE", "NYSE", "NASDAQ", "NYSE", "AMEX", "PNK", "XETRA", "NYSE"][i],
                     "industry": ["Consumer Electronics", "Software", "Oil & Gas", "Banks", "Software", "Insurance", "", "Software", "Software", "REIT - Mortgage"][i],
                     "website": "https://x", "description": "long text " * 50, "ceo": "x", "sector": ["Technology", "Technology", "Energy", "Financial Services", "Technology", "Financial Services", "", "Technology", "Technology", "Real Estate"][i],
                     "country": "US", "fullTimeEmployees": 1000, "phone": "1", "address": "a", "city": "c", "state": "s", "zip": "z", "dcfDiff": 1, "dcf": 1, "image": "i",
                     "ipoDate": "1980-12-12", "defaultImage": "false", "isEtf": "true" if s == "SPY" else "false", "isActivelyTrading": "true", "isAdr": "false", "isFund": "false"})
    s3.store[R + "profile-bulk/part-0/latest.csv.gz"] = csv_gz(prof)
    stocks = [s for s in SYMS if s not in ("SPY", "SAP")]
    s3.store[R + "scores-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "reportedCurrency": "USD", "altmanZScore": 1.0 + i, "piotroskiScore": 3 + (i % 6), "workingCapital": 1, "totalAssets": 2, "retainedEarnings": 3, "ebit": 4, "marketCap": 5, "totalLiabilities": 6, "revenue": 7} for i, s in enumerate(stocks)])
    s3.store[R + "rating-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "rating": "B+", "overallScore": 3 + (i % 3), "discountedCashFlowScore": 3, "returnOnEquityScore": 4, "returnOnAssetsScore": 3, "debtToEquityScore": 2, "priceToEarningsScore": 1, "priceToBookScore": 2} for i, s in enumerate(stocks)])
    s3.store[R + "dcf-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "date": "2026-10-24", "dcf": (100 + 10 * i) * (1.2 if s == "XOM" else 0.9), "Stock Price": 100 + 10 * i} for i, s in enumerate(stocks)])
    s3.store[R + "price-target-summary-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "lastMonthCount": 5, "lastMonthAvgPriceTarget": (100 + 10 * i) * 1.1, "lastQuarterCount": 10, "lastQuarterAvgPriceTarget": 1, "lastYearCount": 1, "lastYearAvgPriceTarget": 1, "allTimeCount": 1, "allTimeAvgPriceTarget": 1, "publishers": "x"} for i, s in enumerate(stocks)])
    s3.store[R + "upgrades-downgrades-consensus-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "strongBuy": 2, "buy": 4, "hold": 2, "sell": 1 if s != "XOM" else 4, "strongSell": 1, "consensus": "Buy"} for s in stocks])
    s3.store[R + "key-metrics-ttm-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "marketCap": 1, "enterpriseValueTTM": 2, "evToEBITDATTM": 8 + 3 * i, "evToSalesTTM": 2, "freeCashFlowYieldTTM": 0.02 + 0.01 * i, "earningsYieldTTM": 0.03, "returnOnInvestedCapitalTTM": 0.05 * (i + 1), "returnOnEquityTTM": 0.2, "returnOnAssetsTTM": 0.1, "netDebtToEBITDATTM": 1.0 + i, "grahamNumberTTM": 50, "daysOfSalesOutstandingTTM": 40} for i, s in enumerate(stocks)])
    s3.store[R + "ratios-ttm-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "priceToEarningsRatioTTM": [30, 35, 12, 11, -5, 15, 9, 8][i], "priceToBookRatioTTM": 5, "priceToSalesRatioTTM": 3, "priceToFreeCashFlowRatioTTM": 25, "grossProfitMarginTTM": 0.4 + 0.02 * i, "netProfitMarginTTM": 0.2, "operatingProfitMarginTTM": 0.3, "dividendYieldTTM": 0.01, "dividendPayoutRatioTTM": 0.2, "currentRatioTTM": 1.1, "debtToEquityRatioTTM": 1.5, "interestCoverageRatioTTM": 20} for i, s in enumerate(stocks)])
    s3.store[R + "peers-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "peersList": "A,B,C"} for s in stocks])
    s3.store[R + "etf-holder-bulk/part-0/latest.csv.gz"] = csv_gz([{"symbol": "SPY", "asset": "AAPL", "name": "Apple", "isin": "x", "cusip": "y", "sharesNumber": 100, "weightPercentage": 6.5, "marketValue": 1e9, "updatedAt": "2026-10-24", "updated": "x"},
                                                                       {"symbol": "QQQ", "asset": "AAPL", "name": "Apple", "isin": "x", "cusip": "y", "sharesNumber": 50, "weightPercentage": 8.0, "marketValue": 5e8, "updatedAt": "2026-10-24", "updated": "x"},
                                                                       {"symbol": "SPY", "asset": "MSFT", "name": "Msft", "isin": "x", "cusip": "y", "sharesNumber": 90, "weightPercentage": 6.0, "marketValue": 9e8, "updatedAt": "2026-10-24", "updated": "x"}])
    s3.store[R + "income-statement-growth-bulk/2025/annual.csv.gz"] = csv_gz([{"symbol": s, "date": "2025-09-30", "fiscalYear": "2025", "period": "FY", "reportedCurrency": "USD", "growthRevenue": 0.05 + 0.02 * i, "growthEPS": 0.1, "growthNetIncome": 0.08, "growthEBITDA": 0.07, "growthOperatingIncome": 0.06, "growthGrossProfit": 0.05, "growthWeightedAverageShsOutDil": -0.01} for i, s in enumerate(stocks)])
    s3.store[R + "income-statement-growth-bulk/2024/annual.csv.gz"] = csv_gz([{"symbol": s, "date": "2024-09-30", "fiscalYear": "2024", "period": "FY", "reportedCurrency": "USD", "growthRevenue": 0.01, "growthEPS": 0.02, "growthNetIncome": 0.0, "growthEBITDA": 0.0, "growthOperatingIncome": 0.0, "growthGrossProfit": 0.0, "growthWeightedAverageShsOutDil": 0.0} for s in stocks])
    s3.store[R + "earnings-surprises-bulk/2026/year.csv.gz"] = csv_gz([{"symbol": s, "date": d, "actualEarningResult": a, "estimatedEarning": e, "lastUpdated": "2026-10-24"} for s in stocks for d, a, e in (("2026-07-31", 1.1, 1.0), ("2026-04-30", 0.9, 1.0), ("2026-01-31", 1.2, 1.0))])
    s3.store[R + "earnings-surprises-bulk/2025/year.csv.gz"] = csv_gz([{"symbol": s, "date": "2025-10-31", "actualEarningResult": 1.0, "estimatedEarning": 0.8, "lastUpdated": "2025-11-01"} for s in stocks])
    s3.store[R + "manifest.json"] = json.dumps({"updated_at": "2026-10-25T23:25:00+00:00", "dated_keys": ["income-statement-growth-bulk/2025/annual", "income-statement-growth-bulk/2024/annual", "earnings-surprises-bulk/2026/year", "earnings-surprises-bulk/2025/year"]}).encode()
    W = lf.WALKER_ROOT
    s3.store[W + "manifest.json"] = json.dumps({"updated_at": "2026-10-25T23:40:00+00:00"}).encode()
    s3.store[W + "shares-float-all/2026-10-24/p000.json.gz"] = jgz([{"symbol": s, "date": "2026-10-24", "freeFloat": 99.0, "floatShares": 1e9 if s != "TINY" else 5e6, "outstandingShares": 1.1e9} for s in stocks])
    s3.store[W + "shares-float-all/2026-10-25/p000.json.gz"] = jgz([{"symbol": s, "date": "2026-10-25", "freeFloat": 98.0, "floatShares": 1e9 if s != "TINY" else 4e6, "outstandingShares": 1.1e9} for s in stocks])
    m = today.strftime("%Y-%m")
    s3.store[W + "earnings-calendar/%s.json.gz" % m] = jgz([{"symbol": "AAPL", "date": (today + timedelta(days=3)).isoformat(), "epsEstimated": 1.5}, {"symbol": "AAPL", "date": (today - timedelta(days=80)).isoformat(), "epsEstimated": 1.4}])
    s3.store[W + "dividends-calendar/%s.json.gz" % m] = jgz([{"symbol": "XOM", "date": (today + timedelta(days=9)).isoformat(), "dividend": 0.99}])
    P = lf.POLY_ROOT
    s3.store[P + "manifest.json"] = json.dumps({"updated_at": "2026-10-25T22:55:00+00:00"}).encode()
    s3.store[P + "short-interest/TINY.json.gz"] = jgz({"results": [{"settlement_date": "2026-09-30", "short_interest": 400000, "avg_daily_volume": 100000, "days_to_cover": 4.0, "ticker": "TINY"},
                                                                     {"settlement_date": "2026-10-15", "short_interest": 1000000, "avg_daily_volume": 125000, "days_to_cover": 8.0, "ticker": "TINY"}]})
    s3.store[P + "short-interest/AAPL.json.gz"] = jgz({"results": [{"settlement_date": "2026-10-15", "short_interest": 10000000, "avg_daily_volume": 50000000, "days_to_cover": 0.2, "ticker": "AAPL"}]})
    s3.store[P + "short-volume/TINY.json.gz"] = jgz({"results": [{"date": "2026-10-%02d" % d, "short_volume": 60, "total_volume": 100, "ticker": "TINY"} for d in range(17, 25)]})
    G = lf.GROUPED_ROOT
    d, n = today, 0
    sessions = []
    while n < 30:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            sessions.append(d)
            n += 1
    sessions.reverse()
    for i, sd in enumerate(sessions):
        rows = []
        for s in ("AAPL", "MSFT", "XOM", "JPM", "TINY", "BRK.B"):
            base = {"AAPL": 100.0, "MSFT": 200.0, "XOM": 50.0, "JPM": 150.0, "TINY": 2.0, "BRK.B": 400.0}[s]
            px = base * (1 + 0.01 * i) if s != "XOM" else base * (1 - 0.005 * i)
            rows.append({"T": s, "o": px, "h": px * 1.01, "l": px * 0.99, "c": px, "v": 1000000, "vw": px, "t": ms(sd), "n": 10})
        s3.store[G + "%d/%s.json.gz" % (sd.year, sd.isoformat())] = jgz({"resultsCount": len(rows), "results": rows})
    return stocks, sessions


def test_join_writes_one_doc_per_us_symbol_with_derived_fields_ranks_and_a_matrix():
    lf, s3 = boot()
    today = date(2026, 10, 26)
    stocks, sessions = seed(lf, s3, today)
    out = lf.lambda_handler({}, None)
    assert out["ok"], out
    assert out["symbols"] == 9 and out["written"] == 9 and out["failed"] == 0, out      # 10 profiles - SAP (Frankfurt) = 9 US-listed; SPY, OTCX, ABR-PD kept with flags
    man = json.loads(s3.store[lf.MANIFEST_KEY])
    assert man["universe"] == 9 and man["etfs"] == 1 and man["otc"] == 1 and man["preferreds"] == 1 and man["common_stocks"] == 6, man
    assert man["coverage"]["scores"] == 8 and man["coverage"]["short"] == 2 and man["coverage"]["float"] == 8 and man["coverage"]["returns"] == 6, man["coverage"]
    assert man["schema_found"]["altman_z"] == "scores.altmanZScore" and man["schema_found"]["pe"] == "ratios_ttm.priceToEarningsRatioTTM", man["schema_found"]
    assert man["as_of"]["fmp-bank"].startswith("2026-10-25") and man["as_of"]["float"] == "2026-10-25" and man["as_of"]["polygon-tape"] == sessions[-1].isoformat()
    aapl = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/AAPL.json.gz"]))
    assert aapl["identity"]["sector"] == "Technology" and "description" not in aapl["profile"] and aapl["profile"]["price"] == 100
    assert aapl["scores"]["altmanZScore"] == 1.0 and aapl["scores"]["piotroskiScore"] == 3
    d = aapl["derived"]
    assert abs(d["dcf_upside_pct"] - (-10.0)) < 1e-6 and abs(d["pt_upside_pct"] - 10.0) < 1e-6, d
    assert d["consensus_n"] == 10 and abs(d["consensus_score"] - 0.5) < 1e-9, d           # (2*2 + 4 - 1 - 2*1) / 10
    assert d["beat_rate_4q"] == 0.75 and d["last_surprise_pct"] == 10.0 and d["last_surprise_date"] == "2026-07-31", d   # 3 of 4 beat, newest first
    assert abs(d["short_pct_float"] - 1.0) < 1e-9 and d["si_settlement_date"] == "2026-10-15", d
    assert d["etf_holder_count"] == 2 and abs(d["etf_weight_sum"] - 14.5) < 1e-9 and aapl["etf_holders"]["top"][0]["etf"] == "QQQ"
    assert d["n_peers"] == 3 and d["next_earnings_date"] == "2026-10-29", d
    assert aapl["growth"]["growthRevenue"] == 0.05 and aapl["growth"]["fiscalYear"] == "2025"             # newest fiscal year wins
    assert aapl["float"]["floatShares"] == 1e9 and aapl["float"]["date"] == "2026-10-25"                   # newest day's pages
    r = aapl["returns"]
    assert r["close"] > 100 and r["ret_1d"] is not None and r["ret_21d"] is not None and "ret_63d" not in r and "high_52w" not in r   # 30 sessions seeded
    assert r["dollar_vol_20d"] > 0 and r["volatility_20d"] is not None
    tiny = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/TINY.json.gz"]))
    assert tiny["short"]["settlement_date"] == "2026-10-15" and tiny["short"]["short_interest"] == 1000000 and tiny["short"]["prev_short_interest"] == 400000
    assert abs(tiny["derived"]["short_pct_float"] - 25.0) < 1e-9 and tiny["derived"]["short_volume_ratio"] == 0.6, tiny["derived"]
    # ranks: XOM is the cheapest positive-PE name among the 6 common stocks (12 vs 30/35/11?) -> JPM 11 is cheapest; TINY's -5 is excluded as not cheap
    jpm = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/JPM.json.gz"]))
    assert jpm["ranks"]["pct_pe"] == 100.0, jpm["ranks"]
    assert tiny["ranks"].get("pct_pe") is None and tiny["ranks"]["pct_float_shares"] == 100.0        # the smallest float ranks highest; too few short rows here for the squeeze composite
    assert all(0 <= v <= 100 for v in aapl["ranks"].values()), aapl["ranks"]
    assert "pct_roic" in aapl["ranks"] and not any(k.startswith("spct_") for k in aapl["ranks"]), aapl["ranks"]   # sector ranks need >= 5 names in the sector (3 here)
    assert set(aapl["composites"]) == {"value", "quality", "growth", "momentum", "squeeze", "street"} and aapl["composites"]["value"] is not None
    spy = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/SPY.json.gz"]))
    assert spy["identity"]["is_etf"] is True and not spy["ranks"], spy["ranks"]                # ETFs are documented but never ranked
    otc = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/OTCX.json.gz"]))
    assert otc["identity"]["is_otc"] is True and not otc["ranks"]
    pref = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/ABR-PD.json.gz"]))
    assert pref["identity"]["is_preferred"] is True and not pref["ranks"] and pref["scores"]            # documented, never ranked
    # the matrix: columnar, every symbol, curated fields + ranks + composites
    m = json.loads(gzip.decompress(s3.store[lf.ROOT + "matrix/core.json.gz"]))
    assert m["n"] == 9 and len(m["symbols"]) == 9 and "altman_z" in m["cols"] and "pct_pe" in m["cols"] and "composite_value" in m["cols"] and "is_preferred" in m["cols"]
    i = m["symbols"].index("AAPL")
    assert m["cols"]["altman_z"][i] == 1.0 and m["cols"]["dcf_upside_pct"][i] == -10.0 and m["cols"]["short_pct_float"][i] == 1.0 and m["cols"]["sector"][i] == "Technology"
    assert "BRK-B" in m["symbols"] and m["cols"]["close"][m["symbols"].index("BRK-B")] > 400                  # dash <-> dot joined to the tape
    uni = json.loads(gzip.decompress(s3.store[lf.ROOT + "universe.json.gz"]))
    assert set(uni["symbols"]) == {"AAPL", "MSFT", "XOM", "JPM", "TINY", "BRK-B", "SPY", "OTCX", "ABR-PD"}
    # second run: nothing changed -> no symbol doc re-put (write-if-changed); matrix/universe/manifest/state refreshed
    puts = s3.puts
    out2 = lf.lambda_handler({}, None)
    assert out2["written"] == 0 and out2["unchanged"] == 9, out2
    ncols = len(json.loads(gzip.decompress(s3.store[lf.ROOT + "matrix/index.json.gz"]))["columns"])
    assert s3.puts - puts == 5 + ncols + 1, (s3.puts - puts, ncols)                                        # lease, matrix, universe, state, manifest + the column files + the index
    # a changed bank value re-writes only that symbol
    s3.store[lf.FMP_ROOT + "scores-bulk/latest.csv.gz"] = csv_gz([{"symbol": s, "reportedCurrency": "USD", "altmanZScore": (9.9 if s == "MSFT" else 1.0 + i), "piotroskiScore": 3 + (i % 6), "workingCapital": 1, "totalAssets": 2, "retainedEarnings": 3, "ebit": 4, "marketCap": 5, "totalLiabilities": 6, "revenue": 7} for i, s in enumerate(stocks)])
    import fmp_bank
    fmp_bank._TABLES.clear()
    fmp_bank._cache.clear()
    out3 = lf.lambda_handler({}, None)
    assert out3["written"] >= 1 and out3["written"] <= 8, out3          # MSFT changed; ranks of its sector peers may move too
    msft = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/MSFT.json.gz"]))
    assert msft["scores"]["altmanZScore"] == 9.9 and msft["ranks"]["pct_altman_z"] == 100.0


def test_lease_missing_bank_and_no_returns_flag():
    lf, s3 = boot()
    out = lf.lambda_handler({}, None)
    assert out["ok"] is False and "profile-bulk" in out["error"], out                                      # nothing banked: says so, writes nothing
    today = date(2026, 10, 26)
    seed(lf, s3, today)
    import fmp_bank
    fmp_bank._MISSING.clear()                       # the reader remembers a miss for 15 min; a fresh day is a fresh container
    fmp_bank._cache.clear()
    s3.store[lf.STATE_KEY] = json.dumps({"lease_until": 9e12}).encode()
    assert lf.lambda_handler({}, None).get("skipped") == "lease_held"
    out = lf.lambda_handler({"force": True, "no_returns": True}, None)
    assert out["ok"] and out["returns"].get("skipped") and out["coverage"]["returns"] == 0, out
    aapl = json.loads(gzip.decompress(s3.store[lf.ROOT + "symbols/AAPL.json.gz"]))
    assert aapl["returns"] is None and aapl["derived"]["price_used"] == 100                                 # profile price when the tape is absent


def test_bank_enrich_sdk_reads_the_book_and_attaches_with_provenance():
    lf, s3 = boot()
    today = date(2026, 10, 26)
    seed(lf, s3, today)
    assert lf.lambda_handler({}, None)["ok"]
    import boto3
    boto3.client = lambda *a, **k: s3
    import bank_enrich as be
    be._tls.c = s3
    be._DOCS.clear()
    be._MATRIX.update(at=0, doc=None)
    be._MANIFEST.update(at=0, doc=None)
    assert be.available() and be.as_of()["fmp-bank"].startswith("2026-10-25")
    f = be.factor("aapl")
    assert f and f["scores"]["altmanZScore"] == 1.0 and f["derived"]["pt_upside_pct"] == 10.0
    assert be.factor("BRK.B")["identity"]["symbol"] == "BRK-B" and be.factor("NOPE") is None
    fs = be.factors(["AAPL", "XOM", "NOPE"])
    assert set(fs) == {"AAPL", "XOM"}
    m = be.matrix()
    assert m["n"] == 9 and be.lookup("JPM", ["pe", "pct_pe"]) == {"pe": 11.0, "pct_pe": 100.0}
    idx = be.index()
    assert idx and idx["n"] == 9 and "altman_z" in idx["columns"] and set(idx["identity"]) >= {"sector", "is_etf", "is_preferred"}
    assert be.column("altman_z")[idx["symbols"].index("MSFT")] == 2.0 and be.column("nope") is None
    assert set(be._COLS) == {"pe", "pct_pe", "altman_z"}                                                   # projection: only the asked columns were read
    cheap = be.screen(lambda r: r["pe"] is not None and 0 < r["pe"] < 20, fields=["pe", "mcap"])
    assert {r["symbol"] for r in cheap} == {"XOM", "JPM", "BRK-B"}, cheap                                 # TINY (-5) and SPY/OTCX/ABR-PD (pe 9, a preferred) excluded
    out = {"generated": "x", "picks": [{"ticker": "AAPL", "score": 1}, {"ticker": "NOPE", "score": 0}], "nested": {"rows": [{"symbol": "TINY"}]}}
    out = be.attach(out)
    b = out["bank_enrichment"]
    assert b["source"] == "justhodl-bank-factors" and b["as_of"]["fmp-bank"] and b["n"] == 2 and b["missing"] == ["NOPE"], b
    assert b["rows"]["AAPL"]["altman_z"] == 1.0 and b["rows"]["AAPL"]["pt_upside_pct"] == 10.0 and b["rows"]["TINY"]["short_pct_float"] == 25.0
    assert set(b["fields"]) == set(be.DEFAULT_FIELDS) and out["picks"][0]["score"] == 1                     # additive: the engine's own keys untouched
    # many symbols -> the matrix path; the same field names
    out2 = be.attach({"rows": [{"symbol": s} for s in ("AAPL MSFT XOM JPM TINY BRK-B SPY OTCX ABR-PD".split() * 3)]}, fields=["altman_z", "composite_value"])
    assert out2["bank_enrichment"]["n"] == 9 and out2["bank_enrichment"]["rows"]["MSFT"]["altman_z"] == 2.0
    # a dead bucket never raises
    be._tls.c = S3Stub()
    be._DOCS.clear()
    be._MATRIX.update(at=0, doc=None)
    be._MANIFEST.update(at=0, doc=None)
    assert be.factor("AAPL") is None and be.matrix() is None and be.screen(lambda r: True) == [] and not be.available()
    assert be.attach({"a": 1}, symbols=["AAPL"])["bank_enrichment"]["missing"] == ["AAPL"]


if __name__ == "__main__":
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
            print("ok", name)
    print("%d tests passed" % n)
