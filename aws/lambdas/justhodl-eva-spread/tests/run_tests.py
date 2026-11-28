"""justhodl-eva-spread offline tests (v1.1.0 bank fusion; v1.2.0 the equity risk premium read from the warehouse): TTM facts from the factor book, the EVA series from the FMP
statement bank, a labelled whole-market pass; with the banks absent the live FMP path runs unchanged.
No network, no AWS, no key.

Run: python3 aws/lambdas/justhodl-eva-spread/tests/run_tests.py
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ["FMP_KEY"] = "test-key-not-real"
os.environ["FRED_KEY"] = ""


class FakeS3:
    def __init__(self):
        self.puts = {}
        self.gets = {}                      # what the warehouse holds: {key: document}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = json.loads(Body)

    def get_object(self, Bucket, Key, **kw):
        if Key not in self.gets:
            raise KeyError(Key)
        import io
        return {"Body": io.BytesIO(json.dumps(self.gets[Key]).encode())}


def row(mcap, roic, ic, ev=None, beta=1.0, tax=0.21, sector="Technology", **extra):
    return dict({"mcap": mcap, "price": 100.0, "beta": beta, "roic": roic, "invested_capital": ic, "ev": ev if ev is not None else mcap * 1.05,
                 "effective_tax_rate": tax, "revenue_per_share": 20.0, "sector": sector, "altman_z": 5.0, "piotroski": 7, "pt_upside_pct": 10.0, "composite_quality": 80.0}, **extra)


class FakeBook(types.ModuleType):
    on = True
    rows = {}

    def available(self):
        return self.on

    def as_of(self):
        return {"fmp-bank": "2026-10-25T23:40:00+00:00", "tape": "2026-10-24"} if self.on else None

    def lookup(self, symbol, fields=None):
        r = self.rows.get(str(symbol).upper())
        return None if r is None else dict({k: r.get(k) for k in (fields or r)}, symbol=str(symbol).upper())

    def screen(self, where, fields=None, limit=None, common_only=True):
        out = []
        for s, r in self.rows.items():
            rr = dict(r, symbol=s)
            if common_only and (rr.get("is_etf") or rr.get("is_otc")):
                continue
            if where(rr):
                out.append(rr)
        return out[:limit] if limit else out


class FakeBank(types.ModuleType):
    on = True
    calls = []

    def statements(self, path, symbol, period="annual", limit=None, years=None):
        self.calls.append((path, symbol, period, limit))
        if not self.on or symbol not in ("AAPL", "MKT1"):
            return None
        if path == "income-statement":
            return [{"date": "2025-09-27", "operatingIncome": 130e9, "effectiveTaxRate": 0.24, "revenue": 416e9},
                    {"date": "2024-09-28", "operatingIncome": 123e9, "effectiveTaxRate": 0.24, "revenue": 391e9},
                    {"date": "2023-09-30", "operatingIncome": 114e9, "effectiveTaxRate": 0.15, "revenue": 383e9}]
        if path == "key-metrics":
            return [{"date": "2025-09-27", "investedCapital": 160e9}, {"date": "2024-09-28", "investedCapital": 150e9}, {"date": "2023-09-30", "investedCapital": 140e9}]
        return None


def boot(book_on=True, bank_on=True):
    import boto3
    s3 = FakeS3()
    boto3.client = lambda *a, **k: s3
    sys.modules["fmp_bankfirst"] = types.ModuleType("fmp_bankfirst")
    sys.modules["fmp_bankfirst"].install = lambda: None
    sys.modules["_fred_shim"] = types.ModuleType("_fred_shim")
    book = FakeBook("bank_enrich")
    book.on = book_on
    book.rows = {"AAPL": row(3.9e12, 0.55, 160e9, ev=3.95e12, beta=1.2, tax=0.24),
                 "MSFT": row(3.5e12, 0.30, 300e9, sector="Technology"),
                 "MKT1": row(50e9, 0.40, 10e9, sector="Industrials"),            # whole-market only (not in the static 50)
                 "LOSER": row(5e9, 0.02, 20e9, sector="Energy"),
                 "SMALL": row(1e9, 0.60, 1e9),                                   # below $2B -> not a market candidate
                 "ADRX": row(90e9, 0.50, 10e9, is_adr=True),                     # ADR -> excluded from the market pass
                 "NOIC": dict(row(80e9, 0.3, None), invested_capital=None)}      # lacks IC -> not a candidate / book facts None
    sys.modules["bank_enrich"] = book
    bank = FakeBank("fmp_bank")
    bank.on = bank_on
    bank.calls = []
    sys.modules["fmp_bank"] = bank
    sys.modules.pop("lambda_function", None)
    import lambda_function as lf
    lf._book, lf._bank = book, bank
    lf.FMP_SLEEP_SEC = 0
    lf.STATIC_TOP50_SPX = [("AAPL", "Technology"), ("MSFT", "Technology"), ("LIVE", "Healthcare")]
    lf.live = []
    lf.http_json = lambda url, retries=4: lf.live.append(url) or None          # live FMP / FRED answer nothing (429 today)
    return lf, s3, book, bank


def test_book_facts_bank_history_market_pass_and_labels():
    lf, s3, book, bank = boot()
    r = lf.lambda_handler({}, None)
    assert json.loads(r["body"])["ok"] is True
    out = s3.puts["data/eva-spread.json"]
    assert out["version"] == "1.2.0" and out["universe"].startswith("STATIC_TOP50_SPX") and out["book_as_of"]["tape"] == "2026-10-24"
    by = {x["ticker"]: x for x in out["all_tickers"]}
    a = by["AAPL"]
    assert a["ok"] and a["source"] == "factor-book (bank close)" and a["history_source"] == "fmp-bank statements"
    assert a["roic_ttm_pct"] == 55.0 and a["eva_momentum_pct"] is not None and a["bank_context"]["piotroski"] == 7
    # WACC from the book's inputs: Rf fallback 4.41 (FRED answered nothing), beta 1.2, tax 0.24, debt = EV - mcap
    assert 8.0 < a["wacc_pct"] < 11.5 and a["eva_spread_pct"] == round(55.0 - a["wacc_pct"], 3)
    assert by["MSFT"]["ok"] and by["MSFT"]["history_source"] == "unavailable"        # unbanked history -> live fallback answered nothing
    assert not by["LIVE"]["ok"] and by["LIVE"]["source"] == "unavailable"            # not in the book, live answers nothing
    assert out["source_counts"] == {"factor-book (bank close)": 2, "unavailable": 1} and out["history_source_counts"] == {"fmp-bank statements": 1, "unavailable": 1}
    assert not [u for u in lf.live if "AAPL" in u], "a banked name must cost no live FMP call: %s" % lf.live
    m = out["market"]
    assert m["available"] and m["n_candidates"] == 4 and m["n_valid"] == 4, m            # AAPL MSFT MKT1 LOSER (SMALL/ADRX/NOIC excluded)
    assert m["n_value_creators"] == 3 and m["pct_value_creators"] == 75.0 and m["regime"] == "CREATING_VALUE_BROAD"
    assert m["top_25_eva_spread"][0]["ticker"] == "AAPL" and m["bottom_10_destroyers"][0]["ticker"] == "LOSER"
    mk = {x["ticker"]: x for x in m["top_25_eva_spread"]}
    assert mk["MKT1"]["eva_momentum_pct"] is not None and mk["MKT1"]["history_source"] == "fmp-bank statements"     # momentum for the top names, bank-only
    assert mk["MSFT"]["eva_momentum_pct"] is None and mk["MSFT"]["history_source"] == "unbanked (bank-only pass)"
    assert "Industrials" in m["sector_breakdown"] and m["universe_basis"].startswith("factor book: US common stocks >= $2B")
    assert all("api" not in u or "financialmodelingprep" not in u or "MKT1" not in u for u in lf.live), "the market pass must not go live"
    assert bank.calls and all(c[2] == "annual" and c[3] == 3 for c in bank.calls)


def test_without_the_banks_the_live_path_runs_unchanged():
    lf, s3, book, bank = boot(book_on=False, bank_on=False)

    def http_json(url, retries=4):
        lf.live.append(url)
        if "AAPL" not in url:
            return None
        if "/quote?" in url:
            return [{"marketCap": 3.8e12, "price": 250.0}]
        if "/profile?" in url:
            return [{"beta": 1.2}]
        if "/key-metrics-ttm?" in url:
            return [{"returnOnInvestedCapitalTTM": 0.5, "investedCapitalTTM": 150e9, "enterpriseValueTTM": 3.85e12}]
        if "/ratios-ttm?" in url:
            return [{"effectiveTaxRateTTM": 0.24, "revenuePerShareTTM": 25.0}]
        if "/income-statement?" in url:
            return [{"date": "2025-09-27", "operatingIncome": 130e9, "effectiveTaxRate": 0.24, "revenue": 416e9}, {"date": "2024-09-28", "operatingIncome": 123e9, "effectiveTaxRate": 0.24, "revenue": 391e9}]
        if "/key-metrics?" in url:
            return [{"date": "2025-09-27", "investedCapital": 160e9}, {"date": "2024-09-28", "investedCapital": 150e9}]
        return None
    lf.http_json = http_json
    lf.lambda_handler({}, None)
    out = s3.puts["data/eva-spread.json"]
    by = {x["ticker"]: x for x in out["all_tickers"]}
    assert by["AAPL"]["ok"] and by["AAPL"]["source"] == "fmp live" and by["AAPL"]["history_source"] == "fmp live" and by["AAPL"]["bank_context"] is None
    assert by["AAPL"]["roic_ttm_pct"] == 50.0 and by["AAPL"]["eva_momentum_pct"] is not None
    assert not by["MSFT"]["ok"] and by["MSFT"]["reason"] == "missing core fields (roic/ic/mcap)"
    assert out["market"] == {"available": False, "reason": "factor book unavailable"} and out["book_as_of"] is None
    assert out["universe_basis"].endswith("(factor book unavailable)")


def premium_doc(pct=4.41, as_of="2026-11-01"):
    return {"us": {"premium_pct": pct, "premium": pct / 100.0, "as_of": as_of, "measure": "sustainable_payout"}}


def test_the_premium_is_read_from_the_warehouse_and_only_a_fresh_one_is_used():
    import datetime as dt
    lf, s3, book, bank = boot()
    day = dt.date(2026, 11, 28)
    assert lf.premium_from_bank(s3, day) is None, "no document in the warehouse: nothing is made up"
    s3.gets[lf.ERP_KEY] = premium_doc()
    got = lf.premium_from_bank(s3, day)
    assert got == {"pct": 4.41, "as_of": "2026-11-01", "measure": "sustainable_payout", "age_days": 27, "fixed": False}, got
    s3.gets[lf.ERP_KEY] = premium_doc(as_of="2026-08-01")
    assert lf.premium_from_bank(s3, day) is None, "119 days old: not used"
    s3.gets[lf.ERP_KEY] = premium_doc(as_of="2026-09-15")
    assert lf.premium_from_bank(s3, day)["age_days"] == 74
    for bad in (0.0441, 44.1, None, "4.41", True):                # a fraction, a wrong scale, nothing, a word, a flag
        s3.gets[lf.ERP_KEY] = premium_doc()
        s3.gets[lf.ERP_KEY]["us"]["premium_pct"] = bad
        assert lf.premium_from_bank(s3, day) is None, bad
    s3.gets[lf.ERP_KEY] = premium_doc(as_of="2026-12-05")
    assert lf.premium_from_bank(s3, day) is None, "a figure dated after today is not believed"
    s3.gets[lf.ERP_KEY] = {"us": "not a block"}
    assert lf.premium_from_bank(s3, day) is None


def test_cost_of_equity_is_built_on_the_premium_of_the_run():
    lf, s3, book, bank = boot()
    lf.set_premium(None)
    w0, c0 = lf.compute_wacc(100.0, 0.0, 1.2, 0.21, 4.5, 1.5)
    assert c0["erp_pct"] == 5.5 and c0["cost_of_equity_pct"] == round(4.5 + 1.2 * 5.5, 3) and w0 == c0["cost_of_equity_pct"]
    lf.set_premium({"pct": 4.41, "as_of": "2026-11-01", "fixed": False, "measure": "sustainable_payout", "age_days": 27})
    w1, c1 = lf.compute_wacc(100.0, 0.0, 1.2, 0.21, 4.5, 1.5)
    assert c1["erp_pct"] == 4.41 and c1["cost_of_equity_pct"] == round(4.5 + 1.2 * 4.41, 3)
    assert round(w0 - w1, 3) == round(1.2 * (5.5 - 4.41), 3), "all equity: the cost of capital falls by beta times the difference"
    w2, c2 = lf.compute_wacc(60.0, 40.0, 1.0, 0.25, 4.5, 1.5)
    assert w2 == round(0.6 * (4.5 + 4.41) + 0.4 * (4.5 + 1.5) * 0.75, 3), "debt is priced as before; only the equity leg moves"
    assert lf.compute_wacc(100.0, 0.0, 1.0, 0.21, 4.5, 1.5, erp_pct=6.0)[1]["erp_pct"] == 6.0, "a premium passed in is obeyed"
    lf.set_premium(None)


def test_the_output_says_which_premium_was_used():
    lf, s3, book, bank = boot()
    s3.gets[lf.ERP_KEY] = premium_doc(as_of=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-01"))
    lf.lambda_handler({}, None)
    w = s3.puts["data/eva-spread.json"]["wacc_inputs"]
    assert w["equity_risk_premium_pct"] == 4.41 and w["erp_fixed"] is False and w["erp_as_of"].endswith("-01") and w["erp_fixed_until_v1_2_pct"] == 5.5
    assert w["source_erp"].startswith("Damodaran implied ERP at the start of ") and "data/equity-risk-premium.json" in w["source_erp"]
    assert "ERP=4.41%" in s3.puts["data/eva-spread.json"]["methodology"]["wacc"]
    a = {x["ticker"]: x for x in s3.puts["data/eva-spread.json"]["all_tickers"]}["AAPL"]
    lf2, s32, _b, _k = boot()                                       # the same run with nothing in the warehouse
    lf2.lambda_handler({}, None)
    w2 = s32.puts["data/eva-spread.json"]["wacc_inputs"]
    assert w2["equity_risk_premium_pct"] == 5.5 and w2["erp_fixed"] is True and w2["erp_as_of"] is None
    assert w2["source_erp"].startswith("fixed 5.50% (the warehouse held no implied premium newer than 75 days)")
    a2 = {x["ticker"]: x for x in s32.puts["data/eva-spread.json"]["all_tickers"]}["AAPL"]
    assert a["wacc_pct"] < a2["wacc_pct"] and a["eva_spread_pct"] > a2["eva_spread_pct"], "a lower premium: a lower hurdle and a wider spread"
    assert s32.puts["data/eva-spread.json"]["data_sources"]["equity_risk_premium"].startswith("data/equity-risk-premium.json")


if __name__ == "__main__":
    tests = [test_book_facts_bank_history_market_pass_and_labels, test_without_the_banks_the_live_path_runs_unchanged,
             test_the_premium_is_read_from_the_warehouse_and_only_a_fresh_one_is_used, test_cost_of_equity_is_built_on_the_premium_of_the_run,
             test_the_output_says_which_premium_was_used]
    for t in tests:
        t()
        print("ok %s" % t.__name__)
    print("%d tests passed" % len(tests))
