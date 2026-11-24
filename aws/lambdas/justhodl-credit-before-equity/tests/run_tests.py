"""justhodl-credit-before-equity offline tests.

An in-memory S3 holds a cut of the REAL warehouse, taken by ops 6942 from the very bytes one build of this engine read on
the runner (tests/fixtures/credit-before-equity-warehouse.json.gz: the bonds of 45 listed companies and two dozen issuers
with no listed stock, the CDS desk's rows for them, the structural model, the Treasury par curve, the Polygon EOD bank's
bars and split rows, and the factor-book rows of 224 listed companies). The same cut carries what that build published
from it; the engine must reproduce it here, figure for figure.

Checked besides: the curve and the duration-matched maturity, the windows, the rule (every branch), how issuer names are
tied to listed companies and the ties that must be refused, the publication guards, the ledger, the handler, and that the
engine asks no provider for anything. No network, no AWS.

Run: python3 aws/lambdas/justhodl-credit-before-equity/tests/run_tests.py
"""
from datetime import datetime, timezone
from types import SimpleNamespace
import copy
import gzip
import hashlib
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
sys.path[:0] = [SRC, os.path.join(ROOT, "aws", "shared")]
if "boto3" not in sys.modules:
    try:
        import boto3  # noqa: F401
        import botocore.config  # noqa: F401
    except ImportError:
        sys.modules["boto3"] = SimpleNamespace(client=lambda *a, **k: None)
        sys.modules["botocore"] = SimpleNamespace(config=SimpleNamespace(Config=lambda **k: None))
        sys.modules["botocore.config"] = sys.modules["botocore"].config

import bank_enrich  # noqa: E402
import cbe_core as C  # noqa: E402
import cbe_names as N  # noqa: E402
import lambda_function as L  # noqa: E402

FIX = json.loads(gzip.decompress(open(os.path.join(ROOT, "tests", "fixtures", "credit-before-equity-warehouse.json.gz"), "rb").read()))
NOW = datetime.fromisoformat(FIX["now"])


class Body:
    def __init__(self, raw):
        self.raw = raw

    def read(self):
        return self.raw


class MemS3:
    def __init__(self, docs=None):
        self.docs = copy.deepcopy(docs if docs is not None else FIX["docs"])
        self.puts, self.gets = {}, []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key in self.puts:
            return {"Body": Body(self.puts[Key])}
        if Key not in self.docs:
            raise KeyError("NoSuchKey " + Key)
        raw = json.dumps(self.docs[Key], separators=(",", ":")).encode()
        return {"Body": Body(gzip.compress(raw, 1) if Key.endswith(".gz") else raw)}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = Body

    def list_objects_v2(self, **kw):
        keys = set(self.docs) | set(self.puts)
        return {"Contents": [{"Key": k} for k in sorted(keys) if k.startswith(kw.get("Prefix") or "")]}


def with_cut(fn):
    """Run fn with the factor book and the publication guards of the cut."""
    keep = bank_enrich.rows, (L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED)
    try:
        bank_enrich.rows = lambda fields=None, where=None, limit=None: [dict(x) for x in FIX["book"]]
        L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = FIX["guards"]
        return fn()
    finally:
        bank_enrich.rows = keep[0]
        L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = keep[1]


def build(docs=None, now=NOW):
    s3 = MemS3(docs)
    return with_cut(lambda: L.build(s3, now, log=lambda *_: None)), s3


def plain(x):
    return json.loads(json.dumps(x, separators=(",", ":"), default=str))


MATCHER = N.Matcher([dict(x) for x in FIX["book"]])
TESTS = []


def test(fn):
    TESTS.append(fn)
    return fn


# ------------------------------------------------------------------------------------------------------ the real cut
@test
def reproduces_what_the_runner_published_from_the_same_cut():
    built, _ = build()
    got, want = plain(built["payload"]), FIX["expected"]
    assert set(got) == set(want), sorted(set(got) ^ set(want))
    for key in want:
        assert got[key] == want[key], "the engine's %s differs from what the runner published from the same cut" % key
    assert plain(built["snapshots"]) == FIX["snapshots"]


@test
def keeps_every_key_the_first_engine_published():
    p = build()[0]["payload"]
    for key in ("version", "generated_at", "n_names", "n_leads", "n_awaiting_history", "names", "leads", "degraded", "gaps", "thresholds", "method", "attribution"):
        assert key in p, key
    model = [x for x in p["names"] if x.get("distance_to_default") is not None]
    assert len(model) >= 18, len(model)
    for x in model:
        for key in ("ticker", "name", "group", "distance_to_default", "synthetic_cds_bp", "default_prob_5y_pct", "regime", "peer_rank", "market_cap_usd_bn", "hist_n", "signal", "credit_direction", "d_price_pct"):
            assert key in x, (x["ticker"], key)
    assert p["thresholds"]["min_dd_move"] == 0.05 and p["thresholds"]["equity_flat_band_pct"] == 3.0


@test
def the_counts_are_the_rows():
    p = build()[0]["payload"]
    names = p["names"]
    leads = [x for x in names if x["signal"] in C.LEADS]
    assert p["n_names"] == len(names) and len({x["ticker"] for x in names}) == len(names)
    assert p["n_leads"] == len(leads) > 0 and p["leads"] == leads[:20]
    assert p["n_awaiting_history"] == sum(1 for x in names if x["signal"] == "INSUFFICIENT_HISTORY")
    cols = p["issuers"]["cols"]
    assert all(len(r) == len(cols) for r in p["issuers"]["rows"])
    table = [dict(zip(cols, r)) for r in p["issuers"]["rows"]]
    assert p["coverage"]["companies"] == len(table) and p["coverage"]["listed"] == sum(1 for x in table if x["ticker"])
    assert len({x["id"] for x in table}) == len(table)


@test
def every_lead_is_credit_past_its_bar_with_a_stock_that_has_not_followed():
    for x in build()[0]["payload"]["names"]:
        if x["signal"] not in C.LEADS:
            continue
        down = x["signal"] == "CREDIT_LEADS_DOWN"
        assert (x["d_price_pct"] > -C.FLAT_BAND_PCT) if down else (x["d_price_pct"] < C.FLAT_BAND_PCT), x
        if x["signal_basis"] == "bond spreads":
            assert abs(x["x_spread_bp"]) >= x["bar_bp"] and (x["d_spread_bp"] >= C.OWN_MOVE_BP if down else x["d_spread_bp"] <= -C.OWN_MOVE_BP), x
            assert x["credit_direction"] == ("DETERIORATING" if down else "IMPROVING")


@test
def a_company_spread_is_its_bonds_over_the_curve_by_other_arithmetic():
    p = build()[0]["payload"]
    issues = FIX["docs"][L.BONDS_KEY]["issues"]
    curve = FIX["docs"][L.PAR_KEY]["rows"][issues["as_of"]]
    ten = sorted((C.TENOR_YEARS[k], v) for k, v in curve.items())

    def par(years):
        for (x0, y0), (x1, y1) in zip(ten, ten[1:]):
            if x0 <= years <= x1:
                return y0 + (y1 - y0) * (years - x0) / (x1 - x0)
        return ten[0][1] if years < ten[0][0] else ten[-1][1]
    row = {x[0]: dict(zip(p["issuers"]["cols"], x)) for x in p["issuers"]["rows"]}["T:AAPL"]
    num = den = n = 0
    for b in issues["bonds"]:
        if not str(b.get("issuer", "")).upper().startswith("APPLE INC") or b.get("ccy") != "USD" or b.get("book") not in C.BOOKS:
            continue
        y, d, v = b.get("rate"), b.get("dur"), b.get("demand") or 0
        if y is None or d is None or d < 1 or not 0 < y < 49.99:
            continue
        yy = y / 100.0
        t = max(30.0 if d * yy >= 0.97 else -math.log(1 - d * yy) / math.log(1 + yy), d)
        left = (datetime.fromisoformat(b["maturity"]) - datetime.fromisoformat(issues["as_of"])).days / 365.25
        t = min(t, max(left, d), 30.0)
        num += round((y - par(t)) * 100, 1) * v
        den += v
        n += 1
    assert n >= 30 and abs(num / den - row["spread_bp"]) < 0.06, (n, num / den, row["spread_bp"])


@test
def an_industry_is_the_middle_company_by_value_not_the_average():
    p = build()[0]["payload"]
    table = [dict(zip(p["issuers"]["cols"], r)) for r in p["issuers"]["rows"]]
    for ind in p["industries"]["all"]:
        have = sorted((x["d_m1"], x["value"]) for x in table if x["industry"] == ind["industry"] and x["d_m1"] is not None)
        if not have:
            assert "m1" not in ind["win"]
            continue
        total, run = sum(v for _, v in have), 0.0
        for d, v in have:
            run += v
            if run >= total / 2.0:
                break
        assert abs(d - ind["win"]["m1"]["d_bp"]) < 0.06, (ind["industry"], d, ind["win"]["m1"])
        assert ind["win"]["m1"]["n"] == len(have) and ind["n"] >= len(have)
    assert sum(i["n"] for i in p["industries"]["all"]) == len(table)
    for book in ("ig", "hy", "em"):
        assert sum(i["n"] for i in p["industries"][book]) == sum(1 for x in table if x["book"] == book)


# ----------------------------------------------------------------------------------------------------------- the core
@test
def the_curve_is_read_straight_between_its_maturities_and_flat_past_the_ends():
    row = {"1M": 4.0, "1Y": 4.5, "2Y": 5.0, "10Y": 6.0, "30Y": 7.0}
    assert C.par_yield(row, 0.01) == 4.0 and C.par_yield(row, 45) == 7.0
    assert abs(C.par_yield(row, 1.5) - 4.75) < 1e-9 and abs(C.par_yield(row, 6) - 5.5) < 1e-9
    assert C.par_yield({}, 5) is None
    rows = {"2026-11-20": row}
    assert C.par_on(rows, "2026-11-23") == (row, "2026-11-20") and C.par_on(rows, "2026-12-05") == (None, None) and C.par_on(rows, "2026-11-19") == (None, None)


@test
def the_matched_maturity_is_a_par_bonds_and_never_past_the_bonds_own():
    for dur, y in ((2.0, 5.0), (7.0, 6.0), (12.0, 5.5), (4.5, 9.0)):
        t = C.tenor_of(dur, y)
        yy = y / 100.0
        back = (1 - (1 + yy) ** -t) / yy                    # the modified duration of an annual-pay par bond of that maturity
        assert abs(back - dur) < 1e-6 and t > dur, (dur, y, t, back)
    assert C.tenor_of(4.0, 8.0, years_left=4.2) == 4.2 and C.tenor_of(4.0, 8.0, years_left=2.0) == 4.0
    assert C.tenor_of(25.0, 6.0) == 30.0


@test
def bonds_that_are_not_a_credit_reading_are_left_out_and_said():
    par = {"2026-11-20": {"1Y": 4.5, "5Y": 4.7, "10Y": 4.9}, "2026-10-30": {"1Y": 4.4, "5Y": 4.7, "10Y": 4.9}}
    base = {"issuer": "ACME CORP", "sector": "Energy", "country": "United States", "ccy": "USD", "book": "US HY", "maturity": "2031-11-20", "demand": 1e6}
    cases = [({"k": "a", "rate": 7.0, "dur": 4.0, "chg": 0.5}, None), ({"k": "b", "rate": 50.0, "dur": 4.0}, "yield at the file's limit"), ({"k": "c", "rate": -5.0, "dur": 4.0}, "yield at the file's limit"),
             ({"k": "d", "rate": -0.4, "dur": 2.0}, "priced to an early call"), ({"k": "e", "rate": 7.0, "dur": 0.6}, "under a year to run"), ({"k": "f", "dur": 4.0}, "no yield printed"),
             ({"k": "g", "rate": 7.0}, "under a year to run")]
    pts, counts, _ = C.bond_points([{**base, **b} for b, _ in cases], "2026-11-20", par, {"chg": "2026-10-30"})
    assert [p["why"] for p in pts] == [w for _, w in cases]
    assert pts[0]["s"] is not None and all(p["s"] is None for p in pts[1:]) and counts["with a spread"] == 1
    assert abs(pts[0]["s"] - (7.0 - C.par_yield(par["2026-11-20"], pts[0]["t"])) * 100) < 0.06
    was = (6.5 - C.par_yield(par["2026-10-30"], pts[0]["t"] + 21 / 365.25)) * 100
    assert abs(pts[0]["ref"]["2026-10-30"] - was) < 0.06
    out = C.bond_points([{**base, "k": "x", "rate": 7.0, "dur": 4.0, "ccy": "EUR"}, {**base, "k": "y", "rate": 7.0, "dur": 4.0, "book": "Convertible"},
                         {**base, "k": "z", "rate": 7.0, "dur": 4.0, "sector": "Local Authority"}, {**base, "k": "w", "rate": 7.0, "dur": 4.0, "left": 1}], "2026-11-20", par, {})
    assert out[0] == [] and out[1]["not in US dollars"] == 1 and out[1]["floating-rate, convertible or another book"] == 1 and out[1]["not a company (government, agency, local authority)"] == 1


@test
def a_window_is_the_stored_day_nearest_its_aim_or_none():
    days = ["2025-12-31", "2026-08-21", "2026-10-30", "2026-11-13", "2026-11-16", "2026-11-19", "2026-11-20"]
    w = C.pick_windows("2026-11-23", days)
    assert w == {"w1": "2026-11-16", "m1": "2026-10-30", "m3": "2026-08-21", "ytd": "2025-12-31"}, w
    assert C.pick_windows("2026-11-23", ["2026-11-20", "2026-11-23"]) == {"w1": None, "m1": None, "m3": None, "ytd": None}
    assert C.pick_windows("2026-01-05", ["2025-12-30", "2025-12-31"])["ytd"] == "2025-12-31"


@test
def the_rule_holds_on_every_side_of_its_bars():
    bar_floor = C.MOVE_FLOOR_BP
    assert C.credit_direction(None) == (None, None) and C.credit_direction({"d_bp": 30.0, "from_bp": 100.0}) == (None, None)
    assert C.credit_direction({"d_bp": 30.0, "x_bp": bar_floor, "from_bp": 60.0})[0] == "DETERIORATING"
    assert C.credit_direction({"d_bp": 30.0, "x_bp": bar_floor - 0.1, "from_bp": 60.0})[0] == "FLAT"
    assert C.credit_direction({"d_bp": 60.0, "x_bp": 49.9, "from_bp": 500.0}) == ("FLAT", 50.0)          # a tenth of where it started
    assert C.credit_direction({"d_bp": 60.0, "x_bp": 50.0, "from_bp": 500.0})[0] == "DETERIORATING"
    assert C.credit_direction({"d_bp": -30.0, "x_bp": -40.0, "from_bp": 200.0})[0] == "IMPROVING"
    assert C.credit_direction({"d_bp": 3.0, "x_bp": -17.0, "from_bp": 150.0})[0] == "FLAT"                # widened less than its book: held up, not improved
    assert C.credit_direction({"d_bp": -3.0, "x_bp": 17.0, "from_bp": 150.0})[0] == "FLAT"
    assert C.credit_direction({"d_bp": 4.9, "x_bp": 30.0, "from_bp": 150.0})[0] == "FLAT" and C.credit_direction({"d_bp": 5.0, "x_bp": 30.0, "from_bp": 150.0})[0] == "DETERIORATING"
    band = C.FLAT_BAND_PCT
    table = {("DETERIORATING", -band): ("NONE", "confirmed"), ("DETERIORATING", -band + 0.01): ("CREDIT_LEADS_DOWN", "flat"), ("DETERIORATING", 0.0): ("CREDIT_LEADS_DOWN", "flat"),
             ("DETERIORATING", band): ("CREDIT_LEADS_DOWN", "opposite"), ("IMPROVING", band): ("NONE", "confirmed"), ("IMPROVING", band - 0.01): ("CREDIT_LEADS_UP", "flat"),
             ("IMPROVING", -band): ("CREDIT_LEADS_UP", "opposite"), ("FLAT", 1.0): ("NONE", "flat"), ("FLAT", 9.0): ("NONE", "moved"),
             ("DETERIORATING", None): ("NONE", None), (None, 2.0): ("INSUFFICIENT_HISTORY", None), (None, None): ("INSUFFICIENT_HISTORY", None)}
    for (direction, px), want in table.items():
        assert C.signal_of(direction, px) == want, (direction, px, C.signal_of(direction, px), want)


@test
def the_middle_by_weight_is_not_moved_by_one_distressed_name():
    assert C.weighted_median([(5, 10), (7, 10), (2000, 1)]) == 7 and C.weighted_median([(5, 10), (7, 10), (9, 10)]) == 7
    assert C.weighted_median([(1, 0), (3, 0), (8, 0)]) == 3 and C.weighted_median([]) is None and C.weighted_median([(4, 1)]) == 4
    assert abs(C.wavg([(100, 3), (200, 1)]) - 125) < 1e-9 and C.wavg([]) is None and C.wavg([(10, 0), (20, 0)]) == 15


@test
def a_cds_past_the_believable_is_shown_in_points_and_not_ranked():
    desk = {"groups": {"us_corp": {"rows": [
        {"name": "Apple Inc.", "spread_bp": 40.0, "chg_1m_bp": 12.0, "liquid": True, "trades_30d": 30, "last_date": "2026-11-20"},
        {"name": "Ford Motor Company", "spread_bp": 150.0, "chg_1m_bp": 12.0, "liquid": True, "trades_30d": 30},
        {"name": "Whirlpool Corporation", "spread_bp": 900.0, "chg_1m_bp": -200.0, "liquid": False, "trades_30d": 3},
        {"name": "Nowhere Holdings LLC", "spread_bp": 36102.7, "chg_1m_bp": 13549.4, "chg_1d_bp": 26677.1, "liquid": True, "trades_30d": 41, "points_upfront": 58.76}],
        "unpriced": [{"name": "Xerox Corporation", "trades_30d": 73}], "dormant": [{"name": "Kohl's Corporation"}]}}}
    priced, bare, by_ticker, by_key = C.cds_rows(desk, MATCHER)
    by_name = {c["name"]: c for c in priced}
    assert by_name["Apple Inc."]["ticker"] == "AAPL" and C.cds_direction(by_name["Apple Inc."]) == "WIDER"          # 12 bp on 28: past 10 bp and a tenth
    assert C.cds_direction(by_name["Ford Motor Company"]) == "FLAT"                                                  # 12 bp on 138: not a tenth
    assert C.cds_direction(by_name["Whirlpool Corporation"]) is None                                                # not liquid: not read
    odd = by_name["Nowhere Holdings LLC"]
    assert odd["believable"] is False and odd["d1m"] is None and odd["d1d"] is None and odd["points_upfront"] == 58.76 and C.cds_direction(odd) is None
    assert [b["status"] for b in bare] == ["trades, no level", "dormant"] and "AAPL" in by_ticker and "NOWHERE HOLDINGS" in by_key


@test
def a_stock_change_runs_from_the_reference_close_to_the_newest():
    closes = {"2026-10-29": 100.0, "2026-10-30": 102.0, "2026-11-02": 99.0, "2026-11-23": 107.1}
    assert C.price_change(closes, "2026-10-30") == (5.0, "2026-10-30", "2026-11-23")
    assert C.price_change(closes, "2026-11-01") == (5.0, "2026-10-30", "2026-11-23")          # a weekend: the close before it
    assert C.price_change(closes, "2026-10-20")[0] is None and C.price_change({}, "2026-10-30") == (None, None, None)
    assert C.price_change({"2026-09-01": 50.0, "2026-11-23": 60.0}, "2026-10-30")[0] is None    # no close within a week of the reference


# ---------------------------------------------------------------------------------------------------------- the names
@test
def names_are_tied_by_what_they_are_and_never_by_what_they_resemble():
    m = N.Matcher([
        {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Technology", "country": "US"}, {"symbol": "LDOS", "name": "Leidos Holdings, Inc.", "sector": "Technology", "country": "US"},
        {"symbol": "COMP", "name": "Compass, Inc.", "sector": "Real Estate", "country": "US"}, {"symbol": "MCB", "name": "Metropolitan Bank Holding Corp.", "sector": "Financial Services", "country": "US"},
        {"symbol": "TOL", "name": "Toll Brothers, Inc.", "sector": "Consumer Cyclical", "country": "US"}, {"symbol": "MUR", "name": "Murphy Oil Corporation", "sector": "Energy", "country": "US"},
        {"symbol": "MUSA", "name": "Murphy USA Inc.", "sector": "Consumer Cyclical", "country": "US"}, {"symbol": "NBHC", "name": "National Bank Holdings Corporation", "sector": "Financial Services", "country": "US"},
        {"symbol": "CHTR", "name": "Charter Communications, Inc.", "sector": "Communication Services", "country": "US"}, {"symbol": "RIO", "name": "Rio Tinto Group", "sector": "Basic Materials", "country": "GB", "is_adr": True},
        {"symbol": "FSTB", "name": "First Star Bancorp", "sector": "Financial Services", "country": "US"}, {"symbol": "FSTE", "name": "First Star Energy Inc", "sector": "Energy", "country": "US"},
        {"symbol": "GOOGL", "name": "Alphabet Inc. Class A Common Stock", "sector": "Communication Services", "country": "US", "dollar_vol_20d": 9e9},
        {"symbol": "GOOG", "name": "Alphabet Inc. Class C Capital Stock", "sector": "Communication Services", "country": "US", "dollar_vol_20d": 5e9},
    ])

    def tie(raw, sector=None, foreign=False):
        key, cut = N.issuer_key(raw)
        co, how = m.match(key, cut, sector, foreign=foreign)
        return (co["symbol"] if co else None), how
    assert tie("APPLE INC")[0] == "AAPL" and tie("APPLE INC             144A")[0] == "AAPL" and tie("APPLE INC (FXD-FRN) MTN")[0] == "AAPL"
    assert tie("FIRST STAR BANCORP") == ("FSTB", "exact") and tie("FIRST STAR BANCORP          144A") == ("FSTB", "exact") and tie("FIRST STAR BANCORP MTN RegS") == ("FSTB", "exact")
    assert tie("LEIDOS INC") == ("LDOS", "core")
    assert tie("COMPASS GROUP PLC")[0] is None                                   # Compass Group is not Compass
    assert tie("TOLL BROTHERS FINANCE CORP") == ("TOL", "subsidiary")
    assert tie("CCO HOLDINGS LLC") == ("CHTR", "alias") and tie("MURPHY OIL USA INC") == ("MUSA", "alias") and tie("MURPHY OIL CORP")[0] == "MUR"
    assert tie("NATIONAL BANK OF CANADA MTN")[0] is None and tie("METROPOLITAN BANK AND TRUST COMPAN MTN RegS", "Financial Institutions", True)[0] is None
    assert tie("RIO TINTO FINANCE USA LTD", "Basic Industry", True) == ("RIO", "alias")
    assert tie("FIRST STAR ENERGY FUNDING LLC", "Banking")[0] is None            # a bank-like issuer and an energy company
    assert tie("FIRST STAR BANCORP FINANCE LLC", "Banking")[0] == "FSTB"
    assert tie("ALPHABET INC") == ("GOOGL", "alias") and tie("BURLINGTON NORTHERN SANTA FE LLC")[0] is None and tie("")[0] is None
    assert tie("PACIFICORP")[1].startswith("reviewed")
    for key, ticker in N.ALIASES.items():
        assert key == N.norm(key) and (ticker is None or ticker == ticker.upper()), key


@test
def a_foreign_issuer_is_not_tied_to_an_american_namesake_by_a_rule():
    m = N.Matcher([{"symbol": "ACME", "name": "Acme Widgets Holdings, Inc.", "sector": "Industrials", "country": "US"},
                   {"symbol": "ACMY", "name": "Acme Global Holdings plc", "sector": "Industrials", "country": "GB", "is_adr": True}])
    assert m.match("ACME WIDGETS", False, "Capital Goods", foreign=False)[1] == "core"
    co, why = m.match("ACME WIDGETS", False, "Capital Goods", foreign=True)
    assert co is None and "foreign" in why
    assert m.match("ACME GLOBAL", False, "Capital Goods", foreign=True)[1] == "core"            # a depositary receipt is a foreign company


@test
def an_industry_is_the_fund_sector_and_a_coarse_one_borrows_the_company():
    assert C.industry_of("Banking") == "Banking" and C.industry_of("Reits") == "REITs & real estate" and C.industry_of("Owned No Guarantee") == "State-owned"
    assert C.industry_of("Local Authority") is None and C.industry_of("Government Sponsored") is None
    assert C.industry_of("Industrial", {"sector": "Energy", "industry": "Oil & Gas E&P"}) == "Energy"
    assert C.industry_of("Industrial", {"sector": "Industrials", "industry": "Airlines"}) == "Transportation"
    assert C.industry_of("Financial Institutions", {"sector": "Financial Services", "industry": "Banks - Regional"}) == "Banking"
    assert C.industry_of("Financial Institutions", None, "GOLOMT BANK") == "Banking" and C.industry_of("Financial Institutions", None, "ACME LEASING") == "Other financial"
    assert C.industry_of("Industrial") == "Other industrial" and C.industry_of("Utility") == "Other utilities"


# -------------------------------------------------------------------------------------------------- guards and ledger
@test
def a_broken_bond_desk_publishes_nothing():
    docs = copy.deepcopy(FIX["docs"])
    docs[L.BONDS_KEY]["issues"]["bonds"] = docs[L.BONDS_KEY]["issues"]["bonds"][:40]
    for mutate, word in ((lambda d: d, "bonds have a spread"), (lambda d: d[L.BONDS_KEY]["issues"].update(bonds=[]), "holds no bonds"), (lambda d: d[L.PAR_KEY].update(rows={}), "no Treasury par curve")):
        mine = copy.deepcopy(docs if word == "bonds have a spread" else FIX["docs"])
        mutate(mine)
        try:
            build(mine)
        except RuntimeError as e:
            assert "refusing to publish" in str(e) and word in str(e), str(e)
        else:
            raise AssertionError("published from a broken bond desk: %s" % word)
    keep = L.MIN_LISTED
    try:
        s3 = MemS3()
        bank_enrich_rows = bank_enrich.rows
        bank_enrich.rows = lambda fields=None, where=None, limit=None: [dict(x) for x in FIX["book"]]
        L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = 100, 20, 5000
        try:
            L.build(s3, NOW, log=lambda *_: None)
        except RuntimeError as e:
            assert "refusing to publish" in str(e) and "listed" in str(e)
        else:
            raise AssertionError("published with too few listed companies")
    finally:
        bank_enrich.rows = bank_enrich_rows
        L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = 5000, 1000, keep


@test
def a_late_desk_or_a_missing_one_is_said_not_hidden():
    docs = copy.deepcopy(FIX["docs"])
    del docs[L.CDS_KEY]
    del docs[L.MODEL_KEY]
    late = datetime(2026, 12, 9, 22, 35, tzinfo=timezone.utc)
    p = build(docs, late)[0]["payload"]
    text = " ".join(p["degraded"])
    assert "CDS desk's document could not be read" in text and "structural model's document could not be read" in text and "holdings date is" in text, p["degraded"]
    assert p["cds"]["priced"] == [] and all(x.get("cds_bp") is None and x.get("distance_to_default") is None for x in p["names"])
    assert p["n_leads"] == len([x for x in p["names"] if x["signal"] in C.LEADS])


@test
def the_ledger_keeps_a_real_day_over_a_worked_back_one():
    built, s3 = build()
    days = [(s["as_of"], bool(s["derived_from"])) for s in built["snapshots"]]
    issues = FIX["docs"][L.BONDS_KEY]["issues"]
    assert days[0] == (issues["as_of"], False) and set(days[1:]) == {(issues["move_as_of"], True), (issues["ytd_as_of"], True)}
    docs = copy.deepcopy(FIX["docs"])
    real = {"as_of": issues["move_as_of"], "derived_from": None, "n": 1, "bonds": {"zz": 123.4}}
    docs[L.LEDGER_ROOT + issues["move_as_of"] + ".json.gz"] = real
    again, _ = build(docs)
    assert [s["as_of"] for s in again["snapshots"] if s["derived_from"]] == [issues["ytd_as_of"]]          # the stored day is not overwritten
    assert again["payload"]["windows"]["m1"]["source"] == "the board's own ledger" and again["payload"]["windows"]["m1"]["n_bonds"] == 1
    assert again["payload"]["windows"]["w1"]["ref"] is None and "ledger" in again["payload"]["windows"]["w1"]["why"]


@test
def a_week_appears_once_the_ledger_holds_one():
    issues = FIX["docs"][L.BONDS_KEY]["issues"]
    first = build()[0]
    docs = copy.deepcopy(FIX["docs"])
    week = {"as_of": "2026-11-16", "derived_from": None, "n": 0, "bonds": {k: v - 7.0 for k, v in first["snapshots"][0]["bonds"].items()}}
    docs[L.LEDGER_ROOT + "2026-11-16.json.gz"] = week
    p = build(docs)[0]["payload"]
    assert issues["as_of"] == "2026-11-23" and p["windows"]["w1"]["ref"] == "2026-11-16" and p["windows"]["w1"]["days"] == 7
    table = [dict(zip(p["issuers"]["cols"], r)) for r in p["issuers"]["rows"]]
    seen = [x["d_w1"] for x in table if x["d_w1"] is not None]
    assert len(seen) > 40 and all(abs(d - 7.0) < 0.06 for d in seen), seen[:5]
    assert all(abs((i["win"]["w1"]["d_bp"]) - 7.0) < 0.06 for i in p["industries"]["all"] if "w1" in i["win"])


@test
def the_handler_writes_the_document_the_ledger_and_the_model_ledger_and_nothing_else():
    s3 = MemS3()
    keep = L._s3
    try:
        L._s3 = lambda: s3
        out = with_cut(lambda: L.lambda_handler({}, None))
    finally:
        L._s3 = keep
    issues = FIX["docs"][L.BONDS_KEY]["issues"]
    want = {L.OUT_KEY, L.HIST_KEY} | {L.LEDGER_ROOT + d + ".json.gz" for d in (issues["as_of"], issues["move_as_of"], issues["ytd_as_of"])}
    assert set(s3.puts) == want, sorted(set(s3.puts) ^ want)
    doc = json.loads(s3.puts[L.OUT_KEY])
    assert doc["contract"] == "credit-before-equity.v2" and doc["version"] == "2.0.0" and out["statusCode"] == 200
    snap = json.loads(gzip.decompress(s3.puts[L.LEDGER_ROOT + issues["as_of"] + ".json.gz"]))
    assert snap["derived_from"] is None and snap["n"] == len(snap["bonds"]) > 500
    hist = json.loads(s3.puts[L.HIST_KEY])
    assert all(len(v) <= L.HIST_KEEP for v in hist["obs"].values()) and "updated_at" in hist
    assert json.loads(json.dumps(doc)) == doc and "NaN" not in json.dumps(doc["thresholds"])


@test
def the_engine_asks_no_provider_for_anything():
    for name in ("lambda_function.py", "cbe_core.py", "cbe_names.py"):
        text = open(os.path.join(SRC, name), encoding="utf-8").read()
        for word in ("urllib", "requests", "http.client", "financialmodelingprep", "polygon.io", "api_key", "apikey", "ssm"):
            assert word not in text.lower().replace("polygon eod bank", ""), (name, word)
    _, s3 = build()
    roots = (L.BONDS_KEY, L.CDS_KEY, L.MODEL_KEY, L.PAR_KEY, L.HIST_KEY, L.EOD_ROOT, L.SPLITS_ROOT, L.LEDGER_ROOT)
    assert all(k.startswith(roots) for k in s3.gets), [k for k in s3.gets if not k.startswith(roots)][:5]


@test
def the_tape_module_is_the_leaders_engines_own():
    mine = open(os.path.join(SRC, "leaders_tape.py"), "rb").read()
    theirs = open(os.path.join(ROOT, "aws", "lambdas", "justhodl-leaders", "source", "leaders_tape.py"), "rb").read()
    assert hashlib.sha256(mine).hexdigest() == hashlib.sha256(theirs).hexdigest()


@test
def a_class_share_ticker_is_read_from_the_price_bank_under_its_dotted_name():
    assert L.bank_names("BRK-B") == ["BRK-B", "BRK.B"] and L.bank_names("AAPL") == ["AAPL"]
    p = build()[0]["payload"]
    brk = next(x for x in p["names"] if x["ticker"] == "BRK-B")
    assert brk["price"] > 100 and brk["d_price_pct"] is not None and brk["spread_bp"] is not None, brk


def main():
    failed = 0
    for fn in TESTS:
        try:
            fn()
            print("ok   %s" % fn.__name__)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print("FAIL %s: %s: %s" % (fn.__name__, type(e).__name__, str(e)[:600]))
    print("%d tests, %d failed" % (len(TESTS), failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
