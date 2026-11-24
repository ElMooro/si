"""justhodl-credit-before-equity offline tests.

An in-memory S3 holds a cut of the REAL warehouse, taken by ops 6942 from the very bytes one build of this engine read on
the runner (tests/fixtures/credit-before-equity-warehouse.json.gz: the bonds of 45 listed companies and two dozen issuers
with no listed stock, the CDS desk's rows for them, the structural model, the Treasury par curve, the Polygon EOD bank's
bars and split rows, and the factor-book rows of 224 listed companies). The same cut carries what that build published
from it; the engine must reproduce it here, figure for figure.

Checked besides: the curve and the duration-matched maturity, the windows, the rule (every branch), how issuer names are
tied to listed companies and the ties that must be refused, the publication guards, the ledger, the handler, and that the
engine asks no provider for anything. Since 2.1: the stress rank (worked again here from the published table), the AI
stack theme and its layers, and that no lead is called when the bonds and a liquid CDS disagree. No network, no AWS.

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
import cbe_themes as TH  # noqa: E402
import lambda_function as L  # noqa: E402

FIX = json.loads(gzip.decompress(open(os.path.join(ROOT, "tests", "fixtures", "credit-before-equity-warehouse.json.gz"), "rb").read()))
NOW = datetime.fromisoformat(FIX["now"])
STORED = {k[len(L.LEDGER_ROOT):-len(".json.gz")] for k in FIX["docs"] if k.startswith(L.LEDGER_ROOT)}        # the ledger days the cut carries, real and worked back
BARE = {k: v for k, v in FIX["docs"].items() if not k.startswith(L.LEDGER_ROOT)}                             # the same warehouse before the board had a ledger


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
    issues = FIX["docs"][L.BONDS_KEY]["issues"]
    fresh, _ = build(BARE)                                              # no ledger yet: today's day, and the two reference days worked back
    days = [(s["as_of"], bool(s["derived_from"])) for s in fresh["snapshots"]]
    assert days[0] == (issues["as_of"], False) and set(days[1:]) == {(issues["move_as_of"], True), (issues["ytd_as_of"], True)}
    built, s3 = build()                                                 # the cut's ledger: a day it already holds is never written again
    assert {s["as_of"] for s in built["snapshots"]} == {issues["as_of"], issues["move_as_of"], issues["ytd_as_of"]} - STORED and "2026-11-16" in STORED
    docs = copy.deepcopy(BARE)
    real = {"as_of": issues["move_as_of"], "derived_from": None, "n": 1, "bonds": {"zz": 123.4}}
    docs[L.LEDGER_ROOT + issues["move_as_of"] + ".json.gz"] = real
    again, _ = build(docs)
    assert [s["as_of"] for s in again["snapshots"] if s["derived_from"]] == [issues["ytd_as_of"]]          # the stored day is not overwritten
    assert again["payload"]["windows"]["m1"]["source"] == "the board's own ledger" and again["payload"]["windows"]["m1"]["n_bonds"] == 1
    assert built["payload"]["windows"]["w1"]["ref"] == "2026-11-16" and built["payload"]["windows"]["w1"]["source"] == "the board's own ledger"     # the cut carries a real ledger day
    empty = build(BARE)[0]["payload"]["windows"]
    assert empty["w1"]["ref"] is None and "ledger" in empty["w1"]["why"] and empty["m3"]["ref"] is None and empty["m1"]["ref"] == issues["move_as_of"]
    # a window that is not there yet says when it can be, and never names a worked-back day as the day the ledger began
    for w in (empty["w1"], empty["m3"], built["payload"]["windows"]["m3"]):
        assert "began on" not in w["why"] and issues["ytd_as_of"] not in w["why"] and w["available_from"] in w["why"], w["why"]
    assert empty["w1"]["available_from"] == "2026-11-27"                # today's own day is four days back then: a week, give or take three
    m3 = built["payload"]["windows"]["m3"]
    assert m3["ref"] is None and m3["available_from"] > issues["as_of"] and m3["available_from"] in m3["why"], m3


@test
def a_week_appears_once_the_ledger_holds_one():
    issues = FIX["docs"][L.BONDS_KEY]["issues"]
    first = build()[0]
    docs = copy.deepcopy(FIX["docs"])
    assert L.LEDGER_ROOT + "2026-11-16.json.gz" in docs                 # replaced here by a day exactly 7 bp tighter on every bond
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
    want = {L.OUT_KEY, L.HIST_KEY} | {L.LEDGER_ROOT + d + ".json.gz" for d in {issues["as_of"], issues["move_as_of"], issues["ytd_as_of"]} - STORED}
    assert set(s3.puts) == want, sorted(set(s3.puts) ^ want)
    doc = json.loads(s3.puts[L.OUT_KEY])
    assert doc["contract"] == "credit-before-equity.v2" and doc["version"] == "2.1.0" and out["statusCode"] == 200
    snap = json.loads(gzip.decompress(s3.puts[L.LEDGER_ROOT + issues["as_of"] + ".json.gz"]))
    assert snap["derived_from"] is None and snap["n"] == len(snap["bonds"]) > 500
    hist = json.loads(s3.puts[L.HIST_KEY])
    assert all(len(v) <= L.HIST_KEEP for v in hist["obs"].values()) and "updated_at" in hist
    assert json.loads(json.dumps(doc)) == doc and "NaN" not in json.dumps(doc["thresholds"])


@test
def a_dry_run_builds_everything_and_writes_nothing():
    s3 = MemS3()
    keep = L._s3
    try:
        L._s3 = lambda: s3
        out = with_cut(lambda: L.lambda_handler({"dry_run": True}, None))
    finally:
        L._s3 = keep
    assert s3.puts == {} and out["dry_run"] is True and out["statusCode"] == 200
    assert out["companies"] == FIX["expected"]["coverage"]["companies"] and out["leads"] == FIX["expected"]["n_leads"] and out["windows"]["w1"] == "2026-11-16"
    issues = FIX["docs"][L.BONDS_KEY]["issues"]
    assert len(out["ledger_days"]) == len({issues["as_of"], issues["move_as_of"], issues["ytd_as_of"]} - STORED) and out["bytes"] > 50000
    assert out["version"] == "2.1.0" and out["stressed"] == FIX["expected"]["stress"]["n_stressed"] and out["themes"] == {"ai": FIX["expected"]["themes"][0]["n"]}


@test
def the_engine_asks_no_provider_for_anything():
    for name in ("lambda_function.py", "cbe_core.py", "cbe_names.py", "cbe_themes.py"):
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


def _table(p):
    cols = p["issuers"]["cols"]
    return [dict(zip(cols, r)) for r in p["issuers"]["rows"]]


def _standing(mine, others):
    """Where `mine` stands among `others` (which holds it), 0..100, ties sharing the middle - worked the long way round."""
    below = sum(1 for v in others if v < mine)
    same = sum(1 for v in others if v == mine)
    return round(100.0 * (below + (same - 1) / 2.0) / (len(others) - 1), 1)


@test
def a_window_that_is_not_there_yet_says_when_it_can_be():
    # a stored day 12 days back: three months (91 days, give or take 21) is first met 58 days on; one week is already met
    assert C.available_from("2026-11-20", ["2026-11-08"], 91, 21) == "2027-01-17"
    assert C.available_from("2026-11-20", ["2026-11-08", "2026-10-30"], 91, 21) == "2027-01-08"
    assert C.available_from("2026-11-20", ["2025-12-31"], 91, 21) is None          # already too old to ever be three months back
    assert C.available_from("2026-11-20", [], 7, 3) is None


@test
def standings_share_the_middle_of_a_tie_and_one_value_has_none():
    assert C.standings([("a", 1.0), ("b", 2.0), ("c", 2.0), ("d", 3.0)]) == {"a": 0.0, "b": 50.0, "c": 50.0, "d": 100.0}
    assert C.standings([("a", 5.0), ("b", 7.0), ("c", 9.0)]) == {"a": 0.0, "b": 50.0, "c": 100.0}
    assert C.standings([("a", 5.0)]) == {} and C.standings([]) == {}
    assert C.long_window({"ytd": "2025-12-31", "m3": None}, "2026-11-20") == "ytd"
    assert C.long_window({"ytd": "2025-12-31", "m3": "2025-10-20"}, "2026-01-20") == "m3"          # a young year is not a long window
    assert C.long_window({"ytd": "2025-12-31", "m3": None}, "2026-01-20") is None


@test
def the_stress_rank_is_where_a_company_stands_in_its_own_book():
    p = build()[0]["payload"]
    t = _table(p)
    assert p["stress"]["long"] == "ytd" and p["stress"]["long_ref"] == p["windows"]["ytd"]["ref"] and p["stress"]["stressed_at"] == C.STRESSED
    seen = 0
    for book in ("ig", "hy", "em"):
        rs = [r for r in t if r["book"] == book]
        level = [r["spread_bp"] for r in rs if r["spread_bp"] is not None]
        year = [r["pct_ytd"] for r in rs if r["pct_ytd"] is not None]
        month = [r["d_m1"] for r in rs if r["d_m1"] is not None]
        if level:
            mid = sorted(level)
            n = len(mid)
            assert p["stress"]["book_spread_bp"][book] == round(mid[n // 2] if n % 2 else (mid[n // 2 - 1] + mid[n // 2]) / 2.0, 1)
        for r in rs:
            want = [None, None, None]
            if r["spread_bp"] is not None and len(level) > 1:
                want[0] = _standing(r["spread_bp"], level)
            if r["pct_ytd"] is not None and len(year) > 1:
                want[1] = _standing(r["pct_ytd"], year)
            if r["d_m1"] is not None and len(month) > 1:
                want[2] = _standing(r["d_m1"], month)
            assert [r["rk_level"], r["rk_year"], r["rk_month"]] == want, (r["id"], [r["rk_level"], r["rk_year"], r["rk_month"]], want)
            moved = [x for x in want[1:] if x is not None]
            stress = round((want[0] + sum(moved)) / (1 + len(moved)), 1) if want[0] is not None and moved else None
            assert r["stress"] == stress, (r["id"], r["stress"], stress)
            seen += stress is not None
    assert seen > 40, seen
    hot = [r for r in t if r["stress"] is not None and r["stress"] >= C.STRESSED]
    assert p["stress"]["n_stressed"] == len(hot) > 0 and p["stress"]["n_stressed_listed"] == sum(1 for r in hot if r["ticker"])
    # the stress rank never looks at the stock: Oracle's bonds are marked down hard and its stock already fell, so it is no lead and it is under stress
    orcl = next(r for r in t if r["ticker"] == "ORCL")
    assert orcl["stress"] >= C.STRESSED and orcl["signal"] == "NONE" and orcl["px_m1"] <= -C.FLAT_BAND_PCT, orcl
    listed = {x["ticker"]: x for x in p["names"]}
    assert listed["ORCL"]["stress"] == orcl["stress"] and listed["ORCL"]["theme"] == "ai" and listed["ORCL"]["layer"] == "hyperscalers"


@test
def the_ai_stack_holds_the_reviewed_names_and_nothing_else():
    assert TH.theme_of("ORCL", "Oracle Corporation") == ("ai", "hyperscalers") and TH.theme_of("NVDA", "NVIDIA") == ("ai", "silicon") and TH.theme_of("DELL", "Dell") == ("ai", "servers")
    assert TH.theme_of("CRWV", "CoreWeave") == ("ai", "neocloud") and TH.theme_of("EQIX", "Equinix") == ("ai", "datacenter_reits")
    assert TH.theme_of(None, "APLD COMPUTECO 2 LLC") == ("ai", "compute_vehicles") and TH.theme_of(None, "GALAXY HELIOS DATA CENTERS II LLC") == ("ai", "compute_vehicles")
    assert TH.theme_of(None, "QTS CENTRAL ISSUER LLC") == ("ai", "compute_vehicles") and TH.theme_of(None, "HUT 8 DC LLC") == ("ai", "compute_vehicles")
    for name in ("BEACON POINT DC LLC", "PR RNO PROPERTY OWNER 1 LLC", "SV RNO PROPERTY OWNER 1 LLC", "MERIDIAN ARC HOLDCO LLC"):          # reviewed by name: each is an AI data center's own borrower
        assert TH.theme_of(None, name) == ("ai", "compute_vehicles"), name
    assert TH.theme_of(None, "SOFTBANK GROUP CORP") == ("ai", "ai_backers") and TH.theme_of(None, "SOFTBANK CORP") == (None, None)       # the group, not the telephone company
    assert TH.LAYER_ORDER[0] == "hyperscalers" and TH.LAYER_ORDER[-2:] == ["compute_vehicles", "ai_backers"] and set(TH.LAYER_ORDER) == set(TH.LAYER_WORDS)
    for ticker, name in (("AAPL", "Apple Inc."), ("ADP", "Automatic Data Processing, Inc."), ("JPM", "JPMorgan Chase & Co."), (None, "COMPUTER SCIENCES CORP"), (None, "PETROLEOS MEXICANOS"),
                         (None, "COMPUTERSHARE US INC"), ("FDS", "FactSet Data Centers"), (None, "ZENITH ARC LLC"), (None, "CLOUD SOFTWARE GROUP LLC"),
                         (None, "CHILDRENS HOSPITAL/DC"), (None, "GALAXY PIPELINE ASSETS BIDCO LTD")):                 # a listed company is in the theme by its ticker only, never by a word in its name
        assert TH.theme_of(ticker, name) == (None, None), (ticker, name)
    every = [t for _, _, ts in TH._LAYERS for t in ts]
    assert len(every) == len(set(every)), sorted(t for t in set(every) if every.count(t) > 1)               # a ticker is in one layer only
    p = build()[0]["payload"]
    t = _table(p)
    ai = [r for r in t if r["theme"] == "ai"]
    assert all(r["theme"] in (None, "ai") for r in t) and all((r["theme"] is None) == (r["layer"] is None) for r in t)
    assert {r["ticker"] for r in ai} >= {"ORCL", "INTC", "AMD", "META", "NVDA", "CRWV"}, sorted(r["ticker"] or r["name"] for r in ai)
    assert any(r["layer"] == "compute_vehicles" and not r["ticker"] for r in ai) and all(r["ticker"] or r["layer"] in ("compute_vehicles", "ai_backers") for r in ai)
    assert len(p["themes"]) == 1
    th = p["themes"][0]
    assert th["key"] == "ai" and th["name"] == "AI stack" and th["n"] == len(ai) and th["n_listed"] == sum(1 for r in ai if r["ticker"]) and th["value"] == round(sum(r["value"] for r in ai), 0)
    assert sum(layer["n"] for layer in th["layers"]) == th["n"] and [layer["key"] for layer in th["layers"]] == [k for k in TH.LAYER_ORDER if any(r["layer"] == k for r in ai)]
    for layer in th["layers"]:
        rs = [r for r in ai if r["layer"] == layer["key"]]
        assert layer["n"] == len(rs) and layer["name"] == TH.LAYER_WORDS[layer["key"]] and layer["value"] == round(sum(r["value"] for r in rs), 0)
        assert layer["spread_bp"] == C.f(C.weighted_median([(r["spread_bp"], r["value"]) for r in rs if r["spread_bp"] is not None]), 1)
        hot = [r for r in rs if r["stress"] is not None and r["stress"] >= C.STRESSED]
        assert layer["n_stressed"] == len(hot) and layer["stressed_value_pct"] == C.f(100.0 * sum(r["value"] for r in hot) / sum(r["value"] for r in rs), 1)
    have = [(r["d_ytd"], r["value"]) for r in ai if r["d_ytd"] is not None]
    assert th["win"]["ytd"]["d_bp"] == C.f(C.weighted_median(have), 1) and th["win"]["ytd"]["n"] == len(have)
    hot = sorted((r for r in ai if r["stress"] is not None and r["stress"] >= C.STRESSED), key=lambda r: (-r["value"], -r["stress"]))
    assert th["largest_stressed"] == [[r["name"], r["stress"]] for r in hot[:3]] and th["largest_stressed"][0][0] == "Oracle Corporation"     # named by size held, not by rank
    # the industries carry the same stress figures, and still hold every company
    for i in p["industries"]["all"]:
        rs = [r for r in t if r["industry"] == i["industry"] and r["n_bonds"]]
        assert i["n"] == len(rs) and i["n_stressed"] == sum(1 for r in rs if r["stress"] is not None and r["stress"] >= C.STRESSED), i["industry"]


@test
def no_lead_is_called_when_the_bonds_and_a_liquid_cds_disagree():
    base = build()[0]["payload"]
    assert all(not (x["signal"] in C.LEADS and x.get("cds_says") == "contradicts") for x in base["names"])
    lead = next(x for x in base["names"] if x["signal"] in C.LEADS and x.get("signal_basis") == "bond spreads")
    worse = lead["signal"] == "CREDIT_LEADS_DOWN"
    docs = copy.deepcopy(FIX["docs"])
    for g in docs[L.CDS_KEY]["groups"].values():                       # whatever the desk printed for it is replaced by one liquid row that says the opposite
        for kind in ("rows", "unpriced", "dormant"):
            g[kind] = [x for x in g.get(kind) or [] if N.issuer_key(x.get("name") or "")[0] != N.issuer_key(lead["name"])[0]]
    docs[L.CDS_KEY]["groups"]["us_corp"]["rows"].append({"name": lead["name"], "sector": "Test", "ccy": "USD", "spread_bp": 300.0, "chg_1m_bp": -90.0 if worse else 90.0, "chg_3m_bp": None,
                                                         "last_date": base["cds"]["as_of"], "liquid": True, "trades_30d": 999999})
    p = build(docs)[0]["payload"]
    x = next(y for y in p["names"] if y["ticker"] == lead["ticker"])
    assert x["cds_says"] == "contradicts" and x["signal"] == "NONE" and x["credit_conflict"] is True and x["credit_state"] == lead["credit_state"], x
    assert p["n_leads"] == base["n_leads"] - 1 and lead["ticker"] not in [y["ticker"] for y in p["leads"]]
    row = next(r for r in _table(p) if r["ticker"] == lead["ticker"])
    assert row["signal"] == "NONE" and row["direction"] == lead["credit_state"]
    # a thin CDS has no say, and one that agrees keeps the lead
    thin = copy.deepcopy(docs)
    thin[L.CDS_KEY]["groups"]["us_corp"]["rows"][-1]["liquid"] = False
    assert next(y for y in build(thin)[0]["payload"]["names"] if y["ticker"] == lead["ticker"])["signal"] == lead["signal"]
    agree = copy.deepcopy(docs)
    agree[L.CDS_KEY]["groups"]["us_corp"]["rows"][-1]["chg_1m_bp"] = 90.0 if worse else -90.0
    y = next(y for y in build(agree)[0]["payload"]["names"] if y["ticker"] == lead["ticker"])
    assert y["signal"] == lead["signal"] and y["cds_says"] == "confirms" and "credit_conflict" not in y


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
