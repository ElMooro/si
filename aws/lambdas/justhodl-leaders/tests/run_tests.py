"""justhodl-leaders offline tests: an in-memory S3 holding a small Polygon EOD bank (the bank's own row layout), the
bank manifest, Polygon's split reference windows, the native ETF desk packet, the canonical flow packet, per-fund ETF
Global histories (one correct, one holding other funds' rows, one stale), the 13F per-ticker table and book, the banked
Form 4 feed, the phase detector and the bottom desk; the factor book faked as rows. The universe selection, the tape
arithmetic (averages, crosses, accumulation / distribution, signed dollar volume), the patterns (double tops / bottoms,
climaxes), the split rebasing, the flow / 13F / insider joins and the full build() + handler are checked.
No network, no AWS.

Run: python3 aws/lambdas/justhodl-leaders/tests/run_tests.py
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
import gzip
import importlib.util
import io
import json
import math
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC]
BEGUN = []
sys.modules["chain_guard"] = SimpleNamespace(begin=lambda event, context=None: BEGUN.append(event) or 0)
for name in ("boto3", "botocore", "botocore.config"):
    if name not in sys.modules:
        try:
            __import__(name)
        except ImportError:
            sys.modules[name] = SimpleNamespace(client=lambda *a, **k: None, Config=lambda **k: None)
for mod in ("polygon_bankfirst", "industry_etf_catalog"):
    _spec = importlib.util.spec_from_file_location(mod, os.path.join(SHARED, mod + ".py"))
    _m = importlib.util.module_from_spec(_spec)
    sys.modules[mod] = _m
    _spec.loader.exec_module(_m)
BOOK = []
sys.modules["bank_enrich"] = SimpleNamespace(rows=lambda fields=None, where=None, limit=None: [dict(r) for r in BOOK], as_of=lambda: {"fmp-bank": "2026-11-04", "polygon-tape": "2026-11-04"})
sys.modules["fmp_bank"] = SimpleNamespace(WALKER_ROOT="data/warm/fmp/")

import lambda_function as lf  # noqa: E402
import leaders_patterns as P  # noqa: E402
import leaders_tape as T  # noqa: E402

NOW = datetime(2026, 11, 5, 3, 50, tzinfo=timezone.utc)
LAST = date(2026, 11, 4)
N = 620


class NoSuchKey(Exception):
    pass


class S3Stub:
    def __init__(self):
        self.store, self.puts = {}, []

    def put(self, key, doc):
        body = json.dumps(doc).encode()
        self.store[key] = gzip.compress(body) if key.endswith(".gz") else body

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise NoSuchKey("NoSuchKey: " + Key)
        return {"Body": io.BytesIO(self.store[Key]), "LastModified": datetime(2026, 11, 5, 3, 13, tzinfo=timezone.utc), "ContentLength": len(self.store[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise NoSuchKey("NoSuchKey: " + Key)
        return {"ContentLength": len(self.store[Key]), "LastModified": datetime(2026, 11, 5, 3, 13, tzinfo=timezone.utc)}

    def put_object(self, **kw):
        self.puts.append(kw)
        self.store[kw["Key"]] = kw["Body"]


def sessions(n, last=LAST):
    out, d = [], last
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return out[::-1]


DATES = sessions(N)


def seg(a, b, n):
    return [a + (b - a) * (i + 1) / n for i in range(n)]


def bars_of(closes, vols=None, spread=0.012, shape=None):
    """Daily bars ending on LAST. shape(i, c, prev) -> (h, l) overrides the symmetric bar."""
    out = []
    dates = DATES[-len(closes):]
    for i, c in enumerate(closes):
        h, l = c * (1 + spread / 2), c * (1 - spread / 2)
        if shape and i:
            h, l = shape(i, c, closes[i - 1])
        out.append({"date": dates[i].isoformat(), "o": c, "h": h, "l": l, "c": c, "v": (vols[i] if vols else 1_000_000.0)})
    return out


def bank_rows(bars):
    rows = []
    for b in bars:
        d = date.fromisoformat(b["date"])
        t = int(datetime(d.year, d.month, d.day, 5, tzinfo=timezone.utc).timestamp() * 1000)
        rows.append([t, b["o"], b["h"], b["l"], b["c"], b["v"], b["c"], 1000])
    return rows


def walk(n, drift, seed, start=100.0, vol=0.004):
    rnd = random.Random(seed)
    px, out = start, []
    for _ in range(n):
        px *= 1 + drift + rnd.gauss(0, vol)
        out.append(px)
    return out


# ------------------------------------------------------------------------------------------------------------- universe
def brow(sym, name, sector, industry, mcap, dv=5e8, adr=False, cik=None, **kw):
    r = {"symbol": sym, "name": name, "sector": sector, "industry": industry, "exchange": "NASDAQ", "is_etf": False, "is_otc": False, "is_fund": False, "is_preferred": False,
         "is_warrant_unit": False, "is_shell": False, "is_adr": adr, "is_debt_or_structured": False, "is_common": True, "security_type": "ADRC" if adr else "CS",
         "mcap": mcap, "dollar_vol_20d": dv, "country": "TW" if adr else "US", "cik": cik, "short_pct_float": 1.5, "days_to_cover": 2.0, "short_volume_ratio": 0.45, "beta": 1.1}
    r.update(kw)
    return r


FAKE_BOOK = [
    brow("NVDA", "NVIDIA", "Technology", "Semiconductors", 4.0e12, cik="0001045810"),
    brow("TSM", "Taiwan Semi", "Technology", "Semiconductors", 2.0e12, adr=True, cik="0001046179"),
    brow("AVGO", "Broadcom", "Technology", "Semiconductors", 1.5e12, cik="0001730168"),
    brow("AMD", "AMD", "Technology", "Semiconductors", 3.0e11, cik="0000002488"),
    brow("MU", "Micron", "Technology", "Semiconductors", 2.0e11, cik="0000723125"),
    brow("INTC", "Intel", "Technology", "Semiconductors", 1.0e11, cik="0000050863"),
    brow("QCOM", "Qualcomm", "Technology", "Semiconductors", 9.0e10, cik="0000804328"),
    brow("TINY", "Tiny Semi", "Technology", "Semiconductors", 5.0e8, dv=2e6, cik="0009999999"),
    brow("HDB", "HDFC Bank", "Financial Services", "Banks - Regional", 1.2e11, adr=True, cik="0001144967"),
    brow("USB", "U.S. Bancorp", "Financial Services", "Banks - Regional", 9.0e10, cik="0000036104"),
    brow("PNC", "PNC", "Financial Services", "Banks - Regional", 8.0e10, cik="0000713676"),
    brow("GOOGL", "Alphabet", "Communication Services", "Internet Content & Information", 3.4e12, dv=1.1e10, cik="0001652044"),
    brow("GOOG", "Alphabet", "Communication Services", "Internet Content & Information", 3.4e12, dv=7.0e9, cik="0001652044"),
    brow("META", "Meta", "Communication Services", "Internet Content & Information", 1.4e12, cik="0001326801"),
    brow("BRK-A", "Berkshire Hathaway", "Financial Services", "Insurance - Diversified", 1.19e12, dv=3.0e8, cik="0001067983"),
    brow("BRK-B", "Berkshire Hathaway", "Financial Services", "Insurance - Diversified", 1.19e12, dv=2.5e9, cik="0001067983"),
    brow("ACGLN", "Arch Capital", "Financial Services", "Insurance - Diversified", 3.7e10, dv=2e6, cik="0000947484", security_type=None),
    brow("ACGL", "Arch Capital", "Financial Services", "Insurance - Diversified", 3.7e10, dv=2e8, cik="0000947484"),
    brow("CELG-RI", "Celgene CVR", "Healthcare", "Medical - Pharmaceuticals", 1.26e11, dv=6e6, cik="0000816284"),
    brow("RPRX", "Royalty Pharma", "Healthcare", "Medical - Pharmaceuticals", 3.0e10, cik="0001802768"),
    brow("AG", "First Majestic", "Basic Materials", "Silver", 7.0e9, cik="0001308648"),
    brow("ACCO", "ACCO Brands", "Industrials", "Business Equipment & Supplies", 4.0e8, dv=2e6, cik="0000712034"),
    brow("JELD", "JELD-WEN", "Industrials", "Construction", 1.0e8, dv=5e5, cik="0001674335"),
    brow("NOBAR", "No Bars Inc", "Energy", "Coal", 5.0e9, cik="0001111111"),
    brow("SPY", "SPDR S&P 500", "Financial Services", "Asset Management", 8.0e11, is_etf=True, is_common=False, security_type="ETF"),
    brow("NSRGY", "Nestle", "Consumer Defensive", "Packaged Foods", 2.6e11, is_otc=True, is_common=False, adr=True),
    brow("SHEL1", "Shell Co", "Financial Services", "Shell Companies", 5.0e9),
    brow("NOCAP", "No Cap", "Technology", "Semiconductors", None),
    brow("PFDX", "Pref", "Financial Services", "Banks - Regional", 9.9e11, is_preferred=True),
]


def test_select_leaders():
    leaders, industries, stats = lf.select_leaders(FAKE_BOOK)
    by = {x["ticker"]: x for x in leaders}
    semis = sorted((x for x in leaders if x["industry"] == "Semiconductors"), key=lambda x: x["rank"])
    assert [x["ticker"] for x in semis] == ["NVDA", "TSM", "AVGO", "AMD", "MU", "INTC"], [x["ticker"] for x in semis]
    assert [x["rank"] for x in semis] == [1, 2, 3, 4, 5, 6] and [x["rank_us"] for x in semis] == [1, None, 2, 3, 4, 5]     # the fifth ex-ADR name is carried
    assert "QCOM" not in by and "TINY" not in by                         # sixth ex-ADR; under $2B
    assert by["NVDA"]["mcap_share_pct"] == round(100 * 4.0e12 / (4.0e12 + 2.0e12 + 1.5e12 + 3.0e11 + 2.0e11 + 1.0e11 + 9.0e10 + 5.0e8), 1)
    # one company, one line: the class that trades the most dollars
    assert "GOOGL" in by and "GOOG" not in by and by["GOOGL"]["classes"] == ["GOOG"]
    assert "BRK.B" in by and "BRK.A" not in by and by["BRK.B"]["symbol"] == "BRK-B" and by["BRK.B"]["classes"] == ["BRK.A"]
    assert "ACGL" in by and "ACGLN" not in by                             # a preferred carrying its issuer's cap folds into the common
    assert "CELG.RI" not in by and by["RPRX"]["rank"] == 1               # a CVR under the issuer's market cap is not a leader
    assert stats["suffix"] == 1 and stats["folded"] == 3
    # ADR-led industry: the ADR ranks first, the US names carry rank_us
    banks = sorted((x for x in leaders if x["industry"] == "Banks - Regional"), key=lambda x: x["rank"])
    assert [(x["ticker"], x["rank"], x["rank_us"], x["adr"]) for x in banks] == [("HDB", 1, None, True), ("USB", 2, 1, False), ("PNC", 3, 2, False)]
    # not common / not rankable
    for t in ("SPY", "NSRGY", "SHEL1", "NOCAP", "PFDX"):
        assert t not in by, t
    # an industry with no $2B company keeps its largest above $300M; below that it has no leader
    assert by["ACCO"]["small"] is True and by["ACCO"]["rank"] == 1 and industries["Business Equipment & Supplies"]["small"] is True
    assert "JELD" not in by and industries["Construction"]["n_eligible"] == 0
    assert stats["industries_with_leader"] == len({x["industry"] for x in leaders}) and stats["leaders"] == len(leaders)
    assert industries["Semiconductors"]["sector"] == "Technology" and industries["Semiconductors"]["n_members"] == 8
    assert lf.canon("BRK-B") == "BRK.B" and lf.spellings("BRK.B") == ["BRK.B", "BRK-B", "BRK/B", "BRKB"]


# ----------------------------------------------------------------------------------------------------------------- tape
def test_moving_averages_and_stack():
    c = [100.0] * 250 + [110.0] * 50
    m = T.ma_block(c)
    assert m["s20"] == 110.0 and m["d20"] == 0.0 and m["s50"] == 110.0
    assert m["s100"] == 105.0 and m["d100"] == round((110 / 105 - 1) * 100, 2)
    assert m["s200"] == 102.5 and m["s250"] == 102.0 and m["d250"] == round((110 / 102 - 1) * 100, 2)
    assert m["of"] == 5 and m["above"] == 3 and m["stack"] == "MIXED"       # the close equals the 20 and 50: not above them
    up = [50 + 0.2 * i for i in range(300)]
    mu = T.ma_block(up)
    assert mu["above"] == 5 and mu["stack"] == "BULL" and mu["stack_n"] == 5 and mu["s50_slope"] > 0 and mu["s200_slope"] > 0
    dn = up[::-1]
    md = T.ma_block(dn)
    assert md["above"] == 0 and md["stack"] == "BEAR" and md["stack_n"] == 0 and md["s200_slope"] < 0
    short = T.ma_block([10.0] * 60)
    assert short["s50"] == 10.0 and short["s100"] is None and short["d200"] is None and short["of"] == 2 and short["stack"] is None
    assert T.ma_block([])["above"] is None
    assert T.ma_block([10.0] * 300)["stack"] == "MIXED" and T.ma_block([10.0] * 300)["above"] == 0                    # a flat line is neither stacked up nor down
    assert T.sma([1, 2, 3, 4], 2) == 3.5 and T.sma([1, 2, 3, 4], 2, 2) == 1.5 and T.sma([1], 2) is None
    assert T.sma_series([1, 2, 3, 4], 2) == [None, 1.5, 2.5, 3.5]


def test_golden_and_death_cross():
    dates = [d.isoformat() for d in DATES]
    c = [100 - 0.1 * i for i in range(250)] + [75 + 0.35 * i for i in range(110)]
    g = T.cross_50_200(c, dates[-len(c):])
    assert g["type"] == "GOLDEN" and g["regime"] == "50>200" and g["gap_pct"] > 0 and g["ago"] == 62 and g["fresh"] is False and g["n_120"] == 1
    assert g["since_pct"] == round((c[-1] / g["px_at"] - 1) * 100, 2) and g["s200_rising"] is True
    # the same series cut 5 sessions after the cross: fresh
    cut = c[:len(c) - 62 + 5]
    gf = T.cross_50_200(cut, dates[-len(cut):])
    assert gf["type"] == "GOLDEN" and gf["ago"] == 5 and gf["fresh"] is True and gf["near"] is None
    d = [50 + 0.2 * i for i in range(250)] + [100 - 0.4 * i for i in range(90)]
    x = T.cross_50_200(d, dates[-len(d):])
    assert x["type"] == "DEATH" and x["regime"] == "50<200" and x["gap_pct"] < 0 and x["s200_rising"] is False
    # near: just before the golden cross, the 50 closing on the 200 from below
    before = c[:len(c) - 62 - 3]
    nr = T.cross_50_200(before, dates[-len(before):])
    assert nr["regime"] == "50<200" and nr["near"] == "GOLDEN NEAR" and 0 < nr["eta"] <= T.NEAR_SESSIONS and nr["type"] is None
    # no cross in a steady trend, nothing for a short history
    up = [50 + 0.2 * i for i in range(400)]
    u = T.cross_50_200(up, dates[-len(up):])
    assert u["type"] is None and u["regime"] == "50>200" and u["near"] is None and u["n_120"] == 0
    assert T.cross_50_200([1.0] * 200, dates[:200]) is None
    # whipsaw: the averages braid
    w = [100 + 3 * math.sin(i / 25.0) for i in range(500)]
    ws = T.cross_50_200(w, dates[-len(w):])
    assert ws["n_120"] >= 1 and ws["type"] in ("GOLDEN", "DEATH")


def acc_bars(up_vol, dn_vol, up_close_high=True, seed=3, n=320):
    """A drifting tape with the volume on one side: up_close_high=True closes up-days on their highs on up_vol
    (accumulation); False closes every day near its low with the volume on the down-days (distribution)."""
    rnd = random.Random(seed)
    closes, vols = [100.0], [1e6]
    for _ in range(n):
        up = rnd.random() < (0.55 if up_close_high else 0.45)
        closes.append(closes[-1] * (1.006 if up else 0.996))
        vols.append(up_vol if up else dn_vol)

    def shape(i, c, prev):
        if up_close_high:
            return (c * 1.001, c * 0.985) if c > prev else (c * 1.010, c * 0.997)
        return (c * 1.012, c * 0.999) if c > prev else (c * 1.015, c * 0.999)
    return bars_of(closes, vols, shape=shape)


def test_accumulation_and_distribution():
    a = T.ad_block(acc_bars(1.6e6, 0.8e6))
    assert a["label"] == "STRONG ACCUMULATION" and a["grade"] == "A" and a["score"] >= T.AD_STRONG and a["cmf21"] > 0.1 and a["ud50"] > 1.5 and a["obv63"] > 0 and a["acc_days"] > a["dist_days"]
    d = T.ad_block(acc_bars(0.8e6, 1.7e6, up_close_high=False))
    assert d["label"] == "STRONG DISTRIBUTION" and d["grade"] == "E" and d["score"] <= -T.AD_STRONG and d["cmf21"] < -0.1 and d["ud50"] < 1 and d["dist_days"] > d["acc_days"]
    flat = bars_of([100.0 + (0.1 if i % 2 else -0.1) for i in range(300)])
    n = T.ad_block(flat)
    assert n["label"] == "NEUTRAL" and n["grade"] == "C" and abs(n["score"]) < T.AD_LEAN and n["acc_days"] == 0 and n["dist_days"] == 0
    assert a["delta_21"] is not None and a["score_21"] is not None
    assert T.ad_block(bars_of([100.0] * 20))["score"] is None                     # not enough sessions
    part = T.ad_block(bars_of([100.0 + 0.2 * i for i in range(40)]))               # 40 sessions: the 21-session read only
    assert part["cmf21"] is not None and part["cmf63"] is None and part["ud50"] is None and part["acc_days"] is None
    assert [T.ad_label(x) for x in (60, 59.9, 20, 19.9, 0, -19.9, -20, -59.9, -60, None)] == ["STRONG ACCUMULATION", "ACCUMULATION", "ACCUMULATION", "NEUTRAL", "NEUTRAL", "NEUTRAL", "DISTRIBUTION", "DISTRIBUTION", "STRONG DISTRIBUTION", None]
    assert [T.ad_grade(x) for x in (75, 30, 0, -30, -75, None)] == ["A", "B", "C", "D", "E", None]
    assert T.footprint_days(bars_of([100.0] * 30)) == (None, None)


def test_signed_dollar_volume():
    # typical price = close here (h and l symmetric around it): +10% then -5% then +2% on 100 / 200 / 300 shares
    b = [{"date": "2026-01-0%d" % (i + 1), "o": c, "h": c + 1, "l": c - 1, "c": c, "v": v} for i, (c, v) in enumerate([(100.0, 50), (110.0, 100), (104.5, 200), (106.59, 300)])]
    m = T.money_flow(b, 3)
    assert m["in"] == round(110.0 * 100 + 106.59 * 300, 0) and m["out"] == round(104.5 * 200, 0) and m["net"] == m["in"] - m["out"]
    assert m["net_pct"] == round(100 * (m["in"] - m["out"]) / (m["in"] + m["out"]), 1)
    assert T.money_flow(b, 4) is None
    mf = T.money_flows(acc_bars(1.6e6, 0.8e6))
    assert set(mf) == {"w", "m", "q", "mfi14"} and mf["q"]["net"] > 0 and 0 <= mf["mfi14"] <= 100
    assert T.money_flows(bars_of([100.0 + i for i in range(80)]))["mfi14"] == 100.0 and T.money_flows(bars_of([200.0 - i for i in range(80)]))["mfi14"] == 0.0
    v = T.volume_now(bars_of([100.0] * 60, [1e6] * 59 + [4e6]))
    assert v["x"] == 4.0 and v["date"] == LAST.isoformat() and v["dir"] == "FLAT"
    assert T.volume_now(bars_of([100.0] * 30)) is None


# ------------------------------------------------------------------------------------------------------------- patterns
ADVANCE = [80.0] * 60 + seg(80, 100, 100)
DECLINE = [120.0] * 60 + seg(120, 90, 100)


def test_double_top():
    forming = P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 95.5, 6)))
    t = forming["double_top"]
    assert t["kind"] == "DOUBLE TOP" and t["state"] == "FORMING" and t["break"] is None and t["sep"] == 30 and t["ago"] == 6 and forming["double_bottom"] is None
    assert t["diff_pct"] < 1 and 9 < t["depth_pct"] < 11 and t["to_neck_pct"] < 0 and t["target"] < t["neck"]["px"] and t["target_pct"] < 0
    conf = P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 89.5, 10)))["double_top"]
    assert conf["state"] == "CONFIRMED" and conf["break"]["ago"] == 0 and conf["p1"]["px"] == t["p1"]["px"]
    # cleared the highs: void; still at the second high with no pullback: a retest, not a top; a steady uptrend: nothing
    assert P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 103, 6)))["double_top"] is None
    assert P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15)))["double_top"] is None
    assert P.double_patterns(bars_of([50 + 0.2 * i for i in range(300)])) == {"double_top": None, "double_bottom": None, "theta_pct": 4.0}
    # the second high far below the first is not a double top; an old break is old news
    assert P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 95.0, 15) + seg(95.0, 92.5, 6)))["double_top"] is None
    assert P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 89.5, 10) + [89.0] * 25))["double_top"] is None
    # volume on the second high against the first
    vols = [1e6] * (len(ADVANCE) + 15 + 15 + 6)
    for i in range(len(ADVANCE) + 27, len(ADVANCE) + 32):                       # the five sessions around the second high (index len(ADVANCE) + 29)
        vols[i] = 0.5e6
    lite = P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 95.5, 6), vols))["double_top"]
    assert lite["vol_ratio"] == 0.5
    # after the break: the measured target printing, the neckline reclaimed, a stand-off going stale
    top = ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 89.5, 10)
    assert conf["target_hit"] is False and conf["reclaim_pct"] < 0 and t["target_hit"] is None and t["reclaim_pct"] is None
    hit = P.double_patterns(bars_of(top + seg(89.5, 79.0, 6)))["double_top"]
    assert hit["state"] == "CONFIRMED" and hit["target_hit"] is True and hit["break"]["ago"] == 6 and hit["target"] > 79.0
    done = P.double_patterns(bars_of(top + seg(89.5, 79.0, 6) + seg(79.0, 96.0, 8)))["double_top"]
    assert done["state"] == "CONFIRMED" and done["target_hit"] is True and done["reclaim_pct"] > 3                       # it worked before it bounced
    failed = P.double_patterns(bars_of(top + seg(89.5, 96.0, 8)))["double_top"]
    assert failed["state"] == "FAILED" and failed["target_hit"] is False and failed["reclaim_pct"] > 3 and failed["break"]["ago"] == 8
    throwback = P.double_patterns(bars_of(top + seg(89.5, 92.0, 4)))["double_top"]
    assert throwback["state"] == "CONFIRMED" and 0 < throwback["reclaim_pct"] < 3                                        # back at the neckline is a throwback, not a failure
    assert P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 95.5, 6) + [95.5] * 30))["double_top"]["state"] == "FORMING"
    assert P.double_patterns(bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 95.5, 6) + [95.5] * 40))["double_top"] is None and P.FORMING_MAX_AGE == 40


def test_double_bottom():
    forming = P.double_patterns(bars_of(DECLINE + seg(90, 99, 15) + seg(99, 90.5, 15) + seg(90.5, 95, 6)))
    b = forming["double_bottom"]
    assert b["kind"] == "DOUBLE BOTTOM" and b["state"] == "FORMING" and b["to_neck_pct"] > 0 and b["target"] > b["neck"]["px"] and forming["double_top"] is None
    conf = P.double_patterns(bars_of(DECLINE + seg(90, 99, 15) + seg(99, 90.5, 15) + seg(90.5, 101, 10)))["double_bottom"]
    assert conf["state"] == "CONFIRMED" and conf["break"]["ago"] == 0 and conf["target_pct"] > 0
    assert P.double_patterns(bars_of(DECLINE + seg(90, 99, 15) + seg(99, 90.5, 15) + seg(90.5, 87, 6)))["double_bottom"] is None      # undercut: void
    bot = DECLINE + seg(90, 99, 15) + seg(99, 90.5, 15) + seg(90.5, 101, 10)
    assert conf["target_hit"] is False and P.double_patterns(bars_of(bot + seg(101, 110.5, 6)))["double_bottom"]["target_hit"] is True
    fell = P.double_patterns(bars_of(bot + seg(101, 94.0, 8)))["double_bottom"]
    assert fell["state"] == "FAILED" and fell["reclaim_pct"] > 3 and fell["kind"] == "DOUBLE BOTTOM"                     # back under the neckline it broke
    assert P.double_patterns(bars_of([100.0] * 100))["theta_pct"] is None                                                           # too short


def test_pivots_alternate():
    bars = bars_of(ADVANCE + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 95.5, 6))
    piv = P.pivots(bars, 4.0)
    kinds = [p["kind"] for p in piv]
    assert all(kinds[i] != kinds[i + 1] for i in range(len(kinds) - 1)) and piv[-1].get("provisional") is True
    assert [p["kind"] for p in piv[-4:]] == ["H", "L", "H", "L"] and P.pivots(bars[:2], 4.0) == [] and P.pivots(bars, None) == []


def climax_base(a, b, c):
    closes = [a] * 200 + seg(a, b, 40) + seg(b, c, 40)
    return bars_of(closes, [1e6] * len(closes))


def test_climax():
    nxt = (LAST + timedelta(days=1)).isoformat()
    sc = climax_base(100, 99, 82) + [{"date": nxt, "o": 81.0, "h": 81.5, "l": 74.0, "c": 78.5, "v": 6e6}]
    s = P.climax(sc)
    assert s["type"] == "SELLING" and s["ago"] == 0 and s["vol_x"] == 6.0 and s["range_atr"] >= 1.5 and s["close_pos"] == 60.0 and s["move_pct"] < -12 and s["held"] is True
    assert s["absorption"] == "close off the low" and s["extreme"] == 74.0
    assert s["day_pct"] == round((78.5 / 82.0 - 1) * 100, 2) and s["gap_pct"] == round((81.0 / 82.0 - 1) * 100, 2)      # the session's own move and its opening gap
    # the same bar closing on its low with no rally after it: capitulation is not yet absorbed
    assert P.climax(climax_base(100, 99, 82) + [{"date": nxt, "o": 81.0, "h": 81.5, "l": 74.0, "c": 74.2, "v": 6e6}]) is None
    # ... absorbed once a later session closes above it
    later = climax_base(100, 99, 82) + [{"date": nxt, "o": 81.0, "h": 81.5, "l": 74.0, "c": 74.2, "v": 6e6}, {"date": "2026-11-06", "o": 75, "h": 77, "l": 74.5, "c": 76.5, "v": 3e6}]
    assert P.climax(later)["absorption"] == "higher close inside three sessions" and P.climax(later)["ago"] == 1
    bc = climax_base(50, 51, 66) + [{"date": nxt, "o": 67.0, "h": 74.0, "l": 66.5, "c": 69.0, "v": 7e6}]
    b = P.climax(bc)
    assert b["type"] == "BUYING" and b["vol_x"] == 7.0 and b["close_pos"] == 33.0 and b["move_pct"] > 20 and b["absorption"] == "close off the high"
    # a breakout that closes on its high is not a buying climax; heavy volume inside a range is neither; a fund needs a smaller decline
    assert P.climax(climax_base(50, 51, 66) + [{"date": nxt, "o": 67.0, "h": 74.0, "l": 66.5, "c": 73.9, "v": 7e6}]) is None
    assert P.climax(bars_of([100.0] * 300, [1e6] * 299 + [9e6])) is None
    fund = climax_base(100, 99.5, 91) + [{"date": nxt, "o": 91.0, "h": 92.4, "l": 89.3, "c": 91.0, "v": 5e6}]      # 11.2% off the high: a fund's climax, not yet a stock's
    assert P.climax(fund, "stock") is None and P.climax(fund, "fund")["type"] == "SELLING"
    assert P.climax(bars_of([100.0] * 200)) is None                                 # the 250-session volume average needs 260 sessions


# --------------------------------------------------------------------------------------------------------------- splits
def test_split_rebasing():
    closes = [100.0] * 300
    raw = bars_of(closes[:250] + [50.0] * 50, [1e6] * 250 + [2e6] * 50)          # a 2-for-1 the bank appended raw
    day = raw[250]["date"]
    rec = [{"date": day, "split_from": 1, "split_to": 2}]
    adj, ev = T.rebase(raw, rec)
    assert len(ev) == 1 and ev[0]["source"] == "polygon-splits" and ev[0]["factor"] == 0.5 and ev[0]["date"] == day
    assert adj[0]["c"] == 50.0 and adj[249]["c"] == 50.0 and adj[250]["c"] == 50.0 and adj[0]["v"] == 2e6      # one continuous series, volume on the new share basis
    assert raw[0]["c"] == 100.0                                                    # the input is not mutated
    # the same split already inside the bars (the bank's backfill was adjusted): applied zero times
    flat = bars_of([50.0] * 300)
    adj2, ev2 = T.rebase(flat, rec)
    assert ev2 == [] and adj2 is flat
    # unrecorded: inferred from a volume-confirmed jump; a crash on exploding volume is not a split
    adj3, ev3 = T.rebase(raw, None)
    assert len(ev3) == 1 and ev3[0]["source"] == "inferred" and adj3[0]["c"] == 50.0
    crash = bars_of(closes[:250] + [50.0] * 50, [1e6] * 250 + [8e6] * 5 + [1e6] * 45)
    assert T.rebase(crash, None)[1] == []
    # a 1-for-3 reverse split, recorded
    rev = bars_of([20.0] * 250 + [60.0] * 50, [3e6] * 250 + [1e6] * 50)
    adj4, ev4 = T.rebase(rev, [{"date": rev[250]["date"], "split_from": 3, "split_to": 1}])
    assert ev4[0]["factor"] == 3.0 and adj4[0]["c"] == 60.0 and adj4[0]["v"] == 1e6
    # a recorded 21-for-20 stock dividend is inside ordinary daily moves: left alone
    assert T.confirm_recorded(flat, [{"date": flat[100]["date"], "split_from": 20, "split_to": 21}]) == []
    # a small recorded split the bars print: 3-for-2
    s32 = bars_of([90.0] * 250 + [60.0] * 50)
    assert T.rebase(s32, [{"date": s32[250]["date"], "split_from": 2, "split_to": 3}])[1][0]["factor"] == round(2 / 3, 6)
    assert T.rebase([], rec) == ([], [])


# ----------------------------------------------------------------------------------------------------------- continuity
def bars_on(dates, closes, vols=None, spread=0.012):
    return [{"date": d.isoformat(), "o": c, "h": c * (1 + spread / 2), "l": c * (1 - spread / 2), "c": c, "v": (vols[i] if vols else 1_000_000.0)} for i, (d, c) in enumerate(zip(dates, closes))]


def test_own_history_cuts_at_a_gap_and_at_the_listing():
    whole = bars_of([100.0] * 300)
    assert T.own_history(whole) == (whole, None) and T.own_history([]) == ([], None)
    # a ticker that printed nothing for months: the bars before belong to another security (BNY, B, CNR, JAN ...)
    old, new = bars_on(DATES[100:400], [10.0] * 300), bars_on(DATES[-116:], [140.0] * 116)
    own, cut = T.own_history(old + new)
    assert own == new and cut["reason"] == "gap" and cut["date"] == DATES[-116].isoformat() and cut["prev"] == DATES[399].isoformat() and cut["dropped"] == 300 and cut["gap_days"] > 100
    # two gaps: only the last continuous stretch is this instrument
    older = bars_on(DATES[0:50], [3.0] * 50)
    assert T.own_history(older + old + new)[0] == new
    # a long weekend is not a gap
    hol = bars_on([date(2026, 6, 26), date(2026, 7, 6), date(2026, 7, 7)], [10.0, 10.1, 10.2])
    assert T.own_history(hol) == (hol, None) and T.GAP_DAYS == 10
    # no gap, but bars older than the listing: the previous holder of the ticker, traded right up to the hand-over
    seam = bars_of([2.0] * 300 + [23.6] * 159)
    own, cut = T.own_history(seam, seam[300]["date"])
    assert len(own) == 159 and cut == {"reason": "listing", "date": seam[300]["date"], "ipo_date": seam[300]["date"], "dropped": 300}
    # a listing date a session or two after the first real print (a when-issued bar): the continuous bars before it stay,
    # the previous holder's - a x11.8 seam away - do not
    own, cut = T.own_history(seam, seam[302]["date"])
    assert len(own) == 159 and cut["date"] == seam[300]["date"] and cut["ipo_date"] == seam[302]["date"] and cut["dropped"] == 300
    wi = bars_of([99.59] + [96.0] * 176)                                                   # Covista: one bar the day before the provider's date
    assert T.own_history(wi, wi[1]["date"]) == (wi, None)
    # ... but not beyond the slack: a continuous series older than its listing date by weeks is cut at the date
    assert T.own_history(whole, whole[200]["date"])[1] == {"reason": "listing", "date": whole[200 - sum(1 for b in whole[:200] if T._days(b["date"], whole[200]["date"]) <= T.IPO_SLACK_DAYS)]["date"], "ipo_date": whole[200]["date"], "dropped": 200 - sum(1 for b in whole[:200] if T._days(b["date"], whole[200]["date"]) <= T.IPO_SLACK_DAYS)}
    assert T.own_history(whole, "2030-01-01") == (whole, None) and T.own_history(whole, "not a date") == (whole, None) and T.own_history(whole, whole[0]["date"]) == (whole, None)


def test_predecessor_needs_the_record_at_the_break():
    start = "2026-05-21"
    gap = {"reason": "gap", "prev": "2026-02-06"}
    ev = T.ticker_events({"results": {"events": [{"ticker_change": {"ticker": "BNY"}, "type": "ticker_change", "date": "2026-05-21"}, {"ticker_change": {"ticker": "BK"}, "type": "ticker_change", "date": "2007-07-02"}]}})
    assert ev == [("2007-07-02", "BK"), ("2026-05-21", "BNY")]
    assert T.predecessor("BNY", start, gap, ev, []) == ("BK", "polygon-ticker-events")
    assert T.predecessor("BNY", "2026-05-26", gap, ev, []) == ("BK", "polygon-ticker-events")            # the event a few days off the first bar
    assert T.predecessor("BNY", "2026-08-21", gap, ev, []) is None                                         # the record does not place the change at this break
    # the record names no earlier symbol (Barrick: only the B event) -> the cut stands
    assert T.predecessor("B", "2025-05-09", {"reason": "gap", "prev": "2025-01-24"}, [("2025-05-09", "B")], []) is None
    # a company back on a symbol it used before, the record stopping at its departure (Fiserv: FISV, FI, FISV)
    fisv = [("2003-09-10", "FISV"), ("2023-06-07", "FI")]
    assert T.predecessor("FISV", "2025-11-11", {"reason": "gap", "prev": "2023-06-06"}, fisv, []) == ("FI", "polygon-ticker-events")
    assert T.predecessor("FISV", "2025-11-11", None, fisv, []) is None and T.predecessor("FISV", "2025-11-11", {"reason": "gap", "prev": "2024-06-06"}, fisv, []) is None
    # a stretch with no symbol at all is not a predecessor (an OTC interlude)
    assert T.predecessor("AYA", "2026-05-04", None, [("2022-04-05", "AYAS"), ("2023-11-18", ""), ("2026-05-04", "AYA")], []) is None
    # an old rename is no licence to bridge a later break
    assert T.predecessor("X", "2024-03-01", {"reason": "gap", "prev": "2023-01-05"}, [("2010-01-01", "A"), ("2015-01-01", "X")], []) is None
    # FMP's list: the new symbol, on the day; share-class spelling folded
    fmp = [{"date": "2026-08-18", "companyName": "Vivmark", "oldSymbol": "EQR", "newSymbol": "VMRK"}, {"date": "2026-08-18", "oldSymbol": "BF-A", "newSymbol": "BF-B"}, "junk", {"date": None, "oldSymbol": "Q", "newSymbol": "VMRK"}]
    assert T.predecessor("VMRK", "2026-08-18", None, [], fmp) == ("EQR", "fmp-symbol-change") and T.predecessor("BF.B", "2026-08-19", None, [], fmp) == ("BF.A", "fmp-symbol-change")
    assert T.predecessor("VMRK", "2026-09-18", None, [], fmp) is None and T.predecessor("EQR", "2026-08-18", None, [], fmp) is None
    assert T.ticker_events(None) == [] and T.ticker_events({"results": [1]}) == [] and T.ticker_events({"results": {"events": [{"type": "other", "date": "2026-01-01"}, {"type": "ticker_change"}]}}) == []


def test_stitch_only_when_the_tape_hands_over():
    own = bars_on(DATES[-116:], [140.0 + 0.1 * i for i in range(116)])
    old = bars_on(DATES[:-116], [100.0 + 0.08 * i for i in range(N - 116)])                 # ends at 140.24 the session before
    bars, info, why = T.stitch(own, old, "BK", "polygon-ticker-events")
    assert why is None and len(bars) == N and bars[:len(old)] == old and bars[-116:] == own
    assert info == {"from": "BK", "on": DATES[-116].isoformat(), "source": "polygon-ticker-events", "sessions": N - 116, "handover_x": round(140.0 / old[-1]["c"], 4), "first": DATES[0].isoformat()}
    # the old symbol went on trading: it is another security (a spin-off, a second class), not this one's past
    assert T.stitch(own, old + bars_on(DATES[-116:-110], [141.0] * 6), "BK", "x")[1:] == (None, "the old symbol kept trading")
    # ... unless the symbol was taken up again much later by someone else
    later = old + bars_on(DATES[-40:], [7.0] * 40)
    assert T.stitch(own, later, "BK", "x")[1]["sessions"] == N - 116
    # the price does not hand over: a merger exchange, not a rename
    assert T.stitch(own, bars_on(DATES[:-116], [60.0] * (N - 116)), "BK", "x")[2].startswith("hand-over x2.333")
    # the old symbol stopped long before
    assert "stopped" in T.stitch(own, bars_on(DATES[:-160], [139.0] * (N - 160)), "BK", "x")[2]
    # the predecessor's own older stretch (behind a gap of its own) is not carried
    two = bars_on(DATES[0:100], [5.0] * 100) + bars_on(DATES[200:-116], [139.0] * (N - 316))
    assert T.stitch(own, two, "BK", "x")[1]["sessions"] == N - 316
    assert T.stitch(own, [], "BK", "x")[1:] == (None, "no predecessor bars") and T.stitch([], old, "BK", "x")[1] is None


def test_separation_cut_needs_a_matching_listing():
    day = DATES[-25].isoformat()
    bars = bars_of([77.0] * 575 + [12.5] * 25, [3.7e6] * 575 + [8e7] * 5 + [1.2e7] * 20)
    mcap = 7.9e9                                                                          # 632M shares at 12.5: (77 - 12.5) x 632M = $40.8B vanished
    lists = [(day, "VYLR", 4.8e10, "Basic Materials"), ("2026-03-02", "SUNB", 3.4e10, "Industrials")]
    assert T.rebase(bars, None)[1] == []                                                  # not a split: the shares traded exploded
    on_day = lambda t: day                                                                # noqa: E731 - the new listing's first bar prints on the session of the collapse
    cut_bars, cut = T.separation_cut(bars, [], "CTVA", mcap, "Basic Materials", lists, on_day)
    assert len(cut_bars) == 25 and cut["reason"] == "separation" and cut["date"] == day and cut["listing"] == "VYLR" and cut["ratio"] == round(12.5 / 77.0, 4) and cut["dropped"] == 575
    assert 0.5 <= cut["listing_mcap_usd"] / cut["vanished_usd"] <= 2.0
    # a crash is a crash: another sector, another size, another week, or the listing is the ticker itself
    assert T.separation_cut(bars, [], "CTVA", mcap, "Healthcare", lists, on_day) == (bars, None)
    assert T.separation_cut(bars, [], "CTVA", mcap, "Basic Materials", [(day, "VYLR", 3.0e9, "Basic Materials")], on_day) == (bars, None)
    assert T.separation_cut(bars, [], "CTVA", mcap, "Basic Materials", [(DATES[-35].isoformat(), "VYLR", 4.8e10, "Basic Materials")], on_day) == (bars, None)
    assert T.separation_cut(bars, [], "VYLR", mcap, "Basic Materials", lists, on_day) == (bars, None)
    assert T.separation_cut(bars, [], "CTVA", None, "Basic Materials", lists, on_day) == (bars, None) and T.separation_cut(bars, [], "CTVA", mcap, "Basic Materials", [], on_day) == (bars, None)
    # Fiserv 2025-10-29: a same-sector listing of the same size two sessions earlier - its first bar is not the collapse's session, so nothing was handed out
    near = [(DATES[-27].isoformat(), "Q", 4.5e10, "Basic Materials")]
    assert T.separation_cut(bars, [], "CTVA", mcap, "Basic Materials", near, lambda t: DATES[-27].isoformat()) == (bars, None)
    assert T.separation_cut(bars, [], "CTVA", mcap, "Basic Materials", near, lambda t: None) == (bars, None)
    assert T.separation_cut(bars, [], "CTVA", mcap, "Basic Materials", near, on_day)[1]["listing"] == "Q"               # the listing date may sit a few days off; the first bar may not
    # a collapse already rebased as a split is not weighed again
    assert T.separation_cut(bars, [{"date": day}], "CTVA", mcap, "Basic Materials", lists, on_day) == (bars, None)
    # what is left as printed is listed, with how long ago
    assert T.jumps_of(bars) == [{"date": day, "ratio": round(12.5 / 77.0, 4), "ago": 24}] and T.jumps_of(bars, [{"date": day}]) == [] and T.jumps_of(bars_of([100.0, 150.0, 90.0])) == []
    assert T.jumps_of(bars_of([100.0, 190.0, 100.0])) == [{"date": DATES[-2].isoformat(), "ratio": 1.9, "ago": 1}, {"date": DATES[-1].isoformat(), "ratio": 0.5263, "ago": 0}]


CONT_BOOK = [
    brow("BNY", "Bank of New York Mellon", "Financial Services", "Banks - Diversified", 1.1e11, cik="0001390777", ipo_date="2007-07-01"),
    brow("VMRK", "Vivmark Residential", "Real Estate", "REIT - Residential", 2.3e10, cik="0000906107", ipo_date="1993-08-12"),
    brow("SPCX", "Space Exploration", "Industrials", "Aerospace & Defense", 1.9e12, cik="0001181412", ipo_date=DATES[-101].isoformat()),
    brow("JAN", "Janus Living", "Real Estate", "REIT - Healthcare Facilities", 6.9e9, cik="0002100805", ipo_date=DATES[-159].isoformat()),
    brow("CTVA", "Corteva", "Basic Materials", "Agricultural Inputs", 7.9e9, cik="0001755672", ipo_date="2019-05-24"),
    brow("VYLR", "Vylor", "Basic Materials", "Agricultural Inputs", 4.8e10, cik="0002012866", ipo_date=DATES[-25].isoformat()),
    brow("LQDA", "Liquidia", "Healthcare", "Biotechnology", 3.8e9, cik="0001819576", ipo_date="2018-07-26"),
    brow("MERG", "Merged Co", "Technology", "Software - Application", 9.0e9, cik="0003333333", ipo_date="2001-01-01"),
    brow("FISV", "Fiserv", "Technology", "Information Technology Services", 3.5e10, cik="0000798354", ipo_date="1986-09-25"),
    brow("Q", "Qnity Electronics", "Technology", "Semiconductors", 2.5e10, dv=3e6, cik="0002058873", ipo_date=DATES[-258].isoformat()),
]


def seed_continuity():
    s3 = seed()
    put = lambda t, b: s3.put(lf.bank_key(t), {"ticker": t, "rows": bank_rows(b)})          # noqa: E731
    # BNY: a municipal fund's bars, three months of silence, then the bank - whose history sits under BK, with a 2-for-1 the bank left raw
    put("BNY", bars_on(DATES[20:504 - 84], [10.2] * (484 - 84), [9e4] * (484 - 84)) + bars_on(DATES[-116:], [139.0 + 0.1 * i for i in range(116)], [3.5e6] * 116))
    bk = [276.0] * (N - 116 - 150) + [138.0 + 0.004 * i for i in range(150)]
    put("BK", bars_on(DATES[:-116], bk, [1.75e6] * (N - 116 - 150) + [3.5e6] * 150))
    s3.put(lf.BANK_EVENTS_ROOT + "BNY.json.gz", {"status": "OK", "results": {"name": "Bank of New York Mellon", "events": [
        {"ticker_change": {"ticker": "BNY"}, "type": "ticker_change", "date": DATES[-116].isoformat()}, {"ticker_change": {"ticker": "BK"}, "type": "ticker_change", "date": "2007-07-02"}]}})
    split_day = DATES[N - 116 - 150].isoformat()
    key = lf.BANK_SPLITS_ROOT + split_day[:7] + ".json.gz"
    s3.put(key, (json.loads(gzip.decompress(s3.store[key])) if key in s3.store else []) + [{"ticker": "BK", "execution_date": split_day, "split_from": 1, "split_to": 2}])
    # VMRK: a rename FMP's list records; no Polygon events file yet
    put("VMRK", bars_on(DATES[-56:], [64.0 + 0.02 * i for i in range(56)]))
    put("EQR", bars_on(DATES[:-56], [60.0 + 0.0065 * i for i in range(N - 56)]))
    s3.put(lf.SYMBOL_CHANGE_KEY, [{"date": DATES[-56].isoformat(), "companyName": "Vivmark Residential", "oldSymbol": "EQR", "newSymbol": "VMRK"}])
    # SPCX: a dead ETF's bars, two months of silence, then the listing - nobody to stitch
    put("SPCX", bars_on(DATES[100:480], [22.0] * 380, [3e3] * 380) + bars_on(DATES[-101:], [160.0 + 0.2 * i for i in range(101)], [3e8] * 101))
    # JAN: the previous holder traded right up to the listing - no gap, the listing date is the only witness
    put("JAN", bars_of([2.2] * 300 + [23.6 + 0.04 * i for i in range(159)], [2e5] * 300 + [5e6] * 159))
    # CTVA hands most of itself to VYLR, which lists that day; LQDA crashes the day before - nothing was handed out
    put("CTVA", bars_of([77.0 + 0.001 * i for i in range(575)] + [12.5 + 0.01 * i for i in range(25)], [3.7e6] * 575 + [8e7] * 5 + [1.2e7] * 20))
    put("VYLR", bars_on(DATES[-25:], [68.0 + 0.05 * i for i in range(25)], [2e7] * 25))
    put("LQDA", bars_of([70.0] * 574 + [30.0] * 26, [1.1e6] * 574 + [1.3e7] * 5 + [2e6] * 21))
    # FISV: a 44% crash two sessions after a same-sector company of the same size got its listing date - a crash, not a hand-out
    put("FISV", bars_of([115.0] * 344 + [64.4 + 0.001 * i for i in range(256)], [4e6] * 344 + [6e7] * 5 + [8e6] * 251))
    put("Q", bars_on(DATES[-254:], [90.0] * 254))                                         # its first bar prints four sessions after its listing date
    # MERG: the record names the old symbol but the price does not hand over (a merger exchange): refused, the cut stands
    put("MERG", bars_on(DATES[-200:], [50.0] * 200))
    put("OLDM", bars_on(DATES[:-200], [20.0] * (N - 200)))
    s3.put(lf.BANK_EVENTS_ROOT + "MERG.json.gz", {"results": {"events": [{"ticker_change": {"ticker": "MERG"}, "type": "ticker_change", "date": DATES[-200].isoformat()}, {"ticker_change": {"ticker": "OLDM"}, "type": "ticker_change", "date": "2012-01-01"}]}})
    return s3


def test_build_reads_each_instrument_as_itself():
    s3 = seed_continuity()
    BOOK[:] = FAKE_BOOK + CONT_BOOK
    lf.MIN_LEADERS = 5
    out = lf.build(s3, NOW, log=lambda *a: None, remaining_s=300.0)
    rows = {r["ticker"]: r for r in out["rows"]}
    h = out["history"]
    assert out["n_errors"] == 0, out["errors"]
    # BNY: cut at the gap, BK put back in front, BK's raw 2-for-1 rebased - a year of the bank, not +1,260% of a fund
    bny = rows["BNY"]
    assert bny["history"]["sessions"] == 600 and bny["history"]["cut"]["reason"] == "gap" and bny["history"]["cut"]["dropped"] == 400
    assert bny["history"]["stitched"] == [{"from": "BK", "on": DATES[-116].isoformat(), "source": "polygon-ticker-events", "sessions": N - 116, "handover_x": round(139.0 / (138.0 + 0.004 * 149), 4), "first": DATES[0].isoformat()}]
    assert bny["splits"][0]["source"] == "polygon-splits" and bny["splits"][0]["factor"] == 0.5 and bny["history"]["jumps"] is None
    assert -5 < bny["h"]["y"] < 15 and bny["ma"]["s250"] is not None and 130 < bny["ma"]["s250"] < 150 and bny["cross"] is not None
    # VMRK: FMP's list names EQR
    assert rows["VMRK"]["history"]["sessions"] == 600 and rows["VMRK"]["history"]["cut"] is None and rows["VMRK"]["history"]["stitched"][0]["from"] == "EQR" and rows["VMRK"]["history"]["stitched"][0]["source"] == "fmp-symbol-change"
    assert rows["VMRK"]["h"]["y"] is not None and abs(rows["VMRK"]["h"]["y"]) < 10
    # SPCX / JAN: no record - the history starts at the listing and longer horizons are null, never a guess
    sp = rows["SPCX"]
    assert sp["history"]["sessions"] == 101 and sp["history"]["cut"]["reason"] == "gap" and sp["history"]["stitched"] is None and sp["h"]["q"] is not None and sp["h"]["h"] is None and sp["h"]["y"] is None
    assert sp["ma"]["s100"] is not None and sp["ma"]["s200"] is None and sp["cross"] is None and sp["vs"]["q"] is not None and sp["strength"] is not None
    jan = rows["JAN"]
    assert jan["history"]["sessions"] == 159 and jan["history"]["cut"]["reason"] == "listing" and jan["history"]["cut"]["ipo_date"] == DATES[-159].isoformat() and jan["h"]["y"] is None and jan["history"]["jumps"] is None
    # CTVA: the collapse is a separation (VYLR listed that day, same sector, matching value): 25 sessions of the remaining company
    ct = rows["CTVA"]
    assert ct["history"]["sessions"] == 25 and ct["history"]["cut"]["reason"] == "separation" and ct["history"]["cut"]["listing"] == "VYLR" and ct["vs"] is None and ct["strength"] is None and ct["h"]["m"] is not None and abs(ct["h"]["m"]) < 5
    assert rows["VYLR"]["history"]["sessions"] == 25 and rows["VYLR"]["history"]["cut"] is None
    # LQDA: a crash stays a crash, and says so
    lq = rows["LQDA"]
    assert lq["history"]["cut"] is None and lq["history"]["sessions"] == 600 and lq["history"]["jumps"] == [{"date": DATES[-26].isoformat(), "ratio": round(30.0 / 70.0, 4), "ago": 25}] and lq["h"]["q"] < -50
    # FISV: (115 - 64.4) x 543M shares = $27.5B vanished beside Q's $25B listing two sessions earlier, same sector - and still a crash
    fi = rows["FISV"]
    assert fi["history"]["cut"] is None and fi["history"]["sessions"] == 600 and fi["history"]["jumps"] == [{"date": DATES[-256].isoformat(), "ratio": round(64.4 / 115.0, 4), "ago": 255}] and fi["h"]["y"] is not None
    # MERG: named by the record, refused by the tape
    assert rows["MERG"]["history"]["sessions"] == 200 and rows["MERG"]["history"]["stitched"] is None and h["refused"] == {"MERG": "OLDM: hand-over x2.500 is not one security continuing"}
    assert h["n_cut"] == 4 and sorted(h["cut"]) == ["BNY", "CTVA", "JAN", "SPCX"] and h["n_stitched"] == 2 and sorted(h["stitched"]) == ["BNY", "VMRK"] and h["n_with_jumps"] == 2
    assert {"CTVA", "VYLR", "SPCX", "JAN", "MERG", "ACCO"} <= set(h["short"]) and "BNY" not in h["short"] and h["symbol_changes"]["rows"] == 1
    # the rest of the universe is untouched by all this
    assert rows["NVDA"]["history"]["cut"] is None and rows["META"]["splits"][0]["source"] == "polygon-splits"


# ---------------------------------------------------------------------------------------------------------------- joins
def flow_rows(ticker, n=80, flow=1e6, nav=50.0, shares=1e7, last=date(2026, 11, 3)):
    out, d = [], last
    while len(out) < n:
        if d.weekday() < 5:
            out.append({"composite_ticker": ticker, "effective_date": d.isoformat(), "processed_date": d.isoformat(), "fund_flow": flow, "nav": nav, "shares_outstanding": shares})
        d -= timedelta(days=1)
    return out


def test_bank_flow_reads_only_the_funds_own_history():
    fl = lf.bank_flow(flow_rows("FDN"), "FDN", "2026-11-04")
    assert fl["source"] == "polygon-bank" and fl["current"] is True and fl["effective"] == "2026-11-03" and fl["aum_usd"] == 5e8 and fl["observations"] == 80
    assert fl["w1"] == {"usd": 1e6, "pct_aum": 0.2, "end": "2026-11-03", "start": "2026-11-03"} and fl["w5"]["usd"] == 5e6 and fl["w21"]["pct_aum"] == 4.2 and fl["w63"]["usd"] == 63e6
    # the v1.1.0 bank file: the first pages of the whole dataset under the fund's name - never this fund's flow
    junk = flow_rows("AAAC", 40) + flow_rows("AAXJ", 40)
    assert lf.bank_flow(junk, "XSD", "2026-11-04") is None
    assert lf.bank_flow(flow_rows("FDN") + flow_rows("SMH", 2), "FDN", "2026-11-04") is None      # one foreign row is enough
    # a history that stopped weeks ago is not current and prints no window
    stale = lf.bank_flow(flow_rows("IAT", last=date(2026, 10, 9)), "IAT", "2026-11-04")
    assert stale["current"] is False and stale["w5"] is None and stale["age_days"] == 26
    # a revision: the later processed row wins; a short history has no 21-observation window
    rows = flow_rows("FDN", 3) + [{"composite_ticker": "FDN", "effective_date": "2026-11-03", "processed_date": "2026-11-04", "fund_flow": 9e6, "nav": 50.0, "shares_outstanding": 1e7}]
    rv = lf.bank_flow(rows, "FDN", "2026-11-04")
    assert rv["w1"]["usd"] == 9e6 and rv["w21"] is None
    assert lf.bank_flow([], "FDN", "2026-11-04") is None and lf.bank_flow(None, "FDN", "2026-11-04") is None


def packet(w1, w5, w21, aum=1e10, valid="2026-11-06T00:00:00+00:00", end="2026-11-03"):
    def win(usd):
        return {"status": "matched_reporting_window", "end_date": end, "flow_usd_decimal": str(usd), "flow_to_end_assets_pct_decimal": str(usd / aum * 100)}
    return {"source_valid_until": valid, "latest_effective_date": end, "quality": {"status": "complete_acquired_history"}, "latest_observation": {"reported_assets_usd_decimal": str(aum)},
            "latest_windows": {"1": win(w1), "5": win(w5), "21": win(w21)}}


def test_packet_flow():
    fl = lf.packet_flow(packet(1e8, -2e8, 5e8), True, NOW.isoformat(), "etf-desk")
    assert fl["current"] is True and fl["w1"] == {"usd": 1e8, "pct_aum": 1.0, "end": "2026-11-03"} and fl["w5"]["usd"] == -2e8 and fl["w21"]["pct_aum"] == 5.0 and fl["aum_usd"] == 1e10
    late = lf.packet_flow(packet(1e8, -2e8, 5e8, valid="2026-11-04T00:00:00+00:00"), True, NOW.isoformat(), "etf-desk")
    assert late["current"] is False and late["w1"] is None and late["w21"] is None
    assert lf.packet_flow(packet(1, 2, 3), False, NOW.isoformat(), "provider-flow-research")["current"] is False
    unmatched = packet(1e8, 2e8, 3e8)
    unmatched["latest_windows"]["5"] = {"status": "incomplete"}
    assert lf.packet_flow(unmatched, True, NOW.isoformat(), "etf-desk")["w5"] is None


def form4(sym, kind, qty, px, who, when="2026-10-20", form="4", ad=None):
    return {"symbol": sym, "filingDate": when, "transactionDate": when, "reportingCik": who, "reportingName": "N " + who, "transactionType": kind, "securitiesTransacted": qty, "price": px,
            "acquisitionOrDisposition": ad or ("A" if kind.startswith("P") else "D"), "formType": form}


def test_insiders():
    day1 = [form4("NVDA", "S-Sale", 100000, 200.0, "a"), form4("NVDA", "S-Sale", 50000, 200.0, "b"), form4("NVDA", "S-Sale", 50000, 200.0, "c"),
            form4("NVDA", "A-Award", 99999, 0, "a"), form4("NVDA", "M-Exempt", 5, 10.0, "a"), form4("NVDA", "G-Gift", 5, 0, "a", ad="D"), form4("AMD", "P-Purchase", 2500, 200.0, "z"),
            form4("AMD", "P-Purchase", 100, 200.0, "y", form="4/A"), form4("BRK-B", "P-Purchase", 1000, 500.0, "w"), form4("XYZ", "P-Purchase", 1, 1.0, "q")]
    day2 = [form4("NVDA", "S-Sale", 100000, 200.0, "a"), form4("AMD", "S-Sale", 10, 100.0, "v", when="2026-06-01")]       # the feed repeats a filing; one is outside the window
    got = lf.fold_insiders([day1, day2], {"NVDA", "AMD", "BRK.B"}, "2026-08-06")
    assert got["NVDA"] == {"buys": 0, "sells": 3, "buy_usd": 0.0, "sell_usd": 4e7, "last": "2026-10-20", "buyers": 0, "sellers": 3, "net_usd": -4e7}
    assert got["AMD"]["buys"] == 1 and got["AMD"]["buy_usd"] == 5e5 and got["AMD"]["sells"] == 0 and got["AMD"]["buyers"] == 1
    assert got["BRK.B"]["buys"] == 1 and "XYZ" not in got
    assert lf.insider_row(form4("NVDA", "S-Sale", 0, 200.0, "a")) is None and lf.insider_row(form4("NVDA", "P-Purchase", 5, 0, "a")) is None
    assert lf.insider_row(form4("NVDA", "P-Purchase", 5, 10.0, "a", ad="D")) is None and lf.insider_row("x") is None
    assert lf.insider_days(date(2026, 11, 5), 3) == ["2026-11-05", "2026-11-04", "2026-11-03"]


def test_smart_verdict():
    acc = {"stance": "ACCUMULATING", "net_usd": 5e8, "value_usd": 1e10}
    dist = {"stance": "DISTRIBUTING", "net_usd": -5e8, "value_usd": 1e10}
    assert lf.smart_verdict(acc, None) == {"verdict": "BUYING", "score": 2, "legs": {"f13": 2}}
    assert lf.smart_verdict(dist, None)["verdict"] == "SELLING"
    assert lf.smart_verdict({"stance": "CONTESTED", "net_usd": 1.2e9, "value_usd": 1e10}, None)["verdict"] == "LEANING BUY"
    assert lf.smart_verdict({"stance": "CONTESTED", "net_usd": -1.2e9, "value_usd": 1e10}, None)["verdict"] == "LEANING SELL"
    assert lf.smart_verdict({"stance": "CONTESTED", "net_usd": 9e8, "value_usd": 1e10}, None)["verdict"] == "QUIET"       # under 10% of the reported value
    assert lf.smart_verdict({"stance": "CONTESTED", "net_usd": 1.2e9, "value_usd": 1e10, "value_prior_usd": 1.5e10}, None)["verdict"] == "QUIET"   # ... measured on the larger of the two disclosures
    assert lf.smart_verdict({"stance": "CONTESTED", "net_usd": -4e8, "value_usd": 0, "value_prior_usd": 3e9}, None)["verdict"] == "LEANING SELL"   # a position closed out
    buy = {"buys": 2, "buy_usd": 5e5, "sell_usd": 0, "sellers": 0}
    sell = {"buys": 0, "buy_usd": 0, "sell_usd": 4e7, "sellers": 3}
    assert lf.smart_verdict(None, buy) == {"verdict": "LEANING BUY", "score": 1, "legs": {"insider": 1}}
    assert lf.smart_verdict(dist, sell)["score"] == -3 and lf.smart_verdict(acc, sell)["verdict"] == "LEANING BUY"       # 13F +2, insiders -1
    assert lf.smart_verdict(None, {"buys": 0, "buy_usd": 0, "sell_usd": 4e7, "sellers": 2})["verdict"] == "QUIET"         # two sellers are a plan, not a pattern
    assert lf.smart_verdict(None, None) == {"verdict": "NO READ", "score": None, "legs": {}}


def test_f13_join_spellings():
    cols = ["n_holders", "n_prior_holders", "holder_delta", "value_usd", "value_prior_usd", "inflow_usd", "outflow_usd", "net_usd", "bull", "bear", "stance", "kind", "bucket", "sector", "industry",
            "n_identities", "conc_n_holders", "conc_holder_delta"]
    tbl = {"NVDA": [11, 11, 0, 3.2e10, 3.0e10, 2e9, 5e9, -3e9, 3, 8, "DISTRIBUTING", "stock", "equities", "Technology", "Semiconductors", 1, 2, 0],
           "BRK/B": [5, 4, 1, 1e10, 8e9, 1e9, 0, 1e9, 4, 0, "ACCUMULATING", "stock", "equities", "Financial Services", "Insurance - Diversified", 1, 1, 0]}
    n = lf.f13_of(tbl, cols, "NVDA")
    assert n["stance"] == "DISTRIBUTING" and n["net_usd"] == -3e9 and n["inflow_usd"] == 2e9 and n["outflow_usd"] == 5e9 and n["net_pct"] == -10.0 and "kind" not in n
    assert lf.f13_of(tbl, cols, "BRK.B")["stance"] == "ACCUMULATING" and lf.f13_of(tbl, cols, "XOM") is None


def test_signals_vocabulary():
    row = {"cross": {"type": "GOLDEN", "fresh": True, "near": None}, "dtop": None, "dbot": {"kind": "DOUBLE BOTTOM", "state": "CONFIRMED"}, "climax": {"type": "SELLING"},
           "tight": {"tier": "VERY TIGHT"}, "ad": {"label": "STRONG ACCUMULATION"}, "ma": {"stack": "BULL"}, "rs_new_high": True, "smart": {"verdict": "BUYING"}}
    assert lf.signals_of(row) == ["GOLDEN CROSS", "DOUBLE BOTTOM CONFIRMED", "SELLING CLIMAX", "VERY TIGHT", "STRONG ACCUMULATION", "BULL STACK", "RS HIGH", "SMART BUYING"]
    quiet = {"cross": {"type": "DEATH", "fresh": False, "near": "GOLDEN NEAR"}, "tight": {"tier": "TIGHT", "flag": "PINNED"}, "ad": {"label": "NEUTRAL"}, "ma": {"stack": "MIXED"}, "smart": {"verdict": "QUIET"}}
    assert lf.signals_of(quiet) == ["GOLDEN NEAR"] and lf.signals_of({}) == []
    # a failed pattern stays on the row and off the signals and the boards
    failed = {"ticker": "X", "dtop": {"kind": "DOUBLE TOP", "state": "FAILED", "ago": 30}, "dbot": {"kind": "DOUBLE BOTTOM", "state": "FORMING", "ago": 3}}
    assert lf.signals_of(failed) == ["DOUBLE BOTTOM FORMING"]
    live = [failed, {"ticker": "A", "dtop": {"kind": "DOUBLE TOP", "state": "FORMING", "ago": 2}}, {"ticker": "B", "dtop": {"kind": "DOUBLE TOP", "state": "CONFIRMED", "ago": 30, "break": {"ago": 4}, "target_hit": False}},
            {"ticker": "C", "dtop": {"kind": "DOUBLE TOP", "state": "CONFIRMED", "ago": 12, "break": {"ago": 1}, "target_hit": True}}, {"ticker": "D", "dtop": {"kind": "DOUBLE TOP", "state": "CONFIRMED", "ago": 9, "break": {"ago": 2}, "target_hit": False}}]
    b = lf.boards_of(live)
    assert b["double_top"] == ["D", "B", "A", "C"] and b["double_bottom"] == ["X"]      # fresh confirmations, then forming, then the ones whose target already printed
    many = [{"ticker": "T%03d" % i, "cross": {"type": "DEATH", "fresh": True, "ago": i % 20}} for i in range(150)]
    assert len(lf.boards_of(many)["death_cross"]) == 150                                  # a board is never truncated: its length is the count


# ---------------------------------------------------------------------------------------------------------- full build
def seed():
    s3 = S3Stub()
    pb = sys.modules["polygon_bankfirst"]
    s3.put(pb.MANIFEST, {"last_session": LAST.isoformat(), "updated_at": "2026-11-05T03:13:28+00:00", "tickers": 21093})
    spy = walk(N, 0.0004, 1, 400.0, 0.003)
    series = {
        "SPY": bars_of(spy),
        "NVDA": acc_bars(1.6e6, 0.8e6, n=N - 1),                                                         # accumulation, leading
        "TSM": bars_of(walk(N, -0.0012, 3, 300.0)),                                                      # lagging
        "AVGO": bars_of([80.0] * (N - 196) + seg(80, 100, 160) + seg(100, 91, 15) + seg(91, 99.6, 15) + seg(99.6, 95.5, 6)),      # double top forming
        "AMD": bars_of([120.0] * (N - 200) + seg(120, 90, 160) + seg(90, 99, 15) + seg(99, 90.5, 15) + seg(90.5, 101, 10)),       # double bottom confirmed
        "MU": bars_of([100.0] * (N - 81) + seg(100, 99, 40) + seg(99, 82, 40) + [78.5], [1e6] * (N - 1) + [6e6]),              # selling climax today
        "INTC": bars_of(seg(20, 40, N - 150) + [40.0 * (1 + 0.012 * math.sin(i / 3.0)) for i in range(150)]),                   # a very tight base
        "GOOGL": bars_of([100 - 0.1 * i for i in range(N - 60)] + [100 - 0.1 * (N - 60) + 0.9 * i for i in range(60)]),
        "META": bars_of([600.0] * (N - 40) + [300.0] * 40, [1e6] * (N - 40) + [2e6] * 40),                 # a 2-for-1 the bank appended raw
        "BRK.B": bars_of([500.0 + 0.01 * i for i in range(N)]),
        "ACGL": bars_of(walk(N, 0.0003, 9, 90.0)), "HDB": bars_of(walk(N, 0.0002, 10, 60.0)), "USB": bars_of(walk(N, 0.0005, 11, 45.0)), "PNC": bars_of(walk(N, 0.0006, 12, 180.0)),
        "RPRX": bars_of(walk(N, 0.0004, 13, 40.0)), "AG": bars_of(walk(N, 0.001, 14, 12.0, 0.02)), "ACCO": bars_of(walk(50, 0.0, 15, 5.0)),
        "SMH": bars_of(walk(N, 0.0015, 20, 250.0)), "SOXX": bars_of(walk(N, 0.0012, 21, 220.0)), "XSD": bars_of(walk(N, 0.001, 22, 240.0)), "KRE": bars_of(walk(N, 0.0002, 23, 55.0)),
        "IAT": bars_of(walk(N, 0.0002, 24, 50.0)), "FDN": bars_of(walk(N, 0.0008, 25, 260.0)), "XLK": bars_of(walk(N, 0.0009, 26, 280.0)), "XLF": bars_of(walk(N, 0.0005, 27, 52.0)),
        "XLC": bars_of(walk(N, 0.0007, 28, 110.0)),
    }
    series["MU"][-1].update(o=81.0, h=81.5, l=74.0)                                                       # the climax bar: a wide range closing off its low
    for t, b in series.items():
        s3.put(lf.bank_key(t), {"ticker": t, "rows": bank_rows(b)})
    split_day = series["META"][N - 40]["date"]
    s3.put(lf.BANK_SPLITS_ROOT + split_day[:7] + ".json.gz", [{"ticker": "META", "execution_date": split_day, "split_from": 1, "split_to": 2},
                                                              {"ticker": "BRK.B", "execution_date": split_day, "split_from": 1, "split_to": 3}])   # recorded but not in the bars: not applied
    s3.put(lf.DESK_KEY, {"generated_at": "2026-11-04T23:06:41+00:00", "funds": {"SMH": {"flows": packet(1.4e9, 1.1e9, 3.3e9, aum=6.7e10), "quality": {"flow_current_eligible": True}},
                                                                               "KRE": {"flows": packet(1.3e8, 1.9e8, 5.0e8, aum=4.3e9), "quality": {"flow_current_eligible": True}},
                                                                               "XLK": {"flows": packet(-4e7, 5e8, 1e9, aum=1.2e11), "quality": {"flow_current_eligible": True}},
                                                                               "XLF": {"flows": packet(-1.7e8, 8e8, -3.4e8, aum=5.4e10, valid="2026-11-04T00:00:00+00:00"), "quality": {"flow_current_eligible": True}}}})
    s3.put(lf.CANON_KEY, {"generated_at": "2026-11-04T22:01:03+00:00", "funds": {"SOXX": packet(-3.6e8, -3.2e8, 5.8e9, aum=4.4e10), "SMH": packet(9, 9, 9)}})
    s3.put(lf.BANK_FLOW_ROOT + "FDN.json.gz", flow_rows("FDN", flow=2e6))
    s3.put(lf.BANK_FLOW_ROOT + "XSD.json.gz", flow_rows("AAAC", 40) + flow_rows("AAXJ", 40))
    s3.put(lf.BANK_FLOW_ROOT + "IAT.json.gz", flow_rows("IAT", last=date(2026, 10, 9)))
    s3.put(lf.BANK_FLOW_ROOT + "XLF.json.gz", {"results": flow_rows("XLF", flow=-1e7, nav=52.0, shares=1e9)})
    s3.put(lf.BANK_FLOW_ROOT + "SMH.json.gz", flow_rows("SMH", flow=3e7, nav=400.0, shares=1e8))      # a desk fund: only its 63-observation window is the bank's
    cols = ["n_holders", "n_prior_holders", "holder_delta", "value_usd", "value_prior_usd", "inflow_usd", "outflow_usd", "net_usd", "bull", "bear", "stance", "kind", "bucket", "sector", "industry",
            "n_identities", "conc_n_holders", "conc_holder_delta"]
    s3.put(lf.TICKERS_KEY, {"contract": "13f-tickers.v1", "period": "2026-06-30", "generated_at": "2026-11-04T21:33:24+00:00", "n_cohort": 15, "columns": cols, "by_ticker": {
        "NVDA": [11, 9, 2, 3.2e10, 2.0e10, 6e9, 1e9, 5e9, 8, 2, "ACCUMULATING", "stock", "equities", "Technology", "Semiconductors", 1, 2, 0],
        "TSM": [11, 11, 0, 9.2e9, 9.2e9, 1.5e8, 3.9e9, -3.8e9, 2, 9, "DISTRIBUTING", "stock", "equities", "Technology", "Semiconductors", 1, 3, 0],
        "BRK/B": [5, 4, 1, 1e10, 8e9, 1e9, 0, 1e9, 4, 0, "ACCUMULATING", "stock", "equities", "Financial Services", "Insurance - Diversified", 1, 1, 0],
        "SMH": [4, 4, 0, 1.6e8, 1.9e8, 1.4e8, 1.8e8, -3.5e7, 3, 3, "CONTESTED", "fund", "equities", None, None, 1, 0, 0]}})
    ind = {"name": "Semiconductors", "sector": "Technology", "level": "industry", "value_usd": 1.46e11, "value_prior_usd": 9.9e10, "value_change_pct": 47.4, "n_identities": 104, "n_managers": 12,
           "tilt_pct": 6.2, "stance": "BALANCED", "inflow_usd": 2.6e10, "outflow_usd": 4.4e10, "net_usd": -1.8e10, "net_pct_of_prior": -18.1, "money_stance": "DISTRIBUTING", "accumulating": ["KLAC"]}
    s3.put(lf.BOOK_KEY, {"version": "1.3.0", "book": {"sectors": {"cohort": {"industries": [ind]}, "concentrated": {"industries": [dict(ind, value_usd=3.5e10, net_usd=4.7e9, stance="DISTRIBUTING", n_managers=3)]}}}})
    s3.put(lf.PHASE_KEY, {"generated_at": "2026-11-04T23:42:00+00:00", "phases_all": {"NVDA": {"p": "MARKUP", "b": "2026-08-01"}, "BRK-B": {"p": "ACCUMULATION", "b": "2026-09-01"}}})
    s3.put(lf.BOTTOM_KEY, {"generated_at": "2026-11-05T03:47:00+00:00", "board_all": [{"ticker": "MU", "frame": "W", "state": "TESTING", "grade": "B", "score": 61.0, "sc_date": "2026-10-20"},
                                                                                     {"ticker": "MU", "frame": "D", "state": "CLIMAX", "grade": "C", "score": 48.0, "sc_date": "2026-11-04"}]})
    s3.put("%s2026/2026-11-03.json.gz" % lf.INSIDER_ROOT, [form4("NVDA", "S-Sale", 100000, 200.0, "a"), form4("NVDA", "S-Sale", 50000, 200.0, "b"), form4("NVDA", "S-Sale", 50000, 200.0, "c"),
                                                          form4("AMD", "P-Purchase", 2500, 200.0, "z"), form4("TSM", "A-Award", 5, 0, "t")])
    s3.put("%s2026/2026-10-30.json.gz" % lf.INSIDER_ROOT, [form4("NVDA", "S-Sale", 100000, 200.0, "a")])
    s3.put(lf.SYMBOL_CHANGE_KEY, [{"date": "2026-10-28", "companyName": "Somebody Else", "oldSymbol": "OLDX", "newSymbol": "NEWX"}])
    return s3


def run_build(s3):
    BOOK[:] = FAKE_BOOK
    lf.MIN_LEADERS = 5
    return lf.build(s3, NOW, log=lambda *a: None, remaining_s=300.0)


def test_build():
    s3 = seed()
    out = run_build(s3)
    assert out["contract"] == "leaders.v1" and out["engine"] == "justhodl-leaders" and out["as_of"] == LAST.isoformat() and out["benchmark"]["ticker"] == "SPY"
    rows = {r["ticker"]: r for r in out["rows"]}
    u = out["universe"]
    assert u["unbanked"].count("NOBAR") == 1 and "NOBAR" not in rows and u["n_leaders"] == len(out["rows"]) and u["top_n"] == 5
    assert set(rows) == {"NVDA", "TSM", "AVGO", "AMD", "MU", "INTC", "HDB", "USB", "PNC", "GOOGL", "META", "BRK.B", "ACGL", "RPRX", "AG", "ACCO"}
    for r in out["rows"]:
        assert r["rank"] <= 5 or (r["rank_us"] is not None and r["rank_us"] <= 5), r["ticker"]
        assert r["kind"] == "stock" and r["as_of"] == LAST.isoformat() and not r["stale"] and r["etf"]
        assert set(r["ma"]) >= {"s20", "s50", "s100", "s200", "s250", "d20", "d50", "d100", "d200", "d250", "above", "stack"}
    # relative strength and the tape
    nv = rows["NVDA"]
    assert nv["vs"]["q"] > 0 and nv["strength"] > rows["TSM"]["strength"] and nv["ad"]["label"] == "STRONG ACCUMULATION" and nv["ma"]["stack"] == "BULL" and nv["mf"]["q"]["net"] > 0
    assert rows["TSM"]["vs"]["h"] < 0 and rows["TSM"]["ma"]["d200"] < 0
    assert rows["AVGO"]["dtop"]["state"] == "FORMING" and "DOUBLE TOP FORMING" in rows["AVGO"]["signals"]
    assert rows["AMD"]["dbot"]["state"] == "CONFIRMED" and "DOUBLE BOTTOM CONFIRMED" in rows["AMD"]["signals"]
    assert rows["MU"]["climax"]["type"] == "SELLING" and rows["MU"]["climax"]["ago"] == 0 and "SELLING CLIMAX" in rows["MU"]["signals"]
    assert rows["INTC"]["tight"]["tier"] == "VERY TIGHT" and "VERY TIGHT" in rows["INTC"]["signals"]
    assert rows["GOOGL"]["cross"]["type"] == "GOLDEN" and rows["GOOGL"]["classes"] == ["GOOG"]
    assert rows["ACCO"]["vs"] is None and rows["ACCO"]["strength"] is None and rows["ACCO"]["cross"] is None and rows["ACCO"]["small"] is True      # 50 sessions: bars, no ranking
    # splits: the raw 2-for-1 is rebased once, the recorded one the bars do not print is not applied
    assert rows["META"]["splits"][0]["source"] == "polygon-splits" and abs(rows["META"]["h"]["q"]) < 1 and rows["BRK.B"]["splits"] is None
    assert out["splits"]["n_rebased"] == 1 and out["splits"]["recorded"] == 1 and out["splits"]["windows_read"] == 1
    # smart money
    assert nv["f13"]["stance"] == "ACCUMULATING" and nv["f13"]["inflow_usd"] == 6e9 and nv["f13"]["net_pct"] == 25.0
    assert nv["insider"]["sells"] == 3 and nv["insider"]["sellers"] == 3 and nv["insider"]["sell_usd"] == 4e7      # the repeated filing counted once
    assert nv["smart"] == {"verdict": "LEANING BUY", "score": 1, "legs": {"f13": 2, "insider": -1}}
    assert rows["TSM"]["smart"]["verdict"] == "SELLING" and rows["TSM"]["insider"] is None and rows["AMD"]["smart"]["verdict"] == "LEANING BUY"
    assert rows["BRK.B"]["f13"]["stance"] == "ACCUMULATING" and rows["BRK.B"]["smart"]["verdict"] == "BUYING" and rows["USB"]["smart"]["verdict"] == "NO READ"
    assert nv["desks"] == {"phase": "MARKUP", "phase_begin": "2026-08-01", "bottom": None} and rows["BRK.B"]["desks"]["phase"] == "ACCUMULATION"
    assert rows["MU"]["desks"]["bottom"]["frame"] == "D" and rows["MU"]["desks"]["bottom"]["state"] == "CLIMAX" and rows["USB"]["desks"] is None
    assert out["insiders"]["days_read"] == 2 and out["insiders"]["newest_day"] == "2026-11-03" and out["insiders"]["n_selling"] == 1 and out["insiders"]["n_buying"] == 1
    # industries
    inds = {x["industry"]: x for x in out["industries"]}
    se = inds["Semiconductors"]
    assert se["leaders"] == ["NVDA", "TSM", "AVGO", "AMD", "MU"] and se["leader"] == "NVDA" and se["n_leaders"] == 5 and se["etfs"] == ["SMH", "SOXX", "XSD", "XLK"]      # INTC is ex-ADR sixth: not in the industry row
    assert se["f13"]["net_usd"] == -1.8e10 and se["f13"]["money_stance"] == "DISTRIBUTING" and se["f13"]["conc"]["net_usd"] == 4.7e9
    assert se["smart"]["f13_net_usd"] == 5e9 - 3.8e9 and se["smart"]["n_f13"] == 2 and se["smart"]["buying"] == 2 and se["smart"]["selling"] == 1
    assert se["etf_flow"]["funds"] == ["SMH", "SOXX"] and se["etf_flow"]["proxy"] is None and se["etf_flow"]["w5"]["usd"] == 1.1e9 - 3.2e8
    assert se["etf_flow"]["w21"]["pct_aum"] == round(100 * (3.3e9 + 5.8e9) / (6.7e10 + 4.4e10), 3)
    assert se["breadth"]["n"] == 5 and se["signals"]["DOUBLE TOP"] == 1 and se["signals"]["DOUBLE BOTTOM"] == 1 and se["signals"]["SELLING CLIMAX"] == 1
    assert se["vs"]["m"] is not None and se["ew_vs"]["m"] is not None and se["rank"] is not None and se["strength"] is not None
    # an industry whose own fund has no current flow falls back to its sector fund, labelled
    ins = inds["Insurance - Diversified"]
    assert ins["etfs"] == ["KIE", "IAK", "XLF"] and ins["etf_flow"]["proxy"] == "sector" and ins["etf_flow"]["funds"] == ["XLF"]
    assert inds["Banks - Regional"]["etf_flow"]["funds"] == ["KRE"] and inds["Business Equipment & Supplies"]["vs"] is None
    assert "Construction" not in inds and "Coal" not in inds and u["industries_without_leader"] == ["Coal", "Construction"]
    for r in out["rows"]:
        assert r["industry_rank"] == inds[r["industry"]].get("rank")
    # funds: sources in order - desk, canonical, the bank's own history; never another fund's rows
    etfs = {r["ticker"]: r for r in out["etfs"]}
    assert etfs["SMH"]["flow"]["source"] == "etf-desk" and etfs["SMH"]["flow"]["w1"]["usd"] == 1.4e9 and etfs["SMH"]["f13"]["stance"] == "CONTESTED"
    assert etfs["SOXX"]["flow"]["source"] == "provider-flow-research" and etfs["FDN"]["flow"]["source"] == "polygon-bank" and etfs["FDN"]["flow"]["w63"]["usd"] == 126e6
    assert etfs["XSD"]["flow"] is None and etfs["IAT"]["flow"]["current"] is False
    assert etfs["XLF"]["flow"]["source"] == "polygon-bank" and etfs["XLF"]["flow"]["current"] is True      # the desk's window expired; the bank's history is newer
    assert etfs["SMH"]["role"] == "industry" and etfs["XLK"]["role"] == "sector" and etfs["SMH"]["industries"] == ["Semiconductors"] and etfs["SMH"]["kind"] == "fund"
    # the desks publish 1 / 5 / 21 observations; the 63-observation window of a desk fund is the banked history's, and says so
    assert etfs["SMH"]["flow"]["w63"] == {"usd": 63 * 3e7, "pct_aum": 4.725, "end": "2026-11-03", "start": flow_rows("SMH")[62]["effective_date"], "source": "polygon-bank"} and etfs["SMH"]["flow"]["w21"]["usd"] != 21 * 3e7
    assert etfs["SOXX"]["flow"]["w63"] is None                                                           # no banked history: no 63-observation window is made up
    pb = out["flows"]["polygon_bank"]
    assert pb["other_funds"] == 1 and pb["this_fund"] == 4 and pb["not_current"] == 1 and pb["w63_filled"] == 1 and pb["asked"] == pb["this_fund"] + pb["other_funds"] + pb["absent"] >= out["flows"]["n_funds"]
    assert out["flows"]["by_source"] == {"etf-desk": 3, "provider-flow-research": 1, "polygon-bank": 2}
    assert "XSD" in out["flows"]["no_flow"] and "IAT" in out["flows"]["no_flow"]
    # boards and summary
    b = out["boards"]["leaders"]
    assert "AVGO" in b["double_top"] and "AMD" in b["double_bottom"] and "MU" in b["selling_climax"] and "INTC" in b["tight"] and "NVDA" in b["accumulation"] and "BRK.B" in b["smart_buying"]
    assert "TSM" in b["smart_selling"] and "NVDA" in b["bull_stack"]
    s = out["summary"]
    assert s["n"] == 15 and s["smart_buying"] == 1 and s["smart_selling"] == 1 and s["accumulation"] >= 1 and 0 <= s["pct_beating_m"] <= 100 and s["pct_above_200"] is not None
    assert out["changes"]["prev_as_of"] is None and out["changes"]["new"] == []
    # history: nothing to cut or stitch here; ACCO is simply short
    h = out["history"]
    assert h["n_cut"] == 0 and h["n_stitched"] == 0 and h["cut"] == {} and h["refused"] == {} and h["short"] == ["ACCO"] and h["symbol_changes"] == {"rows": 1, "newest": "2026-10-28", "last_modified": "2026-11-05T03:13:00+00:00"}
    assert nv["history"] == {"sessions": 600, "from": DATES[-600].isoformat(), "cut": None, "stitched": None, "jumps": None} and rows["ACCO"]["history"]["sessions"] == 50
    assert out["n_errors"] == 0 and set(out["definitions"]) >= {"leaders", "ma", "cross", "double_top", "climax", "tight", "ad", "mf", "f13", "smart", "etf_flow", "history"} and len(out["limitations"]) >= 5
    assert [x["engine"] for x in out["sources"]][:2] == ["justhodl-bank-factors", "justhodl-polygon-eod"] and len(json.dumps(out)) < 2_500_000
    assert any("cohort of 15 managers" in x for x in out["limitations"]) and not any("{" in x for x in out["limitations"])
    return s3, out


def test_changes_against_the_previous_session():
    s3, out = test_build()
    prev = {"contract": "leaders.v1", "as_of": "2026-11-03", "rows": [{"ticker": "MU", "signals": ["BEAR STACK"]}, {"ticker": "NVDA", "signals": list(out["rows"][0]["signals"])}], "etfs": []}
    s3.put(lf.OUT_KEY, prev)
    again = run_build(s3)
    ch = again["changes"]
    assert ch["prev_as_of"] == "2026-11-03" and {"ticker": "MU", "signal": "SELLING CLIMAX", "industry": "Semiconductors", "kind": "stock"} in ch["new"]
    assert all(x["ticker"] in ("MU", "NVDA") for x in ch["new"] + ch["gone"])
    # a re-run on the same session keeps the block it already published
    s3.put(lf.OUT_KEY, {"contract": "leaders.v1", "as_of": LAST.isoformat(), "changes": {"prev_as_of": "2026-11-03", "new": [{"ticker": "X", "signal": "Y"}], "gone": []}, "rows": [], "etfs": []})
    assert run_build(s3)["changes"]["new"] == [{"ticker": "X", "signal": "Y"}]


def test_handler_publishes_and_refuses_a_short_book():
    s3 = seed()
    BOOK[:] = FAKE_BOOK
    lf.MIN_LEADERS = 5
    lf._s3 = lambda: s3
    BEGUN.clear()
    ctx = SimpleNamespace(get_remaining_time_in_millis=lambda: 290000)
    out = lf.lambda_handler({"trigger": "test"}, ctx)
    assert BEGUN == [{"trigger": "test"}] and out["statusCode"] == 200 and out["key"] == "data/leaders.json" and out["leaders"] == 16 and out["etfs"] >= 9 and out["errors"] == 0
    put = s3.puts[-1]
    assert put["Key"] == "data/leaders.json" and put["ContentType"] == "application/json" and put["CacheControl"] == "max-age=300"
    doc = json.loads(put["Body"])
    assert doc["trigger"] == {"kind": "test"} and doc["version"] == lf.VERSION
    # the universe guard: a factor book that yields a handful of leaders is a broken read, not a market
    n = len(s3.puts)
    lf.MIN_LEADERS = 100
    try:
        lf.lambda_handler({}, ctx)
        raise AssertionError("a short book must not publish")
    except RuntimeError as e:
        assert "previous publication stands" in str(e)
    BOOK[:] = []
    try:
        lf.lambda_handler({}, ctx)
        raise AssertionError("an unreadable book must not publish")
    except RuntimeError as e:
        assert "factor book" in str(e)
    assert len(s3.puts) == n


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok", t.__name__)
    print("%d tests passed" % len(tests))
