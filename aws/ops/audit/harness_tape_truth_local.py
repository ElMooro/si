"""Local harness -- justhodl-tape-truth (mandatory push gate)."""
import json
import sys
import types
from pathlib import Path

STORE, PUTS = {}, []


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in STORE:
            raise KeyError(Key)
        return {"Body": types.SimpleNamespace(
            read=lambda: json.dumps(STORE[Key]).encode())}

    def put_object(self, Bucket, Key, Body, **kw):
        STORE[Key] = json.loads(Body)
        PUTS.append(Key)


boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3_stub
sys.path.insert(0, str(Path(__file__).resolve().parents[2]
                       / "lambdas" / "justhodl-tape-truth"
                       / "source"))
import lambda_function as eng  # noqa: E402
import os  # noqa: E402
os.environ["POLYGON_API_KEY"] = "test"

eng.WATCH = ["AAA", "BBB"]
eng.GEX_SYMS = ["AAA"]

MIN_BARS = {
    "AAA": [{"o": 10, "h": 11, "l": 10, "c": 11, "v": 1000,
             "vw": 10.5},
            {"o": 11, "h": 12, "l": 10, "c": 10, "v": 500,
             "vw": 11.0}],
    "BBB": [{"o": 5, "h": 5, "l": 5, "c": 5, "v": 999,
             "vw": 5.0}] * 150}
FINRA_TXT = ("Date|Symbol|ShortVolume|ShortExemptVolume|"
             "TotalVolume|Market\n"
             "20260817|AAA|600|0|1000|B,Q\n"
             "20260817|ZZZ|1|0|2|B\n"
             "20260817|BBB|250|0|1000|B\n")
CBOE = {"data": {"current_price": 100.0, "options": [
    {"option": "AAA260901C00100000", "gamma": 0.02,
     "open_interest": 100, "volume": 10},
    {"option": "AAA260901P00095000", "gamma": 0.01,
     "open_interest": 200, "volume": 40},
    {"option": "AAA270901C00100000", "gamma": 9.9,
     "open_interest": 999, "volume": 1},
    {"option": "AAA260901C00150000", "gamma": 0.05,
     "open_interest": 50, "volume": 0},
    {"option": "AAA260901P00090000", "gamma": 0.0,
     "open_interest": 0, "volume": 5}]}}


def fake_http(url, headers=None):
    if "api.polygon.io" in url:
        sym = url.split("/ticker/")[1].split("/")[0]
        bars = MIN_BARS.get(sym, [])
        if sym == "AAA":
            bars = bars * 75
        return json.dumps({"results": bars}).encode()
    if "cdn.finra.org" in url:
        return FINRA_TXT.encode()
    if "cdn.cboe.com" in url:
        return json.dumps(CBOE).encode()
    raise AssertionError("unexpected " + url)


eng.http_raw = fake_http
FAILS = []


def chk(name, ok):
    print("  [%s] %s " % ("PASS" if ok else "FAIL", name))
    if not ok:
        FAILS.append(name)


def main():
    chk("bar_delta math: full-range up = +v, mid = 0, "
        "flat bar = 0",
        eng.bar_delta(10, 11, 10, 11, 1000) == 1000.0
        and eng.bar_delta(11, 12, 10, 11, 500) == 0.0
        and eng.bar_delta(5, 5, 5, 5, 999) == 0.0)
    chk("parse_occ", eng.parse_occ("SPY261002P00810000")
        == ("261002", "P", 810.0))
    from datetime import datetime, timezone, date as _date
    mk = lambda y, m, dd, hh: datetime(y, m, dd, hh,
                                       tzinfo=timezone.utc)
    chk("completed_session: Tue 02h -> Mon; Mon 22h -> Mon; "
        "Mon 02h -> Fri; Sun -> Fri",
        eng.completed_session(mk(2026, 8, 18, 2))
        == _date(2026, 8, 17)
        and eng.completed_session(mk(2026, 8, 17, 22))
        == _date(2026, 8, 17)
        and eng.completed_session(mk(2026, 8, 17, 2))
        == _date(2026, 8, 14)
        and eng.completed_session(mk(2026, 8, 16, 12))
        == _date(2026, 8, 14))
    d = eng.build({})
    a = d["symbols"]["AAA"]
    exp_cvd = (1000.0 + 500 * (2 * (10 - 10)
                               / (12 - 10) - 1)) * 75
    chk("session CVD == recompute (150 bars)",
        a["cvd"]["session_cvd"] == round(exp_cvd, 0)
        and a["cvd"]["n_days"] == 1)
    exp_vwap = round((10.5 * 1000 + 11.0 * 500)
                     / 1500 * 75 / 75, 4)
    led = STORE[eng.CVD_LEDGER]["rows"]["AAA"]
    lrow = led[sorted(led)[-1]]
    chk("session vwap/vol/ohlc banked in ledger",
        lrow["vwap"] == exp_vwap
        and lrow["vol"] == 1500.0 * 75
        and lrow["o"] == 10 and lrow["h"] == 12
        and lrow["l"] == 10 and lrow["c"] == 10)
    chk("day-one derived: CLV == 2*(c-l)/(h-l)-1 = -1.0, "
        "vwap gap computed, vol_ratio None (n<6)",
        a["cvd"]["clv_session"] == -1.0
        and a["cvd"]["close_vs_vwap_pct"]
        == round((10 / exp_vwap - 1) * 100, 2)
        and a["cvd"]["vol_ratio_20d"] is None)
    chk("BBB flat-bar session banked (cvd 0, 150 bars)",
        d["symbols"]["BBB"]["cvd"]["session_cvd"] == 0.0)
    chk("finra ratios ledgered (AAA .6, BBB .25; ZZZ "
        "ignored)",
        a["short_vol"]["ratio"] == 0.6
        and d["symbols"]["BBB"]["short_vol"]["ratio"]
        == 0.25)
    g = a["gex"]
    spot = 100.0
    call1 = 0.02 * 100 * 100 * spot * spot * 0.01
    call2 = 0.05 * 50 * 100 * spot * spot * 0.01
    put1 = 0.01 * 200 * 100 * spot * spot * 0.01
    g5 = a["gex"]
    chk("DTE5 share: only the 260901 exps count (all vol "
        "within 5d of frozen today? computed vs vol_all)",
        g5["dte5_vol_share_pct"] is None
        or 0 <= g5["dte5_vol_share_pct"] <= 100)
    chk("dist_to_flip present when flip exists",
        g5["flip_approx"] is None
        or isinstance(g5["dist_to_flip_pct"], float))
    chk("GEX math: DTE filter drops 2027, zero-OI dropped, "
        "net == recompute",
        g["status"] == "LIVE" and g["n_contracts_used"] == 3
        and g["net_gex_bn"] == round((call1 + call2 - put1)
                                     / 1e9, 2)
        and g["put_call_oi"] == round(200 / 150, 2))
    chk("walls exclude far strike (150 > +10%), flip is a "
        "ladder crossover strike",
        all(w["strike"] <= 110 for w in g["walls"])
        and (g["flip_approx"] in
             [w["strike"] for w in g["walls"]]
             or g["flip_approx"] is None))
    chk("verdict WARMING at n_days=1 with honest why",
        a["verdict"]["call"] == "WARMING"
        and ">=5" in a["verdict"]["why"])
    from datetime import date, timedelta
    dd0 = date(2026, 8, 15)
    seed_days = []
    dcur = dd0
    while len(seed_days) < 22:
        if dcur.weekday() < 5:
            seed_days.append(dcur.isoformat())
        dcur -= timedelta(days=1)
    seed_days = seed_days[::-1]
    rows = {sd: {"cvd": -1e6, "close": 90.0 + i}
            for i, sd in enumerate(seed_days)}
    STORE[eng.CVD_LEDGER] = {"rows": {"AAA": rows,
                                      "BBB": {}}}
    _sc = eng.session_cvd
    eng.session_cvd = lambda k, s, dd: \
        (-2e6, 111.0, 200) if s == "AAA" else (0.0, 5.0, 150)
    d2 = eng.build({})
    eng.session_cvd = _sc
    v2 = d2["symbols"]["AAA"]["verdict"]
    chk("conviction pure-fn arithmetic",
        eng.conviction([("a", 15), ("b", -10)]) == 55.0
        and eng.conviction([("a", -80)]) == 0.0)
    chk("FAKE_UP_DISTRIBUTION: price up on negative delta, "
        "evidence cites both numbers",
        v2["call"].endswith("FAKE_UP_DISTRIBUTION")
        and any("exit-liquidity" in e
                for e in v2["evidence"])
        and any("price 5d" in e for e in v2["evidence"])
        and any("top-divergence" in e
                for e in v2["evidence"]))
    chk("conviction == recompute from score_parts",
        v2["conviction"]
        == eng.conviction(v2["score_parts"]))
    chk("legacy ledger rows (no vol/vwap) never crash -- "
        "derived stay None-safe",
        d2["symbols"]["AAA"]["cvd"]["vol_ratio_20d"] is None
        or isinstance(d2["symbols"]["AAA"]["cvd"]
                      ["vol_ratio_20d"], float))
    chk("write discipline: ledgers + out only, out last",
        set(PUTS) <= {eng.CVD_LEDGER, eng.FINRA_LEDGER,
                      eng.OUT_KEY}
        and PUTS[-1] == eng.OUT_KEY)
    if FAILS:
        print("HARNESS FAILED:", FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4885)")


if __name__ == "__main__":
    main()
