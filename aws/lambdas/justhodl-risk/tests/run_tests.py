"""Cloud-free regressions for justhodl-risk: transforms, point-in-time percentiles, alignment, scorecard, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import risk_model as RM  # noqa: E402
import lambda_function as L  # noqa: E402


def _daily(start, values):
    cal = RM.business_days(start, "2030-12-31")
    return [[cal[i], v] for i, v in enumerate(values)]


def test_transforms():
    pts = _daily("2020-01-01", [100, 110, 121, 99, 90])
    assert RM.transform(pts, "neg")[0][1] == -100
    dd = RM.transform(pts, "ddath")
    assert dd[2][1] == 0.0 and abs(dd[4][1] - (100 * (1 - 90 / 121))) < 1e-9
    dd2 = RM.transform(pts, "dd252")
    assert dd2[-1][1] == dd[-1][1]
    pts2 = _daily("2000-01-03", list(range(1, 400)))
    yoy = RM.transform(pts2, "yoy")
    assert yoy and yoy[-1][1] > 0
    chg = RM.transform(pts2, "chg63")
    assert chg[0][1] == 63 and len(chg) == len(pts2) - 63
    ret = RM.transform(pts2, "ret252")
    assert len(ret) == len(pts2) - 252
    ma = RM.transform(pts2, "ma200")
    assert ma and ma[-1][1] > 0 and RM.transform(pts2, "ma200neg")[-1][1] == -ma[-1][1]
    rv = RM.transform(pts2, "rvol20")
    assert rv and all(v[1] >= 0 for v in rv)


def test_expanding_percentile_is_point_in_time():
    pts = _daily("2020-01-01", [1, 2, 3, 4, 5, 100, 6])
    pct = RM.expanding_percentile(pts, min_n=3)
    # the 100 ranks top of its history at the time; later values do not change earlier ranks
    assert pct[0][1] == 100 * (2 + 0.5) / 3          # value 3 vs {1,2,3}
    assert pct[3][1] == 100 * (5 + 0.5) / 6          # value 100 vs 6 values -> top
    assert pct[4][1] == 100 * (5 + 0.5) / 7          # value 6 ranks under the 100
    rp = RM.rolling_pct([None, 1, 2, 3, 4, 5], win=3, min_n=2)
    assert rp[0] is None and rp[-1] == 100 * 2.5 / 3


def test_align_freshness():
    cal = RM.business_days("2024-01-01", "2024-01-31")
    pts = [["2024-01-02", 1.0], ["2024-01-10", 2.0]]
    al = RM.align(pts, cal, 5)
    assert al[0] is None                              # before first obs
    assert al[cal.index("2024-01-03")] == 1.0
    assert al[cal.index("2024-01-15")] == 2.0        # 5 days: still fresh
    assert al[cal.index("2024-01-16")] is None       # 6 days: stale, drops out


def test_pillars_composite_and_breadth():
    cal = RM.business_days("2024-01-01", "2024-01-10")
    comps = {"a": {"pillar": "p1", "pct_aligned": [90.0] * len(cal)},
             "b": {"pillar": "p1", "pct_aligned": [70.0] * len(cal)},
             "c": {"pillar": "p2", "pct_aligned": [None] * len(cal)},
             "d": {"pillar": "p3", "pct_aligned": [10.0] * len(cal)},
             "e": {"pillar": "p4", "pct_aligned": [50.0] * len(cal)},
             "f": {"pillar": "p5", "pct_aligned": [85.0] * len(cal)}}
    sp, cnt = RM.pillar_scores(comps, cal, ["p1", "p2", "p3", "p4", "p5"])
    assert sp["p1"][0] == 80.0 and sp["p2"][0] is None and cnt["p1"][0] == 2
    comp = RM.composite(sp, {"p1": 2.0, "p2": 1.0, "p3": 1.0, "p4": 1.0, "p5": 1.0}, cal, min_pillars=3)
    assert abs(comp[0] - (2 * 80 + 10 + 50 + 85) / 5) < 1e-9
    assert RM.composite(sp, {"p1": 1.0, "p2": 1.0}, cal, min_pillars=3)[0] is None
    assert RM.breadth(comps, cal)[0] == 40.0          # a, f of five live gauges


def test_scorecard_and_thresholds():
    cal = RM.business_days("2019-01-01", "2021-12-31")
    stress = [20.0] * len(cal)
    a, b = RM._idx(cal, "2020-02-19"), RM._idx(cal, "2020-03-23")
    for i in range(a, b + 1):
        stress[i] = 20 + 70 * (i - a) / max(1, b - a)
    froth = [30.0] * len(cal)
    for i in range(RM._idx(cal, "2019-11-01"), a):
        froth[i] = 75.0
    eps = [("2020-02-19", "2020-03-23", "COVID crash", "equity")]
    rows = RM.episode_scorecard(stress, froth, cal, eps, RM.rolling_pct(stress, win=200, min_n=50), RM.rolling_pct(froth, win=200, min_n=50))
    r = rows[0]
    assert r["stress_max"] == 90.0 and r["stress_max_date"] == "2020-03-23" and r["stress_peak_minus_trough_days"] == 0
    assert r["froth_max_pre_peak"] == 75.0 and r["stress_alert_hit"] and r["stress_extreme_hit"] and r["froth_alert_hit"]
    flags = RM.in_windows(cal, [(e[0], e[1]) for e in eps])
    f1 = RM.f1_at(stress, flags, cal, 65.0, cal[0], cal[-1])
    assert f1["precision"] == 100.0 and 0 < f1["recall"] < 100
    best, _ = RM.best_threshold(stress, flags, cal, cal[0], cal[-1])
    assert best["thr"] == 50.0
    fa = RM.false_alarms(froth, flags, cal, 70.0, 365, cal[0], cal[-1])
    assert fa["false_alarm_pct"] == 0.0
    summ = RM.scorecard_summary(rows)
    assert summ["episodes"] == 1 and summ["stress_alert_hit"] == 1


def test_parsers():
    fsi = RM.parse_fsi_csv("Date,OFR FSI,Credit,Equity valuation,Safe assets,Funding,Volatility,United States,Other advanced economies,Emerging markets\n"
                           "2000-01-03,2.14,0.54,-0.051,0.67,0.472,0.509,1.769,0.521,-0.15\n")
    assert fsi["ofr_fsi"] == [("2000-01-03", 2.14)] and fsi["ofr_em"][0][1] == -0.15
    ecb = RM.parse_ecb_csv("KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM,INSTRUMENT_FM,PROVIDER_FM_ID,DATA_TYPE_FM,TIME_PERIOD,OBS_VALUE\n"
                           "CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX,D,US,Z0Z,4F,EC,SS_CIN,IDX,2026-10-08,0.0082\n"
                           "CLIFS.M.AT._Z.4F.EC.CLIFS_CI.IDX,M,AT,_Z,4F,EC,CLIFS_CI,IDX,1990-01,0.133\n", "ciss_")
    assert ecb["ciss_US"] == [("2026-10-08", 0.0082)] and ecb["ciss_AT"] == [("1990-01-15", 0.133)]
    cm = RM.parse_coinmetrics_csv("time,CapMVRVCur,PriceUSD\n2010-07-18,146.0,0.09\n2026-05-24,,\n")
    assert cm["btc_price"] == [("2010-07-18", 0.09)] and cm["btc_mvrv"] == [("2010-07-18", 146.0)]
    assert RM.fred_points({"observations": [{"date": "2020-01-02", "value": "."}, {"date": "2020-01-03", "value": 1.5}]}) == [("2020-01-03", 1.5)]


def test_specs_consistent():
    ids = [c[0] for c in RM.STRESS_COMPONENTS + RM.FROTH_COMPONENTS]
    assert len(ids) == len(set(ids))
    for cid, pillar, label, src, kind, fresh, freq in RM.STRESS_COMPONENTS:
        assert pillar in RM.PILLAR_LABELS and freq in RM.MIN_HISTORY and fresh > 0
    for cid, pillar, label, src, kind, fresh, freq in RM.FROTH_COMPONENTS:
        assert pillar in RM.FROTH_LABELS
    assert set(RM.STRESS_WEIGHTS) == set(RM.PILLAR_LABELS) and set(RM.FROTH_WEIGHTS) == set(RM.FROTH_LABELS)
    for sid in L.FRED_KEYS:
        assert "/" not in sid
    assert all(k in L.FRED_KEYS or k in L.WARM_SERIES or k.startswith(("ofr_", "ciss_", "sovciss_", "btc_", "ebp", "gz_"))
               or k in ("vix_minus_vxv", "sofr_minus_iorb", "ccc_minus_bb")
               for k in {c[3] for c in RM.STRESS_COMPONENTS + RM.FROTH_COMPONENTS})


def test_zones_and_doctrine():
    assert L.stress_zone(85)[0].startswith("extreme") and L.stress_zone(10)[0] == "calm" and L.stress_zone(None)[0] == "n/a"
    assert L.froth_zone(75)[0].startswith("frothy") and L.froth_zone(50)[0] == "neutral"
    pk = RS.Packet(L.SLUG, "Risk", "t", "d", {"provider": "JustHodl"})
    pk.add_series("STRESS", "s", [["2026-10-08", 30.0], ["2026-10-09", 32.6]], unit="pct")
    pk.note("Descriptive engine — decision.call null.")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/risk/"
    assert L._path({"a": {"b": 3}}, "a.b") == 3 and L._path({"a": 1}, "a.b") is None


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for n, f in tests:
        try:
            f()
            print("PASS", n)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL", n, repr(exc))
    sys.exit(1 if failed else 0)
