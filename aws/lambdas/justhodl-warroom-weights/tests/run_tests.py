#!/usr/bin/env python3
"""Tests of the canaries' record (canary_record.py, record_io.py) on a real cut of the warehouse.

    python3 aws/lambdas/justhodl-warroom-weights/tests/run_tests.py

No network, no AWS. The fixture is tests/fixtures/canary-record-cut.json.gz (ops 6996 / 6997, 2026-11-27).
"""
import ast
import gzip
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SRC = ROOT / "aws/lambdas/justhodl-warroom-weights/source"
sys.path.insert(0, str(SRC))
import canary_record as R  # noqa: E402
import record_io as IO  # noqa: E402

FX = json.loads(gzip.decompress((ROOT / "tests/fixtures/canary-record-cut.json.gz").read_bytes()))
DEFS = json.loads((SRC / "canary_defs.json").read_text(encoding="utf-8"))
CLOSES, DROPPED = R.clean_closes(FX["spx"])
VOTING = [k for k, v in FX["ciss"].items() if v.get("pctile") is not None][:40]
CISS = {k: dict(v, votes_live=(k in VOTING)) for k, v in FX["ciss"].items()}
_REC = {}


def record():
    if "r" not in _REC:
        _REC["r"] = R.build(DEFS, FX["series"], CLOSES, ciss=CISS, cut_day=CLOSES[-1][0], live=FX["live"])
    return _REC["r"]


def near(a, b, tol=1e-9):
    return abs(a - b) <= tol


# ── the index ───────────────────────────────────────────────────────────────────────────────────────────────────────
def test_the_weekday_and_week_arithmetic_agree_with_the_calendar():
    import datetime
    for d in (19271230, 19871019, 20000229, 20240101, 20240106, 20240107, 20261125):
        assert R._ord(d) % 7 == datetime.date(d // 10000, d // 100 % 100, d % 100).weekday(), d
    assert R._months_back(20260131, 3) == 202510 and R._months_back(20260331, 3) == 202512 and R._months_back(20261125, 12) == 202511


def test_stray_quarterly_rows_are_taken_out_of_the_index_and_no_fall_is_invented():
    assert DROPPED == {"weekend": 51, "second_row": 112, "stray_quarter_row": 7}, DROPPED
    days = [d for d, _ in CLOSES]
    assert len(days) == len(set(days)) and days == sorted(days)
    raw = dict()
    for d, v in FX["spx"]:
        raw.setdefault(d, []).append(v)
    assert raw[20210301] == [3901.82, 4204.11] and dict(CLOSES)[20210301] == 3901.82     # the daily close stays, the quarter's last close goes
    assert 20240901 in raw and 20240901 not in dict(CLOSES)                               # a Sunday row
    moves = [(CLOSES[i][0], CLOSES[i][1] / CLOSES[i - 1][1] - 1) for i in range(1, len(CLOSES)) if CLOSES[i][0] > 19500101]
    big = sorted(d for d, r in moves if abs(r) > 0.07)
    assert big == [19871019, 19871021, 19871026, 20080929, 20081009, 20081013, 20081015, 20081028, 20090323, 20200309, 20200312, 20200313, 20200316, 20200324, 20200406, 20250409], big
    peaks = [e["peak"] for e in R.episodes(CLOSES)]
    for known in (19870825, 20000324, 20071009, 20100423, 20110429, 20200219, 20220103, 20250219):
        assert known in peaks, known
    assert not any(20210101 <= p <= 20211231 for p in peaks) and not any(20231101 <= p <= 20240930 for p in peaks)   # the falls the stray rows had invented


def test_what_followed_a_month_end_is_counted_on_closes():
    mkt = {m["day"]: m for m in R.market(CLOSES)}
    px = dict(CLOSES)
    days = [d for d, _ in CLOSES]
    for day in (20200131, 20071031, 20170630, 19870831):
        i = days.index(day)
        lo = min(px[d] for d in days[i + 1:i + 127])
        assert near(mkt[day]["fall_dd"], lo / px[day] - 1.0), day
        assert mkt[day]["fall"] == (lo / px[day] - 1.0 <= -0.10)
        hi = max(px[d] for d in days[i - 251:i + 1])
        assert mkt[day]["near_high"] == (px[day] >= 0.95 * hi)
    assert mkt[20200131]["fall"] and mkt[20071031]["fall"] and mkt[19870831]["fall"] and not mkt[20170630]["fall"]
    assert mkt[days[-1]]["fall"] is None                                                  # its window has not closed: never counted
    eps = R.episodes(CLOSES)
    assert all(e["depth"] <= -0.10 for e in eps) and all(e["peak"] < e["crossed"] <= e["low"] for e in eps)


# ── one canary ──────────────────────────────────────────────────────────────────────────────────────────────────────
def test_a_macro_grid_canary_is_rebuilt_by_the_grids_own_rule():
    defn = next(d for d in DEFS["grid"] if d["key"] == "building_permits")              # yoy, limit 200, a fall is stress, monthly
    days = [m["day"] for m in R.market(CLOSES)]
    path, used, freq, lag = R.grid_path(defn, FX["series"], days, CLOSES[-1][0])
    assert used == "PERMIT" and freq == "m"
    pts = FX["series"]["PERMIT"]["p"]
    day = 20190628
    avail = [i for i, (d, _) in enumerate(pts) if R._ord(d) + lag <= R._ord(day)]
    j = avail[-1]
    yoy = [pts[i][1] / pts[i - 12][1] - 1.0 for i in range(12, j + 1)]
    window = yoy[-(defn["limit"] - 12):]
    z = (window[-1] - statistics.mean(window)) / statistics.stdev(window)
    assert near(path[day], round(max(0.0, min(100.0, 50 - 22 * z)), 1)), (path[day], z)


def test_a_daily_series_kept_weekly_uses_the_same_span():
    defn = next(d for d in DEFS["grid"] if d["key"] == "yield_curve")                   # level, 2,600 daily observations, a fall is stress
    days = [m["day"] for m in R.market(CLOSES)]
    path, used, freq, lag = R.grid_path(defn, FX["series"], days, CLOSES[-1][0])
    assert used == "T10Y2Y" and freq == "d" and R._scaled(2600, "d") == 520 and R._scaled(63, "d") == 13 and R._scaled(52, "w") == 52
    pts = FX["series"]["T10Y2Y"]["p"]
    day = 20070330
    j = [i for i, (d, _) in enumerate(pts) if R._ord(d) + lag <= R._ord(day)][-1]
    window = [v for _, v in pts[j - 519:j + 1]]
    z = (window[-1] - statistics.mean(window)) / statistics.stdev(window)
    assert near(path[day], round(max(0.0, min(100.0, 50 - 22 * z)), 1))
    assert path[day] > 65 and path[day] > path[20030331] + 30                            # the curve was inverted in March 2007, steep in March 2003


def test_a_plumbing_canary_is_its_levels_percentile_by_polarity():
    days = [m["day"] for m in R.market(CLOSES)]
    s = FX["series"]["WTREGEN"]
    pts = [(int(a), float(b)) for a, b in s["p"]]
    up, lag = R.pct_path(pts, s["f"], days, CLOSES[-1][0], 1, 1300)
    down, _ = R.pct_path(pts, s["f"], days, CLOSES[-1][0], -1, 1300)
    day = 20190930
    j = [i for i, (d, _) in enumerate(pts) if R._ord(d) + lag <= R._ord(day)][-1]
    hist = [v for _, v in pts[max(0, j - 1299):j]]
    pct = round(100.0 * sum(1 for v in hist if v < pts[j][1]) / len(hist), 1)
    assert near(up[day], pct) and near(down[day], round(100.0 - 100.0 * sum(1 for v in hist if v < pts[j][1]) / len(hist), 1))


def test_nothing_published_later_enters_an_earlier_reading():
    days = [m["day"] for m in R.market(CLOSES)]
    defn = next(d for d in DEFS["grid"] if d["key"] == "initial_claims")
    base, _, _, lag = R.grid_path(defn, FX["series"], days, CLOSES[-1][0])
    edge = 20150101
    bent = {"IC4WSA": {"f": FX["series"]["IC4WSA"]["f"], "p": [[d, v * (7.0 if d >= edge else 1.0)] for d, v in FX["series"]["IC4WSA"]["p"]]}}
    other, _, _, _ = R.grid_path(defn, bent, days, CLOSES[-1][0])
    before = [d for d in base if d < edge]
    assert len(before) > 300 and all(base[d] == other[d] for d in before)                 # every reading before the edge is untouched
    assert any(base[d] != other[d] for d in base if d > edge + 10000)
    assert lag >= 1 and all(R._ord(d) >= R._ord(FX["series"]["IC4WSA"]["p"][23][0]) + lag for d in base)


def test_a_stopped_series_gives_no_reading_after_it_stops():
    days = [m["day"] for m in R.market(CLOSES)]
    s = FX["series"]["TEDRATE"]                                                           # ended January 2022
    path, lag = R.pct_path([(int(a), float(b)) for a, b in s["p"]], s["f"], days, CLOSES[-1][0], 1, 1300)
    assert max(path) < 20220801 and lag == R.LAG_DAYS["d"]


# ── scoring ─────────────────────────────────────────────────────────────────────────────────────────────────────────
def test_the_score_is_the_area_under_the_curve():
    assert R.auc([(1, False), (2, False), (3, True), (4, True)]) == 1.0
    assert R.auc([(1, True), (2, True), (3, False), (4, False)]) == 0.0
    assert R.auc([(1, False), (1, True)]) == 0.5 and R.auc([(5, True), (5, True), (5, False), (5, False)]) == 0.5
    assert R.auc([(1, True)]) is None and R.auc([]) is None
    assert near(R.auc([(1, False), (2, True), (3, False), (4, True)]), 0.75)
    a, se = R.auc_with_error([(1990 + i // 4, i % 7, i % 3 == 0) for i in range(80)])
    assert 0 <= a <= 1 and se is not None and se >= 0


def test_verdicts_and_weights_follow_the_stated_rule():
    base = {"months": 300, "falls_covered": 20, "fired_share": 0.4, "auc_early": 0.70, "auc_early_se": 0.05}
    assert R.verdict(base) == "leads"
    assert R.verdict(dict(base, auc_early=0.30)) == "wrong way"
    assert R.verdict(dict(base, auc_early=0.55)) == "no edge shown"
    assert R.verdict(dict(base, fired_share=0.97)) == "stuck" and R.verdict(dict(base, fired_share=0.0)) == "stuck"
    assert R.verdict(dict(base, months=80)) == "short record" and R.verdict(dict(base, falls_covered=2)) == "short record"
    assert R.verdict({"months": 0}) == "no record"
    assert R.weight_of(None) == 1.0 and R.weight_of({"months": 50, "auc_early": 0.9, "falls_covered": 9}) == 1.0
    assert near(R.weight_of(base), round(1 + 5 * 0.2 * 20 / 25.0, 3))
    assert R.weight_of(dict(base, auc_early=1.0, falls_covered=500)) == R.WEIGHT_CEIL and R.weight_of(dict(base, auc_early=0.0, falls_covered=500)) == R.WEIGHT_FLOOR


def test_bands_and_tables_cover_every_month_once():
    assert [R.band_of(p) for p in (0, 39.9, 40, 69.9, 70, 90, 97, 100)] == ["CALM", "CALM", "GUARDED", "GUARDED", "ELEVATED", "HIGH RISK", "CRISIS", "CRISIS"]
    assert R.band_of(None) == "NO DATA" and R.band_in(95, R.TURN_BANDS) == "RISING FAST" and R.band_in(10, R.TURN_BANDS) == "EASING"
    rec = record()
    t3 = rec["barometer"]["turns"]["3"]
    assert sum(r["months"] for r in t3["reliability"]) == sum(c["months"] for c in rec["barometer"]["level_against_turn"])
    lvl = rec["barometer"]["views"]["per_mechanism"]
    assert sum(r["months"] for r in lvl["reliability"]) == lvl["months"]
    for r in t3["reliability"] + lvl["reliability"]:
        assert r["fell"] <= r["months"] and r["early_fell"] <= r["early_months"] <= r["months"]


# ── the whole record on the real cut ────────────────────────────────────────────────────────────────────────────────
def test_the_record_on_the_real_cut():
    rec = record()
    assert rec["version"] == R.VERSION and rec["defined"] == 192 and rec["tested"] == 164
    assert rec["verdicts"] == {"no edge shown": 129, "no record": 28, "wrong way": 7, "short record": 5, "leads": 16, "stuck": 7}, rec["verdicts"]
    m = rec["market"]
    assert (m["months"], m["fell"], m["early_months"], m["early_fell"]) == (646, 138, 397, 58) and m["fall_share"] == 0.2136 and m["early_fall_share"] == 0.1461
    b = rec["barometer"]
    assert len(b["months"]) == 653 and b["from"] == 19720731 and b["months"][-1]["day"] == 20261125
    lvl, t3 = b["views"]["per_mechanism"], b["turns"]["3"]
    assert (lvl["auc"], lvl["auc_early"]) == (0.5948, 0.5081)                             # the level confirms, it does not warn
    assert (t3["earned"]["months"], t3["earned"]["auc"], t3["earned"]["auc_early"], t3["earned"]["auc_early_se"]) == (508, 0.669, 0.6478, 0.0633)
    assert t3["weights_now"] == {"ciss": 0.0, "macro_grid": 0.1311, "plumbing": 0.0733}
    shares = [r["fall_share"] for r in t3["reliability"]]
    assert shares == [0.1064, 0.189, 0.3626, 0.3077]
    assert [x["earned"]["auc_early"] for x in (b["turns"]["1"], b["turns"]["6"], b["turns"]["12"])] == [0.5803, 0.54, 0.4537]   # every span tried is kept
    assert b["months"][-1]["turn"] == 5.29 and b["months"][-1]["pct_turn"] == 90.1


def test_every_canary_row_says_what_it_is_and_why_it_has_no_record():
    rec = record()
    ids = [c["id"] for c in rec["canaries"]]
    assert len(ids) == len(set(ids)) == 192
    for c in rec["canaries"]:
        assert c["mechanism"] in ("macro_grid", "plumbing", "ciss") and c["name"] and c["rule"] and c["verdict"]
        if not c.get("months"):
            assert c["verdict"] == "no record" and c["why_not"], c["id"]
        else:
            assert c["why_not"] is None and 0 <= c["fired_share"] <= 1 and c["from"] < c["to"] and R.WEIGHT_FLOOR <= c["weight"] <= R.WEIGHT_CEIL
    by = {c["id"]: c for c in rec["canaries"]}
    assert by["macro_grid:hyg_lqd"]["verdict"] == "no record" and by["plumbing:TGCR"]["why_not"] == "not a warehouse series (NYFED_RATES)"
    assert by["plumbing:SBCACBW027SBOG"]["verdict"] == "stuck" and by["plumbing:TEMPHELPS"]["votes_live"] is False
    assert by["macro_grid:em_hy_ytw"]["verdict"] == "leads" and by["macro_grid:swap_line_usage"]["verdict"] == "wrong way"
    assert sum(1 for c in rec["canaries"] if c["mechanism"] == "ciss" and c["votes_live"]) == 40


def test_a_rebuilt_reading_far_from_the_engines_own_is_flagged():
    rec = record()
    by = {c["id"]: c for c in rec["canaries"]}
    semi = by["macro_grid:semiconductor_ip"]                                              # the warehouse copy mixes two vintages from July 2026
    assert "newest 18 months are left out" in semi["flag"] and semi["last_day"] <= 20250530
    assert "rebuilt 52.2 against the engine's own 94.4" in by["macro_grid:hy_credit_oas"]["flag"]
    lc = rec["live_check"]
    assert lc["canaries"] == 114 and lc["median_gap"] == 0.2 and "macro_grid:semiconductor_ip" in lc["flagged"]
    assert by["macro_grid:building_permits"].get("flag") is None and abs(by["macro_grid:building_permits"]["gap_to_live"]) < 2


def test_todays_reading_is_placed_on_the_records_own_scale():
    rec = record()
    now = R.live_reading(rec, FX["live"], today=20261126)
    assert now["canaries"] == 114 and now["of_tested"] == 164
    assert now["level"] == now["per_mechanism"] and 0 <= now["level"]["percentile"] <= 100
    assert now["level"]["band"] == R.band_of(now["level"]["percentile"]) and now["level"]["after"]["band"] == now["level"]["band"]
    t = now["turn"]
    assert t["against_day"] == 20260831 and t["days_back"] == 87 and t["against_from"].startswith("the record")
    want = sum(t["weights"][m] * t["by_mechanism"][m] for m in t["weights"]) / sum(t["weights"].values())
    assert abs(t["change"] - want) < 0.06 and t["band"] == R.band_in(t["percentile"], R.TURN_BANDS)
    assert now["cell"]["level"] in ("low", "middle", "high") and now["cell"]["turn"] in ("easing", "steady", "rising")
    mine = R.live_reading(rec, FX["live"], today=20261126, base={"day": 20260826, "mech": {"macro_grid": 40.0, "plumbing": 60.0, "ciss": 30.0}, "from": "the war room's own history"})
    assert mine["turn"]["against_day"] == 20260826 and mine["turn"]["by_mechanism"]["macro_grid"] == round(now["level"]["by_mechanism"]["macro_grid"] - 40.0, 1)
    assert R.live_reading(rec, {}) is None and R.live_reading(rec, {"macro_grid:hyg_lqd": 50}) is None      # an untested canary does not count


# ── reading and writing ─────────────────────────────────────────────────────────────────────────────────────────────
class FakeS3:
    def __init__(self, objects):
        self.objects, self.puts = objects, []

    def get_object(self, Bucket, Key):
        body = self.objects[Key]

        class Body:
            def read(self_inner):
                return body
        return {"Body": Body()}

    def put_object(self, **kw):
        self.puts.append(kw)


def _world():
    import datetime
    series = FX["series"]

    def ask(sid):
        s = series.get(sid)
        if not s:
            return None
        if s["f"] != "d":
            return [["%04d-%02d-%02d" % (d // 10000, d // 100 % 100, d % 100), v] for d, v in s["p"]]
        rows = []                                    # the cut kept a daily series weekly: hand it back daily, Monday to its last day of each week
        for d, v in s["p"]:
            o = R._ord(d)
            for k in range(o - o % 7, o + 1):
                x = datetime.date.fromordinal(k + 1)             # R._ord counts from one day later than date.toordinal
                rows.append([x.isoformat(), v])
        return rows

    import datetime
    bars = [[int(datetime.datetime(d // 10000, d // 100 % 100, d % 100, 14, 30, tzinfo=datetime.timezone.utc).timestamp()), 0, 0, 0, c, 0] for d, c in FX["spx"]]
    ciss_doc = {"generated_at": "2026-11-27T07:12:24+00:00", "series": [
        {"label": v["label"], "category": v["category"], "freq": v["freq"], "stats": {"pctile": v["pctile"]},
         "chart_points": [["%04d-%02d-%02d" % (d // 10000, d // 100 % 100, d % 100), x] for d, x in v["p"]]} for _, v in sorted(FX["ciss"].items())]}
    grid_doc = {"signals": [{"key": k.split(":", 1)[1], "available": True, "stress": s} for k, s in FX["live"].items() if k.startswith("macro_grid:")]}
    plumb_doc = {"raw_indicators": {k.split(":", 1)[1]: {"stress_score": s} for k, s in FX["live"].items() if k.startswith("plumbing:")}}
    s3 = FakeS3({IO.SPX_KEY: gzip.compress(json.dumps({"bars": bars}).encode()), "data/ciss-stress.json": json.dumps(ciss_doc).encode(),
                 "data/canary-grid.json": json.dumps(grid_doc).encode(), "data/plumbing-stress.json": json.dumps(plumb_doc).encode()})
    return s3, ask


def test_the_reader_shapes_what_the_warehouse_answers():
    assert IO._ymd("2026-11-25") == 20261125 and IO._ymd("2026-11") == 20261128 and IO._ymd("1998-W53") is None and IO._ymd("") is None
    daily = [["2026-11-%02d" % d, float(d)] for d in (16, 17, 18, 19, 20, 23, 24, 25)]
    got = IO.shape(daily)
    assert got["f"] == "d" and got["n_raw"] == 8 and got["p"] == [[20261120, 20.0], [20261125, 25.0]]      # the last observation of each week
    monthly = IO.shape([{"date": "2026-%02d-01" % m, "value": str(m)} for m in range(1, 9)] + [{"date": "2026-09-01", "value": "."}])
    assert monthly["f"] == "m" and len(monthly["p"]) == 8
    assert IO.shape([["2026-01-01", 1.0]]) is None
    ids = IO.series_ids(DEFS)
    assert len(ids) == len(set(ids)) and "BAMLC0A4CBBB" in ids and "BAMLC0A1CAAA" in ids and "TRESEGJPM052N" in ids and "RRPONTSYD" in ids
    assert not any(":" in x or "/" in x for x in ids) and "TGCR" not in ids                              # only plain warehouse series are asked
    src = (SRC / "record_io.py").read_text(encoding="utf-8")
    assert "api_key" not in src and "stlouisfed" not in src and "financialmodelingprep" not in src         # no provider, no key


def test_a_run_writes_one_document_and_a_dry_run_writes_none():
    s3, ask = _world()
    summary, rec = IO.run(s3, "bucket", dry_run=True, ask=ask, sleep=lambda s: None, log=lambda *a: None)
    assert summary["ok"] and summary["written"] is False and s3.puts == []
    assert rec["tested"] == 164 and rec["inputs"]["series_missing"] == ["LRUNTTTTCHM156S", "LRHUTTTTCHM156S"] and rec["inputs"]["closes_dropped"] == DROPPED
    assert rec["barometer"]["turns"]["3"]["earned"]["auc_early"] == record()["barometer"]["turns"]["3"]["earned"]["auc_early"]   # the same record through the reader
    summary, rec = IO.run(s3, "bucket", dry_run=False, ask=ask, sleep=lambda s: None, log=lambda *a: None)
    assert summary["written"] and [p["Key"] for p in s3.puts] == ["data/canary-record.json"]
    doc = json.loads(s3.puts[0]["Body"])
    assert doc["document"] == "canary-record.v1" and doc["engine"] == "justhodl-warroom-weights" and doc["generated_at"] and len(doc["canaries"]) == 192


def test_a_thin_record_is_not_published():
    s3, ask = _world()
    few = set(IO.series_ids(DEFS)[:10])
    summary, _ = IO.run(s3, "bucket", dry_run=False, ask=lambda sid: ask(sid) if sid in few else None, sleep=lambda s: None, log=lambda *a: None)
    assert not summary["ok"] and summary["written"] is False and s3.puts == [] and "canaries could be rebuilt" in summary["refused"]
    assert IO.fit_to_stand({"tested": 150, "barometer": {"months": [0] * 500}, "market": {"closes": 100}}).startswith("only 100 index closes")
    assert IO.fit_to_stand({"tested": 150, "barometer": {"months": [0] * 500}, "market": {"closes": 24000}}) is None


def test_the_handler_keeps_the_proxy_study_and_adds_the_record():
    src = (SRC / "lambda_function.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    handlers = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "lambda_handler"]
    assert len(handlers) == 2 and "_proxy_study = lambda_handler" in src                    # the study as it was, wrapped
    wrapper = ast.get_source_segment(src, handlers[1])
    assert 'if not dry and not event.get("record_only")' in wrapper and "record_io.run(S3, BUCKET, dry_run=dry)" in wrapper
    assert "url[:80]" not in src and src.count('% (_shown(url), ') == 2                     # a key in a query string never reaches a log line
    for name in ("CRISES", "event_study", "build_proxies", "mech_proxy", "roll_pctile"):
        assert name in src
    cfg = json.loads((SRC.parent / "config.json").read_text(encoding="utf-8"))
    assert cfg["timeout"] == 900 and len(cfg["description"]) <= 256


def main():
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print("ok   %s" % name)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            import traceback
            print("FAIL %s: %s" % (name, exc))
            traceback.print_exc(limit=3)
    print("%d tests, %d failed" % (len(tests), failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
