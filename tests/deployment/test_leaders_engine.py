"""justhodl-leaders (Khalid 2026-11-04: "build a new engine called leaders and track how leaders on each industry are
doing compared to sp500 and whether they are in accumulation or distribution. Is smart money buying or selling them.
Buying inflow and outflows. The industry etfs inflows and outflows, how are they compared to their 250, 200, 100, 50,
20 moving average. Which are seeing golden cross and death cross. Also which are seeing double tops and double
bottoms. Climax, tight price. I want to be able to sort them ascending and descending. Make sure i can access it from
chart.html put an icon for it next to 13f").

Pins: a reader (no provider URL, no urllib, no Lambda invoke), the trigger cap at its entry, its declared output, the
Scheduler entry in the committed manifest matching its config (03:50 UTC, after the factor book, the Polygon EOD
bank's fold and the bottom desk), the cadence words its observability block prints against the manifest, the
definitions it shares with the strength desk (the coil and the split inference - one definition, two engines), the
reviewed industry -> fund catalog and its use by the Polygon bank, and the page that renders the artifact.
"""
import importlib.util
import json
import math
import random
import re
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
CONFIG = json.loads((ROOT / "aws/lambdas/justhodl-leaders/config.json").read_text(encoding="utf-8"))
SRC_DIR = ROOT / "aws/lambdas/justhodl-leaders/source"
SOURCE = SRC_DIR / "lambda_function.py"
TEXT = SOURCE.read_text(encoding="utf-8")
ALL_TEXT = "".join((SRC_DIR / n).read_text(encoding="utf-8") for n in ("lambda_function.py", "leaders_tape.py", "leaders_patterns.py"))
_CACHE = {}


def entries():
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in MANIFEST.get(sec, [])}


def minutes(expr):
    m = re.match(r"cron\((\d+) (\d+) ", expr)
    return int(m.group(2)) * 60 + int(m.group(1))


def load(name, path, extra=None):
    """One module from a file under a private name; the source directory is on sys.path only while it loads."""
    if name in _CACHE:
        return _CACHE[name]
    added = [str(ROOT / "aws/shared")] + ([str(extra)] if extra else [])
    for p in added:
        sys.path.insert(0, p)
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
    finally:
        for p in added:
            if p in sys.path:
                sys.path.remove(p)
    _CACHE[name] = mod
    return mod


def engine():
    return load("leaders_engine", SOURCE, SRC_DIR)


def tape():
    engine()
    return sys.modules["leaders_tape"]


def strength():
    return load("strength_engine_for_leaders", ROOT / "aws/lambdas/justhodl-strength/source/lambda_function.py")


def catalog():
    return load("industry_etf_catalog_for_leaders", ROOT / "aws/shared/industry_etf_catalog.py")


def test_reader_only_with_the_trigger_cap():
    assert ".invoke(" not in ALL_TEXT and 'boto3.client("lambda"' not in ALL_TEXT, "the leaders engine is a reader: it must not start engines"
    assert "urllib" not in ALL_TEXT and "requests" not in ALL_TEXT and "api.polygon.io" not in ALL_TEXT and "financialmodelingprep" not in ALL_TEXT and "massive.com" not in ALL_TEXT, "no provider call: the warehouse answers"
    assert "chain_guard.begin(event, context)" in TEXT
    assert 'OUT_KEY = os.environ.get("OUT_KEY", "data/leaders.json")' in TEXT and "put_json(s3, OUT_KEY, out)" in TEXT
    assert TEXT.count("put_object(") == 1 and "delete_object" not in ALL_TEXT, "one write: the artifact"
    assert "release_validation" not in CONFIG, "an ungoverned desk: no release_validation block"
    assert CONFIG["env"]["OUT_KEY"] == "data/leaders.json" and len(CONFIG["description"]) <= 256


def test_schedule_declared_after_its_inputs():
    sch = CONFIG["eventbridge_scheduler"]
    ent = entries()
    e = ent[("scheduler", sch["schedule_name"])]
    assert e["expr"] == sch["cron"] == "cron(50 3 * * ? *)" and e["state"] == "ENABLED" and e["timezone"] == "UTC" and e["targets"][0]["arn"].endswith(":function:justhodl-leaders")
    assert sch["role_arn"].endswith(":role/justhodl-scheduler-role") and CONFIG["runtime"] == "python3.12" and CONFIG["timeout"] == 300 and CONFIG["memory"] >= 2048
    mine = minutes(e["expr"])
    for name in ("justhodl-bank-factors-daily", "justhodl-polygon-eod-daily", "justhodl-bottom-daily"):
        assert minutes(ent[("scheduler", name)]["expr"]) < mine, "%s must have run before the leaders desk reads it" % name
    assert mine - minutes(ent[("scheduler", "justhodl-polygon-eod-daily")]["expr"]) >= 30, "the bank needs its fold time"
    # once per 24 hours, and nobody else declared for the same minute against this function
    assert [k for k, v in ent.items() if v["targets"] and v["targets"][0]["arn"].endswith(":function:justhodl-leaders")] == [("scheduler", "justhodl-leaders-daily")]
    assert "rate(" not in e["expr"] and e["expr"].split()[2:] == ["*", "*", "?", "*)"]


def test_sources_and_their_cadence_words():
    mod = engine()
    ent = entries()
    pins = {"justhodl-bank-factors": ("scheduler", "justhodl-bank-factors-daily"), "justhodl-polygon-eod": ("scheduler", "justhodl-polygon-eod-daily"), "justhodl-13f-book": ("scheduler", "justhodl-13f-book-daily"),
            "justhodl-etf-global-desk": ("events", "justhodl-etf-global-desk-daily"), "justhodl-etf-fund-flows": ("events", "justhodl-etf-fund-flows-daily"), "justhodl-polygon-bank": ("scheduler", "justhodl-polygon-bank-daily"),
            "justhodl-fmp-walker": ("scheduler", "justhodl-fmp-walker-daily"), "justhodl-phase-detector": ("scheduler", "justhodl-phase-detector-daily"), "justhodl-bottom": ("scheduler", "justhodl-bottom-daily")}
    assert {s["engine"] for s in mod.SOURCES} == set(pins), "every source engine is pinned to its schedule"
    for s in mod.SOURCES:
        e = ent[pins[s["engine"]]]
        m = re.match(r"cron\((\d+) (\d+) ", e["expr"])
        assert "%02d:%02d UTC" % (int(m.group(2)), int(m.group(1))) in s["schedule"], (s["engine"], e["expr"], s["schedule"])
        assert e["state"] == "ENABLED" and s["expected_h"] >= 24 and s["key"].startswith("data/")
    by = {(s["engine"], s["key"]) for s in mod.SOURCES}
    for pair in (("justhodl-polygon-eod", "data/warm/polygon-full/eod/manifest.json"), ("justhodl-13f-book", "data/13f-tickers.json"), ("justhodl-13f-book", "data/13f-book.json"),
                 ("justhodl-etf-global-desk", "data/etf-desk-research.json"), ("justhodl-etf-fund-flows", "data/provider-fund-flow-research.json")):
        assert pair in by, pair
    assert "daily 03:50 UTC" in (ROOT / "jh-leaders.js").read_text(encoding="utf-8"), "the page prints this desk's own schedule"


def test_contract_constants():
    mod, T = engine(), tape()
    assert mod.CONTRACT == "leaders.v1" and mod.ENGINE == "justhodl-leaders" and mod.BENCH == "SPY" and mod.TOP_N == 5 and mod.OUT_KEY == "data/leaders.json"
    assert T.MA_WINDOWS == (20, 50, 100, 200, 250), "Khalid: 250, 200, 100, 50, 20"
    assert mod.LOOKBACK >= 262 + 252, "the climax reads 262 sessions back from each of the last 30; a year of context on top"
    assert dict(T.HORIZONS) == {"d": 1, "w": 5, "m": 21, "q": 63, "h": 126, "y": 252} and abs(sum(w for _, w in T.WEIGHTS) - 1.0) < 1e-12 and abs(sum(w for _, w in T.AD_WEIGHTS) - 1.0) < 1e-12
    assert (T.AD_STRONG, T.AD_LEAN) == (60.0, 20.0) and T.CROSS_FRESH == 20 and T.GAP_DAYS == 10 and (T.HANDOVER_MIN, T.HANDOVER_MAX) == (0.7, 1.4)
    assert mod.MIN_CAP == 2e9 and mod.MIN_DV == 5e6 and mod.MIN_LEADERS >= 100 and mod.SMART_LEAN_SHARE == 0.10
    for k in ("leaders", "vs", "strength", "ma", "cross", "double_top", "double_bottom", "climax", "tight", "ad", "mf", "f13", "insider", "smart", "etf_flow", "industry", "history", "changes"):
        assert k in mod.DEFINITIONS and len(mod.DEFINITIONS[k]) > 40, k
    assert len(mod.LIMITATIONS) >= 6 and sum("{n_cohort}" in x for x in mod.LIMITATIONS) == 1


def _bars(closes, vols=None, last=date(2026, 11, 4)):
    days, d = [], last
    while len(days) < len(closes):
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    days = days[::-1]
    return [{"date": days[i].isoformat(), "o": c, "h": c * 1.006, "l": c * 0.994, "c": c, "v": (vols[i] if vols else 1_000_000.0)} for i, c in enumerate(closes)]


def _walk(n, drift, seed, start=100.0, vol=0.012):
    rnd = random.Random(seed)
    px, out = start, []
    for _ in range(n):
        px *= 1 + drift + rnd.gauss(0, vol)
        out.append(px)
    return out


def test_one_definition_of_the_coil_and_of_split_inference():
    """leaders_tape carries the strength desk's coil and its split inference; the two engines must read the same bars
    the same way - a change in one is a change in both, or this fails."""
    S, T = strength(), tape()
    spy = _bars(_walk(520, 0.0004, 1, 400.0, 0.008))
    series = {
        "random walk": _walk(520, 0.0003, 7),
        "tight base": _walk(380, 0.001, 8, 50.0) + [None] * 0,
        "long base": [80.0 * (1 + 0.02 * math.sin(i / 5.0)) for i in range(520)],
        "decline": _walk(520, -0.001, 9, 200.0),
    }
    series["tight base"] = series["tight base"] + [series["tight base"][-1] * (1 + 0.01 * math.sin(i / 3.0)) for i in range(140)]
    for name, closes in series.items():
        bars = _bars(closes)
        _, c_al, s_al = S.align(bars, spy)
        assert T.align(bars, spy)[1:] == (c_al, s_al), name
        rs_s, rs_t = S.rs_metrics(c_al, s_al), T.rs_metrics(c_al, s_al)
        assert rs_s["rs_trend_63"] == rs_t["rs_trend_63"], name
        assert S.coil(bars, c_al, s_al, rs_s["rs_trend_63"]) == T.coil(bars, c_al, s_al, rs_t["rs_trend_63"]), name
        assert S.bb_width(closes) == T.bb_width(closes) and S.base_lengths(closes) == T.base_lengths(closes), name
    assert (S.BASE_TIERS, S.BASE_MAX, S.CONTRACTION_MAX) == (T.BASE_TIERS, T.BASE_MAX, T.CONTRACTION_MAX) and dict(S.WEIGHTS) == dict(T.WEIGHTS) and S.COMMON_RATIOS == T.COMMON_RATIOS
    cases = {
        "2-for-1 the bank left raw": ([100.0] * 300 + [50.0] * 60, [1e6] * 300 + [2e6] * 60),
        "1-for-10 reverse": ([2.0] * 300 + [20.0] * 60, [1e7] * 300 + [1e6] * 60),
        "a 50% crash on eight times the volume": ([100.0] * 300 + [50.0] * 60, [1e6] * 300 + [8e6] * 5 + [1e6] * 55),
        "a x2.8 pop on news": ([60.0] * 300 + [168.0] * 60, [5e6] * 300 + [9e7] * 5 + [2e7] * 55),
        "a 3-for-2": ([90.0] * 300 + [60.0] * 60, [2e6] * 300 + [3e6] * 60),
        "nothing": (_walk(360, 0.0, 3), None),
    }
    for name, (closes, vols) in cases.items():
        bars = _bars(closes, vols)
        a, b = S.infer_splits(bars), T.infer_splits(bars)
        assert [(e["date"], e["factor"]) for e in a] == [(e["date"], e["factor"]) for e in b], name
        assert S.apply_splits(bars, a) == T.apply_splits(bars, b), name
    assert len(T.infer_splits(_bars(*cases["2-for-1 the bank left raw"]))) == 1 and T.infer_splits(_bars(*cases["a 50% crash on eight times the volume"])) == [] and T.infer_splits(_bars(*cases["a x2.8 pop on news"])) == []
    assert tuple(int(x) for x in S.VERSION.split(".")) >= (1, 0, 6), "the strength desk carries the volume bound on ratio-matched splits"


def test_industry_fund_catalog_and_the_bank_that_feeds_it():
    cat = catalog()
    assert len(cat.INDUSTRY_ETFS) >= 150 and len(cat.ALL_ETFS) >= 100 and set(cat.SECTOR_ETF.values()) <= set(cat.ALL_ETFS) and len(cat.SECTOR_ETF) == 11
    for t in cat.ALL_ETFS:
        assert t == t.upper() and re.fullmatch(r"[A-Z]{2,5}", t), t
    assert len(set(cat.ALL_ETFS)) == len(cat.ALL_ETFS) and cat.THIN <= set(cat.ALL_ETFS)
    for industry, funds in cat.INDUSTRY_ETFS.items():
        for t, role in funds:
            assert role in ("industry", "theme") and t in cat.ALL_ETFS, (industry, t, role)
    for industry in cat.INDUSTRY_ETFS:
        for sector in cat.SECTOR_ETF:
            got = cat.etfs_for(industry, sector)
            assert got and got[-1] == {"ticker": cat.SECTOR_ETF[sector], "role": "sector", "thin": False} or got[-1]["ticker"] != cat.SECTOR_ETF[sector]
            assert len({x["ticker"] for x in got}) == len(got)
            own = [x for x in got if x["role"] != "sector"]
            assert not own or not own[0]["thin"] or all(x["thin"] for x in own), "a thin fund never leads while a liquid one exists: %s" % industry
            break
    assert [x["ticker"] for x in cat.etfs_for("Semiconductors", "Technology")] == ["SMH", "SOXX", "XSD", "XLK"]
    assert cat.etfs_for("An industry FMP adds tomorrow", "Healthcare") == [{"ticker": "XLV", "role": "sector", "thin": False}] and cat.etfs_for("Unknown", None) == []
    # the Polygon bank pulls every catalog fund's ETF Global flow history first, every run, under the fund's own name
    spec = json.loads((ROOT / "aws/lambdas/justhodl-polygon-bank/source/polygon-bank-spec.json").read_text(encoding="utf-8"))
    assert spec["universes"]["desk_etfs"] == {"module": "industry_etf_catalog", "attr": "ALL_ETFS"}
    flows = next(d for d in spec["datasets"] if d["id"] == "etf-global-fund-flows")
    assert flows["priority_universe"] == "desk_etfs" and flows["entity_param"] == "composite_ticker" and flows["entity_field"] == "composite_ticker" and flows.get("epoch")
    for ds in ("etf-global-constituents", "etf-global-profiles"):
        d = next(x for x in spec["datasets"] if x["id"] == ds)
        assert d["entity_param"] == "composite_ticker" and d["entity_field"] == "composite_ticker" and d.get("epoch"), ds
    assert engine().BANK_FLOW_ROOT == "data/warm/polygon-bank/etf-global-fund-flows/"
    deps = (ROOT / "scripts/shared_dependents.py")
    assert deps.is_file() and "import industry_etf_catalog" in (ROOT / "aws/lambdas/justhodl-polygon-bank/source/lambda_function.py").read_text(encoding="utf-8")


def test_the_page_renders_what_the_engine_publishes():
    mod = engine()
    js = (ROOT / "jh-leaders.js").read_text(encoding="utf-8")
    html = (ROOT / "leaders.html").read_text(encoding="utf-8")
    assert 'KEY = "data/leaders.json"' in js and 'CONTRACT = "%s"' % mod.CONTRACT in js and '/jh-leaders.js' in html
    boards = set(mod.boards_of([]))
    page_boards = set(re.findall(r'\["([a-z_]+)", "[^"]+", "(?:up|dn|amber)", ', js))
    assert page_boards and page_boards <= boards, page_boards - boards
    for want in ("golden_cross", "death_cross", "double_top", "double_bottom", "buying_climax", "selling_climax", "tight", "accumulation", "distribution", "smart_buying", "smart_selling"):
        assert want in page_boards, want
    labels = set(re.findall(r"([a-z_0-9]+): \"", re.search(r"var RULE_LABEL = \{(.*?)\};", js, re.S).group(1)))
    assert labels >= set(mod.DEFINITIONS), "every definition the engine publishes has a reader-facing label: %s" % (set(mod.DEFINITIONS) - labels)
    signals = set(re.findall(r'"([A-Z ]+)": \["[^"]+", "(?:up|dn|s-up|s-dn|amber)"\]', js))
    vocab = set(mod.signals_of({"cross": {"type": "GOLDEN", "fresh": True, "near": "GOLDEN NEAR"}, "dtop": {"kind": "DOUBLE TOP", "state": "FORMING"}, "dbot": {"kind": "DOUBLE BOTTOM", "state": "CONFIRMED"}, "climax": {"type": "BUYING"},
                                    "tight": {"tier": "VERY TIGHT"}, "rs_new_high": True, "rs_new_low": True}))
    assert vocab <= signals | {"BULL STACK", "BEAR STACK"}, vocab - signals
    # the chart opens the desk from the button beside 13F
    eng = (ROOT / "jh-chart-engine.js").read_text(encoding="utf-8")
    rail = (ROOT / "jh-chart-tvrail.js").read_text(encoding="utf-8")
    i13, ild = eng.index("id=btn-13f"), eng.index("id=btn-ldr")
    assert 0 < i13 < ild and eng[i13:ild].count("<button") == 1 and 'window.jhOpenWorkspace("leaders")' in eng
    assert 'leaders: ["Leaders · industry leaders vs S&P 500", "/leaders.html"]' in rail
    order = (ROOT / "docs/ops/PAGE_ORDER.md").read_text(encoding="utf-8")
    assert "`leaders.html`" in order
