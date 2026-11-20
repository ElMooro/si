"""justhodl-strength (Khalid 2026-11-04: "Strength vs S&P 500 ... most matched fund flows is missing; also improve the
strength and add which one has been in long accumulation with very very tight price spread").

Pins: a reader (no provider URL, no urllib, no Lambda invoke), the trigger cap at its entry, its declared outputs, the
Scheduler entry in the committed manifest matching its config (03:40 UTC, after the Polygon EOD bank's 03:10 fold), the
sources it stands on (the bank manifest, the native ETF desk, the canonical flow packet, the 13F per-ticker table) and
the cadence words its observability block prints.
"""
import importlib.util
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
CONFIG = json.loads((ROOT / "aws/lambdas/justhodl-strength/config.json").read_text(encoding="utf-8"))
SOURCE = ROOT / "aws/lambdas/justhodl-strength/source/lambda_function.py"
TEXT = SOURCE.read_text(encoding="utf-8")
WIDE = (ROOT / "aws/lambdas/justhodl-strength/source/wide.py").read_text(encoding="utf-8")       # 1.1.0: the whole-market pass
TAPE = ROOT / "aws/lambdas/justhodl-strength/source/leaders_tape.py"


def entries():
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in MANIFEST.get(sec, [])}


def load_engine():
    shared = str(ROOT / "aws/shared")
    if shared not in sys.path:
        sys.path.insert(0, shared)
    spec = importlib.util.spec_from_file_location("strength_engine", SOURCE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["strength_engine"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_reader_only_with_the_trigger_cap():
    assert ".invoke(" not in TEXT and 'boto3.client("lambda"' not in TEXT, "the strength engine is a reader: it must not start engines"
    assert "urllib" not in TEXT and "api.polygon.io" not in TEXT and "financialmodelingprep" not in TEXT, "no provider call: the bank answers"
    assert "chain_guard.begin(event, context)" in TEXT
    assert 'OUT_KEY = os.environ.get("OUT_KEY", "data/strength.json")' in TEXT and "put_json(s3, OUT_KEY, out)" in TEXT


def test_schedule_declared_after_the_bank_fold():
    sch = CONFIG["eventbridge_scheduler"]
    e = entries()[("scheduler", sch["schedule_name"])]
    assert e["expr"] == sch["cron"] == "cron(40 3 * * ? *)" and e["state"] == "ENABLED" and e["targets"][0]["arn"].endswith(":function:justhodl-strength")
    # 1.1.0: 900 s - the desk keeps its 300 (DESK_BUDGET_S); the rest is the whole-market pass over ~7,400 bank files
    assert sch["role_arn"].endswith(":role/justhodl-scheduler-role") and CONFIG["runtime"] == "python3.12" and CONFIG["timeout"] == 900 and CONFIG["memory"] >= 2048
    assert "DESK_BUDGET_S = 300.0" in TEXT and "build(s3, now, remaining_s=min(remaining, DESK_BUDGET_S))" in TEXT
    bank = entries()[("scheduler", "justhodl-polygon-eod-daily")]
    m = re.match(r"cron\((\d+) (\d+) ", bank["expr"])
    bank_min = int(m.group(2)) * 60 + int(m.group(1))
    assert bank_min < 3 * 60 + 40, "the strength desk must run after the Polygon EOD bank folds the last session"


def test_sources_and_their_cadence_words():
    mod = load_engine()
    ent = entries()
    pins = {"justhodl-polygon-eod": ("scheduler", "justhodl-polygon-eod-daily"), "justhodl-etf-global-desk": ("events", "justhodl-etf-global-desk-daily"),
            "justhodl-etf-fund-flows": ("events", "justhodl-etf-fund-flows-daily"), "justhodl-13f-book": ("scheduler", "justhodl-13f-book-daily")}
    by = {s["engine"]: s for s in mod.SOURCES}
    for engine, (kind, name) in pins.items():
        e = ent[(kind, name)]
        m = re.match(r"cron\((\d+) (\d+) ", e["expr"])
        assert "%02d:%02d UTC" % (int(m.group(2)), int(m.group(1))) in by[engine]["schedule"], (engine, e["expr"], by[engine]["schedule"])
    assert by["justhodl-polygon-eod"]["key"] == "data/warm/polygon-full/eod/manifest.json" and by["justhodl-13f-book"]["key"] == "data/13f-tickers.json"
    assert mod.CONTRACT == "strength.v1" and mod.BENCH == "SPY" and dict(mod.WEIGHTS) == {"d": 0.10, "w": 0.20, "m": 0.30, "q": 0.25, "h": 0.15}
    assert sum(w for _, w in mod.WEIGHTS) == 1.0 and mod.LOOKBACK >= 400


def test_desk_sleeves_match_the_page_universe():
    """The engine owns the sleeves; strong.html renders what the artifact carries - the old page's list is the floor."""
    mod = load_engine()
    tickers = {t for items in mod.SLEEVES.values() for t, _ in items}
    for t in ("QQQ", "IWM", "XLK", "SMH", "GLD", "TLT", "AAPL", "NVDA", "SLV", "GDX", "IBIT", "ETHA"):
        assert t in tickers
    # 1.0.7 (Khalid 2026-11-05: "make sure it covers all asset classes: stocks, etfs, bonds, high yields bonds, gold, silver, metal everything")
    assert set(mod.SLEEVES) == {"ETF", "STOCK", "SECTOR", "COUNTRY", "BOND", "HIGH YIELD", "METAL", "COMMODITY", "CURRENCY", "CRYPTO", "VOLATILITY"}
    for t in ("HYG", "JNK", "USHY", "BKLN", "ANGL", "TLT", "IEF", "LQD", "AGG", "TIP", "EMB", "GLD", "IAU", "SLV", "SIVR", "PPLT", "PALL", "CPER", "DBB", "GDX", "SIL", "XME",
              "USO", "UNG", "DBC", "DBA", "UUP", "FXY", "IBIT", "ETHA", "VXX", "EFA", "EEM", "EWJ", "XLE", "XLV", "KRE"):
        assert t in tickers, t
    assert mod.CLASS_ORDER == ["stock", "equity_etf", "country", "bond", "high_yield", "cash", "gold", "silver", "metals", "commodity", "currency", "crypto", "volatility", "leveraged"]
    assert mod.asset_class_of("HYG", "fund", "") == ("high_yield", "corporate") and mod.asset_class_of("SLV", "fund", "") == ("silver", "bullion")
    assert mod.asset_class_of("TQQQ", "fund", "")[0] == "leveraged" and mod.asset_class_of("SQQQ", "fund", "")[0] == "leveraged", "leveraged / inverse funds are labelled and kept out of the ranks"
    assert mod.THESIS_DAYS == 252 and mod.DUMP_PCT == -1.0 and mod.FLAT_PCT == 0.25
    assert "thesis_block" in dir(mod) and mod.HISTORY_PREFIX == "data/strength/history/"
    # 1.0.9 (Khalid 2026-11-05 19:59 CT: "look back a month or a quarter back ... dump less than sp500 dump, rally more than sp500 rally, entire industry seeing
    # strength is even better"): three definitions of strong walked forward over 21 / 63 / 252 sessions; the asymmetry rank is the WEAKER side (both must hold)
    assert mod.PREDICTORS == ("strength", "asymmetry", "bigdays") and mod.THESIS_WINDOWS == (21, 63, 252) and mod.ASYM_N == 63 and mod.BIG_N == 126 and mod.BIG_MIN == 3 and mod.RALLY_PCT == 1.0
    cap = mod.capture([100.0, 99.0, 101.0, 98.0, 100.5, 99.8, 102.0, 100.0], [100.0, 98.5, 100.0, 97.0, 99.0, 98.0, 100.0, 99.5], 7)
    assert cap["both_pp"] == min(cap["down_excess_pp"], cap["up_excess_pp"]) and "bigday_pp" in cap and "dump_excess_pp" in cap
    # 1.0.9: a fund's fresh split (the iShares 2026-11-05 splits printed as -68% to -77% days with no sessions after them) is inferred from the split day alone
    bars = [{"date": "2026-%02d-%02d" % (1 + i // 28, 1 + i % 28), "c": 240.0 if i < 59 else 55.0, "v": 1e6 if i < 59 else 4.5e6} for i in range(60)]
    got = mod.infer_splits(bars, fund=True)
    assert len(got) == 1 and got[0]["factor"] == 0.25 and got[0]["evidence"].startswith("fresh"), got
    assert mod.infer_splits(bars, fund=False) == [], "a stock's fresh jump must sit within 6% of a common ratio"
    assert mod.FUND_SPLIT_TOL == 1.15


def test_the_whole_market_pass_is_a_guarded_reader():
    """1.1.0 (Khalid 2026-11-20: "i cant see which stocks and etfs are showing strength and dumping less than sp500"): the run also
    writes data/strength-wide.json - every listed stock and ETF against SPY. It reads S3 only, starts after the desk's own artifact
    is written, is imported inside its guard (a fault in it can never cost the desk its artifact) and shares the desk's constants."""
    import hashlib
    for text in (WIDE, TAPE.read_text(encoding="utf-8")):
        assert ".invoke(" not in text and "urllib" not in text and "api.polygon.io" not in text and "financialmodelingprep" not in text and "requests" not in text
        assert "put_object" not in text, "the whole-market module builds a document; only the handler writes"
    assert 'WIDE_KEY = os.environ.get("WIDE_KEY", "data/strength-wide.json")' in TEXT and CONFIG["env"]["WIDE_KEY"] == "data/strength-wide.json"
    assert TEXT.index("n = put_json(s3, OUT_KEY, out)") < TEXT.index("w = wide_pass(s3, now, out.get(\"as_of\"), left)"), "the desk's artifact is written before the pass starts"
    body = TEXT[TEXT.index("def wide_pass("):TEXT.index("def lambda_handler(")]
    assert body.index("try:") < body.index("import wide") and "except Exception" in body and "put_json(s3, WIDE_KEY, doc)" in body
    assert "\nimport wide" not in TEXT.split("def wide_pass(")[0], "wide is imported inside its guard, never at module load"
    assert '"flat_pct": FLAT_PCT, "dump_pct": DUMP_PCT, "rally_pct": RALLY_PCT' in body, "one definition of flat, dump and rally for the desk, the table and the page"
    assert 'CONTRACT = "strength-wide.v1"' in WIDE and "raise RuntimeError" in WIDE and "partial table is not published" in WIDE
    leaders = (ROOT / "aws/lambdas/justhodl-leaders/source/leaders_tape.py").read_bytes()
    assert hashlib.sha256(TAPE.read_bytes()).hexdigest() == hashlib.sha256(leaders).hexdigest(), "leaders_tape.py must stay byte-identical to the leaders engine's"
    # the page lays the 5-minute Polygon lane's forming bars over the table: the key the lane writes is the key the table names
    today = (ROOT / "aws/lambdas/justhodl-polygon-today/source/lambda_function.py").read_text(encoding="utf-8")
    assert 'TODAY_KEY = "data/warm/polygon-full/today.json.gz"' in WIDE and '"data/warm/polygon-full/today.json.gz"' in today
