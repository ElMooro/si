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
    assert sch["role_arn"].endswith(":role/justhodl-scheduler-role") and CONFIG["runtime"] == "python3.12" and CONFIG["timeout"] == 300 and CONFIG["memory"] >= 2048
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
