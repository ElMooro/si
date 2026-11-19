"""justhodl-stock-volatility draws its universe by the campaign scan's rule, and runs once a day.

Two engines read "every listed common stock and every plain ETF" from the factor book. The rule (what is a common
stock, which funds are geared or inverse, the dollar-volume floor) is written in both; this test runs both on the same
rows and fails when they stop agreeing, so the two desks never disagree about what a stock is.
"""
import importlib.util
import json
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LAMBDAS = ROOT / "aws" / "lambdas"
LOCAL = ("leaders_tape", "vol_measures", "chart_scans", "lambda_function")


def _load(engine):
    source = LAMBDAS / engine / "source"
    stubs = {"boto3": types.SimpleNamespace(client=lambda *a, **k: None), "botocore": types.ModuleType("botocore"),
             "botocore.config": types.SimpleNamespace(Config=lambda **k: k), "chain_guard": types.SimpleNamespace(begin=lambda *a, **k: 0),
             "bank_enrich": types.SimpleNamespace(rows=lambda **k: [])}
    saved = {k: sys.modules.get(k) for k in list(stubs) + list(LOCAL) + ["polygon_bankfirst"]}
    path = list(sys.path)
    for k in LOCAL:
        sys.modules.pop(k, None)
    sys.modules.update(stubs)
    sys.path[:0] = [str(source), str(ROOT / "aws" / "shared")]
    try:
        spec = importlib.util.spec_from_file_location(engine.replace("-", "_") + "_under_test", source / "lambda_function.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path[:] = path
        for k, old in saved.items():
            if old is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = old


def _row(sym, **kw):
    r = {"symbol": sym, "name": sym + " Inc", "sector": "Technology", "industry": "Software", "is_common": True, "is_etf": False, "is_otc": False, "mcap": 5e9,
         "dollar_vol_20d": 2e7, "ipo_date": "2015-01-02"}
    r.update(kw)
    return r


ROWS = [
    _row("AAA"), _row("BRK-B"), _row("OTCX", is_otc=True), _row("PREF", is_common=False, is_preferred=True), _row("WAR-WS"), _row("UNIT-U"), _row("SHEL", is_shell=True),
    _row("NOFLAG", is_common=None), _row("THIN", dollar_vol_20d=249999), _row("EDGE", dollar_vol_20d=250000), _row("NODV", dollar_vol_20d=None), _row("", name="no symbol"),
    _row("FUND", is_common=False, is_fund=True), _row("NOTE", is_debt_or_structured=True),
] + [_row("E%d" % i, is_etf=True, is_common=False, name=n) for i, n in enumerate([
    "Vanguard Total Stock Market ETF", "ProShares UltraPro QQQ", "Direxion Daily Semiconductor Bull 3X Shares", "ProShares Short S&P500", "iShares Short Treasury Bond ETF",
    "PIMCO Enhanced Short Maturity Active ETF", "JPMorgan Ultra-Short Income ETF", "ProShares Ultra Bloomberg Crude Oil", "First Trust Long/Short Equity ETF",
    "GraniteShares 2x Long NVDA Daily ETF", "Simplify Short Term Treasury Futures Strategy ETF", "AdvisorShares Ranger Equity Bear ETF", "Invesco S&P 500 Inverse ETF",
    "SPDR Bloomberg 1-3 Month T-Bill ETF", "ProShares Short VIX Short-Term Futures ETF", "Leveraged Loan ETF"])]


def test_universe_rule_is_the_campaign_scans():
    vol, camp = _load("justhodl-stock-volatility"), _load("justhodl-campaign-scan")
    assert vol.GEARED_RX.pattern == camp.GEARED_RX.pattern and vol.NOT_COMMON_SUFFIX.pattern == camp.NOT_COMMON_SUFFIX.pattern
    assert vol.MIN_DV == camp.MIN_DV and vol.STALE_SESSIONS == camp.STALE_SESSIONS and vol.JUMP_FLAG == camp.JUMP_FLAG and vol.JUMP_SESSIONS == camp.JUMP_SESSIONS
    for r in ROWS:
        assert vol.kind_of(r) == camp.kind_of(r), r["symbol"]
    a, sa = vol.select_universe(ROWS)
    b, sb = camp.select_universe(ROWS)
    assert a == b and sa == sb
    kinds = {t: m["kind"] for t, m in a.items()}
    assert kinds == {"AAA": "stock", "BRK.B": "stock", "EDGE": "stock", "E0": "etf", "E4": "etf", "E5": "etf", "E6": "etf", "E8": "etf", "E10": "etf", "E13": "etf"}, kinds


def test_reads_the_bank_the_same_way():
    vol, camp = _load("justhodl-stock-volatility"), _load("justhodl-campaign-scan")
    for t in ("AAPL", "BRK.B", "^GSPC", "a b/c"):
        assert vol.bank_key(t) == camp.bank_key(t)
    assert vol.canon("brk-b") == camp.canon("brk-b") == "BRK.B"
    assert vol.months_between("2025-11-03", "2026-02-01") == camp.months_between("2025-11-03", "2026-02-01")
    jumps = [{"date": "a", "ratio": 4.0, "ago": 12}, {"date": "b", "ratio": 2.0, "ago": 3}]
    assert {k: v for k, v in vol.flagged_jump(jumps).items() if k != "ago"} == camp.flagged_jump(jumps)


def test_config_is_one_run_a_day_and_matches_the_source():
    cfg = json.loads((LAMBDAS / "justhodl-stock-volatility" / "config.json").read_text(encoding="utf-8"))
    vol = _load("justhodl-stock-volatility")
    assert cfg["env"]["OUT_KEY"] == vol.OUT_KEY == "data/stock-volatility.json"
    assert float(cfg["env"]["MIN_DOLLAR_VOL"]) == vol.MIN_DV and int(cfg["env"]["MIN_SESSIONS"]) == vol.MIN_SESSIONS and int(cfg["env"]["MIN_SCANNED"]) == vol.MIN_SCANNED
    assert float(cfg["env"]["BOARD_MIN_MCAP"]) == vol.BOARD_MIN_MCAP and int(cfg["env"]["BOARD_N"]) == vol.BOARD_N
    sched = cfg["eventbridge_scheduler"]
    assert sched["schedule_name"] == "justhodl-stock-volatility-daily" and sched["cron"] == "cron(6 4 * * ? *)" and sched["timezone"] == "UTC"
    assert cfg["timeout"] == 900 and cfg["memory"] == 2048 and "invoke" not in (LAMBDAS / "justhodl-stock-volatility" / "source" / "lambda_function.py").read_text(encoding="utf-8").split('"""', 2)[2].lower().replace("no invoke", "")
    assert len(vol.COLS) == len(set(vol.COLS)) == 51 and set(vol.RANK_COL.values()) <= set(vol.COLS) and vol.CONTRACT == "stock-volatility.v1"
