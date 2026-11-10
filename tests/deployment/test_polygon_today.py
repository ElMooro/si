"""Deploy gate for the forming-bar lane (Khalid 2026-11-10: "SPY definitely is delayed almost 24 hours ... I want all the stocks
and ETFs data to be accurate"): justhodl-polygon-today writes today's day bar for every US stock / ETF from ONE Polygon
all-tickers snapshot every 5 minutes of the session, and justhodl-symdir appends it to every daily bank read. The two must
agree on the key, the member must stay in the 5-minute lane, and the filter must never promote a stale print to today."""
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
ENGINE = ROOT / "aws/lambdas/justhodl-polygon-today/source/lambda_function.py"
CONFIG = json.loads((ROOT / "aws/lambdas/justhodl-polygon-today/config.json").read_text(encoding="utf-8"))
ROUTING = ROOT / "aws/lambdas/justhodl-symdir/source/warehouse_routing.py"
NY = ZoneInfo("America/New_York")


def load(name, path, stubs):
    for k, v in stubs.items():
        sys.modules.setdefault(k, v)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_the_engine_and_symdir_agree_on_the_overlay_key_and_the_bank_stamp():
    eng = load("polygon_today_gate", ENGINE, {"chain_guard": SimpleNamespace(begin=lambda e, c=None: 0), "managed_secret": SimpleNamespace(managed_secret=lambda *a, **k: ""),
                                              "boto3": SimpleNamespace(client=lambda *a, **k: None)})
    rt = load("warehouse_routing_gate", ROUTING, {})
    assert eng.OUT_KEY == rt.TODAY_KEY == "data/warm/polygon-full/today.json.gz" == CONFIG["env"]["OUT_KEY"]
    assert CONFIG["timeout"] <= 60 and CONFIG["memory"] <= 1024, "one request, one small write: a light lane member"
    assert len(CONFIG["description"]) <= 256
    assert "eventbridge_scheduler" not in CONFIG, "the fan-out tick invokes it; no trigger of its own (the cadence floor)"
    # the bank's daily stamp is 09:30 America/New_York, the same convention as justhodl-tv-bars' session_open_ts
    assert eng.session_open_ts(datetime(2026, 11, 10).date()) == int(datetime(2026, 11, 10, 9, 30, tzinfo=NY).timestamp())
    assert rt.TODAY_TTL_S <= 120, "the overlay refreshes every 5 minutes; a resident process may not hold it longer than 2"


def test_the_member_is_in_the_five_minute_lane_everywhere_it_is_declared():
    fan = json.loads((ROOT / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    lane = json.loads((ROOT / "config/intraday-lane.json").read_text(encoding="utf-8"))
    assert "justhodl-polygon-today" in fan["ticks"]["session-5min"]
    assert fan["assignment"]["justhodl-polygon-today"]["ticks"] == ["session-5min"]
    assert "justhodl-polygon-today" in fan["intraday_lane_2026_10_21"]["members"]
    assert "justhodl-polygon-today" in lane["members"] and lane["tick"] == "session-5min"
    assert sorted(fan["ticks"]["session-5min"]) == sorted(fan["intraday_lane_2026_10_21"]["members"]) == sorted(lane["members"])


def test_only_this_sessions_prints_become_forming_bars():
    eng = sys.modules.get("polygon_today_gate") or load("polygon_today_gate", ENGINE, {"chain_guard": SimpleNamespace(begin=lambda e, c=None: 0), "managed_secret": SimpleNamespace(managed_secret=lambda *a, **k: ""), "boto3": SimpleNamespace(client=lambda *a, **k: None)})
    now = datetime(2026, 11, 10, 9, 52, tzinfo=NY)
    upd = int(now.timestamp() * 1000) - 15 * 60 * 1000
    tickers = [{"ticker": "SPY", "updated": upd * 1_000_000, "day": {"o": 747.94, "h": 749.16, "l": 747.05, "c": 749.115, "v": 3594789, "vw": 748.4}},
               {"ticker": "OLD", "updated": int(datetime(2026, 11, 9, 16, 0, tzinfo=NY).timestamp() * 1e9), "day": {"o": 1, "h": 1, "l": 1, "c": 1, "v": 1}},
               {"ticker": "PRE", "updated": int(datetime(2026, 11, 10, 9, 0, tzinfo=NY).timestamp() * 1e9), "day": {"o": 1, "h": 1, "l": 1, "c": 1, "v": 1}},
               {"ticker": "NONE", "updated": upd * 1_000_000, "day": {"c": 0}}]
    rows, newest, n_stale = eng.forming_rows(tickers, now.astimezone(timezone.utc))
    assert list(rows) == ["SPY"] and rows["SPY"][3] == 749.115 and newest == upd and n_stale == 2
    # the merge: a bank ending yesterday gains the bar; a bank already on the session does not; a crypto symbol never does
    rt = sys.modules.get("warehouse_routing_gate") or load("warehouse_routing_gate", ROUTING, {})
    overlay = {"session": "2026-11-10", "t": eng.session_open_ts(now.date()), "as_of": "2026-11-10T14:37:00+00:00", "delay_minutes": 15, "rows": {"SPY": rows["SPY"]}}
    bank = {"warehouse_empty": False, "source_span": "day", "source_mult": 1, "bars": [[eng.session_open_ts(datetime(2026, 11, 9).date()), 750, 753, 749, 752.69, 6e7]], "bar_sources": ["x"]}
    out = rt.with_forming_bar(bank, "SPY", rt._banks_for("SPY"), overlay, "day", 1)
    assert len(out["bars"]) == 2 and out["bars"][-1][4] == 749.115 and out["forming_bar"]["session"] == "2026-11-10" and out["bar_sources"][-1].startswith("polygon-snapshot:forming-day")
    assert bank["bars"] and len(bank["bars"]) == 1, "pure: the bank dict is not mutated"
    held = dict(bank, bars=[[overlay["t"], 748, 749, 747, 749.0, 1e6]])
    assert rt.with_forming_bar(held, "SPY", rt._banks_for("SPY"), overlay, "day", 1) is held
    assert rt.with_forming_bar(bank, "BTC-USD", rt._banks_for("BTC-USD"), overlay, "day", 1) is bank
    assert rt.with_forming_bar(bank, "SPY", rt._banks_for("SPY"), overlay, "minute", 1) is bank
