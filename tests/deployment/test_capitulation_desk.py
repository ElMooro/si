"""The capitulation desk (Khalid 2026-11-05: "Capitulation · panic volume at the lows which is right on chart.html under
capitulation. audit it and see if we can improve it. maybe add etfs, crypto, metal, bonds and other asset class too").

Pins: the bottom engine's capitulation module (a pure scan over the same loaded bars: every desk incl. the crypto lane),
its own artifact key -- data/capitulation-desk.json, NOT data/capitulation.json which belongs to the extremes-research
engine justhodl-capitulation -- the slim pointer block inside bottom.json, the desk page that renders it, the chart's
Capitulation workspace routed to that page, the Wyckoff tab's text made consistent with its own filter, and the crypto
bank's Coinbase fallback (completed UTC days only; a product Coinbase does not list stays flagged).
"""
import importlib.util
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "aws/lambdas/justhodl-bottom/source"
ENGINE = (SRC_DIR / "lambda_function.py").read_text(encoding="utf-8")
MODULE = (SRC_DIR / "capitulation.py").read_text(encoding="utf-8")
PAGE = (ROOT / "capitulation-desk.html").read_text(encoding="utf-8")
RAIL = (ROOT / "jh-chart-tvrail.js").read_text(encoding="utf-8")
CHART = (ROOT / "chart.html").read_text(encoding="utf-8")
WYCKOFF = (ROOT / "wyckoff-desk.html").read_text(encoding="utf-8")
EXTREMES = (ROOT / "aws/shared/extremes_native_model.py").read_text(encoding="utf-8")


def _cap():
    if "capitulation_under_test" in sys.modules:
        return sys.modules["capitulation_under_test"]
    spec = importlib.util.spec_from_file_location("capitulation_under_test", SRC_DIR / "capitulation.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["capitulation_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_artifact_key_does_not_collide_with_the_extremes_research_engine():
    assert 'CAPIT_KEY = "data/capitulation-desk.json"' in ENGINE
    assert "'capitulation':('data/capitulation.json'" in EXTREMES, "the extremes-research engine owns data/capitulation.json"
    assert '"data/capitulation.json"' not in ENGINE and '"data/capitulation.json"' not in MODULE and "data/capitulation.json" not in PAGE
    assert 'KEY="data/capitulation-desk.json"' in PAGE and 'data-research-context="data/capitulation-desk.json"' in PAGE


def test_engine_scans_every_desk_and_the_crypto_lane_and_writes_the_block():
    assert 'VERSION = "1.3.0"' in ENGINE and "import capitulation as CAP" in ENGINE
    assert "cap = CAP.Accumulator(dates)" in ENGINE
    assert ENGINE.count("cap.add(") == 2, "the equities/wrappers loop and the crypto loop both feed the scan"
    assert 'cap.add(sym, "crypto", "crypto", sym, b.o, b.h, b.l, b.c, b.v, b.d, cdates)' in ENGINE
    assert "capit = CAP.build(cap, wy, lambda t, cls: confirmations(t, cls, F), _extra, cap_bench)" in ENGINE
    assert "nb = s3_put_json(CAPIT_KEY, capit)" in ENGINE
    assert '"capitulation": CAP.slim(capit) if capit else None' in ENGINE, "bottom.json carries the slim pointer block"
    assert '"capitulation": "The cross-asset panic-print scan (data/capitulation-desk.json' in ENGINE, "DEFINITIONS names the block"
    # the Wyckoff same-bar link: the sequence rows' climax date against the print date
    assert 'wy = {r["ticker"]: {k: r.get(k) for k in ("sc_date", "state", "grade", "score", "frame", "actionable")} for r in rows}' in ENGINE
    assert "CAP.bench_prints(" in ENGINE and '"BTC", "crypto"' in ENGINE


def test_module_contract_and_desk_floors():
    cap = _cap()
    assert cap.CONTRACT == "capitulation.v1"
    assert cap.DESK_ORDER == ["stocks", "equity_etfs", "countries", "bonds", "gold_metals", "commodities", "currencies", "crypto"]
    assert set(cap.DESK) == set(cap.DESK_ORDER), "one floor per desk the bottom engine groups into"
    floors = {d: cap.DESK[d]["ret_min"] for d in cap.DESK_ORDER}
    assert floors["bonds"] > floors["equity_etfs"] > floors["stocks"] > floors["crypto"], floors
    assert floors["currencies"] > floors["bonds"], "currency wrappers move least"
    for d in cap.DESK_ORDER:
        assert cap.DESK[d]["decline_min"] < cap.DESK[d]["ret_min"] < 0
    assert cap.P["vol_x_min"] == 2.5 and cap.P["range_x_min"] == 1.5 and cap.P["loc_max"] == 0.30
    for k in ("print", "context", "score", "after", "wyckoff", "breadth", "follow_through", "not_a_call"):
        assert k in cap.DEFINITIONS
    assert "not a buy" in cap.DEFINITIONS["not_a_call"]


def test_scan_is_pure_and_a_bar_never_feeds_its_own_gate():
    cap = _cap()
    # a quiet tape, then one panic bar: the ATR gate measures the bar against the bars BEFORE it, so the print is found
    o, h, l, c, v = [], [], [], [], []
    p = 50.0
    for i in range(120):
        nx = p * (1 + (0.002 if i % 2 else -0.002))
        o.append(p); c.append(nx); h.append(max(p, nx) * 1.003); l.append(min(p, nx) * 0.997); v.append(2_000_000.0); p = nx
    rng = (sum(h[-14:]) - sum(l[-14:])) / 14.0 * 3.0
    close = p * 0.93
    o.append(p * 0.995); c.append(close); l.append(close - 0.05 * rng); h.append(l[-1] + rng); v.append(9_000_000.0)
    found = cap.scan(o, h, l, c, v, "stocks")
    assert len(found) == 1 and found[0]["i"] == 120 and found[0]["context"] == "SHOCK" and found[0]["crash"] is True
    assert found[0]["vol_x"] == 4.5 and found[0]["range_x"] >= 2.5 and found[0]["loc"] <= 0.1
    # the same bar with the volume of an ordinary session is not a print
    v2 = list(v); v2[-1] = 2_400_000.0
    assert cap.scan(o, h, l, c, v2, "stocks") == []
    # follow-through on the last bar is empty, never fabricated
    ft = cap.follow_through(120, c, l, v)
    assert ft["sessions_since"] == 0 and ft["held"] is None and ft["bounce_pct"] is None and ft["vol_after_x"] is None


def test_slim_pointer_block_shape():
    cap = _cap()
    acc = cap.Accumulator(["2026-11-0%d" % d for d in range(1, 8)])
    block = cap.build(acc)
    slim = cap.slim(block)
    assert slim["key"] == "data/capitulation-desk.json" and slim["contract"] == "capitulation.v1"
    for k in ("read", "regime", "sum_5", "sum_5_percentile", "n_scored", "last_session", "counts", "n_prints_60", "latest", "follow_through"):
        assert k in slim, k
    assert "boards" not in slim and "breadth" not in slim, "the pointer block carries the verdict, not the boards"
    assert block["read"].startswith("selective panic")


def test_page_renders_the_artifact_and_hands_tickers_to_the_chart():
    assert '<meta name="jh-primary-engine" content="justhodl-bottom">' in PAGE
    assert 'DOC.contract!=="capitulation.v1"' in PAGE
    for sec in ("Verdict: panic regime, breadth and follow-through", "Breadth: prints per session across every asset class", "Prints by asset class",
                "Follow-through: what the tape did after the prints", "The market's own prints: benchmarks", "Reading rules and definitions", "Sources and engine observability"):
        assert 'aria-label="%s"' % sec in PAGE, sec
    order = [PAGE.index('aria-label="%s' % s) for s in ("Verdict: panic regime", "Breadth: prints per session", "Prints by asset class", "Follow-through", "The market's own prints", "Reading rules", "Sources and engine observability")]
    assert order == sorted(order), "verdict -> breakdown -> boards -> drill-downs -> provenance (docs/ops/PAGE_ORDER.md)"
    assert 'th data-sort=' in PAGE and "blanks last either way" in PAGE, "own three-state headers: /jh-sortable.js leaves them alone"
    assert 'window.top.jhOpenSymbol(a.getAttribute("data-t"))' in PAGE and 'getElementById("wsx")' in PAGE, "inside the chart's workspace a ticker loads in the chart behind it"
    assert 'r.asset_class==="crypto"?r.ticker+"-USD":r.ticker' in PAGE, "crypto rows hand the chart its -USD symbol"
    assert "no fake" not in PAGE.lower() and "demo" not in PAGE.lower()
    assert "is not a buy" in PAGE
    assert 'LABEL={stocks:"Stocks",equity_etfs:"Equity & sector ETFs",countries:"Countries",bonds:"Bonds",gold_metals:"Gold & metals",commodities:"Commodities",currencies:"Currencies",crypto:"Crypto"}' in PAGE


def test_chart_capitulation_workspace_routes_to_the_desk_page():
    assert 'capit: ["Capitulation · panic volume at the lows", "/capitulation-desk.html"],' in RAIL
    assert "/wyckoff-desk.html?tab=capit" not in RAIL
    assert "id=btn-capit title='Capitulation · panic volume at the lows'" in (ROOT / "jh-chart-engine.js").read_text(encoding="utf-8")
    assert "&capit=20261105&ldr=20261105" in CHART, "the rail's cache-bust carries the re-route"
    assert "/capitulation-desk.html" in WYCKOFF and "volume ≥1.75×" not in WYCKOFF, "the Wyckoff tab's words match its own filter and point at the full desk"
    assert 'kpis.push(["Panic breadth", cap.regime])' in WYCKOFF


def test_crypto_bank_falls_back_to_coinbase_on_completed_days_only():
    assert 'CB_BASE = "https://api.exchange.coinbase.com"' in ENGINE
    assert "def cb_candles(product, frm, to):" in ENGINE and "granularity=86400" in ENGINE
    assert "if d >= to:\n                continue" in ENGINE, "the in-progress UTC day is never banked"
    assert 'fresh = cb_candles("%s-USD" % sym, frm, today)' in ENGINE
    assert 'return doc, "err:%s|coinbase %s" % (err or "no-key", str(e)[:40])' in ENGINE, "a product Coinbase does not list stays flagged, never fabricated"
    assert 'doc["last_source"] = src' in ENGINE
    assert re.search(r'DEGRADED\.append\("crypto lane: %d/%d bank errors \(polygon and coinbase\)"', ENGINE)
