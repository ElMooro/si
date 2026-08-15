"""
End-to-end wiring test with a fully monkeypatched fleet_io — no real AWS
call is made. This proves run_tier1 -> run_tier2 -> run_tier3 actually
connects correctly, not just that scoring.py's math is right in isolation.
All fixture numbers are synthetic and clearly not live market data.

Run: pytest aws/lambdas/justhodl-invest/tests/ -v
"""
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

import fleet_io  # noqa: E402
import causal_graph  # noqa: E402


# ── a tiny in-memory fake S3 ────────────────────────────────────────────

class FakeFleet:
    """Fake fleet snapshot: 8 days of history so zscore() has n>=8, then
    today's print is a clear step-up on the legs we want to CONFIRM."""

    def __init__(self):
        self.docs = {}
        self.history = {"days": [
            {"date": f"2026-07-{10+i:02d}",
             "legs": {"korea_export_value_yoy": 8.0 + i * 0.2,
                      "korea_export_value_yoy_flash": 8.0 + i * 0.2,
                      "korea_port_volume": -1.0 - i * 0.1,
                      "copper_price_yoy": 3.0,
                      "chile_copper_export": 3.0,
                      "peru_copper_production": 3.0,
                      "taiwan_export_orders_yoy": 2.0,
                      "taiwan_moea_detail": 2.0,
                      "china_tsf_yoy": 5.0,
                      "china_liquidity_impulse": 0.1,
                      "port_throughput_pulse": 1.0,
                      "freight_composite_z": 0.2,
                      "grid_executed_mw": 4.0 + i * 0.15,
                      "pjm_queue_detail": 4.0 + i * 0.15,
                      "lumber_price_yoy": 1.0,
                      "construction_housing_pmi": 1.0}}
            for i in range(8)
        ]}
        # today's leg values: Korea export value spikes hard, port volume
        # stays mildly soft (the boom-stage value/volume divergence shape).
        # Grid buildout also spikes, to exercise an SPDR-mapped industry.
        self.leg_values = {
            "korea_export_value_yoy": 52.3,
            "korea_export_value_yoy_flash": 50.1,
            "korea_port_volume": -4.3,
            "copper_price_yoy": None,          # deliberately unavailable leg
            "chile_copper_export": None,
            "peru_copper_production": None,
            "taiwan_export_orders_yoy": None,
            "taiwan_moea_detail": None,
            "china_tsf_yoy": None,
            "china_liquidity_impulse": None,
            "port_throughput_pulse": 1.1,
            "freight_composite_z": 0.3,
            "grid_executed_mw": 22.0,
            "pjm_queue_detail": 21.0,
            "lumber_price_yoy": 1.1,
            "construction_housing_pmi": 0.9,
        }

        self.docs["data/forward-returns.json"] = {"assets": {
            "SPY": {"forward_er_10y_pct": 2.66, "risk": {"vol_pct_annualized": 15.0}},
            "XLI": {"forward_er_10y_pct": 12.0, "risk": {"vol_pct_annualized": 19.0}},
            # deliberately NO SMH key -> semis_memory Tier2 must come back
            # INSUFFICIENT_DATA, proving the honest-gap path works.
            # Shape matches the real, confirmed-live schema (2026-08-15
            # smoke test): assets is a dict KEYED BY TICKER, not a list --
            # the first draft got this wrong and crashed on first live
            # invoke ('str' object has no attribute 'get').
        }}
        self.docs["data/industry-boom.json"] = {"league": [
            {"industry": "Electrical Equipment & Parts", "top_names": ["ETN", "VRT", "PWR"]},
        ]}
        self.docs["data/backlog-miner.json"] = {"rows": [
            {"ticker": "ETN", "backlog_yoy_pct": 24.0},
            {"ticker": "VRT", "backlog_yoy_pct": 31.0},
            {"ticker": "PWR", "backlog_yoy_pct": 6.0},
        ]}
        self.docs["data/backlog.json"] = {"rows": []}
        self.docs["data/catalyst.json"] = {"rows": [
            {"ticker": "ETN", "weight": 0.8},
            {"ticker": "VRT", "weight": 0.9},
            {"ticker": "PWR", "weight": 0.2},
        ]}
        self.docs["data/stock-buying.json"] = {"rows": [
            {"ticker": "ETN", "peg": 1.1, "net_buyback_yield": 2.0,
             "qoq_acceleration_pct": 5.0, "pe_5y_percentile": 55, "margin_percentile": 40},
            {"ticker": "VRT", "peg": 0.9, "net_buyback_yield": 3.0,
             "qoq_acceleration_pct": 12.0, "pe_5y_percentile": 60, "margin_percentile": 45},
            {"ticker": "PWR", "peg": 1.8, "net_buyback_yield": -1.0,
             "qoq_acceleration_pct": -2.0, "pe_5y_percentile": 20, "margin_percentile": 88},
        ]}

    # fleet_io-compatible surface
    def get_json(self, key):
        return self.docs.get(key)

    def load_history(self):
        return self.history

    def read_leg_value(self, source):
        # Resolve by exact source string -> leg_id via the real causal_graph
        # (this is what production fleet_io.read_leg_value effectively does
        # through S3+dotted-path; here we just skip the S3 round trip).
        leg_id = _SOURCE_TO_LEGID.get(source)
        return self.leg_values.get(leg_id)


_SOURCE_TO_LEGID = {
    leg.source: leg.leg_id
    for ind in causal_graph.LEADING_INDICATORS
    for leg in ind.legs
}


def test_full_pipeline_wiring(monkeypatch):
    import lambda_function as lf

    fake = FakeFleet()
    monkeypatch.setattr(fleet_io, "get_json", fake.get_json)
    monkeypatch.setattr(fleet_io, "load_history", fake.load_history)
    monkeypatch.setattr(fleet_io, "read_leg_value", fake.read_leg_value)
    monkeypatch.setattr(fleet_io, "save_history", lambda h: None)
    monkeypatch.setattr(fleet_io, "put_json", lambda k, v: None)
    # lambda_function imported fleet_io by reference at module load time --
    # patch the names it actually calls through too
    monkeypatch.setattr(lf.fleet_io, "get_json", fake.get_json)
    monkeypatch.setattr(lf.fleet_io, "load_history", fake.load_history)
    monkeypatch.setattr(lf.fleet_io, "read_leg_value", fake.read_leg_value)
    monkeypatch.setattr(lf.fleet_io, "save_history", lambda h: None)
    monkeypatch.setattr(lf.fleet_io, "put_json", lambda k, v: None)

    tier1, _ = lf.run_tier1()
    korea = next(r for r in tier1 if r["indicator_id"] == "korea_semiconductor_exports")
    assert korea["status"] in ("CONFIRMED", "TURNING")
    assert korea["conflicting_legs"] == 0  # value/volume divergence isn't a conflict

    copper = next(r for r in tier1 if r["indicator_id"] == "copper_demand_pulse")
    assert copper["status"] == "INSUFFICIENT_DATA"  # all 3 legs unavailable in fixture

    grid = next(r for r in tier1 if r["indicator_id"] == "grid_buildout_pulse")
    assert grid["status"] == "CONFIRMED"

    tier2 = lf.run_tier2(tier1)
    # semis_memory has no forward-returns row for SMH -> honest gap, not a crash
    assert tier2["semis_memory"]["status"] == "INSUFFICIENT_DATA"
    # grid_electrical_infra maps to XLI, which DOES have forward-returns coverage
    assert tier2["grid_electrical_infra"]["status"] == "OK"
    assert tier2["grid_electrical_infra"]["excess_return_pp"] == round(12.0 - 2.66, 2)

    tier3 = lf.run_tier3(tier2)
    tickers_scored = {p["ticker"] for p in tier3 if p.get("status") == "OK"}
    assert tickers_scored == {"ETN", "VRT", "PWR"}

    vrt = next(p for p in tier3 if p["ticker"] == "VRT")
    pwr = next(p for p in tier3 if p["ticker"] == "PWR")
    # VRT: high backlog, cheap PEG, strong catalyst -> should outrank PWR,
    # which has weak backlog, expensive PEG, and a peak-margin cycle flag
    assert vrt["composite_score"] > pwr["composite_score"]
    assert pwr["cycle_flag"] == "PEAK_MARGIN_TRAP"

    grading = lf.build_grading_candidates(tier2, tier3)
    assert any(g["signal_type"] == "invest_industry_outperform" for g in grading)
    assert any(g["signal_type"].startswith("invest_stock_") for g in grading)


def test_lookup_handles_both_by_ticker_dict_and_rows_list_shapes():
    """_lookup's fallback chain was extended (2026-08-15 live-data fix) to
    check by_ticker first -- confirmed the real shape for backlog.json and
    catalyst.json -- while staying backward-compatible with a plain
    rows-list shape for docs not yet confirmed. Exercise both directly."""
    import lambda_function as lf

    by_ticker_doc = {"by_ticker": {"ETN": {"weight": 0.8}, "VRT": {"weight": 0.9}}}
    assert lf._lookup(by_ticker_doc, "ETN", "weight") == 0.8
    assert lf._lookup(by_ticker_doc, "ZZZ", "weight") is None

    rows_doc = {"rows": [{"ticker": "ETN", "weight": 0.5}]}
    assert lf._lookup(rows_doc, "ETN", "weight") == 0.5

    assert lf._lookup(None, "ETN", "weight") is None
    assert lf._lookup({}, "ETN", "weight") is None


def test_dig_bracket_search_resolves_tagged_list_rows():
    """canary-grid's signals / portwatch's exporters publish a LIST of
    tagged dict rows rather than a dict keyed by name -- confirmed live
    2026-08-15. dig() needed a bracket-search step to address these:
    'signals[key=korea_exports].value'."""
    doc = {"signals": [
        {"key": "korea_exports", "value": 47.96, "available": False},
        {"key": "copper", "value": 37.79, "available": True},
    ]}
    assert fleet_io.dig(doc, "signals[key=copper].value") == 37.79
    assert fleet_io.dig(doc, "signals[key=korea_exports].available") is False
    assert fleet_io.dig(doc, "signals[key=does_not_exist].value") is None
    assert fleet_io.dig(doc, "no_such_field[key=copper].value") is None
    assert fleet_io.dig({"signals": "not_a_list"}, "signals[key=copper].value") is None
    # still backward-compatible with a plain dotted path
    assert fleet_io.dig({"a": {"b": 3}}, "a.b") == 3
