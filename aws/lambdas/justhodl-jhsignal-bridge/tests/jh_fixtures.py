"""Fixtures shared by the bridge and fusion test suites (imported by both conftest.py files).

Artifact fixtures mirror the REAL shapes written by the producing engines
(field names taken from their source in aws/lambdas/*/source/lambda_function.py
on 2026-09-07), so an adapter test failing here means the adapter is wrong,
not the fixture.
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]          # .../aws
REPO = ROOT.parent
for p in (ROOT / "shared", ROOT / "lambdas" / "justhodl-jhsignal-bridge" / "source", ROOT / "lambdas" / "justhodl-jh-fusion" / "source"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

NOW = datetime(2026, 9, 7, 16, 30, tzinfo=timezone.utc)


def ts(hours_ago=0.0):
    return (NOW - timedelta(hours=hours_ago)).isoformat().replace("+00:00", "Z")


@pytest.fixture
def now():
    return NOW


@pytest.fixture
def registry():
    from jh_registry import EngineRegistry
    return EngineRegistry.load()


@pytest.fixture
def universe_doc():
    from jh_registry import load_universe
    return load_universe()


@pytest.fixture
def universe(universe_doc):
    from jh_registry import universe_index
    return universe_index(universe_doc)


@pytest.fixture
def flags():
    from jh_registry import DEFAULT_FLAGS
    return dict(DEFAULT_FLAGS)


@pytest.fixture
def artifacts():
    """One realistic artifact per pilot engine, keyed by S3 key."""
    return {
        "data/regime-composite.json": {"generated_at": ts(1), "composite_score": 38.0, "meta_regime": "BROAD_RISK_ON", "meta_class": "RISK_ON",
                                       "dimensions": {"VOL": 0.4, "RISK_ON": 0.6}, "n_modules_with_data": 12, "n_modules_total": 15, "prior_regime": "BROAD_RISK_ON", "regime_changed_from_prior": False},
        "data/liquidity-credit-engine.json": {"generated_at": ts(6), "regime": "NORMAL", "composite": {"score": 12.5, "n_firing": 0, "by_category": {}},
                                              "series": {"a": {"available": True}, "b": {"available": True}, "c": {"available": False}}},
        "data/global-business-cycle.json": {"generated_at": ts(20), "aggregate": {"global_phase": "GLOBAL_EXPANSION", "global_avg_cli": 100.6, "classification_coverage_pct": 88.0, "expansion_breadth_pct": 61.0, "contraction_breadth_pct": 12.0},
                                            "downturn_probability_6m": {"ok": True, "probability_now": 0.354, "in_sample_auc": 0.693, "n_obs": 265}, "countries_with_fresh_data": 30, "countries_total": 34},
        "data/risk-gate.json": {"generated_at": ts(2), "posture": "NEUTRAL", "composite": 1.5, "sizing_multiplier": 0.75,
                                "legs": {"credit": {"value": 1}, "funding": {"value": 0}, "vol": {"value": -1}, "curve": {"value": None}}},
        "data/crisis-composite.json": {"generated_at": ts(1), "master_crisis_score": 22.0, "defcon_level": 5, "defcon_name": "ALL CLEAR",
                                       "components": [{"score": 10, "available": True}, {"score": 30, "available": True}, {"score": None, "available": False}]},
        "data/tail-risk.json": {"generated_at": ts(3), "indices": [{"ticker": "SPY", "tail_stress": 41.0, "p_drop_10": 0.07, "skew_index": 128, "spot": 640.1, "put_skew_slope": 0.05, "risk_reversal_25": -0.03},
                                                                    {"ticker": "QQQ", "tail_stress": 76.0, "p_drop_10": 0.16, "skew_index": 140, "spot": 570.0}], "tail_regime": "ELEVATED"},
        "data/insider-radar.json": {"generated_at": ts(5), "clusters": [{"ticker": "NVDA", "n_insiders": 3, "n_buys": 4, "total_value": 6_200_000, "span_days": 9, "first": "2026-08-28", "last": "2026-09-05", "ret_60d_pct": -12.0, "names": ["A", "B", "C"]},
                                                                        {"ticker": "XYZ", "n_insiders": 2, "n_buys": 2, "total_value": 250_000, "span_days": 3}]},
        "data/13f-flows-by-ticker.json": {"as_of": ts(30), "t": {"NVDA": {"wn": 850_000_000, "wb": 1_000_000_000, "ws": 150_000_000, "nf": 40, "na": 3, "fb": ["F1", "F2", "F3"], "fs": []},
                                                                  "META": {"wn": -120_000_000, "wb": 10_000_000, "ws": 130_000_000, "nf": 12, "na": -2, "fb": [], "fs": ["F9"]},
                                                                  "ZERO": {"wn": 0, "fb": [], "fs": []}}},
        "data/etf-flows.json": {"generated_at": ts(4), "by_etf": {"SPY": {"ticker": "SPY", "dvol_z_score": 2.4, "return_1d_pct": 0.8, "flow_signal": "HEAVY_INFLOW", "aum_b": 600.0, "today_dollar_vol_b": 40.0},
                                                                  "QQQ": {"ticker": "QQQ", "dvol_z_score": -0.4, "return_1d_pct": -0.2, "flow_signal": "NORMAL", "aum_b": 300.0},
                                                                  "SHY": {"ticker": "SHY", "category": "RATES_TREASURIES", "dvol_z_score": None, "avg_60d_dollar_vol_b": None, "dvol_5d_vs_20d_pct": 31.5, "return_1d_pct": -0.02, "flow_signal": "ROTATION_OUT", "aum_b": 26.2},
                                                                  "SMH": {"ticker": "SMH", "dvol_z_score": None, "dvol_5d_vs_20d_pct": None}}},
        "data/dark-pool.json": {"generated_at": ts(40), "board": [{"ticker": "AAPL", "state": "ACCUMULATION", "score": 72.0, "dark_pool_pct": 38.0, "offex_pct": 55.0, "dark_accel": 0.3, "week_return_pct": 1.2, "venue_fingerprint": "MIXED"},
                                                                  {"ticker": "MSFT", "state": "NEUTRAL", "score": 40.0}]},
        "data/estimate-revisions.json": {"generated_at": ts(8), "estimate_strength_leaders": [{"ticker": "NVDA", "eps_rev_pct": 6.5, "estimate_strength": 84, "n_analysts": 40, "revenue_confirms": True, "days_to_earnings": 45, "earnings_date": "2026-10-22"}],
                                         "upward_revisions": [], "downward_revisions": [{"ticker": "AAPL", "eps_rev_pct": -3.0, "estimate_strength": 41, "n_analysts": 35, "revenue_confirms": False, "days_to_earnings": 4}]},
        "data/momentum-leaders.json": {"generated_at": ts(2), "all_scored": [{"ticker": "NVDA", "momentum_score": 88.0, "rank": 3, "perf_20d_pct": 9.1, "perf_60d_pct": 21.0, "rs_spy_20d_pct": 6.2, "wk52_proximity": 0.98, "volume_surge": 1.4},
                                                                             {"ticker": "META", "momentum_score": 31.0, "rank": 900, "perf_20d_pct": -4.0, "rs_spy_20d_pct": -6.0}]},
        "data/fortress.json": {"as_of": ts(12), "session": "2026-09-04", "board": [{"ticker": "GOOGL", "tier": "COILED", "composite": 66.0, "conviction": 58.0, "asymmetry": 3.1, "gates_passed": 5, "coil_state": "COILING", "sector": "Communication Services", "industry": "Internet Content & Information", "resilience_confidence": 0.8, "cap_bucket": "mega"},
                                                                 {"ticker": "NOPE", "tier": "SCREENED", "composite": 40, "conviction": 20}],
                               "etfs": [{"ticker": "IWM", "tier": "ACCUMULATING", "composite": 55.0, "conviction": 45.0, "asymmetry": 1.5, "gates_passed": 4}]},
        "data/katlin.json": {"generated_at": ts(11), "picks": [{"ticker": "BTC", "asset_class": "crypto", "tier": "READY", "composite": 71.0, "conviction": 63.0, "structure_state": "DOUBLE_BOTTOM",
                                                                "catalysts": [{"name": "spot ETF inflows 5d", "kind": "flow", "source": "crypto-etf-flows"}, {"name": "earnings 2026-10-01", "kind": "calendar"}], "plan": {"stop": 58000, "target_1": 71000}, "sniper": {"state": "WAIT"}},
                                                               {"ticker": "TSM", "asset_class": "stock", "tier": "BASING", "composite": 55.0, "conviction": 40.0, "catalysts": []}]},
        "data/bottom.json": {"generated_at": ts(10), "session": "2026-09-08", "board_all": [
            {"ticker": "NVDA", "asset_class": "stock", "desk": "stocks", "frame": "D", "state": "TRIGGERED", "grade": "A", "score": 82.0, "sc_date": "2026-08-12", "st_date": "2026-09-02", "trigger_date": "2026-09-05", "st_vol_ratio_sc": 0.31, "st_depth_class": "HIGHER_LOW", "bars_in_state": 2, "weekly_state": "TESTING", "n_confirm": 3, "dist_sc_low_pct": 6.1},
            {"ticker": "TLT", "asset_class": "etf", "desk": "bonds", "frame": "W", "state": "ST_CONFIRMED", "grade": "B", "score": 64.0, "sc_date": "2026-06-05", "st_date": "2026-08-28", "st_vol_ratio_sc": 0.52, "st_depth_class": "EQUAL_LOW", "bars_in_state": 1, "weekly_state": None, "n_confirm": 1, "dist_sc_low_pct": 1.4},
            {"ticker": "ETH", "asset_class": "crypto", "desk": "crypto", "frame": "D", "state": "FAILED", "grade": "D", "score": 18.0, "sc_date": "2026-08-20", "st_date": "2026-09-01", "st_vol_ratio_sc": 0.95, "bars_in_state": 3, "n_confirm": 0},
            {"ticker": "OLDFAIL", "asset_class": "stock", "desk": "stocks", "frame": "D", "state": "FAILED", "grade": "D", "score": 12.0, "bars_in_state": 40},
            {"ticker": "GONE", "asset_class": "stock", "desk": "stocks", "frame": "D", "state": "EXPIRED", "grade": "D", "score": 30.0, "bars_in_state": 2}]},
        "data/catalyst.json": {"as_of": ts(9), "by_ticker": {"AMZN": {"score": 4.2, "top_class": "contract_win", "catalysts": [{"class": "contract_win", "evidence": "AWS $2B DoD contract", "src": "pr", "weight": 2.2}, {"class": "guidance_raise", "evidence": "raised FY", "src": "pr", "weight": 2.0}]},
                                                                    "EMPTY": {"score": 0.0, "catalysts": []}}},
        "data/dealer-gex.json": {"generated_at": ts(1), "underlyings": {"SPY": {"symbol": "SPY", "regime": "POSITIVE_GAMMA", "total_dealer_gex_billions": 2.1, "pcr_oi": 1.3, "spot": 640.1, "zero_gamma_flip_level": 628.0, "spot_above_flip": True, "n_contracts_modeled": 6000},
                                                                       "TSLA": {"symbol": "TSLA", "regime": "STRONG_NEGATIVE_GAMMA", "total_dealer_gex_billions": -0.8, "pcr_oi": 0.6, "n_contracts_modeled": 2000},
                                                                       "BAD": {"err": "no chain"}}},
        "data/short-interest.json": {"generated_at": ts(7), "by_ticker": {"MSFT": {"ticker": "MSFT", "signal": "COVERING", "score": 60.0, "latest_short_pct": 44.0, "days_to_cover": 2.1, "si_change_pct": -8.0, "trend_pct": -6.0},
                                                                          "AMZN": {"ticker": "AMZN", "signal": "NEUTRAL", "score": 10.0}}},
    }


class FakeS3:
    """In-memory S3 with get_object/put_object semantics used by jh_state_store.S3Store."""
    def __init__(self, objects=None, now=NOW):
        self.objects = {}
        self.now = now
        for k, v in (objects or {}).items():
            self.put_object(Bucket="b", Key=k, Body=json.dumps(v).encode(), ContentType="application/json")

    def put_object(self, **kw):
        self.objects[kw["Key"]] = {"Body": kw["Body"], "LastModified": self.now}
        return {}

    def get_object(self, **kw):
        o = self.objects.get(kw["Key"])
        if o is None:
            raise KeyError("NoSuchKey: %s" % kw["Key"])
        import io
        return {"Body": io.BytesIO(o["Body"]), "LastModified": o["LastModified"]}


@pytest.fixture
def fake_s3(artifacts):
    return FakeS3(artifacts)


@pytest.fixture
def signals_from_artifacts(artifacts, registry, universe, now):
    """Run every adapter over the fixtures -> list of valid signals + reports."""
    from jh_adapters import adapter_for
    sigs, reports = [], {}
    for spec in registry.active():
        doc = artifacts.get(spec["artifact"])
        res = adapter_for(spec, universe, now=now).parse_existing_output(doc, {"last_modified": ts(0)})
        reports[spec["engine_id"]] = res
        sigs.extend(res.signals)
    return sigs, reports


@pytest.fixture
def snapshot(signals_from_artifacts, registry, now, flags):
    from jh_state_store import build_snapshot
    sigs, reports = signals_from_artifacts
    return build_snapshot(sigs, registry_doc=registry.doc, run_id="test-run", now=now, adapter_reports=[r.as_dict() for r in reports.values()], flags=flags)
