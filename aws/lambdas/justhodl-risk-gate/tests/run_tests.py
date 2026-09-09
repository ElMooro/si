"""justhodl-risk-gate -- audit 2026-09-08 FR-06/07/08/09 tests (dependency-free; no AWS)."""
from __future__ import annotations

import importlib.util
import inspect
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
# shared modules (managed_secret etc.) are bundled into the Lambda zip; make them importable here too
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))


class _NoAWS:
    def __getattr__(self, name):
        def _boom(*a, **k):
            raise AssertionError("AWS call attempted in unit test: %s" % name)
        return _boom


def _load():
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: _NoAWS()
    sys.modules["boto3"] = fake
    spec = importlib.util.spec_from_file_location("risk_gate_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _months(start_year, start_month, values):
    out = {}
    y, m = start_year, start_month
    for v in values:
        out["%04d-%02d-01" % (y, m)] = v
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def test_replay_is_pure_of_live_feeds(mod):
    src = inspect.getsource(mod.compute_posture)
    assert "get_object" not in src and "_feed(" not in src and "read_feed(" not in src, "compute_posture must not read live artifacts"
    assert "replay_purity" in inspect.getsource(mod.lambda_handler)


def test_sahm_rule_on_native_months_matches_the_official_formula(mod):
    # the audit's reproduction: 4% for a year, then 5% for three months -> 1.0pp (was 0 / CLEAR)
    ur = _months(2025, 1, [4.0] * 14 + [5.0] * 3)
    r = mod.sahm_rule(ur, "2026-06-30")
    assert r and r["value"] == 1.0, r
    assert r["observation_date"] == "2026-05-01"
    # daily forward-filled copies must not change it: the helper only sees native points
    assert mod.sahm_rule(ur, "2026-05-15")["value"] == 1.0
    assert mod.sahm_rule(_months(2025, 1, [4.0] * 10), "2026-06-30") is None, "insufficient native history is None, never zero"


def test_truck_yoy_on_native_months(mod):
    tr = _months(2025, 1, [100.0] * 12 + [90.0])
    r = mod.truck_yoy(tr, "2026-01-31")
    assert r and r["value"] == -10.0, r
    assert r["base_observation_date"] == "2025-01-01" and r["observation_date"] == "2026-01-01"
    assert mod.truck_yoy(_months(2025, 1, [100.0] * 5), "2026-01-31") is None


def test_compute_indicators_renders_truck_and_sahm_end_to_end(mod):
    ur = _months(2025, 1, [4.0] * 14 + [5.0] * 3)
    tr = _months(2025, 1, [100.0] * 12 + [90.0] + [91.0] * 4)
    F = {"UNRATE": {"2026-06-%02d" % d: 5.0 for d in range(1, 31)}, "TRUCKD11": {"2026-06-%02d" % d: 91.0 for d in range(1, 31)}}
    cal = sorted(F["UNRATE"].keys())
    ind = mod.compute_indicators(F, cal, len(cal) - 1, native={"UNRATE": ur, "TRUCKD11": tr})["indicators"]
    t = ind["truck_transport"]
    assert t["value"] == -9.0 and t["level"] == 91.0 and t["year_ago_level"] == 100.0 and t["basis"], t
    assert ind["sahm_rule"]["value"] == 1.0


def test_indicators_use_native_series_not_forward_fill(mod):
    # F is the forward-filled daily view (repeated rows), native carries the months
    ur = _months(2025, 1, [4.0] * 14 + [5.0] * 3)
    F = {"UNRATE": {"2026-06-%02d" % d: 5.0 for d in range(1, 31)}}
    cal = sorted(F["UNRATE"].keys())
    ind = mod.compute_indicators(F, cal, len(cal) - 1, native={"UNRATE": ur})["indicators"]
    assert ind["sahm_rule"]["value"] == 1.0 and ind["sahm_rule"]["signal"] == "RECESSION TRIGGERED", ind["sahm_rule"]
    ind2 = mod.compute_indicators(F, cal, len(cal) - 1, native={})["indicators"]
    assert "pending_source" in ind2["sahm_rule"], "no native months -> pending, never computed from daily repeats"


def test_live_overlays_are_disclosed_and_stale_ones_do_not_apply(mod):
    rows, total = mod.live_overlays({"band": "SEIZING", "composite": 85}, 2.0, {"dollar_leg": {"legs_firing": 2, "available": 3, "status": "STRESS"}}, 3.0)
    by = {r["name"]: r for r in rows}
    assert by["collateral"]["contribution"] == -0.35 and by["collateral"]["applied"]
    assert by["foreign_official"]["contribution"] == -0.15
    assert total == -0.5
    rows, total = mod.live_overlays({"band": "SEIZING", "composite": 85}, 200.0, None, None)
    by = {r["name"]: r for r in rows}
    assert by["collateral"]["status"] == "STALE" and by["collateral"]["contribution"] == 0.0 and by["collateral"]["eligible"] is False
    assert by["foreign_official"]["status"] == "MISSING" and total == 0.0


def test_posture_bands_and_leg_states(mod):
    assert mod.posture_from(0.5, 0, 0) == "RISK_ON"
    assert mod.posture_from(0.0, 0, 0) == "NEUTRAL"
    assert mod.posture_from(-0.5, 0, 0) == "RISK_OFF"
    assert mod.posture_from(-1.5, 0, 0) == "SEVERE"
    assert mod.posture_from(0.9, -2, -1) == "SEVERE", "plumbing override"
    assert mod.leg_state(0.4) == "RISK-ON" and mod.leg_state(-0.5) == "RISK-OFF" and mod.leg_state(None) == "UNKNOWN"



def test_acm_uses_actual_donor_contract_and_never_slope_proxy(mod):
    from datetime import datetime, timezone, timedelta
    from copy import deepcopy
    from donor_contracts import term_premium_indicator
    now = datetime(2026, 9, 9, tzinfo=timezone.utc)
    doc = {"engine": "justhodl-term-premium", "version": "1.0.0", "generated_at": now.isoformat(),
           "latest": {"date": "2026-09-08", "tp10": 0.7, "tp5": 0.4, "tp2": 0.1, "y10": 4.1, "rn10": 3.4},
           "decomposition": {"term_premium_10y_pct": 0.7, "risk_neutral_10y_pct": 3.4, "acm_fitted_10y_pct": 4.1},
           "z_10y": 1.2, "deltas_bps": {"d5": 5, "d21": 10, "d63": 15}, "regime": {"level": "LOW_NORMAL"}}
    value = term_premium_indicator(doc, now)
    assert value["status"] == "OK" and value["value"] == 0.7 and value["unit"] == "pp"
    assert value["evidence"]["latest"]["tp5"] == 0.4
    F, cal = {"T10Y2Y": {"2026-09-08": -100}}, ["2026-09-08"]
    out = mod.compute_indicators(F, cal, 0, term_premium=value)["indicators"]["acm_term_premium"]
    assert out["value"] == 0.7 and out["source"] == "data/term-premium.json"
    assert "pending_source" in mod.compute_indicators(F, cal, 0)["indicators"]["acm_term_premium"]
    for field, mutation, status in [
        ("latest", {**doc["latest"], "date": "2000-01-01"}, "STALE"),
        ("generated_at", (now + timedelta(days=1)).isoformat(), "INVALID"),
        ("latest", {**doc["latest"], "tp10": 70}, "INVALID"),
        ("latest", {**doc["latest"], "tp10": float("nan")}, "INVALID"),
        ("version", "unknown", "INVALID")]:
        bad = deepcopy(doc); bad[field] = mutation
        result = term_premium_indicator(bad, now)
        assert result["status"] == status and result["value"] is None
        import json; json.dumps(result, allow_nan=False)


def test_treasury_fails_scope_arithmetic_and_observation_freshness(mod):
    from datetime import datetime, timezone
    from copy import deepcopy
    from donor_contracts import treasury_fails_input
    now = datetime(2026, 9, 9, tzinfo=timezone.utc)
    treasury = {"scope": "US_TREASURY_INCLUDING_TIPS", "unit": "USD_bn_par", "complete": True,
                "as_of": "2026-09-02", "ftd_bn": 10, "ftr_bn": 20, "gross_bn": 30,
                "stats": {"gross": {"z": 2.5, "as_of": "2026-09-02"}}}
    doc = {"engine": "settlement-fails", "version": "1.0.0", "generated_at": now.isoformat(), "treasury": treasury,
           "totals": {"gross_bn": 9999999}, "signal": {"score": 0}}
    result = treasury_fails_input(doc, now)
    assert result["status"] == "OK" and result["value"] == 2.5 and result["score_adj"] == -0.4
    assert result["max_age_h"] == 240 and result["age_h"] == 168
    for key, value, status in [("scope", "ALL_ASSETS", "INVALID"), ("unit", "USD", "INVALID"),
                               ("gross_bn", 31, "INVALID"), ("complete", False, "INVALID"),
                               ("as_of", "2026-08-26", "STALE")]:
        bad = deepcopy(doc); bad["treasury"][key] = value
        result = treasury_fails_input(bad, now)
        assert result["status"] == status and result["score_adj"] == 0 and result["value"] is None


def test_jplg_provenance_blocks_loan_levels_and_stale_yoy(mod):
    from datetime import datetime, timezone
    from donor_contracts import jplg_input
    now = datetime(2026, 9, 9, tzinfo=timezone.utc)
    row = {"symbol": "JPLG", "value": 3000000, "source": "imf:MFS_DC (family)", "adapter": "family:LG", "status": "LIVE", "asof": "2026-08"}
    result = jplg_input({"symbols": [row]}, now)
    assert result["status"] == "INVALID" and result["value"] is None and result["score_adj"] == 0
    row.update(value=-0.2, source="bank-of-japan", resolved_via="boj:MD11:DLCLAADBLTTO", asof="boj:202608 YoY")
    result = jplg_input({"symbols": [row]}, now)
    assert result["status"] == "OK" and result["value"] == -0.2 and result["score_adj"] == -0.4
    row["asof"] = "boj:200001 YoY"
    assert jplg_input({"symbols": [row]}, now)["status"] == "STALE"


def _handler_fixture(mod, event=None):
    import contextlib, io, copy, json
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    date = now.date().isoformat()
    donor = {"engine": "justhodl-term-premium", "version": "1.0.0", "generated_at": now.isoformat(),
             "latest": {"date": date, "tp10": 0.7, "tp5": 0.4, "tp2": 0.1, "y10": 4.1, "rn10": 3.4},
             "decomposition": {"term_premium_10y_pct": 0.7, "risk_neutral_10y_pct": 3.4, "acm_fitted_10y_pct": 4.1},
             "z_10y": 1.2, "regime": {"level": "LOW_NORMAL"}}
    fails = {"engine": "settlement-fails", "version": "1.0.0", "generated_at": now.isoformat(),
             "treasury": {"scope": "US_TREASURY_INCLUDING_TIPS", "unit": "USD_bn_par", "complete": True,
                          "as_of": date, "ftd_bn": 10, "ftr_bn": 20, "gross_bn": 30,
                          "stats": {"gross": {"z": 2.5, "as_of": date}}}}
    docs = {"data/term-premium.json": donor, "data/settlement-fails.json": fails}
    months = _months(2025, 1, [4.0] * 17 + [5.0] * 3)
    trucks = _months(2025, 1, [100.0] * 12 + [90.0] * 8)
    other = _months(2025, 1, [100.0] * 20)
    mod.fred = lambda sid: copy.deepcopy(months if sid == "UNRATE" else trucks if sid == "TRUCKD11" else other)
    reads, writes = [], {}
    def feed(key):
        reads.append(key)
        return docs.get(key), 0.0 if key in docs else None
    mod._feed = feed
    mod.read_feed = lambda key: docs.get(key)
    class MemoryS3:
        def put_object(self, **args): writes[args["Key"]] = json.loads(args["Body"])
        def __getattr__(self, name): raise AssertionError("Unexpected AWS operation " + name)
    mod.s3 = MemoryS3()
    with contextlib.redirect_stdout(io.StringIO()):
        response = mod.lambda_handler(event or {}, None)
    return response, writes, reads


def test_actual_handler_consumes_scoped_donors_once_and_preserves_live_replay_separation(mod):
    response, writes, reads = _handler_fixture(_load())
    out = writes["data/risk-gate.json"]
    assert reads.count("data/term-premium.json") == 1
    assert reads.count("data/settlement-fails.json") == 1
    assert "data/ofr-stfm.json" not in reads
    assert out["indicators"]["indicators"]["acm_term_premium"]["value"] == 0.7
    fails = out["fleet_context"]["inputs"]["funding.treasury_fails_gross_z"]
    assert fails["delta"] == -0.4 and fails["evidence"]["gross_bn"] == 30
    assert fails["freshness_basis"] == "observation_time" and fails["max_age_h"] == 240
    assert out["composite_identity"]["check_ok"] is True
    assert out["composite"] < out["replay_composite_fred_only"]


def test_validation_only_computes_real_artifact_without_writes(mod):
    for event in ({"validate_only": True}, {"mode": "validate_only"}):
        response, writes, reads = _handler_fixture(_load(), event)
        assert response["ok"] is True and response["validation_only"] is True
        assert response["schema_version"] == "risk-gate.v2.5"
        assert response["artifact_size_bytes"] > 1000 and not writes
        assert "data/term-premium.json" in reads

if __name__ == "__main__":
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn(mod)
        print("ok", name)
    print("risk-gate tests passed: %d" % len(tests))
