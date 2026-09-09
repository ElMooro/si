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


if __name__ == "__main__":
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn(mod)
        print("ok", name)
    print("risk-gate tests passed: %d" % len(tests))
