"""justhodl-backtest-engine -- audit 2026-09-08 INST-09/10 tests (no AWS)."""
from __future__ import annotations
import importlib.util, sys, types
from pathlib import Path
HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
sys.path.insert(0, str(HERE.parents[2] / "shared"))


def _load():
    fake = types.ModuleType("boto3"); fake.client = lambda *a, **k: types.SimpleNamespace(); fake.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda n: None)
    sys.modules["boto3"] = fake
    dyn = types.ModuleType("boto3.dynamodb"); cond = types.ModuleType("boto3.dynamodb.conditions"); cond.Attr = lambda *a, **k: None; cond.Key = lambda *a, **k: None
    dyn.conditions = cond; sys.modules["boto3.dynamodb"] = dyn; sys.modules["boto3.dynamodb.conditions"] = cond
    ms = types.ModuleType("managed_secret"); ms.managed_secret = lambda *a, **k: ""; sys.modules["managed_secret"] = ms
    sl = types.ModuleType("_sentry_lite"); sl.track_errors = lambda f: f; sys.modules["_sentry_lite"] = sl
    spec = importlib.util.spec_from_file_location("bt_under_test", SRC); mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


def test_sunday_snapshot_is_not_applied_to_that_weeks_tuesday_trade(mod):
    # the audit's reproduction: snapshot created Sunday 2026-09-06 12:00, week starting 2026-08-31, selected for a 2026-09-01 trade
    hist = [{"week_start": "2026-08-31", "week_end": "2026-09-06", "snapshot_id": "cal-20260906T120000Z-2026-W36",
             "available_at": "2026-09-06T12:00:00+00:00", "weights": {"sig": 1.3}, "accuracy": {}}]
    w, src = mod.resolve_weight_walkforward("sig", "2026-09-01T14:30:00+00:00", hist)
    assert w is None and src == "trade_predates_all_snapshots", (w, src)
    w, src = mod.resolve_weight_walkforward("sig", "2026-09-06T12:00:01+00:00", hist)
    assert w == 1.3 and src.startswith("walkforward:cal-20260906"), (w, src)
    # a bare date is the START of that day
    w, src = mod.resolve_weight_walkforward("sig", "2026-09-06", hist)
    assert w is None
    # newest available snapshot wins
    hist2 = hist + [{"week_start": "2026-09-07", "week_end": "2026-09-13", "snapshot_id": "cal-later", "available_at": "2026-09-13T12:00:00+00:00", "weights": {"sig": 2.0}, "accuracy": {}}]
    w, src = mod.resolve_weight_walkforward("sig", "2026-09-20T00:00:00+00:00", hist2)
    assert w == 2.0


def test_walkforward_curve_is_labelled_signal_attribution_not_nav(mod):
    src = SRC.read_text()
    assert '"curve_semantics": "signal_attribution"' in src and '"headline_eligible": False' in src and '"tradable_portfolio_nav": False' in src


if __name__ == "__main__":
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn(mod); print("ok", name)
    print("backtest-engine tests passed: %d" % len(tests))
