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


class MemoryS3:
    def __init__(self): self.objects={}
    def put_object(self, **kw):
        if kw.get("IfNoneMatch")=="*": assert kw["Key"] not in self.objects
        self.objects[kw["Key"]]=kw["Body"]
    def get_object(self, Bucket, Key):
        import io
        return {"Body":io.BytesIO(self.objects[Key])}
    def get_paginator(self, name):
        return types.SimpleNamespace(paginate=lambda **kw:[{"Contents":[{"Key":k} for k in self.objects if k.startswith(kw["Prefix"])]}])


def test_real_snapshotter_preserves_same_week_and_same_second_versions(mod):
    from datetime import datetime,timezone
    spec=importlib.util.spec_from_file_location("snapshotter", HERE.parents[1]/"justhodl-calibration-snapshotter/source/lambda_function.py")
    producer=importlib.util.module_from_spec(spec); spec.loader.exec_module(producer)
    s3=MemoryS3(); producer.S3=s3; mod.S3=s3
    producer.safe_get_ssm=lambda name:{"sig":1.3} if name.endswith("weights") else {}
    producer.count_outcomes_60d=lambda:({},0)
    producer.datetime=types.SimpleNamespace(now=lambda tz:datetime(2026,9,6,12,0,tzinfo=timezone.utc))
    producer.lambda_handler(); producer.lambda_handler()
    producer.datetime=types.SimpleNamespace(now=lambda tz:datetime(2026,9,6,18,0,tzinfo=timezone.utc))
    producer.lambda_handler()
    history,info=mod.load_weight_history()
    assert len(history)==3,info
    assert mod.resolve_weight_walkforward("sig","2026-09-06T15:00:00+00:00",history)[0]==1.3
    assert mod.resolve_weight_walkforward("sig","2026-09-06T13:00:00+02:00",history)[0] is None


def test_publication_gate_blocks_every_attribution_version(mod):
    mod.S3=MemoryS3()
    doc={name:{"data_sufficient":True,"coverage_pct":100,"annualized_return_pct":120473.55} for name in ("summary","realistic_summary","honest_summary","walkforward_summary")}
    mod.gate_performance_results(doc,"2026-09-08T12:00:00Z")
    assert doc["publication"]["status"]=="BLOCKED" and doc["portfolio_performance"]["nav_curve"]==[]
    for name in ("summary","realistic_summary","honest_summary","walkforward_summary"):
        assert doc[name]["headline_eligible"] is False and doc[name]["tradable_portfolio_nav"] is False
    assert all(s["status"]=="BLOCKED" for s in doc["historical_inputs"]["sources"].values())


def test_vintage_join_uses_known_on_and_conservative_date_availability(mod):
    doc={"vintages":[{"date":"2026-08-01","known_on":"2026-09-04","value":4.2},
                     {"date":"2026-08-01","known_on":"2026-09-08","value":4.5}]}
    assert mod.vintage_at_decision(doc,"2026-09-04T23:00:00Z") is None
    assert mod.vintage_at_decision(doc,"2026-09-05T01:00:00Z")["value"]==4.2
    assert mod.vintage_at_decision(doc,"2026-09-08T23:00:00Z")["value"]==4.2


def test_page_never_promotes_attribution_metrics(mod):
    import subprocess
    subprocess.run(["node",str(HERE/"publication.test.cjs")],check=True)


def test_validate_only_real_backtest_handler_writes_nothing(mod):
    mod.get_weights=lambda:{}; mod.get_horizon_weights=lambda:{}
    mod.scan_scored_outcomes=lambda:([],0)
    mod.fetch_spy_window=lambda *a:{}
    mod.S3=MemoryS3()
    result=mod.lambda_handler({"mode":"validate_only"},None)
    assert result["ok"] and result["validation_only"] and result["status"]=="BLOCKED" and result["artifact_size_bytes"]>0,result
    assert not mod.S3.objects


def test_validate_only_snapshotter_writes_nothing(mod):
    from datetime import datetime,timezone
    spec=importlib.util.spec_from_file_location("snapshotter_readonly", HERE.parents[1]/"justhodl-calibration-snapshotter/source/lambda_function.py")
    producer=importlib.util.module_from_spec(spec); spec.loader.exec_module(producer)
    producer.S3=MemoryS3()
    producer.safe_get_ssm=lambda name:{"sig":1.3} if name.endswith("weights") else {}
    producer.count_outcomes_60d=lambda:({},0)
    result=producer.lambda_handler({"mode":"validate_only"},None)
    assert result["ok"] and result["validation_only"] and result["artifact_size_bytes"]>0
    assert not producer.S3.objects


if __name__ == "__main__":
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn(mod); print("ok", name)
    print("backtest-engine tests passed: %d" % len(tests))
