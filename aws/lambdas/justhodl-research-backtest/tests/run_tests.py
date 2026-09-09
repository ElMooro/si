"""justhodl-research-backtest -- audit 2026-09-08 INST-11 tests (no AWS)."""
from __future__ import annotations
import importlib.util, sys, types
from pathlib import Path
HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
sys.path.insert(0, str(HERE.parents[2] / "shared"))


def _load():
    fake = types.ModuleType("boto3"); fake.client = lambda *a, **k: types.SimpleNamespace()
    sys.modules["boto3"] = fake
    ms = types.ModuleType("managed_secret"); ms.managed_secret = lambda *a, **k: ""; sys.modules["managed_secret"] = ms
    spec = importlib.util.spec_from_file_location("rb_under_test", SRC); mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


def test_critique_joins_only_when_available_at_decision_time(mod):
    sept = {"ticker": "X", "generated_at": "2026-09-08T10:00:00+00:00", "critique": {"disagreement_score": 80}}
    assert mod.critique_available_at(sept, "2026-08-01T00:00:00+00:00") is None, "September critique must not colour an August call"
    assert mod.critique_available_at(sept, "2026-09-08T12:00:00+00:00") is sept
    assert mod.critique_available_at({}, "2026-09-08") is None


def test_no_alpha_claim_without_significance_and_coverage(mod):
    calls = [{"disagreement_score": 10, "rating_diverges": False, "ticker_return_pct": 5.0, "alpha_pct": 3.0},
             {"disagreement_score": 90, "rating_diverges": True, "ticker_return_pct": -2.0, "alpha_pct": -4.0}]
    r = mod.build_ensemble_attribution(calls)
    assert r["alpha_spread_pct"] == 7.0 and r["significant"] is False and "no alpha claim" in r["interpretation"] or "too small" in r["interpretation"], r
    # 30 vs 30 with a clear, low-variance spread and full coverage -> significant
    big = [{"disagreement_score": 10, "rating_diverges": False, "ticker_return_pct": 5 + (i % 3), "alpha_pct": 4 + (i % 3)} for i in range(30)] + \
          [{"disagreement_score": 90, "rating_diverges": True, "ticker_return_pct": -3 + (i % 3), "alpha_pct": -4 + (i % 3)} for i in range(30)]
    r = mod.build_ensemble_attribution(big)
    assert r["significant"] is True and r["t_stat"] > 2 and r["ci95_spread_pct"][0] > 0, r


def test_no_latest_regime_fallback_in_source(mod):
    src = SRC.read_text()
    assert 'latest_doc.get("regime_at_generation")' not in src
    assert '"regime_source"' in src and '"critique_source"' in src


def test_real_attribution_builder_returns_historical_rows_without_future_join(mod):
    docs = {"current":{"ticker":"X","generated_at":"2026-09-08T09:00:00Z","quote":{"price":120},"regime_at_generation":{"regime":"TODAY"}},
            "entry":{"ticker":"X","generated_at":"2026-08-01T09:00:00Z","quote":{"price":100},"verdict":{"rating":"BUY"}},
            "critic":{"ticker":"X","generated_at":"2026-09-08T09:00:00Z","critique":{"alternative_rating":"SELL","disagreement_score":99}}}
    mod.list_keys_under = lambda prefix: ["current"] if prefix == mod.RESEARCH_PREFIX else ["critic"]
    mod.read_s3_json = lambda key: docs[key]
    mod.list_history_for_ticker = lambda ticker:[("2026-08-01","entry")]
    rows=mod.build_per_call_attribution({"X":120},100,{"2026-08-01":100})
    assert len(rows)==2 and rows[0]["disagreement_score"] is None and rows[0]["regime_at_generation"] is None
    rows.sort(key=lambda r:r["alpha_pct"] if r["alpha_pct"] is not None else -999, reverse=True)
    assert rows[0]["alpha_pct"]==20
    critique={"generated_at":"2026-09-06T12:00:00+00:00"}
    assert mod.critique_available_at(critique,"2026-09-06T13:00:00+02:00") is None


def test_significance_requires_twenty_actual_alpha_pairs(mod):
    calls=[]
    for contested,vals in ((False,[4,6]),(True,[-4,-6])):
        for i in range(20):
            calls.append(dict(disagreement_score=90 if contested else 10,rating_diverges=contested,
                              ticker_return_pct=1,alpha_pct=vals[i] if i<2 else None))
    result=mod.build_ensemble_attribution(calls)
    assert result["significant"] is False and result["n_alpha_consensus"]==2


if __name__ == "__main__":
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn(mod); print("ok", name)
    print("research-backtest tests passed: %d" % len(tests))
