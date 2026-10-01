"""Frozen invented price/feature replay; no Lambda import, provider or AWS calls."""
import ast
import bisect
import copy
from datetime import date, timedelta
import json
import hashlib
import math
from pathlib import Path
import types
import unittest
import sys

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1] / "source/lambda_function.py"
BEFORE = ROOT / "tests/fixtures/katlin-oos-before-availability.py.txt"


def functions(path):
    return {n.name: n for n in ast.parse(path.read_text(encoding="utf-8")).body if isinstance(n, ast.FunctionDef)}


def scope(legacy=False):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    names = {"run_backtest", "validation_summary", "oos_boundary_evidence", "mean", "median", "rnd", "clamp",
             "_stats", "_bk", "feature_buckets", "row_to_vals", "learned_prior", "gates_and_tier",
             "composite", "pct_rank", "trade_plan", "build_basket"}
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    constants = {"P", "WEIGHTS", "GATE_PILLARS", "FEATURES", "HORIZONS", "ENGINE", "VERSION", "BACKTEST_KEY"}
    selected += [n for n in tree.body if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id in constants for t in n.targets)]
    if legacy:
        old = functions(BEFORE)
        selected = [old.get(n.name, n) if isinstance(n, ast.FunctionDef) else n for n in selected]
    env = {"math": math, "time": types.SimpleNamespace(time=lambda: 0.0)}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), "exec"), env)
    return env


def row(ticker, group):
    return {"ticker": ticker, "asset_class": "stock", "last":100, "knife":False,
            "quality":{"red_flags":[]}, "structure_state":"CONFIRMED", "dist_sma200_pct":-30,
            "dist_sma250_pct":-25,"dd_52w_pct":-50,"rsi_w":20 if group == 0 else 55,
            "vol_ann_pct":40,"gates":{k:True for k in ("location","washout","oversold","accumulation",
                                  "inflows","structure","catalyst","not_knife","quality")},
            "pillars":{k:75 for k in ("structure","accumulation","inflows","oversold","location",
                                       "catalyst","momentum","quality")}}


def replay(legacy=False, mutate_after=None):
    env = scope(legacy)
    dates = [(date(2020,1,1)+timedelta(days=i)).isoformat() for i in range(1000)]
    class FixtureBars:
        def __init__(self, group, benchmark=False):
            self.group=group; self.d=list(range(1000))
            self.c=[100 * math.exp((.0002 if benchmark else .0004 + group*.0004)*i)
                    * (1 + .04*math.sin(i/31 + group)) for i in self.d]
            if mutate_after is not None and not benchmark:
                self.c=[v if i<mutate_after else v*(1.5 + .001*(i-mutate_after)) for i,v in enumerate(self.c)]
            self.v=[1000000]*1000
        def pos_at_or_before(self, i): return bisect.bisect_right(self.d,i)-1
    stock={"S%02d" % i: 3e9 for i in range(60)}
    bars={t:FixtureBars(i%2) for i,t in enumerate(stock)}
    bars["SPY"]=FixtureBars(0,True)
    def features(b,p,spy_c,dates,cls,market):
        fixture=row("unused",b.group)
        return {"location":True,"knife":False,"washout":True,"oversold":True,"accumulation":True,
                "structure":True,"confirmed":True,
                "b":env["feature_buckets"](env["row_to_vals"](fixture,market))}
    captured={}
    env.update(session_keys=lambda n: ["fixture-only"], s3_json=lambda *args: {},
               build_universe=lambda f:(stock,{},set(stock)), load_bars=lambda *args:(dates,bars),
               pit_features=features, now_iso=lambda:"2022-09-27T12:00:00Z", log=lambda message:None,
               s3_put_json=lambda key,doc:captured.update(doc))
    env["run_backtest"]({"step":30,"n_stocks":60})
    rows=[row("S%02d" % i,i%2) for i in range(6)]
    env["learned_prior"](rows,captured,True)
    for r in rows:
        e=r["alpha_prior"]["126s"]["expected_excess_pct"]
        r["learned_excess_126s_pct"]=e
        r["pillars"]["learned"]=env["rnd"](env["clamp"](50+3*e),1)
    ranks={}
    for k in env["WEIGHTS"]:
        ranks[k]=dict(zip((r["ticker"] for r in rows),env["pct_rank"]([r["pillars"].get(k) for r in rows])))
    for r in rows:
        env["gates_and_tier"](r); env["composite"](r,ranks); env["trade_plan"](r)
    basket=env["build_basket"](rows,{"exposure_cap_pct":50,"entries_allowed":True,"posture":"SELECTIVE"})
    return captured, rows, basket


def evidence():
    old, oldrows, oldbasket = replay(True)
    new, newrows, newbasket = replay()
    def digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
    fields = ('ticker','learned_excess_126s_pct','pillars','tier','gates','composite','conviction','plan')
    return {
        'fixture_kind':'Invented prices and feature buckets; actual before/after producer and decision functions',
        'baseline_commit':'15698384ce4b0709b37dba24968ff45436d37ac5',
        'source_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        'predecessor_fixture_sha256':hashlib.sha256(BEFORE.read_bytes()).hexdigest(),
        'fixture_definition':'60 invented stocks, 1000 invented calendar sessions, 30-session observations; deterministic exponential/sinusoidal prices in replay(); no historical-data claim',
        'observations':new['n_obs'], 'observation_dates':new['n_dates'],
        'before_oos':old['oos'], 'after_oos':new['oos'], 'after_oos_validation':new['oos_validation'],
        'full_history_priors':{'before_sha256':digest(old['feature_stats']),'after_sha256':digest(new['feature_stats']),
                              'changed':old['feature_stats'] != new['feature_stats']},
        'before_decisions':[{k:r.get(k) for k in fields} for r in oldrows],
        'after_decisions':[{k:r.get(k) for k in fields} for r in newrows],
        'changed_decision_rows':sum(a != b for a,b in zip(oldrows,newrows)),
        'before_basket':oldbasket, 'after_basket':newbasket,
        'basket_changed':oldbasket != newbasket,
        'legacy_validation_projection':scope()['validation_summary'](old),
    }


class Tests(unittest.TestCase):
    def test_actual_producer_withholds_oos_and_preserves_all_prior_decisions(self):
        old, oldrows, oldbasket=replay(True)
        new, newrows, newbasket=replay()
        self.assertEqual(set(old["oos"]), {"63s","126s"})
        self.assertEqual(new["oos"], {})
        self.assertEqual(new["oos_validation"]["status"], "BLOCKED")
        for key in ("feature_stats","cohorts","regime_126s","per_date","n_obs","universe"):
            self.assertEqual(old[key],new[key],key)
        self.assertEqual(oldrows,newrows)
        self.assertEqual(oldbasket,newbasket)
        self.assertTrue(newbasket["core"])

    def test_endpoint_equality_crossing_missing_and_horizon_specific_counts(self):
        audit=scope()["oos_boundary_evidence"]
        for h in (21,63,126,252):
            obs=[{"i":400-h+d,"fwd":{h:(0,0,0,False)}} for d in (-1,0,1)]
            obs += [{"i":10,"fwd":{}},{"i":True,"fwd":{h:(0,0,0,False)}}]
            got=audit(obs,400,[str(i) for i in range(1000)],(h,))["folds"][str(h)+"s"]
            self.assertEqual(got["entry_before_split_with_label"],3)
            self.assertEqual(got["label_endpoint_at_or_after_split"],2)
            self.assertEqual(got["nominal_label_endpoint_before_split"],1)
            self.assertEqual(got["missing_verified_label_availability"],3)
            self.assertEqual(got["eligible_training_observations"],0)
            self.assertIsNone(got["split_decision_at"])

    def test_test_price_mutations_never_restore_validation(self):
        original,_,_=replay()
        start=original["oos_validation"]["folds"]["63s"]["split_session_index"]
        changed,_,_=replay(mutate_after=start)
        self.assertEqual(original["oos_validation"],changed["oos_validation"])
        self.assertEqual(changed["oos"],{})
        # Matured full-history priors SHOULD reflect changed realized prices.
        # Their intentional sensitivity must not be confused with OOS leakage.
        self.assertNotEqual(original["feature_stats"],changed["feature_stats"])

    def test_legacy_artifact_is_masked_without_mutating_prior_input(self):
        old,_,_=replay(True); original=copy.deepcopy(old)
        view=scope()["validation_summary"](old)
        self.assertEqual(view["oos"],{})
        self.assertEqual(view["feature_stats_126s"],old["feature_stats"]["126s"])
        self.assertEqual(view["oos_validation"]["status"],"BLOCKED")
        self.assertIn("legacy OOS metrics are withheld",view["note"])
        self.assertEqual(old,original)

    def test_no_self_promoted_availability_and_empty_split(self):
        audit=scope()["oos_boundary_evidence"]
        obs=[{"i":0,"fwd":{63:(1,1,0,False)},"available_at":"1900-01-01T00:00:00Z"}]
        got=audit(obs,100,[str(i) for i in range(200)])
        self.assertEqual(got["folds"]["63s"]["eligible_training_observations"],0)
        self.assertEqual(audit([],None,[])["status"],"BLOCKED")
        for horizon in (True,0,-1,1.5):
            with self.assertRaises(ValueError):audit([],None,[],(horizon,))

    def test_preservation_guard_rejects_unrelated_fit_and_output_mutations(self):
        sys.path.insert(0, str(ROOT / 'tests'))
        from katlin_oos_test_support import assert_oos_only_change
        old, current = functions(BEFORE), functions(SOURCE)
        for name in ('run_backtest', 'validation_summary'):
            assert_oos_only_change(self, old[name], current[name])
        altered = copy.deepcopy(current['run_backtest'])
        fit = next(n for n in altered.body if isinstance(n, ast.FunctionDef) and n.name == 'fit')
        fit.body[1].value = ast.Constant(999)
        with self.assertRaises(AssertionError): assert_oos_only_change(self, old['run_backtest'], altered)
        altered = copy.deepcopy(current['validation_summary'])
        result = altered.body[-1].value
        result.values[next(i for i,k in enumerate(result.keys) if k.value == 'feature_stats_126s')] = ast.Dict(keys=[], values=[])
        with self.assertRaises(AssertionError): assert_oos_only_change(self, old['validation_summary'], altered)


if __name__ == "__main__":
    if sys.argv[1:] == ['--evidence']:
        print(json.dumps(evidence(), indent=2, allow_nan=False))
    else:
        unittest.main()
