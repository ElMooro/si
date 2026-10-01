from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

SOURCE=Path(__file__).parents[1]/"source"
sys.path.insert(0,str(SOURCE))
from risk_engine import build_output, validate_output  # noqa:E402

NOW=datetime(2026,9,4,20,0,tzinfo=timezone.utc)
REGISTRY=json.loads((SOURCE/"input_registry.json").read_text())

def base_inputs():
    ts=NOW.isoformat()
    feeds={
      "risk_gate":{"generated_at":ts,"posture":"RISK_ON","composite":1,"sizing_multiplier":0.8},
      "crisis":{"generated_at":ts,"defcon_level":4,"master_crisis_score":20,"components_available":4,"components":[{"available":True,"age_hours":1} for _ in range(4)]},
      "bond_warroom":{"generated_at":ts,"heartbeat":{"score":20,"regime":"CALM"},"equity_risk":{"score":20,"state":"CALM"},"eurodollar_shortage":{"score":20,"state":"CALM"},"panels":{"rates":[{"key":"UST10","last":4.0}]}},
      "eurodollar_stress":{"generated_at":ts,"composite_score":20,"severity":"CALM"},
      "credit_composite":{"generated_at":ts,"composite":20},
    }
    metas={s["id"]:{"last_modified":ts,"error":None} for s in REGISTRY["inputs"]}
    for spec in REGISTRY["inputs"]:
        if spec["id"] not in feeds: metas[spec["id"]]["error"]="missing optional fixture"
    return feeds,metas

def settlement(as_of="2026-09-03",regime="CALM",score=20,ftd=10,ftr=20):
    return {"generated_at":NOW.isoformat(),"treasury":{"scope":"US_TREASURY_INCLUDING_TIPS","as_of":as_of,"unit":"USD_bn_par","ftd_bn":ftd,"ftr_bn":ftr,"gross_bn":ftd+ftr,"ftd":[[as_of,ftd]],"ftr":[[as_of,ftr]],"gross":[[as_of,ftd+ftr]],"stats":{"ftd":{"latest":ftd,"z":0,"pctile":20},"ftr":{"latest":ftr,"z":0,"pctile":30},"gross":{"latest":ftd+ftr,"z":0,"pctile":25}},"regime":regime,"score":score,"components":[{"key":"ust_ex_tips"},{"key":"tips"}],"complete":True}}

def test_separate_ftd_ftr_combined_stats_regime_and_as_of_are_visible():
    feeds,metas=base_inputs(); feeds["settlement_fails"]=settlement(ftd=11,ftr=23); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    card=next(c for c in out["domains"] if c["id"]=="settlement_fails")
    metrics={m["label"]:m["value"] for m in card["metrics"]}
    assert metrics["Fails to deliver"]==11
    assert metrics["Fails to receive"]==23
    assert metrics["Gross fails"]==34
    assert metrics["Gross z-score"]==0
    assert metrics["Gross percentile"]==25
    assert metrics["Observation as-of"]=="2026-09-03"
    assert card["state"]=="CALM"
    assert out["treasury_fails"]["scope"]=="US_TREASURY_INCLUDING_TIPS"
    assert out["treasury_fails"]["unit"]=="USD_bn_par"
    assert out["treasury_fails"]["ftd_bn"]==11
    assert out["treasury_fails"]["ftr_bn"]==23
    assert out["treasury_fails"]["gross_bn"]==34
    assert out["treasury_fails"]["stats"]["gross"]["pctile"]==25
    assert out["treasury_fails"]["status"]=="FRESH"
    assert 0 <= out["coverage"]["ratio"] <= 1
    assert out["coverage"]["fresh"] >= 6
    assert out["freshness"]["status"]=="DEGRADED"
    assert "Missing or stale critical evidence never counts as an all-clear." in out["plain_english"]

def test_weekly_settlement_freshness_uses_observation_not_daily_generation():
    feeds,metas=base_inputs(); stale=(NOW-timedelta(hours=241)).date().isoformat(); feeds["settlement_fails"]=settlement(as_of=stale,regime="CRISIS",score=99); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="settlement_fails")
    assert health["status"]=="STALE"
    assert health["freshness_basis"]=="weekly_observation"
    assert out["treasury_fails"]["status"]=="STALE"
    assert out["treasury_fails"]["ftd_bn"] is None
    assert out["policy"]["mode"]=="SELECTIVE_RISK_ON"
    assert not any("settlement fails" in r.lower() for r in out["hard_vetoes"])

def test_severe_fails_veto_and_tighten_capital():
    feeds,metas=base_inputs(); feeds["settlement_fails"]=settlement(regime="CRISIS",score=90); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    assert out["policy"]["mode"]=="DEFENSIVE"
    assert out["policy"]["allows_new_entries"] is False
    assert out["exposure_cap_pct"]<=10
    assert out["capital_decision"]=="STAY IN CASH / SHORT-TERM TREASURIES"
    assert any("settlement fails" in r.lower() for r in out["hard_vetoes"])

def test_malformed_and_nonfinite_critical_data_fail_closed():
    feeds,metas=base_inputs(); feeds["credit_composite"]["composite"]=float("nan")
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="credit_composite")
    assert health["status"]=="INVALID"
    assert out["policy"]["mode"]=="DATA_HOLD"
    assert out["exposure_cap_pct"]==0
    validate_output(out)

def test_optional_malformed_data_does_not_fabricate_neutral_score():
    feeds,metas=base_inputs(); feeds["dollar_radar"]={"generated_at":NOW.isoformat(),"dollar_pressure":"NaN"}; metas["dollar_radar"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    card=next(c for c in out["domains"] if c["id"]=="dollar_radar")
    assert card["status"]=="INVALID" and card["score"] is None

def test_independent_lenses_never_loosen_master_veto():
    feeds,metas=base_inputs(); feeds["risk_gate"].update(posture="RISK_OFF",composite=-1,sizing_multiplier=0.2); feeds["settlement_fails"]=settlement(); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    assert out["policy"]["mode"]=="DEFENSIVE"
    assert out["exposure_cap_pct"]<=10
    assert out["capital_decision"]=="STAY IN CASH / SHORT-TERM TREASURIES"

def test_missing_critical_feed_fails_closed_only_by_documented_criticality():
    feeds,metas=base_inputs(); del feeds["risk_gate"]; metas["risk_gate"]["error"]="NoSuchKey"
    out=build_output(REGISTRY,feeds,metas,NOW)
    assert out["status"]=="DATA_HOLD" and out["policy"]["mode"]=="DATA_HOLD"

def test_materially_future_critical_timestamp_is_invalid_and_fails_closed():
    feeds,metas=base_inputs()
    feeds["risk_gate"]["generated_at"]=(NOW+timedelta(hours=1)).isoformat()
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="risk_gate")
    assert health["status"]=="INVALID"
    assert "future" in health["error"]
    assert health["age_h"] < 0
    assert out["status"]=="DATA_HOLD"
    assert out["policy"]["allows_new_entries"] is False

def test_small_clock_skew_does_not_invalidate_risk_source():
    feeds,metas=base_inputs()
    feeds["risk_gate"]["generated_at"]=(NOW+timedelta(minutes=4)).isoformat()
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="risk_gate")
    assert health["status"]=="FRESH"
    assert health["age_h"] < 0


def withheld_inputs():
    """Invented contract fixtures; never market observations or qualified scores."""
    feeds, metas = base_inputs()
    feeds['risk_gate'].update(contract='risk-gate-research.v1', posture='UNAVAILABLE',
                             composite=None, sizing_multiplier=None, calls_eligible=False, sizing_eligible=False)
    feeds['eurodollar_stress'].update(contract='eurodollar-native-research.v1', composite_score=None,
                                    calls_eligible=False, sizing_eligible=False)
    feeds['credit_composite'].update(contract='credit-composite-abstention.v1', composite=None,
                                    calls_eligible=False, sizing_eligible=False)
    feeds['bond_warroom']['eurodollar_shortage'].update(state='UNQUALIFIED', score=None, points=None,
                                                      calls_eligible=False, sizing_eligible=False)
    return feeds, metas


def before_diagnostics():
    import hashlib
    fixture = SOURCE.parent / 'tests/fixtures/risk_engine_before_diagnostics.py.txt'
    raw = fixture.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == 'c13582bae17050bd5ad94074b3522d08f81507bdd157f6169e6b416fd1cef099'
    namespace = {'__name__': 'before_diagnostics'}
    exec(compile(raw, str(fixture), 'exec'), namespace)
    return namespace


def without_diagnostics(value):
    if isinstance(value, dict):
        return {k: without_diagnostics(v) for k, v in value.items() if k != 'authority_diagnostic'}
    if isinstance(value, list):
        return [without_diagnostics(v) for v in value]
    return value


def test_explicit_withdrawals_explain_each_independent_critical_rejection():
    feeds, metas = withheld_inputs()
    out = build_output(REGISTRY, feeds, metas, NOW)
    rejected = {h['name']: h for h in out['critical_failures']}
    assert set(rejected) == {'risk_gate', 'bond_warroom', 'eurodollar_stress', 'credit_composite'}
    assert {h['status'] for h in rejected.values()} == {'INVALID'}
    for name, h in rejected.items():
        assert h['authority_diagnostic']['source_id'] == name
        assert h['authority_diagnostic']['code'] == 'PRODUCER_AUTHORITY_WITHHELD'
        assert h['authority_diagnostic']['effect'] == 'EXPLANATION_ONLY'
    assert out['policy']['mode'] == 'DATA_HOLD'
    assert out['policy']['allows_new_entries'] is False
    assert out['policy']['sizing_multiplier'] == out['exposure_cap_pct'] == 0
    assert out['critical_failures'][0]['error'] == 'missing required fields: composite, sizing_multiplier'
    assert out['source_health'][1]['name'] == 'crisis'
    assert 'authority_diagnostic' not in out['source_health'][1]


def test_whole_output_and_inputs_preserved_across_policy_and_failure_matrix():
    old = before_diagnostics()['build_output']
    cases = [base_inputs(), withheld_inputs()]
    for name in ('risk_gate', 'bond_warroom', 'eurodollar_stress', 'credit_composite'):
        # Each withdrawal alone must still hold; fixing only the bond cannot release other failures.
        f, m = base_inputs(); f[name] = withheld_inputs()[0][name]; cases.append((f, m))
        for mutation in ('missing', 'empty', 'stale', 'future', 'transport'):
            f, m = withheld_inputs()
            if mutation == 'missing': f.pop(name)
            elif mutation == 'empty': f[name] = {}
            elif mutation == 'stale': f[name]['generated_at'] = (NOW - timedelta(days=10)).isoformat()
            elif mutation == 'future': f[name]['generated_at'] = (NOW + timedelta(days=1)).isoformat()
            else: m[name]['error'] = 'invented transport failure'
            cases.append((f, m))
    for posture in ('RISK_ON', 'NEUTRAL', 'RISK_OFF', 'SEVERE', 'UNAVAILABLE'):
        f, m = base_inputs(); f['risk_gate']['posture'] = posture; cases.append((f, m))
    for score in (None, -1, 0, 25, 45, 65, 80, 100, 101, True, 'invalid'):
        f, m = base_inputs(); f['credit_composite']['composite'] = score; cases.append((f, m))
    for feeds, metas in cases:
        before = deepcopy((feeds, metas))
        expected = old(REGISTRY, feeds, metas, NOW)
        actual = build_output(REGISTRY, feeds, metas, NOW)
        assert without_diagnostics(actual) == expected
        assert (feeds, metas) == before
    assert len(cases) == 42


def test_diagnostic_does_not_infer_withdrawal_from_nulls_or_unknown_contracts():
    from risk_engine import withheld_authority_diagnostic
    for name in ('risk_gate', 'eurodollar_stress', 'credit_composite'):
        original = withheld_inputs()[0][name]
        for field, value in [('contract', 'unknown.v9'), ('contract', None),
                             ('calls_eligible', None), ('calls_eligible', 0), ('calls_eligible', True),
                             ('sizing_eligible', 'false'), ('sizing_eligible', 0)]:
            payload = deepcopy(original); payload[field] = value
            assert withheld_authority_diagnostic(name, payload) is None
        score_field = 'composite_score' if name == 'eurodollar_stress' else 'composite'
        for value in (0, False, 'invalid', float('nan')):
            payload = deepcopy(original); payload[score_field] = value
            assert withheld_authority_diagnostic(name, payload) is None
    payload = withheld_inputs()[0]['bond_warroom']
    for field, value in [('state', 'UNKNOWN'), ('score', 0), ('calls_eligible', 0), ('sizing_eligible', None)]:
        mutated = deepcopy(payload); mutated['eurodollar_shortage'][field] = value
        assert withheld_authority_diagnostic('bond_warroom', mutated) is None
    assert withheld_authority_diagnostic('unknown', {}) is None


def test_diagnostics_do_not_copy_free_text_or_mask_transport_failures():
    feeds, metas = withheld_inputs()
    for name, payload in feeds.items():
        payload['explanation'] = 'DO_NOT_COPY_FREE_TEXT'
        payload['replay'] = {'location': 'DO_NOT_COPY_LOCATIONS'}
    out = build_output(REGISTRY, feeds, metas, NOW)
    raw = json.dumps([h.get('authority_diagnostic') for h in out['source_health']])
    assert 'DO_NOT_COPY' not in raw
    metas['risk_gate']['error'] = 'invented transport failure'
    out = build_output(REGISTRY, feeds, metas, NOW)
    h = next(h for h in out['source_health'] if h['name'] == 'risk_gate')
    assert h['status'] == 'MISSING' and 'authority_diagnostic' not in h


def test_entire_preexisting_policy_ast_is_unchanged():
    import ast
    baseline = ast.parse((SOURCE.parent / 'tests/fixtures/risk_engine_before_diagnostics.py.txt').read_text())
    current = ast.parse((SOURCE / 'risk_engine.py').read_text())
    current.body = [n for n in current.body if not isinstance(n, ast.FunctionDef) or n.name != 'withheld_authority_diagnostic']
    audit = next(n for n in current.body if isinstance(n, ast.FunctionDef) and n.name == 'audit_sources')
    loop = next(n for n in audit.body if isinstance(n, ast.For))
    addition = loop.body.pop()
    assert isinstance(addition, ast.If)
    assert ast.unparse(addition.test) == "status == 'INVALID'"
    assert ast.unparse(addition.body[0].value) == 'withheld_authority_diagnostic(source_id, payload)'
    assert ast.dump(current) == ast.dump(baseline)


def test_existing_khalid_consumer_accepts_same_closed_policy_with_diagnostics():
    import ast
    from risk_engine import number
    path = SOURCE.parents[1] / 'justhodl-khalid/source/lambda_function.py'
    tree = ast.parse(path.read_text())
    nodes = [n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name == '_risk_payload_error')
             or (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id.startswith('KHALID_RISK_') for t in n.targets))]
    namespace = {'number': number}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    feeds, metas = withheld_inputs()
    output = build_output(REGISTRY, feeds, metas, NOW)
    check = namespace['_risk_payload_error']
    assert check('khalid_risk', output) is None
    assert check('khalid_risk', without_diagnostics(output)) is None
    for field, value in [('allows_new_entries', True), ('exposure_cap_pct', 10), ('sizing_multiplier', 0.1)]:
        mutant = deepcopy(output); mutant['policy'][field] = value
        assert check('khalid_risk', mutant) is not None
