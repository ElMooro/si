"""Synthetic complete-input and actual native concern regressions; no HTTP."""
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
import importlib.util,json,math,sys,types
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import auction_concerns as model
import auction_cross_observations as cross_model
from auction_weighting import weighted_summary
TODAY=date(2026,9,29)


def inputs():
    row={'auction_date':TODAY.isoformat(),'cusip':'SYNTHETIC_001','instrument_contract':'treasury-instrument.v1',
         'instrument_classification_status':'verified','instrument_kind':'NOMINAL_COUPON','tenor_bucket':'coupons_gt_3y',
         'indicator_scores':{'pd_absorption':80,'indirect_collapse':0},'composite_score':40,'accepted_billions':1}
    frames=json.loads((ROOT/'tests/fixtures/auction-cross-observations.json').read_bytes())['cases']['valid']['source_frames']
    rows=[row,dict(deepcopy(row),auction_date=(TODAY-timedelta(days=7)).isoformat(),cusip='SYNTHETIC_002')]
    return refresh([rows,{}, {},{},cross_model.build(frames,TODAY)])


def refresh(v):
    history=[]
    for n in range(7,-1,-1):
        end=TODAY-timedelta(days=n);begin=end-timedelta(days=14)
        selected=[row for row in v[0] if begin<=date.fromisoformat(row['auction_date'])<=end]
        result=weighted_summary(selected)['composite']
        history.append({'date':end.isoformat(),'composite':round(result,1) if result is not None else None})
    v[1]={'current':deepcopy(history[-1]),'series':history}
    rows=[row for row in v[0] if row['tenor_bucket']=='coupons_gt_3y' and TODAY-timedelta(days=14)<=date.fromisoformat(row['auction_date'])<=TODAY]
    value=weighted_summary(rows)['composite']
    v[2]={'coupons_gt_3y':{'composite':round(value,1) if value is not None else None,'n_auctions':len(rows),'latest_date':max((r['auction_date'] for r in rows),default=None)}}
    return v


def run(values=None):return model.calculate(*(values or inputs()),TODAY)


def unavailable(out):
    assert all(row['heuristic_score'] is None and row['status']=='unavailable' for row in out.values())


def test_complete_inputs_preserve_formula_and_legacy_alias():
    out=run();soft=out['p_soft_demand_30d']
    assert soft['heuristic_score']==45 and soft['calculation']['sum_before_cap']==45
    assert soft['input_trace']['participation']['n_coupon_observations']==2
    assert out['p_failed_auction_30d']==dict(soft,deprecated=True,alias_of='p_soft_demand_30d')
    assert out['p_supply_volatility_30d']['heuristic_score'] is not None
    assert out['p_regime_escalation_14d']['heuristic_score'] is None


def test_real_zero_inputs_remain_measured_and_fixed_intercept_is_explicit():
    v=inputs()
    for row in v[0]:row['composite_score']=0;row['indicator_scores']['pd_absorption']=0
    refresh(v)
    out=run(v)['p_soft_demand_30d']
    assert out['heuristic_score']==5 and out['drivers']['momentum']==0
    assert out['drivers']['pd_concern'] is False and out['calculation']['terms']['base']==5


def test_missing_long_tenor_and_momentum_never_become_a_low_score():
    v=inputs();v[1]['series']=[];v[2]['coupons_gt_3y']['composite']=None
    out=run(v);unavailable(out)
    assert out['p_soft_demand_30d']['drivers']['momentum'] is None
    assert out['p_soft_demand_30d']['drivers']['coupons_long_stress'] is None


def test_exact_seven_calendar_day_difference_uses_eight_daily_rows():
    v=inputs();prototype=v[0][0]
    v[0]=[dict(deepcopy(prototype),cusip='SYNTHETIC_'+str(i),auction_date=(TODAY-timedelta(days=7-i)).isoformat(),composite_score=10+2*i) for i in range(8)]
    refresh(v)
    out=run(v)['p_soft_demand_30d']
    assert out['drivers']['momentum']==7 and out['heuristic_score']==37
    trace=out['input_trace']['momentum'];assert trace['start_date']=='2026-09-22' and trace['end_date']=='2026-09-29'


def test_sparse_history_does_not_substitute_a_28_day_change():
    v=inputs();days=['2026-09-01','2026-09-05','2026-09-10','2026-09-15','2026-09-20','2026-09-25','2026-09-29']
    v[1]={'series':[{'date':d,'composite':i} for i,d in enumerate(days)]};v[1]['current']=deepcopy(v[1]['series'][-1])
    unavailable(run(v))


def test_missing_interior_day_does_not_change_exact_available_endpoints():
    v=inputs();v[1]['series'].pop(3)
    assert run(v)['p_soft_demand_30d']['heuristic_score']==45
    v[1]['series'][2]['composite']=None
    assert run(v)['p_soft_demand_30d']['heuristic_score']==45


def test_explicit_null_or_malformed_current_never_crashes():
    for current in (None,[],{},True,{'date':'2026-09-28','composite':30},{'date':'2026-09-29','composite':False}):
        v=inputs();v[1]['current']=current;unavailable(run(v))
    for history in (None,[],True,{}):
        v=inputs();v[1]=history;unavailable(run(v))


def test_duplicate_future_and_invalid_history_dates_are_not_silently_selected():
    for row in ({'date':'2026-09-22','composite':99},{'date':'2026-09-30','composite':30},{'date':'2026-02-31','composite':30}):
        v=inputs();v[1]['series'].append(row);unavailable(run(v))
    v=inputs();v[1]['current']['composite']=99;unavailable(run(v))


def test_participation_uses_every_eligible_row_after_position_twenty():
    v=inputs();first=v[0][0];first['indicator_scores']['pd_absorption']=0
    v[0]=[dict(deepcopy(first),cusip='SYNTHETIC_'+str(i),auction_date=(TODAY-timedelta(days=7)).isoformat() if i==0 else TODAY.isoformat()) for i in range(25)]
    v[0][-1]['indicator_scores']['pd_absorption']=80;refresh(v)
    out=run(v)['p_soft_demand_30d'];assert out['heuristic_score']==45
    assert len(out['input_trace']['participation']['observations'])==25
    assert out['input_trace']['participation']['coverage']['pd_absorption']['n_observations']==25


def test_bill_scores_and_old_coupon_rows_do_not_enter_coupon_flags():
    v=inputs()
    for row in v[0]:row['indicator_scores']['pd_absorption']=0
    bill=dict(deepcopy(v[0][0]),cusip='SYNTHETIC_BILL',instrument_kind='BILL',tenor_bucket='bills_lt_90d')
    bill['indicator_scores']={'pd_absorption':100};v[0].append(bill)
    old=dict(deepcopy(v[0][0]),cusip='SYNTHETIC_OLD',auction_date='2026-09-14');old['indicator_scores']['pd_absorption']=100;v[0].append(old)
    refresh(v);out=run(v)['p_soft_demand_30d'];assert out['heuristic_score']==25 and out['drivers']['pd_concern'] is False
    assert out['input_trace']['participation']['n_coupon_observations']==2


def test_missing_coupon_dimensions_and_invalid_identities_withhold_scores():
    for change in ('missing_pd','missing_indirect','unknown_kind','missing_cusip','object_cusip','duplicate','future','invalid_date'):
        v=inputs();row=v[0][0]
        if change=='missing_pd':row['indicator_scores'].pop('pd_absorption')
        elif change=='missing_indirect':row['indicator_scores']['indirect_collapse']=None
        elif change=='unknown_kind':row['instrument_kind']='UNKNOWN'
        elif change=='missing_cusip':row['cusip']=None
        elif change=='object_cusip':row['cusip']={}
        elif change=='duplicate':v[0].append(deepcopy(row))
        elif change=='future':row['auction_date']='2026-09-30'
        else:row['auction_date']='garbage'
        assert run(v)['p_soft_demand_30d']['heuristic_score'] is None,change


def test_tenor_mean_is_recomputed_with_all_known_weights():
    for change in ('wrong_count','wrong_score','wrong_date','missing_weight','negative_weight','wrong_kind'):
        v=inputs();row=v[0][0];tenor=v[2]['coupons_gt_3y']
        if change=='wrong_count':tenor['n_auctions']=True
        elif change=='wrong_score':tenor['composite']=41
        elif change=='wrong_date':tenor['latest_date']='2026-09-28'
        elif change=='wrong_kind':row['instrument_kind']='TIPS'
        else:row['accepted_billions']={'missing_weight':None,'negative_weight':-1,'zero_weight':0}[change]
        assert run(v)['p_soft_demand_30d']['heuristic_score'] is None,change


def test_zero_weight_is_known_but_an_all_zero_denominator_is_unavailable():
    v=inputs();v[0][0]['accepted_billions']=0
    assert run(v)['p_soft_demand_30d']['heuristic_score']==45
    assert run(v)['p_soft_demand_30d']['input_trace']['long_coupon']['weighting']['n_zero_weights']==1
    v[0][1]['accepted_billions']=0
    unavailable(run(v))


def test_consistent_dated_history_cannot_override_its_underlying_auction_weights():
    v=inputs();v[1]['series'][-1]['composite']=31;v[1]['current']['composite']=31
    unavailable(run(v))
    v=inputs();v[1]['series'][0]['composite']=99
    unavailable(run(v))


def test_boolean_strings_and_nonfinite_values_never_become_measured_scores():
    for value in (True,False,'30',math.inf,math.nan,-1,101):
        v=inputs();v[1]['series'][0]['composite']=value
        out=run(v);unavailable(out);json.dumps(out,allow_nan=False)
        v=inputs();v[0][0]['indicator_scores']['pd_absorption']=value
        out=run(v);assert out['p_soft_demand_30d']['heuristic_score'] is None;json.dumps(out,allow_nan=False)


def test_legacy_anchor_rank_and_similarity_cannot_supply_amplification():
    for similarity in (None,0,1,True,math.inf):
        v=inputs();v[3]={'comparability_verified':True,'top_matches':[{'regime':'GFC_PEAK','date':'2008-09-17','similarity':similarity}]}
        out=run(v)['p_regime_escalation_14d']
        assert out['heuristic_score'] is None and out['input_trace']['historical_anchor']['amplifier'] is None
        assert 'historical_anchor_comparability_unverified' in out['missing_inputs'];json.dumps(out,allow_nan=False)


def test_cross_context_must_match_reviewed_dated_source_arithmetic():
    for change in ('missing','changed','stale_day','legacy','false_zero'):
        v=inputs()
        if change=='missing':v[4]={}
        elif change=='changed':v[4]['repo_stress']['spread_bp']+=1
        elif change=='stale_day':v[4]['calculation_as_of']='2026-09-28'
        elif change=='legacy':v[4].pop('measurement_contract')
        else:v[4]['dollar_strength']['change_30d_target_pct']=False
        out=run(v);assert out['p_supply_volatility_30d']['heuristic_score'] is None
        assert out['p_soft_demand_30d']['heuristic_score']==45


def test_order_independence_no_input_mutation_and_finite_output():
    v=inputs();before=deepcopy(v);a=run(v);assert v==before
    v[1]['series'].reverse();b=run(v)
    assert [r['heuristic_score'] for r in a.values()]==[r['heuristic_score'] for r in b.values()]
    json.dumps(a,allow_nan=False)
    for row in a.values():
        assert row['probability'] is None and row['forecast_horizon_days'] is None and row['calibrated'] is False
        assert all(row[key] is False for key in model.PERMISSIONS)


def test_actual_native_adapter_uses_explicit_date_and_preserves_all_public_keys():
    source=ROOT/'aws/lambdas/justhodl-auction-crisis-detector/source/auction_crisis_v2.py'
    with patch.dict(sys.modules,{'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'TEST_ONLY')}):
        spec=importlib.util.spec_from_file_location('concern_native_test',source);native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
    class Fixed(datetime):
        @classmethod
        def now(cls,tz=None):return datetime(2026,9,29,tzinfo=timezone.utc)
    with patch.object(native,'datetime',Fixed),patch.object(native.urllib.request,'urlopen',side_effect=AssertionError('No provider request')):
        assert native.compute_tail_risk(*inputs())==run()
        assert all(v['heuristic_score'] is None for v in native.compute_tail_risk([],{'current':None},{},{},{}).values())


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction concern regressions passed:',len(tests))
