"""Forward heuristic input/coverage boundaries and actual native population wiring."""
from pathlib import Path
from datetime import date,datetime,timezone
from copy import deepcopy
import json,runpy,sys
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import auction_forward as model
TODAY=date(2026,9,29)


def fixture():
    upcoming={'instrument_contract':'treasury-instrument.v1','instrument_classification_status':'verified',
              'instrument_kind':'NOMINAL_COUPON','security_term':'10-Year','tenor_bucket':'coupons_gt_3y',
              'auction_date':'2026-09-30','days_ahead':1,'offering_amount_billions':50}
    tenors={name:{'n_auctions':0,'composite':None} for name in model.TENORS}
    tenors['coupons_gt_3y']={'n_auctions':1,'composite':0}
    rows=[{**upcoming,'auction_date':'2026-09-17','cusip':'TEST00001','accepted_billions':50}]
    return upcoming,tenors,rows


def call(upcoming=None,tenors=None,rows=None):
    u,t,r=fixture();return model.calculate(u if upcoming is None else upcoming,t if tenors is None else tenors,r if rows is None else rows,TODAY)


def test_missing_every_input_is_unavailable_and_never_high_confidence():
    result=model.calculate({}, {}, [], TODAY)
    assert result['forecast_score'] is None and result['forecast_label']=='UNAVAILABLE'
    assert result['confidence']=='UNVALIDATED' and result['missing_inputs']
    assert 'Expected normal clearing' not in result['narrative']
    assert not any(result[k] for k in ('calls_eligible','sizing_eligible','forecast_eligible','execution_eligible','calibrated'))


def test_real_zero_offering_is_a_minus_hundred_percent_size_comparison():
    u,t,r=fixture();u['offering_amount_billions']=0;out=model.calculate(u,t,r,TODAY)
    assert out['forecast_score']==0 and out['components']['size_shock_pct']==-100
    assert out['size_baseline']['offering_usd_bn']==0 and out['confidence']=='UNVALIDATED'


def test_missing_invalid_or_negative_offering_does_not_borrow_the_mean():
    for bad in (None,True,False,'NaN','inf',-1,'1e999'):
        u,t,r=fixture();u['offering_amount_billions']=bad;out=model.calculate(u,t,r,TODAY)
        assert out['forecast_score'] is None and out['components']['size_shock_pct'] is None
        assert 'offering_amount' in out['missing_inputs']


def test_all_complete_zero_inputs_remain_real_zero_without_forecast_permission():
    out=call();assert out['forecast_score']==0 and out['forecast_label']=='CALM'
    assert out['status']=='complete_unvalidated_heuristic' and out['confidence']=='UNVALIDATED'
    assert out['components']['own_tenor_stress']==out['components']['cross_tenor_contagion']==0


def test_a_missing_populated_tenor_cannot_silently_leave_the_cross_mean():
    u,t,r=fixture();t['tips']={'n_auctions':2,'composite':None};out=model.calculate(u,t,r,TODAY)
    assert out['forecast_score'] is None and out['components']['cross_tenor_contagion'] is None
    assert len(out['cross_tenor_population'])==6
    for change in ({}, {'n_auctions':True,'composite':0}, {'n_auctions':1,'composite':'0'}, {'n_auctions':1,'composite':10**1000}):
        t['tips']=change;assert model.calculate(u,t,r,TODAY)['forecast_score'] is None


def test_missing_bucket_or_unknown_extra_bucket_withholds_population_completeness():
    u,t,r=fixture();t.pop('frn');assert model.calculate(u,t,r,TODAY)['forecast_score'] is None
    u,t,r=fixture();t['unexpected']={'n_auctions':1,'composite':99};out=model.calculate(u,t,r,TODAY)
    assert out['forecast_score'] is None and out['unexpected_tenor_buckets']==['unexpected']


def test_horizon_must_match_a_real_upcoming_date_and_never_supplies_default_zero():
    for days in (None,True,'1',-1,31,2):
        u,t,r=fixture();u['days_ahead']=days;out=model.calculate(u,t,r,TODAY)
        assert out['forecast_score'] is None and out['components']['time_decay_factor'] is None
    u,t,r=fixture();u['auction_date']='2026-02-31';assert model.calculate(u,t,r,TODAY)['forecast_score'] is None


def test_cohorts_separate_instrument_kind_full_term_and_unknown_identity():
    u,t,r=fixture()
    for change in ({'instrument_kind':'TIPS'},{'security_term':'5-Year'},{'instrument_contract':'legacy'}):
        r.append({**r[0],**change,'accepted_billions':999})
    out=model.calculate(u,t,r,TODAY)
    assert out['size_baseline']['n_supplied_observations']==4
    assert out['size_baseline']['n_cohort_members']==1 and out['size_baseline']['mean_accepted_usd_bn']==50
    assert out['size_baseline']['n_incomparable_identity']==3
    u['tenor_bucket']='bills_lt_90d';assert model.calculate(u,t,r,TODAY)['forecast_score'] is None


def test_every_missing_comparable_size_withholds_the_mean_instead_of_biasing_it():
    for value in (None,True,'nan',-1):
        u,t,r=fixture();r.append({**r[0],'cusip':'TEST00002','accepted_billions':value});out=model.calculate(u,t,r,TODAY)
        assert out['forecast_score'] is None and out['size_baseline']['mean_accepted_usd_bn'] is None
        assert out['size_baseline']['n_cohort_members']==2 and out['size_baseline']['n_invalid_amounts']==1


def test_zero_sizes_count_in_the_mean_and_zero_denominator_is_not_imputed():
    u,t,r=fixture();r.append({**r[0],'cusip':'TEST00002','accepted_billions':0});out=model.calculate(u,t,r,TODAY)
    assert out['size_baseline']['mean_accepted_usd_bn']==25 and out['components']['size_shock_pct']==100
    for row in r:row['accepted_billions']=0
    out=model.calculate(u,t,r,TODAY);assert out['forecast_score'] is None and out['size_baseline']['n_valid_amounts']==2


def test_future_and_old_baseline_rows_do_not_leak_into_the_current_window():
    u,t,r=fixture()
    for when in ('2026-09-30','2026-06-01'):r.append({**r[0],'auction_date':when,'accepted_billions':999})
    out=model.calculate(u,t,r,TODAY);assert out['size_baseline']['mean_accepted_usd_bn']==50
    assert out['size_baseline']['n_outside_window']==2
    r.append({**r[0],'auction_date':'bad'});assert model.calculate(u,t,r,TODAY)['forecast_score'] is None


def test_complete_arithmetic_and_time_parameter_remain_descriptive():
    u,t,r=fixture();u['offering_amount_billions']=100;t['coupons_gt_3y']['composite']=40
    t['tips']={'n_auctions':1,'composite':60};out=model.calculate(u,t,r,TODAY)
    assert out['components']['cross_tenor_contagion']==50 and out['forecast_score']==45
    assert out['confidence']=='UNVALIDATED' and out['forecast_horizon_days'] is None
    assert not out['size_baseline']['reopening_strata_verified']


def test_overflow_size_comparison_withholds_output_and_never_serializes_nonfinite():
    u,t,r=fixture();u['offering_amount_billions']=1e308;r[0]['accepted_billions']=1e-300
    out=model.calculate(u,t,r,TODAY);assert out['forecast_score'] is None
    assert 'finite_size_comparison' in out['missing_inputs'];json.dumps(out,allow_nan=False)


def test_all_comparable_members_survive_and_inputs_are_not_mutated():
    u,t,r=fixture();r=[{**r[0],'cusip':str(i)} for i in range(250)]
    before=deepcopy((u,t,r));out=model.calculate(u,t,r,TODAY)
    assert out['size_baseline']['n_cohort_members']==len(out['size_baseline']['observations'])==250
    assert out['size_baseline']['observations'][-1]['cusip']=='249' and (u,t,r)==before


def test_actual_native_forward_adapter_uses_new_complete_input_contract():
    support=runpy.run_path(str(ROOT/'tests/test_auction_benchmarks.py'));m=support['engine']()
    out=m.predict_auction_stress({}, {}, [])
    assert out['measurement_contract']==model.CONTRACT and out['forecast_score'] is None and out['confidence']=='UNVALIDATED'


def test_actual_native_size_context_keeps_rows_without_bid_to_cover():
    support=runpy.run_path(str(ROOT/'tests/test_auction_originals.py'));client,handler=support['native_setup']();env=handler.__globals__
    today=datetime.now(timezone.utc).date().isoformat();received=[]
    def calendar(upcoming,tenors,measurements):received.extend(measurements);return []
    env['build_forward_calendar']=calendar
    rows=[support['row'](auction_date=today,cusip='TEST00001'),
          support['row'](auction_date=today,cusip='TEST00002',bid_to_cover_ratio=None)]
    with patch('urllib.request.urlopen',return_value=support['response'](rows)):
        assert handler({},None)['statusCode']==200
    assert len(received)==2 and received[1]['btc'] is None and received[1]['cusip']=='TEST00002'


if __name__=='__main__':
    tests=[value for name,value in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction forward eligibility regressions passed:',len(tests))
