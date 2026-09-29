"""Actual current native code with isolated synthetic inputs; no cloud I/O."""
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from copy import deepcopy
from contextlib import redirect_stdout
from unittest.mock import patch
import io
import math
import runpy
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import auction_participation as model
import auction_buybacks
import json
support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))


def row(day='2026-09-29', **changes):
    return {'auction_date':day,'cusip':'TEST00001','type':'Note','term':'10-Year','reopening':False,
            'instrument_kind':'NOMINAL_COUPON','instrument_contract':'treasury-instrument.v1',
            'instrument_classification_status':'verified','btc':2.15,'pd':11.5,'indirect':63.,'direct':25.5,
            'total_accepted':110,'competitive_accepted':100,'high_yield':4.,'tenor':'10Y',**changes}


def history(n=4):
    return [row((date(2026,9,29)-timedelta(days=n-i)).isoformat(),btc=2+i/10,pd=10+i,indirect=60+2*i,direct=30-3*i) for i in range(n)]


def graded(source=None, prior=None):
    source=source or row();prior=prior or history()
    trace=model.grade(source,prior)
    return {**source,'grade':trace['grade'],'demand_score':trace['score'],'grading_inputs':trace,'verdict':'Synthetic participation context'}


def test_exact_three_feature_population_can_produce_a_genuine_zero_grade():
    out=model.grade(row(),history())
    assert out['status']=='complete' and out['score']==0 and out['grade']=='C'
    assert out['denominator']==3 and len(out['prior'])==4
    assert all(trace['n']==4 and trace['status']=='complete' for trace in out['features'].values())


def test_bidder_denominator_is_complete_categories_not_gross_or_zero_fallback():
    out=model.shares(row(total_accepted=999,competitive_accepted=0,direct=0,pd=10,indirect=90))
    assert out['direct_pct']==0 and out['pd_pct']==10 and out['indirect_pct']==90
    assert out['bidder_share_inputs']['denominator_usd']==100
    assert out['bidder_share_inputs']['reported_competitive_accepted_usd']==0
    for value in (None,True,-1,float('nan'),float('inf'),'10'):
        out=model.shares(row(direct=value))
        assert out['pd_pct'] is out['direct_pct'] is out['indirect_pct'] is None
    assert model.shares(row(pd=0,direct=0,indirect=0))['bidder_share_inputs']['status']=='no_positive_bidder_total'


def test_missing_current_or_any_prior_observation_never_renormalizes_grade():
    for key in ('btc','pd','direct','indirect'):
        current=row(**{key:None});out=model.grade(current,history())
        assert out['score'] is None and out['grade']=='n/a'
        prior=history();prior[1][key]=None;out=model.grade(row(),prior)
        assert out['score'] is None and len(out['prior'])==4
        assert any(trace['missing_history_indices']==[1] for trace in out['features'].values())


def test_constant_history_has_no_standardized_zero_even_when_current_equals_mean():
    for value in (2,3):
        out=model.standardize(value,[2,2,2,2])
        assert out['z'] is None and out['reason']=='zero_variance'
        assert out['mean']==2 and out['population_standard_deviation']==0
    prior=history()
    for item in prior:item['btc']=2
    out=model.grade(row(btc=3),prior)
    assert out['grade']=='n/a' and 'btc' in out['missing_features']


def test_cohort_identity_dates_duplicates_and_complete_sizes_are_required():
    for mutate in (lambda rows:rows[0].update(reopening=True),lambda rows:rows[0].update(instrument_kind='TIPS'),
                   lambda rows:rows[0].update(auction_date='2026-09-29'),lambda rows:rows[0].update(cusip=None),
                   lambda rows:rows.__setitem__(1,deepcopy(rows[0]))):
        prior=history();mutate(prior);assert model.grade(row(),prior)['score'] is None
    for n in (0,3,13):assert model.grade(row(),history(n))['score'] is None
    assert model.grade(row(instrument_classification_status='unverified'),history())['grade']=='n/a'


def test_unknown_and_partial_coupon_days_do_not_become_strong_or_bills_only():
    strong=graded(row(btc=2.5,pd=5,direct=15,indirect=80));assert strong['grade']=='A'
    missing=graded(row(btc=None,cusip='MISSING01'))
    out=model.classify([strong,missing],[])
    assert out['cohort_classes']==['unclassified'] and out['auction_count']==2
    for grade in (None,'','n/a',True):
        source={**strong,'grade':grade};assert model.classify([source],[])['cohort_classes']==['unclassified']
    for kind in (None,'UNKNOWN'):
        assert model.classify([row(type='Unknown',instrument_kind=kind)],[])['cohort_classes']==['unclassified']


def test_declared_frn_does_not_enter_bill_class_and_bill_grade_is_not_a_coupon_input():
    frn=graded(row(type='Bill',instrument_kind='FRN'),[dict(r,instrument_kind='FRN') for r in history()])
    assert model.classify([frn],[])['cohort_classes']==['coupon_mixed']
    bill=row(type='Bill',instrument_kind='BILL',term='4-Week',grade='n/a')
    assert model.classify([bill],[])['cohort_classes']==['bills_only']
    weak=graded(row(btc=1,pd=40,direct=20,indirect=40));assert weak['grade']=='F'
    assert model.classify([bill,weak],[])['cohort_classes']==['coupon_weak']


def test_buyback_category_is_recomputed_and_cannot_bypass_an_incomplete_day():
    raw={'operation_date':'2026-09-28','operation_type':'Liquidity support','security_type':'Nominal','maturity_bucket':'7-10 years',
         'par_amt_accepted':9e9,'max_par_amt':10e9}
    op=auction_buybacks.normalize(raw);buy=auction_buybacks.analyze(op,[op],'2026-09-29')
    buy['liquidity_signal']='light'
    assert model.classify([], [{'accepted':9e9,'max_par':10e9}])['cohort_classes']==['unclassified']
    assert model.classify([], [buy])['cohort_classes']==['buyback_strong']
    out=model.classify([row(instrument_kind='UNKNOWN')],[buy])
    assert 'buyback_strong' in out['classes'] and out['cohort_classes']==['unclassified']
    for accepted in (None,True,-1,11e9):
        assert model.classify([], [{**buy,'accepted':accepted}])['cohort_classes']==['unclassified']
    zero=auction_buybacks.normalize({**raw,'par_amt_accepted':0})
    assert model.classify([], [auction_buybacks.analyze(zero,[zero],'2026-09-29')])['cohort_classes']==['buyback_other']


def test_actual_analyzer_retains_missing_btc_and_same_day_does_not_shorten_prior_twelve():
    scope=support['load']('auction-desk',support['Store']())
    prior=[row((date(2026,9,29)-timedelta(days=13-i)).isoformat(),btc=2+i/100,pd=10+i/10,
               indirect=60+i/10,direct=30-i/5) for i in range(13)]
    same_day=[row(cusip='CURRENT01'),row(cusip='CURRENT02',btc=None)]
    out=scope['analyze_bank'](prior+same_day,{},'2026-09-29')
    assert len(out)==15 and out[-1]['btc'] is None and out[-1]['grade']=='n/a'
    assert len(out[-1]['grading_inputs']['prior'])==12
    assert out[-1]['grading_inputs']['prior']==out[-2]['grading_inputs']['prior']
    assert all(item['auction_date']<'2026-09-29' for item in out[-1]['grading_inputs']['prior'])


def test_native_day_description_withholds_partial_demand_and_unknown_amount_totals():
    scope=support['load']('auction-desk',support['Store']())
    strong=graded(row(btc=2.5,pd=5,direct=15,indirect=80));missing=graded(row(btc=None,cusip='MISSING01'))
    out=scope['day_verdict']('2026-09-29',[strong,missing],[])
    assert 'DEMAND STRONG' not in out['tags'] and 'PARTICIPATION UNAVAILABLE' in out['tags']
    assert 'participation unavailable' in out['headline']
    out=scope['day_verdict']('2026-09-29',[graded(),row(instrument_kind='UNKNOWN')],[])
    assert out['bills_accepted'] is out['coupons_accepted'] is None
    assert out['known_coupons_accepted']==110 and out['unknown_instrument_count']==1


def test_actual_handler_keeps_incomplete_source_rows_and_all_old_detector_ownership():
    store=support['Store']();scope=support['load']('auction-desk',store);handler=scope['lambda_handler'];env=handler.__globals__
    raw={**support['auction'](),'bid_to_cover_ratio':None}
    env.update(fetch_fd=lambda endpoint,*a,**k:[raw] if endpoint=='auctions_query' else [],
               fetch_td=lambda *a,**k:[],load_assets=lambda **k:{'series':{}},
               load_full_bank=lambda **k:{'rows':{}},fetch_fred_daily=lambda *a:{})
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):handler({},None)
    out=store.docs['data/auction-desk.json']
    assert len(out['auctions'])==1 and out['auctions'][0]['btc'] is None
    assert out['auctions'][0]['grade']=='n/a' and out['auctions'][0]['grading_inputs']['status']=='unavailable'
    assert 'data/auction-crisis.json' not in store.writes


def test_complete_grade_is_still_not_source_verification_forecast_or_sizing():
    trace=model.grade(row(),history())
    assert trace['status']=='complete'
    assert all(trace[key] is False for key in model.FLAGS)


def test_nonfinite_legacy_evidence_is_retained_as_diagnostic_and_numeric_output_unavailable():
    scope=support['load']('auction-desk',support['Store']())
    out=scope['analyze_auction'](row(btc=float('nan'),pd=float('inf')),history(),{})
    assert out['btc'] is None and out['pd'] is None and out['grade']=='n/a'
    assert out['grading_inputs']['current']['btc_original']=={'invalid_numeric':'nan'}
    assert out['invalid_numeric_inputs']['pd']=={'invalid_numeric':'inf'}
    json.dumps(out,allow_nan=False)


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction participation regressions passed:',len(tests))
