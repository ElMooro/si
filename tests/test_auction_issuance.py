"""Pure/source-owned synthetic cases; no archived code execution or network."""
from pathlib import Path
from datetime import datetime, timedelta, timezone
from contextlib import redirect_stdout
from copy import deepcopy
from fractions import Fraction
from unittest.mock import patch
import io
import json
import runpy
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
from auction_issuance import IssuanceLedger, apply_overlay, amount

DAY = '2026-09-29'
COVER = [{'start': '2025-08-25', 'end': DAY}]
support = runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))


def raw(day=DAY, accepted='100', **changes):
    return {**support['auction'](), 'auction_date': day, 'total_accepted': accepted,
            'security_type': 'Bill', 'security_term': '4-Week', 'low_discnt_rate': '4', **changes}


def calc(rows, coverage=COVER):
    return IssuanceLedger(rows, DAY, coverage).calculate(DAY, include_ledger=True)


def test_missing_amount_prevents_spurious_ninety_score_without_losing_row():
    out = calc([raw(), raw('2026-07-31','20'), raw('2026-07-30',None)])
    assert out['score'] is out['pct_above_baseline'] is None
    assert out['baseline']['counts']['missing_amount'] == 1 and len(out['ledger']['rows']) == 3
    assert out['baseline']['sum_usd'] is None and out['baseline']['known_sum_usd']['numerator'] == '120'


def test_zero_bill_day_stays_in_denominator_and_exact_sums_replay():
    out = calc([raw(accepted='0'), raw('2026-09-28','10'), raw('2026-07-31','100')])
    assert out['pct_above_baseline'] == -86.4 and out['score'] == 0
    assert out['recent']['auction_days'] == 2 and out['baseline']['auction_days'] == 3
    assert out['pct_exact'] == {'numerator':'-950','denominator':'11'}
    for key in ('recent','baseline'):
        win = out[key]; a,b = win['source_slice']; rows = out['ledger']['rows'][a:b]
        total = sum(amount(row['source_fields']['total_accepted']) for row in rows if row['instrument_kind']=='BILL')
        exact = win['sum_usd']; assert total == Fraction(int(exact['numerator']),int(exact['denominator']))


def test_strict_thresholds_use_exact_unrounded_percent():
    for pct,score in [('15',0),('15.000000000000001',30),('30',30),('30.000000000000001',60),('50',60),('50.000000000000001',90)]:
        # Two auction days, baseline mean exactly 100; recent amount=100+pct.
        from decimal import Decimal
        p = Decimal(pct)
        out = calc([raw(accepted=str(100+p)), raw('2026-07-31',str(100-p))])
        assert out['score']==score,(pct,out)


def test_unknown_flags_do_not_turn_frn_or_tips_into_bills():
    for fields in ({'floating_rate':'Yes'},{'inflation_index_security':'Yes'}):
        out=calc([raw(**fields)])
        assert out['status']=='not_applicable' and out['baseline']['counts']['bill']==0
    out=calc([raw(floating_rate='Yes',inflation_index_security='Yes')])
    assert out['status']=='unavailable' and out['baseline']['counts']['unknown']==1


def test_future_malformed_missing_identity_and_duplicate_occurrences_fail_whole():
    for row in (raw('2026-09-30'),raw('bad-date'),raw(cusip=None),raw()):
        out=calc([raw(),row]);assert out['score'] is None
        assert out['ledger']['row_count']==2
    assert calc([raw(),raw('2026-07-31')])['status']=='complete' # reopening on another date


def test_strict_amounts_do_not_coerce_boolean_missing_or_nonfinite_to_zero():
    for value in (None,True,False,'','null','NaN','Infinity','-1','1_000',{},'1e-999999'):
        assert amount(value) is None,value
        assert calc([raw(accepted=value)])['score'] is None
    assert amount('0')==0 and amount('1.0000000000000001')!=1


def test_zero_baseline_no_recent_bills_and_no_bills_are_distinct():
    assert calc([raw(accepted='0')])['reasons']==['zero_baseline']
    assert calc([raw('2026-07-31')])['reasons']==['no_recent_bill_auction_days']
    out=calc([raw(security_type='Note')]);assert out['status']=='not_applicable'
    assert out['score']==0 and out['pct_above_baseline'] is None
    assert apply_overlay(0,out)['composite']==0


def test_full_boundaries_overlap_and_future_relative_to_history_are_explicit():
    rows=[raw('2025-09-28','900'),raw('2025-09-29','100'),raw('2026-09-01','100'),raw('2026-09-29','100')]
    ledger=IssuanceLedger(rows,DAY,[{'start':'2025-08-01','end':DAY}]);out=ledger.calculate(DAY)
    assert out['recent']['source_slice']==[2,4] and out['baseline']['source_slice']==[1,4]
    assert out['recent']['calendar_dates']==29 and out['baseline']['calendar_dates']==366
    earlier=ledger.calculate('2026-09-28');assert earlier['baseline']['source_slice'][1]==3


def test_population_coverage_requires_whole_range_without_single_day_holes():
    rows=[raw(),raw('2026-07-31')]
    for intervals in ([],[{'start':'2026-07-31','end':DAY}],
                      [{'start':'2025-09-29','end':'2026-01-01'},{'start':'2026-01-03','end':DAY}]):
        out=calc(rows,intervals);assert out['score'] is None
        assert 'unverified_population_coverage' in out['reasons']
    out=calc(rows,[{'start':'2025-09-29','end':'2026-01-01'},{'start':'2026-01-02','end':DAY}])
    assert out['score']==0


def test_overlay_keeps_base_separate_caps_and_never_fills_missing():
    issuance=calc([raw(),raw('2026-07-31','20')]);assert issuance['score']==90
    assert apply_overlay(20,issuance)['composite']==33.5
    assert apply_overlay(95,issuance)['composite']==100
    missing=calc([raw(accepted=None)])
    assert apply_overlay(20,missing)['base_composite']==20 and apply_overlay(20,missing)['composite'] is None
    for bad in (None,True,-1,101,'0',float('inf')):assert apply_overlay(bad,issuance)['composite'] is None


def test_actual_native_current_history_and_desk_share_comparison_and_score():
    store,scope=support['detector_fixture']();handler=scope['lambda_handler'];env=handler.__globals__
    today=datetime.now(timezone.utc).date();older=(today-timedelta(days=60)).isoformat()
    rows=[raw(today.isoformat(),'100000000000'),raw(older,'20000000000')]
    env['fetch_fiscal_auctions']=lambda *a,**kw:rows
    with patch('urllib.request.urlopen',side_effect=AssertionError('No provider request')),redirect_stdout(io.StringIO()):handler({},None)
    packet=store.docs['data/auction-crisis.json'];comparison=packet['issuance_anomaly']
    assert comparison['score']==90
    assert packet['composite_score']==packet['composite_history']['current']['with_issuance_composite']
    assert packet['composite_score']==packet['base_composite_score']+13.5
    desk=support['load']('auction-desk',support['Store']())
    history=desk['build_composite_history']({'a':rows[0],'b':rows[1]}, {today.isoformat():4.25,older:4.25},
        population_coverage=[{'start':(today-timedelta(days=400)).isoformat(),'end':today.isoformat()}])
    current=history['series'][-1]
    assert current['composite']==packet['composite_score']
    assert current['issuance']['pct_exact']==comparison['pct_exact']
    assert all(comparison[k] is False for k in ('calls_eligible','sizing_eligible','forecast_eligible','execution_eligible'))


def test_native_empty_refresh_clears_catalog_current_vector_and_combined_history():
    store,scope=support['detector_fixture']();handler=scope['lambda_handler'];env=handler.__globals__
    store.docs['data/auction-crisis.json']={'historical_analog':{'top_matches':[{'similarity':1}]},'composite_history':{'current':{'composite':100}}}
    env['fetch_fiscal_auctions']=lambda *a,**kw:[]
    with patch('urllib.request.urlopen',side_effect=AssertionError('No provider request')),redirect_stdout(io.StringIO()):handler({},None)
    packet=store.docs['data/auction-crisis.json']
    assert packet['historical_analog']['top_matches']==[]
    assert all(v is None for v in packet['historical_analog']['current_vector'])
    assert packet['composite_history']['series']==[] and packet['composite_calculation']['composite'] is None


def test_desk_acquisition_retains_missing_btc_amount_and_unknown_source_fields():
    today=datetime.now(timezone.utc).date();old=(today-timedelta(days=20)).isoformat()
    legacy=raw(old);store=support['Store']()
    scope=support['load']('auction-desk',store);fn=scope['load_full_bank'];env=fn.__globals__
    env['_s3_json']=lambda *args:{'rows':{old+'|'+legacy['cusip']:legacy},'legacy_metadata':'keep'}
    row=raw(today.isoformat(),None,bid_to_cover_ratio=None,extra_original='retained')
    def fetch(*args,**kwargs):
        kwargs['proof_out'].append({'complete':True,'rows':2,'pages':1});return [legacy,row]
    env['fetch_fd']=fetch
    out=fn();assert out['rows'][today.isoformat()+'|'+row['cusip']]==row
    assert out['legacy_metadata']=='keep' and out['population_coverage'][0]['start']==(today-timedelta(days=30)).isoformat()
    history=scope['build_composite_history'](out['rows'],{},population_coverage=out['population_coverage'])
    assert history['series'][-1]['issuance']['status']=='unavailable'


def test_incomplete_desk_pagination_never_claims_coverage_or_publishes_partial_bank():
    scope=support['load']('auction-desk',support['Store']());fn=scope['fetch_fd'];env=fn.__globals__
    for bodies in ([{'data':[raw()],'meta':{'total-count':2,'total-pages':2}}, {'data':[],'meta':{'total-count':2,'total-pages':2}}],
                   [{'data':[raw()],'meta':{'total-count':1,'total-pages':61}}],
                   [{'data':[raw()],'meta':{}}]):
        sequence=iter(bodies);env['_get_json']=lambda *a,**k:next(sequence);proof=[]
        try:fn('auctions_query',{},max_pages=60,proof_out=proof)
        except ValueError:pass
        else:raise AssertionError('Incomplete population accepted')
        assert proof==[]


def test_desk_completed_pagination_and_failed_bank_update_are_atomic():
    store=support['Store']();scope=support['load']('auction-desk',store);fn=scope['fetch_fd'];env=fn.__globals__
    today=datetime.now(timezone.utc).date().isoformat();row=raw(today)
    env['_get_json']=lambda *a,**k:{'data':[row],'meta':{'total-count':1,'total-pages':1}}
    proof=[];assert fn('auctions_query',{},proof_out=proof)==[row] and proof[0]['complete']
    old={'rows':{today+'|'+row['cusip']:row}}
    env['_s3_json']=lambda *args:deepcopy(old)
    env['_get_json']=lambda *a,**k:{'data':[row,row],'meta':{'total-count':2,'total-pages':1}}
    out=scope['load_full_bank']();assert out['rows']==old['rows'] and 'duplicate identity' in out['error']
    assert store.writes==[] and 'population_coverage' not in out


def test_replay_verifies_whole_input_ledger_before_calculation():
    from auction_issuance import replay
    out=calc([raw(),raw('2026-07-31','20')]);doc=out.pop('ledger')
    assert replay(doc)==out
    for mutate in (lambda x:x['rows'].pop(),lambda x:x['rows'][0]['source_fields'].update(total_accepted='999'),
                   lambda x:x['rows'][0].update(source_ordinal=9),lambda x:x.update(calls_eligible=True),
                   lambda x:x['coverage'][0].update(start='2026-01-01')):
        changed=deepcopy(doc);mutate(changed)
        try:replay(changed)
        except ValueError:pass
        else:raise AssertionError('Tampered ledger accepted')


def test_history_adds_supply_to_unrounded_base_before_the_single_final_round():
    store,scope=support['detector_fixture']();env=scope['lambda_handler'].__globals__
    today=datetime.now(timezone.utc).date();current=today.isoformat();older=(today-timedelta(days=60)).isoformat()
    ledger=IssuanceLedger([raw(current),raw(older,'20')],current,[{'start':(today-timedelta(days=400)).isoformat(),'end':current}])
    rows=[{'auction_date':current,'accepted_billions':1,'composite_score':0},
          {'auction_date':current,'accepted_billions':1,'composite_score':.3}]
    history=env['build_composite_history'](rows,days=0,issuance_ledger=ledger)
    point=history['current'];expected=apply_overlay(.15,ledger.calculate(current))
    assert point['composite']==round(.15,1)
    assert point['composite_calculation']['base_composite']==.15
    assert point['with_issuance_composite']==expected['composite']


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction issuance regressions passed:',len(tests))
