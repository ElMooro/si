"""Dated Treasury benchmarks and actual native adapters; synthetic inputs only."""
from pathlib import Path
from datetime import date,datetime,timezone
from copy import deepcopy
import importlib.util,io,json,runpy,sys,types
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import auction_benchmarks as model
TODAY=date(2026,9,29)


def row(**changes):
    return {'instrument_contract':'treasury-instrument.v1','instrument_classification_status':'verified',
            'instrument_kind':'NOMINAL_COUPON','quote_basis':'nominal_yield_pct','security_type':'Note',
            'security_term':'10-Year','auction_date':'2026-09-17','issue_date':'2026-09-18','high_rate':4.2,**changes}


def history():
    return {'DGS10':{'2026-09-21':4.0,'2026-09-22':4.01,'2026-09-23':4.02,
                     '2026-09-24':4.03,'2026-09-25':4.04,'2026-09-28':4.05,'2026-09-29':4.06}}


def engine():
    source=ROOT/'aws/lambdas/justhodl-auction-crisis-detector/source/auction_crisis_v2.py'
    with patch.dict(sys.modules,{'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'TEST_ONLY')}):
        spec=importlib.util.spec_from_file_location('auction_benchmark_v2_test',source)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_complete_compound_term_and_invalid_tenors():
    assert model.term_days('9-Year 11-Month')==9*365+11*30
    assert model.term_days('29-Year 11-Month')==29*365+11*30
    assert model.term_days('3-Year')==1095
    for bad in ('-1-Year','0-Year','2-Year 3-Year','3-Year garbage','10','51-Year',True,None):
        assert model.term_days(bad) is None


def test_calendar_flags_prevent_nominal_substitution_and_zero_is_not_missing():
    base={'securityType':'Note','securityTerm':'3-Year','tips':'No','floatingRate':'No'}
    assert model.calendar_identity(base)['tenor_bucket']=='coupons_lt_3y'
    assert model.calendar_identity({**base,'tips':'Yes'})['tenor_bucket']=='tips'
    assert model.calendar_identity({**base,'floatingRate':'Yes'})['tenor_bucket']=='frn'
    assert model.calendar_identity({**base,'tips':'Yes','floatingRate':'Yes'})['tenor_bucket']=='unknown'
    assert model.calendar_identity({'securityType':'Note','securityTerm':'10-Year'})['instrument_kind']=='UNKNOWN'
    assert model.number(0)==model.number('0')==0
    for value in (True,False,'nan','inf','1e999','1_000',{},[]):assert model.number(value) is None


def test_preauction_observation_intervals_have_exact_dates_and_correct_sign():
    result=model.preauction([row()],history(),TODAY)[0]
    assert result['concession_1d_bp']==1 and result['concession_5d_bp']==5
    trace=result['concession_measurement'];assert trace['status']=='complete'
    assert trace['comparisons']['1']['start_date']=='2026-09-28'
    assert trace['comparisons']['5']['start_date']=='2026-09-22'
    assert trace['comparisons']['5']['calendar_days']==7
    assert trace['comparisons']['5']['end_date']=='2026-09-29'
    assert not trace['calls_eligible'] and not trace['source_capture_verified']
    assert 'investor motive' in result['concession_interpretation']


def test_preauction_weekend_preserves_observation_dates_not_fabricated_days():
    result=model.preauction([row()],history(),date(2026,9,28))[0]
    assert result['concession_measurement']['comparisons']['1']['start_date']=='2026-09-25'
    assert result['concession_measurement']['comparisons']['1']['calendar_days']==3
    assert result['concession_1d_bp']==1


def test_preauction_stale_missing_invalid_or_gapped_data_stays_unavailable():
    for series,status in [({'2026-08-01':4},'stale_latest_observation'),
                          ({'not-a-date':4},'invalid_series_observation'),
                          ({'2026-09-29':float('nan')},'invalid_series_observation'),
                          ({'2026-09-29':False},'invalid_series_observation'),({},'no_available_observations')]:
        result=model.preauction([row()],{'DGS10':series},TODAY)[0]
        assert result['concession_1d_bp'] is None and result['concession_5d_bp'] is None
        assert result['concession_measurement']['status']==status
    result=model.preauction([row()],{'DGS10':{'2026-09-01':4,'2026-09-29':4.1}},TODAY)[0]
    assert result['concession_1d_bp'] is None and result['concession_5d_bp'] is None


def test_preauction_real_zero_survives_and_future_print_is_never_used():
    values={d:0 for d in history()['DGS10']};values['2026-09-30']=99
    result=model.preauction([row()],{'DGS10':values},TODAY)[0]
    assert result['concession_today_yield']==result['concession_1d_bp']==result['concession_5d_bp']==0
    assert result['concession_today_date']=='2026-09-29'


def test_incomparable_quotes_cannot_get_nominal_pre_or_post_comparisons():
    for kind in ('TIPS','FRN','UNKNOWN'):
        item=row(instrument_kind=kind,concession_5d_bp=999,postissue_1d_bp=999)
        pre=model.preauction([item],history(),TODAY)[0];post=model.postissue([item],history(),TODAY)[0]
        assert pre['concession_5d_bp'] is None and post['postissue_1d_bp'] is None
        assert pre['concession_measurement']['status']==post['postissue_measurement']['status']=='incomparable_instrument'
    bill=row(instrument_kind='BILL',quote_basis='discount_rate_pct')
    assert model.postissue([bill],history(),TODAY)[0]['postissue_1d_bp'] is None


def test_postissue_exposes_calendar_targets_and_bounded_actual_endpoints():
    result=model.postissue([row()],history(),TODAY)[0]
    trace=result['postissue_measurement'];one=trace['comparisons']['1'];five=trace['comparisons']['5']
    assert one['target_date']=='2026-09-19' and one['observation_date']=='2026-09-21'
    assert five['target_date']==five['observation_date']=='2026-09-23'
    assert result['postissue_1d_bp']==20 and result['postissue_5d_bp']==18
    assert result['postissue_30d_bp'] is None and trace['comparisons']['30']['status']=='pending'
    assert result['postissue_classification']=='DESCRIPTIVE_ONLY'
    assert not trace['sizing_eligible']


def test_postissue_cannot_use_remote_or_future_endpoint_as_one_day_outcome():
    for day_label in ('2026-09-29','2026-10-25'):
        result=model.postissue([row()],{'DGS10':{day_label:4}},TODAY)[0]
        assert result['postissue_1d_bp'] is None
    result=model.postissue([row()],{'DGS10':{'2026-09-21':4.2}},TODAY)[0]
    assert result['postissue_1d_bp']==0


def test_postissue_invalid_issue_identity_yield_and_stale_window_clear_prior_fields():
    for change in ({'issue_date':None},{'issue_date':'2026-09-01'},{'auction_date':'2026-02-31'},
                   {'high_rate':True},{'high_rate':float('nan')},{'issue_date':'2026-09-30'},
                   {'issue_date':'2026-06-01','auction_date':'2026-05-25'}):
        result=model.postissue([row(postissue_1d_bp=999,**change)],history(),TODAY)[0]
        assert all(result['postissue_'+str(n)+'d_bp'] is None for n in (1,5,30))


def test_pure_comparison_keeps_all_rows_and_does_not_mutate_inputs():
    rows=[row(cusip=str(n)) for n in range(151)];before=deepcopy(rows);data=history();before_data=deepcopy(data)
    assert len(model.preauction(rows,data,TODAY))==len(model.postissue(rows,data,TODAY))==151
    assert rows==before and data==before_data


def test_actual_upcoming_adapter_preserves_all_provider_flags_and_zero_offering():
    m=engine();today=datetime.now(timezone.utc).date().isoformat()
    rows=[{'auctionDate':today,'securityType':'Note','securityTerm':'3-Year','tips':'No',
           'floatingRate':'No','offeringAmount':0,'cusip':'TEST00001'},
          {'auctionDate':today,'securityType':'Note','securityTerm':'2-Year','tips':'No',
           'floatingRate':'Yes','offeringAmount':None,'cusip':'TEST00002'}]
    with patch.object(m.urllib.request,'urlopen',return_value=io.BytesIO(json.dumps(rows).encode())):
        out=m.fetch_upcoming_auctions()
    assert len(out)==2 and out[0]['offering_amount_billions']==0
    assert out[0]['tenor_bucket']=='coupons_lt_3y' and out[1]['instrument_kind']=='FRN'
    assert out[1]['offering_amount_billions'] is None and out[1]['provider_record']==rows[1]


def test_actual_lookback_cache_does_not_truncate_postissue_history_or_persist_between_invocations():
    m=engine();calls=[]
    m._fred_get_history=lambda series,days:(calls.append((series,days)) or [('2026-09-29',float(days))])
    a=m.fetch_cmt_yield_history(30);b=m.fetch_cmt_yield_history(90)
    assert len(calls)==22 and a['DGS10']!=b['DGS10']
    assert m.fetch_cmt_yield_history(30) is a and len(calls)==22
    m.reset_yield_cache();assert not m._yield_cache
    m.fetch_cmt_yield_history(30);assert len(calls)==33


def test_actual_native_handler_resets_warm_cache_before_any_provider_request():
    support=runpy.run_path(str(ROOT/'tests/test_auction_originals.py'))
    client,handler=support['native_setup']();env=handler.__globals__
    cache=env['reset_yield_cache'].__globals__['_yield_cache'];cache['old invocation']={'old':1}
    def provider(*a,**k):
        assert cache=={}
        return support['response']([])
    with patch('urllib.request.urlopen',side_effect=provider):assert handler({},None)['statusCode']==200
    assert cache=={}


def test_both_native_comparison_adapters_share_the_full_window_without_extra_requests():
    m=engine();calls=[]
    m._fred_get_history=lambda series,days:(calls.append((series,days)) or [('2026-09-29',4.0)])
    m.compute_preauction_concession([row()]);m.compute_postissue_performance([row()])
    assert len(calls)==11 and all(days==90 for _,days in calls)


if __name__=='__main__':
    tests=[value for name,value in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction benchmark regressions passed:',len(tests))
