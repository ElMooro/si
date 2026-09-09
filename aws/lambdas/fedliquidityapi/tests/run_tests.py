"""Real Fed summary handler: calendar comparisons, missingness and public errors."""
import importlib.util
import json
import sys
import types
from pathlib import Path

sys.modules['_fred_shim']=types.ModuleType('_fred_shim')
secret=types.ModuleType('managed_secret');secret.managed_secret=lambda *a:'';sys.modules['managed_secret']=secret
spec=importlib.util.spec_from_file_location('fed_under_test',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
real_get_series_metadata=mod.get_series_metadata
mod.get_series_metadata=lambda *a:{'frequency':'Weekly, Ending Wednesday','units':'Millions of U.S. Dollars','title':'Fixture'}

def test_calendar_changes_use_observation_dates_and_preserve_zero():
    mod.fetch_fred_data=lambda *a,**kw:[{'date':'2026-09-09','value':0},{'date':'2026-09-08','value':120},
        {'date':'2026-09-02','value':100},{'date':'2026-08-07','value':50}]
    _,row=mod.summarize_series('WALCL')
    assert row['latest_value']==0 and row['week_change']==-100 and row['month_change']==-100
    assert row['week_comparison_date']=='2026-09-02' and row['month_comparison_date']=='2026-08-07'

def test_month_end_clamps_to_previous_month_end():
    mod.fetch_fred_data=lambda *a,**kw:[{'date':'2026-03-31','value':110},{'date':'2026-03-02','value':105},{'date':'2026-02-28','value':100}]
    _,row=mod.summarize_series('WALCL')
    assert row['month_comparison_date']=='2026-02-28' and row['month_change']==10

def test_actual_summary_reports_partial_series_and_timezone():
    mod.fetch_fred_data=lambda series_id,**kw:[{'date':'2026-09-09','value':100}] if series_id=='WALCL' else []
    result=json.loads(mod.lambda_handler({'queryStringParameters':{'series':'summary'}},None)['body'])
    assert result['status']=='PARTIAL' and len(result['missing_series'])==11
    assert result['summary']['WALCL']['week_change'] is None and result['last_updated'].endswith('+00:00')

def test_summary_error_never_publishes_provider_response():
    def fail(*a,**kw): raise RuntimeError('PRIVATE_CANARY_API_KEY')
    mod.fetch_fred_data=fail
    result=mod.lambda_handler({'queryStringParameters':{'series':'summary'}},None)
    assert result['statusCode']==500 and json.loads(result['body'])=={'error':'DATA_UNAVAILABLE'}

def test_monthly_series_never_invents_a_weekly_return_and_stale_dates_are_flagged():
    mod.get_series_metadata=lambda *a:{'frequency':'Monthly','units':'Billions of Dollars','title':'Money'}
    mod.fetch_fred_data=lambda *a,**kw:[{'date':'2020-08-01','value':110},{'date':'2020-07-01','value':100}]
    _,row=mod.summarize_series('M2SL')
    assert row['week_change'] is None and row['month_change']==10 and row['data_quality']=='STALE'
    mod.get_series_metadata=lambda *a:{'frequency':'Weekly, Ending Wednesday','units':'Millions of U.S. Dollars','title':'Fixture'}

def test_weekly_month_comparison_uses_last_available_week_without_future_observation():
    mod.fetch_fred_data=lambda *a,**kw:[{'date':'2026-09-02','value':110},
        {'date':'2026-08-05','value':105},{'date':'2026-07-29','value':100}]
    _,row=mod.summarize_series('WALCL')
    assert row['month_comparison_date']=='2026-07-29' and row['month_change']==10
    mod.fetch_fred_data=lambda *a,**kw:[{'date':'2026-09-02','value':110},{'date':'2026-07-22','value':100}]
    assert mod.summarize_series('WALCL')[1]['month_change'] is None

def test_summary_uses_active_stress_index_and_retains_historical_catalog():
    mod.fetch_fred_data=lambda *a,**kw:[]
    result=json.loads(mod.lambda_handler({'queryStringParameters':{'series':'summary'}},None)['body'])
    assert 'STLFSI4' in result['requested_series'] and 'STLFSI3' not in result['requested_series']
    assert 'STLFSI3' in mod.FED_LIQUIDITY_SERIES

def test_metadata_cache_has_expiry_and_does_not_cache_failed_provider_responses():
    from unittest.mock import patch
    class Response:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self):return json.dumps({'seriess':[{'id':'VIXCLS','title':'Fixture','units':'Index','frequency':'Daily'}]}).encode()
    mod._metadata_cache={}
    with patch.object(mod.urllib.request,'urlopen',return_value=Response()) as request:
        first=real_get_series_metadata('VIXCLS');first['frequency']='tampered'
        assert real_get_series_metadata('VIXCLS')['frequency']=='Daily' and request.call_count==1
        mod._metadata_cache['VIXCLS']=(mod.time.monotonic()-7*3600,mod._metadata_cache['VIXCLS'][1])
        real_get_series_metadata('VIXCLS');assert request.call_count==2
    mod._metadata_cache={}
    with patch.object(mod.urllib.request,'urlopen',side_effect=RuntimeError('PRIVATE_CANARY')):
        assert real_get_series_metadata('VIXCLS') is None and mod._metadata_cache=={}


tests=[fn for name,fn in sorted(globals().items()) if name.startswith('test_')]
for test in tests:test()
print('Fed liquidity handler tests passed:',len(tests))
