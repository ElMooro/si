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

tests=[fn for name,fn in sorted(globals().items()) if name.startswith('test_')]
for test in tests:test()
print('Fed liquidity handler tests passed:',len(tests))
