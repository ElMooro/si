"""Actual FMP handler tests with synthetic provider responses only."""
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import urllib.error
from unittest.mock import patch

source=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
sys.modules['managed_secret']=types.SimpleNamespace(managed_secret=lambda *args:'SYNTHETIC_SECRET')
spec=importlib.util.spec_from_file_location('fmp_under_test',source);fmp=importlib.util.module_from_spec(spec);spec.loader.exec_module(fmp)


def test_http_provider_failure_never_returns_url_secret_or_body():
    error=urllib.error.HTTPError('https://example.invalid/?apikey=SYNTHETIC_SECRET',403,'SYNTHETIC_SECRET',{},io.BytesIO(b'SYNTHETIC_SECRET'))
    with patch.object(fmp.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=lambda *args,**kw:(_ for _ in ()).throw(error))):
        rows,diagnostic=fmp.fetch_rows('batch-quote',{},'SYNTHETIC_SECRET')
    assert rows==[] and diagnostic['http_status']==403 and error.closed
    assert 'SYNTHETIC_SECRET' not in json.dumps(diagnostic)


def test_nonfinite_or_error_json_is_not_successful_data():
    class Response(io.BytesIO):
        def __enter__(self):return self
        def __exit__(self,*args):self.close()
    for raw in (b'[{"price":NaN}]',b'{"Error Message":"SYNTHETIC_SECRET"}',b'[null]'):
        with patch.object(fmp.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=lambda *args,**kw:Response(raw))):
            rows,diagnostic=fmp.fetch_rows('batch-quote',{},'SYNTHETIC_SECRET')
        assert rows==[] and diagnostic['status']=='UNAVAILABLE'
        assert 'SYNTHETIC_SECRET' not in json.dumps(diagnostic)


def test_snapshot_preserves_every_mover_and_nested_provider_field():
    def fetch(endpoint,params,key):
        rows=[{'symbol':symbol,'price':0,'extra':{'zero':0,'flag':False}} for symbol in params.get('symbols',','.join('S'+str(i) for i in range(30))).split(',')]
        return rows,{'endpoint':endpoint,'status':'AVAILABLE','row_count':len(rows)}
    with patch.object(fmp,'fetch_rows',side_effect=fetch):doc=fmp.snapshot()
    assert len(doc['movers']['gainers'])==30 and len(doc['watchlist_quotes'])==24
    assert doc['watchlist_quotes']['AAPL']['extra']=={'zero':0,'flag':False}
    assert doc['execution_eligible'] is False and doc['generated_at'].endswith('+00:00')
    assert doc['status']=='PARTIAL' and doc['quotes_ok']==0 and len(doc['invalid_quote_symbols'])==24


def test_empty_and_partial_provider_coverage_cannot_be_ready():
    def empty(endpoint,params,key):return [],{'endpoint':endpoint,'status':'EMPTY','row_count':0}
    with patch.object(fmp,'fetch_rows',side_effect=empty):doc=fmp.snapshot()
    assert doc['status']=='UNAVAILABLE' and doc['quotes_err']==24
    def partial(endpoint,params,key):
        if params.get('symbols','').startswith('AAPL'):return [{'symbol':'AAPL','price':123,'timestamp':fmp.datetime.now(fmp.timezone.utc).timestamp()}],{'endpoint':endpoint,'status':'AVAILABLE','row_count':1}
        return empty(endpoint,params,key)
    with patch.object(fmp,'fetch_rows',side_effect=partial):doc=fmp.snapshot()
    assert doc['status']=='PARTIAL' and doc['quotes_err']==23


def test_handler_cache_keeps_original_collection_time_and_expires():
    fmp.CACHE=None;fmp.CACHE_UNTIL=0
    with patch.object(fmp,'snapshot',side_effect=[{'generated_at':'first'},{'generated_at':'second'}]) as make, patch.object(fmp.time,'monotonic',side_effect=[100,120,161,161]):
        a=fmp.lambda_handler({},None);b=fmp.lambda_handler({},None);c=fmp.lambda_handler({},None)
    assert json.loads(a['body'])==json.loads(b['body'])=={'generated_at':'first'}
    assert json.loads(c['body'])=={'generated_at':'second'} and make.call_count==2


def test_health_and_preflight_do_not_fetch_or_claim_provider_health():
    with patch.object(fmp,'snapshot',side_effect=AssertionError('provider call')):
        result=fmp.lambda_handler({'rawPath':'/health'},None)
        assert json.loads(result['body'])['provider_data_verified'] is False
        assert fmp.lambda_handler({'httpMethod':'OPTIONS'},None)['statusCode']==204
        assert fmp.lambda_handler({'httpMethod':'POST'},None)['statusCode']==405
        assert fmp.lambda_handler({'rawPath':'/debug'},None)['statusCode']==404


def test_function_url_is_the_only_cors_header_owner():
    for event in ({'rawPath':'/health'},{'httpMethod':'OPTIONS'},{'httpMethod':'POST'},{'rawPath':'/debug'}):
        result=fmp.lambda_handler(event,None)
        assert not any(name.lower().startswith('access-control-') for name in result['headers'])
        assert result['headers']['Cache-Control']=='no-store'


def test_internal_exception_never_leaks_trace_or_secret():
    fmp.CACHE=None
    with patch.object(fmp,'snapshot',side_effect=RuntimeError('SYNTHETIC_SECRET')):result=fmp.lambda_handler({},None)
    assert result['statusCode']==503 and 'SYNTHETIC_SECRET' not in json.dumps(result) and 'trace' not in result['body']


if __name__=='__main__':
    tests=[v for k,v in sorted(globals().items()) if k.startswith('test_') and callable(v)]
    for test in tests:test()
    print(len(tests),'FMP handler tests PASS')
