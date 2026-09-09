import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch, MagicMock
import urllib.error

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/shared'))
import public_provider_json as provider

def handler(name,filename='lambda_function.py'):
    secret=types.ModuleType('managed_secret');secret.managed_secret=lambda *a:'synthetic-provider-token'
    auth=types.ModuleType('api_auth');auth.authorize=lambda *a,**k:({},None)
    spec=importlib.util.spec_from_file_location(name.replace('-','_'),ROOT/'aws/lambdas'/name/'source'/filename)
    module=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'managed_secret':secret,'api_auth':auth}):spec.loader.exec_module(module)
    return module

class ProviderTests(unittest.TestCase):
    def test_untrusted_destinations_never_open(self):
        with patch.object(provider.urllib.request,'build_opener') as opener:
            for url in ['http://data.nasdaq.com/','https://data.nasdaq.com.evil.invalid/','https://token@data.nasdaq.com/','https://data.nasdaq.com:444/']:
                with self.assertRaises(provider.ProviderError):provider.get_json(url)
            opener.assert_not_called()
    def test_http_error_body_and_url_are_not_exposed(self):
        error=urllib.error.HTTPError('https://data.nasdaq.com/?apikey=synthetic-private',401,'synthetic-private',{},io.BytesIO(b'synthetic-private'))
        with patch.object(provider.urllib.request,'build_opener') as build:
            build.return_value.open.side_effect=error
            with self.assertRaises(provider.ProviderError) as caught:provider.get_json('https://data.nasdaq.com/')
            self.assertEqual(provider.error_metadata(caught.exception),{'error':'PROVIDER_HTTP_ERROR','http_status':401})
            self.assertNotIn('synthetic-private',str(caught.exception));self.assertTrue(error.closed)
    def test_size_nan_schema_and_timeout(self):
        for raw in [b'[]',b'{"x":NaN}',b'{"x":1e999}',b'x'*21]:
            with patch.object(provider.urllib.request,'build_opener') as build:
                response=build.return_value.open.return_value.__enter__.return_value;response.read.return_value=raw
                with self.assertRaises(provider.ProviderError):provider.get_json('https://data.nasdaq.com/',maximum=20)
                response.read.assert_called_once_with(21)
                self.assertEqual(build.return_value.open.call_args.kwargs['timeout'],10)
    def test_redirects_are_disabled(self):self.assertIsNone(provider.NoRedirect().redirect_request(None,None,302,'',{},'https://elsewhere.invalid'))

class NasdaqTests(unittest.TestCase):
    def setUp(self):self.module=handler('nasdaq-datalink-agent')
    def test_all_history_and_missing_latest_are_retained(self):
        rows=[[f'2026-08-{day:02}',None if day==24 else day] for day in range(24,0,-1)]
        self.module.get_json=lambda u:{'dataset_data':{'column_names':['Date','Value'],'data':rows}}
        result=self.module.fetch('FRED/GDP')
        self.assertEqual(len(result['history']),24);self.assertIsNone(result['value']);self.assertIsNone(result['change_pct'])
        self.assertEqual(result['error'],'LATEST_VALUE_UNAVAILABLE');self.assertEqual(result['previous']['Value'],23)
    def test_zero_preserved_and_change_not_mislabeled_daily(self):
        self.module.get_json=lambda u:{'dataset_data':{'column_names':['Date','Value'],'data':[['2026-06-01',0],['2026-03-01',2]]}}
        result=self.module.fetch('FRED/GDP');self.assertEqual(result['value'],0);self.assertEqual(result['change_pct'],-100)
        self.assertIn('provider observation',result['change_scope'])
    def test_health_and_debug_never_call_provider(self):
        self.module.get_json=MagicMock(side_effect=AssertionError('no network'))
        self.assertEqual(json.loads(self.module.lambda_handler({'rawPath':'/health'},None)['body'])['status'],'HANDLER_READY')
        self.assertEqual(self.module.lambda_handler({'rawPath':'/debug'},None)['statusCode'],404)
        self.module.get_json.assert_not_called()
    def test_all_unavailable_does_not_report_ready_or_tracebacks(self):
        self.module.fetch=lambda code:{'error':'PROVIDER_HTTP_ERROR','http_status':401}
        r=self.module.lambda_handler({},None);d=json.loads(r['body'])
        self.assertEqual(d['status'],'UNAVAILABLE');self.assertEqual(d['metrics_err'],24)
        self.assertEqual(sum(len(v) for v in d['categories'].values()),24)
        self.assertNotIn('Access-Control-Allow-Origin',r['headers']);self.assertNotIn('traceback',r['body'])

    def test_direct_fred_fallback_retains_provenance_and_all_observations(self):
        def get(url):
            if 'data.nasdaq.com' in url:raise provider.ProviderError('PROVIDER_HTTP_ERROR',403)
            return {'realtime_start':'2026-09-09','realtime_end':'2026-09-09','units':'lin','observations':[
                {'date':'2026-08-01','value':'0','realtime_start':'2026-09-09'},
                {'date':'2026-07-01','value':'2','realtime_start':'2026-09-09'}]}
        self.module.get_json=get
        d=self.module.fetch('FRED/GDP')
        self.assertEqual(d['provider'],'FRED_DIRECT');self.assertTrue(d['fallback_used'])
        self.assertEqual(d['value'],0);self.assertEqual(d['change_pct'],-100)
        self.assertEqual(d['primary_provider_status']['http_status'],403)
        self.assertEqual(len(d['provider_observations']),2);self.assertEqual(len(d['history']),2)
        self.assertIn('not point-in-time',d['vintage_scope'])
    def test_fred_missing_latest_and_index_access_remain_explicit(self):
        self.module.fetch_nasdaq=lambda *args:{'error':'PROVIDER_HTTP_ERROR','http_status':403}
        self.module.get_json=lambda url:{'observations':[{'date':'2026-08-01','value':'.'},{'date':'2026-07-01','value':'2'}]}
        d=self.module.fetch('FRED/GDP');self.assertIsNone(d['value']);self.assertIsNone(d['change_pct']);self.assertEqual(len(d['history']),2)
        self.module.fetch_fred=MagicMock(side_effect=AssertionError('index is not a FRED series'))
        d=self.module.fetch('NASDAQOMX/NDX-NASDAQ');self.assertFalse(d['fallback_used']);self.assertEqual(d['http_status'],403)
        self.module.fetch_fred.assert_not_called()

class TechnicalTests(unittest.TestCase):
    def setUp(self):self.module=handler('alphavantage-technical-analysis')
    def test_invalid_input_does_not_spend_provider_quota(self):
        self.module.get_json=MagicMock()
        for data in [[],{'body':'{'},{'symbol':'SPY&apikey=bad'},{'function':[]},{'time_period':True,'function':'RSI'}]:
            self.assertEqual(self.module.lambda_handler(data,None)['statusCode'],400)
        self.module.get_json.assert_not_called()
    def test_rate_limit_details_never_become_public_errors(self):
        self.module.get_json=lambda u:{'Information':'synthetic-private provider key rejected'}
        result=self.module.lambda_handler({},None)
        self.assertEqual(result['statusCode'],503);self.assertNotIn('synthetic-private',result['body'])
    def test_all_indicator_rows_and_metadata_returned(self):
        data={'Meta Data':{'scope':'full'},'Technical Analysis: RSI':{str(i):{'RSI':i} for i in range(250)}}
        self.module.get_json=lambda u:data
        result=self.module.lambda_handler({'function':'RSI'},None)
        self.assertEqual(json.loads(result['body']),data);self.assertNotIn('Access-Control-Allow-Origin',result['headers'])

class MarketTests(unittest.TestCase):
    def setUp(self):self.module=handler('alphavantage-market-agent','lambda_alphavantage_agent.py')
    def test_missing_sector_data_cannot_be_neutral(self):
        d=self.module.analyze_market_breadth({})
        self.assertEqual(d['market_breadth'],'UNKNOWN');self.assertIsNone(d['breadth_ratio'])
    def test_mixed_observation_dates_block_breadth(self):
        rows={s:{'status':'OBSERVED','change':1,'latest_trading_day':'2026-09-01' if i else '2026-08-31'} for i,s in enumerate(self.module.ENDPOINTS['sector_etfs'])}
        self.assertEqual(self.module.analyze_market_breadth({'sector_etfs':rows})['market_breadth'],'UNKNOWN')
    def test_complete_unchanged_sectors_are_explicit(self):
        rows={s:{'status':'OBSERVED','change':0,'latest_trading_day':'2026-09-01'} for s in self.module.ENDPOINTS['sector_etfs']}
        d=self.module.analyze_market_breadth({'sector_etfs':rows});self.assertEqual(d['market_breadth'],'UNCHANGED');self.assertIsNone(d['breadth_ratio'])
    def test_all_expected_rows_and_no_fabricated_sentiment(self):
        self.module.fetch_quote=lambda s,k:{'symbol':s,'status':'UNAVAILABLE','error':'PROVIDER_HTTP_ERROR'}
        response=self.module.lambda_handler({},None)
        self.assertFalse(any(k.lower().startswith('access-control-') for k in response['headers']))
        d=json.loads(response['body'])
        self.assertEqual(d['coverage'],{'expected_quotes':17,'observed_quotes':0});self.assertEqual(d['status'],'UNAVAILABLE')
        self.assertIsNone(d['market_data']['sentiment']['vix']);self.assertIsNone(d['market_data']['sentiment']['fear_greed'])
        self.assertEqual(d['recommendations'],[]);self.assertFalse(d['execution_eligible'])
        self.module.fetch_quote=MagicMock(side_effect=AssertionError('cache should avoid provider quota'))
        self.assertTrue(json.loads(self.module.lambda_handler({},None)['body'])['served_from_warm_cache'])
        self.module.fetch_quote.assert_not_called()
    def test_bad_price_does_not_become_zero(self):
        self.module.get_json=lambda u:{'Global Quote':{'01. symbol':'SPY','05. price':'NaN','07. latest trading day':'2026-09-01'}}
        self.assertEqual(self.module.fetch_quote('SPY','synthetic')['status'],'UNAVAILABLE')

if __name__=='__main__':unittest.main()

def test_provider_adapters_offline_regressions():
    suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(cls) for cls in (ProviderTests,NasdaqTests,TechnicalTests,MarketTests))
    result=unittest.TestResult();suite.run(result)
    assert result.wasSuccessful(),str(result.errors+result.failures)
