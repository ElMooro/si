from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
import ast
import datetime
import json
import sys
import unittest
from io import BytesIO
import hashlib,urllib.error,urllib.parse
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[4];SOURCE=ROOT/'aws/lambdas/justhodl-buyback-engine/source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
from buyback_measurements import CONTRACT,dossier,number,decode
import buyback_store as store
import buyback_sources as capture_sources


def sources():
    profile=[{'symbol':'TEST','cik':'0000000001','sector':'Industrials','industry':'Machines','companyName':'Test company'}]
    cash=[]
    for end,start,period in [('2026-06-30','2026-04-01','Q2'),('2026-03-31','2026-01-01','Q1'),('2025-12-31','2025-10-01','Q4'),('2025-09-30','2025-07-01','Q3')]:
        cash.append({'symbol':'TEST','cik':'0000000001','date':end,'startDate':start,'period':period,'reportedCurrency':'USD',
                     'commonStockRepurchased':-100,'commonStockIssuance':0,'netCommonStockIssuance':0,'netStockIssuance':-500,
                     'commonDividendsPaid':0,'netDividendsPaid':-900,'stockBasedCompensation':5,'netDebtIssuance':100,
                     'operatingCashFlow':0,'netCashProvidedByOperatingActivities':900,'capitalExpenditure':-20})
    metrics=[{'symbol':'TEST','date':'2026-06-30','reportedCurrency':'USD','marketCap':10000}]
    shares=[{'symbol':'TEST','date':'2026-06-30','numberOfShares':80},{'symbol':'TEST','date':'2025-06-30','numberOfShares':100}]
    return [profile,cash,metrics,shares]


def calculate(data=None):return dossier('TEST',*(data if data is not None else sources()),'2026-09-27')


def native(**extra):
    tree=ast.parse((SOURCE/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and not n.name.startswith('_legacy_')]
    ns={'CONTRACT':CONTRACT,'dossier':dossier,'number':number,'decode':decode,'datetime':datetime,'json':json,'EXCLUDE_TICKERS':set(),
        'S3_BUCKET':'fixture-only','OUT_KEY':'data/buyback-engine.json','time':SimpleNamespace(sleep=lambda seconds:None),
        'FMP_KEY':'fixture-secret','Capture':capture_sources.Capture,'load_head':store.load_head,'publish':store.publish}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated actual buyback>','exec'),ns);ns.update(extra);return ns



class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self,writes=None):
        self.raw={store.HEAD:b'{"version":"1.1.0", "tickers":{"OLD":{"symbol":"OLD","unknown":123}}}'}
        self.writes=writes if writes is not None else {};self.puts=[];self.reads=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.raw:raise StorageError('NoSuchKey')
        raw=self.raw[key];return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.puts.append(kw)
        if kw.get('IfNoneMatch')=='*' and key in self.raw:raise StorageError('PreconditionFailed')
        if 'IfMatch' in kw and (key not in self.raw or kw['IfMatch']!=hashlib.sha256(self.raw[key]).hexdigest()):raise StorageError('PreconditionFailed')
        self.raw[key]=kw['Body'];self.writes[key]=kw['Body'] if key.startswith(capture_sources.PREFIX) else json.loads(kw['Body'])


def captured_handler(*,one_row=False,retry=False,unavailable=False,excluded=False):
    ns,writes,scanner=MeasurementTests().handler();db=ns['s3'];calls=[];bodies=[];waits=[];data=sources()
    if one_row:data[1]=data[1][:1]
    def opener(req,timeout):
        url=urllib.parse.urlsplit(req.full_url);name=url.path.split('/')[-1];calls.append(req.full_url)
        assert timeout==18
        if retry and name=='profile' and sum('/profile?' in u for u in calls)==1:
            raise urllib.error.URLError('synthetic unavailable credentialed URL is not published')
        if name=='earnings-calendar':
            query=dict(urllib.parse.parse_qsl(url.query));value=[{'symbol':'TEST','date':query['from'],'unknown':123}]
        elif excluded and name=='profile' and dict(urllib.parse.parse_qsl(url.query)).get('symbol')=='FUND':
            value=[{'symbol':'FUND','isFund':True,'whole_profile':'retained'}]
        elif unavailable:value=None
        else:value=data[{'profile':0,'cash-flow-statement':1,'key-metrics':2,'enterprise-values':3}[name]]
        raw=(json.dumps(value,ensure_ascii=False,indent=1)+'\n').encode();bodies.append(raw)
        class Response(BytesIO):pass
        result=Response(raw);result.code=200;result.headers={'Content-Length':str(len(raw))};return result
    # Recompile actual functions together so injected capture and helpers share a namespace.
    ns=native(_read=ns['_read'],s3=db,time=SimpleNamespace(sleep=waits.append))
    if excluded:
        original_read=ns['_read']
        ns['_read']=lambda key,default=None:({'tickers':{'FUND':{}}} if key=='data/attention-confluence.json' else original_read(key,default))
    ns['Capture']=lambda db,bucket,ua,key:capture_sources.Capture(db,bucket,ua,key,opener=opener)
    return ns,db,calls,bodies,waits


class MeasurementTests(unittest.TestCase):
    def test_duplicate_underflow_and_nonfinite_json_fail_without_a_fake_value(self):
        for raw in (b'{"val":0,"val":1}',b'{"val":1e-999}',b'{"val":1e999}',b'{"val":NaN}'):
            with self.assertRaises(ValueError):decode(raw)
        self.assertEqual(decode(b'{"val":0}'),{'val':0})

    def test_explicit_zero_cannot_be_replaced_by_broader_stock_or_dividend_alias(self):
        p=calculate();self.assertEqual(p['net_buyback_ttm'],0);self.assertEqual(p['dividend_yield'],0)
        self.assertEqual(p['gross_repurchases_ttm'],400)
        self.assertEqual(p['fcf_yield_annualized'],-0.8)
        self.assertEqual(p['provider_responses']['cash_flow'],sources()[1])

    def test_two_rows_and_absent_durations_do_not_become_a_year(self):
        data=sources();data[1]=data[1][:2];self.assertIsNone(calculate(data)['gross_repurchases_ttm'])
        data=sources()
        for r in data[1]:r.pop('startDate')
        p=calculate(data);self.assertIsNone(p['gross_repurchases_ttm'])
        self.assertEqual(len(p['measurements']['cashflow_observations']),4)
        self.assertFalse(p['measurements']['cashflow_window']['reported_calendar_duration_aligned'])

    def test_mixed_currency_and_issuer_quarters_cannot_be_summed(self):
        for key,value in [('reportedCurrency','JPY'),('cik','0000000002')]:
            data=sources();data[1][1][key]=value
            p=calculate(data);self.assertIsNone(p['gross_repurchases_ttm']);self.assertIsNone(p['net_buyback_yield'])
            self.assertEqual(p['provider_responses']['cash_flow'][1][key],value)

    def test_missing_boolean_and_malformed_inputs_are_not_zeros(self):
        for value in (None,False,'0',float('nan')):
            data=sources();data[1][0]['commonStockRepurchased']=value
            self.assertIsNone(calculate(data)['gross_repurchases_ttm'])
        data=sources();data[1][0].pop('operatingCashFlow');data[1][0].pop('netCashProvidedByOperatingActivities')
        self.assertIsNone(calculate(data)['fcf_yield_annualized'])
        data=sources();data[1][0]['operatingCashFlow']=False
        self.assertIsNone(calculate(data)['fcf_yield_annualized'])

    def test_cashflow_order_is_irrelevant_and_conflicting_fifth_observation_is_retained(self):
        data=sources();data[1].reverse();self.assertEqual(calculate(data)['gross_repurchases_ttm'],400)
        data=sources();extra=deepcopy(data[1][-1]);extra['commonStockRepurchased']=-900;data[1].append(extra)
        p=calculate(data);self.assertIsNone(p['gross_repurchases_ttm']);self.assertEqual(len(p['provider_responses']['cash_flow']),5)

    def test_invalid_future_and_noncontiguous_periods_abstain(self):
        for key,value in [('date','2026-02-30'),('date','2026-12-31'),('startDate','2026-01-03'),('period','FY')]:
            data=sources();data[1][0][key]=value;self.assertIsNone(calculate(data)['gross_repurchases_ttm'])
        data=sources();data[1][2]['period']='Q2';self.assertIsNone(calculate(data)['gross_repurchases_ttm'])

    def test_market_cap_must_have_explicit_matching_date_and_currency(self):
        for field,value in [('reportedCurrency',None),('reportedCurrency','JPY'),('date','2026-03-31'),('marketCap',False),('marketCap',0)]:
            data=sources();data[2][0][field]=value;p=calculate(data)
            self.assertEqual(p['gross_repurchases_ttm'],400);self.assertIsNone(p['gross_buyback_yield'])

    def test_nine_month_share_change_is_not_yoy_and_split_adjustment_is_not_asserted(self):
        data=sources();data[3][1]['date']='2025-09-30';self.assertIsNone(calculate(data)['share_count_reduction_yoy'])
        p=calculate();self.assertEqual(p['share_count_reduction_yoy'],20)
        self.assertFalse(p['measurements']['reported_shares']['split_and_corporate_action_adjusted'])
        self.assertIsNone(p['net_issuer']);self.assertIsNone(p['active_execution']);self.assertIsNone(p['debt_funded'])

    def test_combined_yield_overflow_and_malformed_profile_labels_do_not_break_publication(self):
        data=sources();data[0][0].update(sector=123,industry=True)
        data[2][0]['marketCap']=100
        for row in data[1]:
            row['netCommonStockIssuance']=-2.5e307
            row['commonDividendsPaid']=-2.5e307
        p=calculate(data)
        self.assertEqual(p['net_buyback_yield'],1e308)
        self.assertEqual(p['dividend_yield'],1e308)
        self.assertIsNone(p['shareholder_yield'])
        self.assertEqual(p['provider_responses']['profile'],data[0])
        json.dumps(p,allow_nan=False)

    def test_latest_invalid_or_duplicate_share_record_cannot_fall_back_to_older_value(self):
        for bad in (False,None,-1):
            data=sources();data[3][0]['numberOfShares']=bad;self.assertIsNone(calculate(data)['share_count_reduction_yoy'])
        data=sources();data[3].append(deepcopy(data[3][0]));self.assertIsNone(calculate(data)['share_count_reduction_yoy'])

    def test_actual_acquisition_keeps_one_row_without_extra_provider_requests(self):
        calls=[];data=sources();data[1]=data[1][:1]
        def fmp(path):calls.append(path);return data[0] if path.startswith('profile?') else data[1]
        p=native(fmp=fmp)['analyze_ticker']('TEST')
        self.assertEqual(len(calls),2);self.assertEqual(len(p['provider_responses']['cash_flow']),1)
        self.assertIsNone(p['gross_repurchases_ttm'])

    def test_source_calculations_and_self_reported_flags_cannot_authorize_forecast_scores(self):
        ns=native();p=calculate();p.update(calls_eligible=True,forecast_qualified=True)
        self.assertEqual(ns['classify_and_score'](p,99,True),(None,'RESEARCH_ONLY',False,False))

    def handler(self,unavailable=False):
        writes={};data=sources()
        scanner={'top_opportunities':[{'ticker':'TEST','authorization_usd':99999,'market_cap':10000,'company':'Test company'}]}
        def read(key,default=None):
            return {'data/buyback-scanner.json':scanner,'data/attention-confluence.json':{'tickers':{'TEST':{}}},
                    'data/earnings-blackout.json':{},'data/share-flows.json':{}}[key]
        def original_fmp(path):
            if path.startswith('earnings-calendar?'):return []
            if unavailable:return None
            for prefix,index in [('profile?',0),('cash-flow-statement?',1),('key-metrics?',2),('enterprise-values?',3)]:
                if path.startswith(prefix):return deepcopy(data[index])
            raise AssertionError(path)
        def fmp(path,capture=None):
            value=original_fmp(path);return (value,[]) if capture is not None else value
        ns=native(_read=read,fmp=fmp,s3=Memory(writes))
        return ns,writes,scanner

    def test_actual_handler_preserves_records_and_abstains_without_null_comparison_crashes(self):
        ns,writes,scanner=self.handler();ns['lambda_handler']();p=writes['data/buyback-engine.json']
        self.assertEqual(p['n_scored'],0);self.assertEqual(p['n_research_rows'],1)
        self.assertIsNone(p['tickers']['TEST']['buyback_score']);self.assertIsNone(p['tickers']['TEST']['auth_pct_mcap'])
        self.assertEqual(p['unverified_scanner_records'],scanner['top_opportunities'])
        self.assertEqual(p['high_conviction_pumps'],[]);self.assertFalse(p['calls_eligible'])
        self.assertEqual(p['tickers']['TEST']['provider_responses']['cash_flow'],sources()[1])

    def test_total_acquisition_failure_preserves_previous_output(self):
        ns,writes,_=self.handler(unavailable=True);result=ns['lambda_handler']()
        self.assertTrue(result['kept_prior']);self.assertEqual(writes,{})


    def test_native_whole_originals_prior_current_history_and_offline_replay(self):
        ns,db,calls,bodies,waits=captured_handler();prior=db.raw[store.HEAD];ns['lambda_handler']();raw=db.raw[store.HEAD];p=json.loads(raw)
        self.assertEqual(len(calls),11);self.assertEqual(waits,[0.25])
        self.assertEqual(db.raw[p['previous_publication']['key']],prior)
        self.assertEqual(db.raw[store.reference(raw)['key']],raw);self.assertEqual(p['source_files'],store.source_identity())
        for body in bodies:self.assertEqual(db.raw[capture_sources.reference(body)['key']],body)
        replay=capture_sources.replay(p,lambda ref:capture_sources.read_original(db,'fixture',ref))
        self.assertEqual(replay['retained_responses'],11);self.assertEqual(replay['current_issuers'],1)
        self.assertEqual(replay['cashflow_observations'],4);self.assertEqual(replay['calendar_windows'],7)
        self.assertFalse(replay['selection_context_replayed']);self.assertNotIn('fixture-secret',json.dumps(p))
        self.assertEqual(p['tickers']['TEST']['gross_repurchases_ttm'],400)

    def test_one_statement_keeps_original_nine_requests_without_synthetic_ttm(self):
        ns,db,calls,_,_=captured_handler(one_row=True);ns['lambda_handler']();p=json.loads(db.raw[store.HEAD])
        self.assertEqual(len(calls),9);self.assertIsNone(p['tickers']['TEST']['gross_repurchases_ttm'])
        self.assertEqual(capture_sources.replay(p,lambda ref:capture_sources.read_original(db,'fixture',ref))['cashflow_observations'],1)

    def test_existing_retry_count_and_delay_are_preserved_and_replayed(self):
        ns,db,calls,_,waits=captured_handler(retry=True);ns['lambda_handler']();p=json.loads(db.raw[store.HEAD])
        self.assertEqual(len(calls),12);self.assertEqual(waits,[0.4,0.25])
        result=capture_sources.replay(p,lambda ref:capture_sources.read_original(db,'fixture',ref))
        self.assertEqual(result['request_attempts'],12);self.assertEqual(result['retained_responses'],11)
        self.assertFalse(result['whole_provider_http_replay_verified']);self.assertTrue(result['retained_http_responses_replayed'])

    def test_original_replay_rejects_type_byte_binding_calendar_and_projection_tampering(self):
        ns,db,_,_,_=captured_handler();ns['lambda_handler']();original=json.loads(db.raw[store.HEAD])
        for change in (lambda p:p['tickers']['TEST'].update(calls_eligible=0),lambda p:p['tickers']['TEST'].update(gross_repurchases_ttm=999),
                       lambda p:p['calendar_sources'].pop(),lambda p:p['tickers']['TEST'].update(in_blackout=1),
                       lambda p:p['provider_sources']['attempts'][0].update(request_index=True),
                       lambda p:p['tickers']['TEST']['provider_attempts']['profile'][0].update(request_index=0)):
            p=deepcopy(original);change(p)
            with self.assertRaises(ValueError):capture_sources.replay(p,lambda ref:capture_sources.read_original(db,'fixture',ref))
        ref=original['provider_sources']['attempts'][0]['original_ref'];db.raw[ref['key']]=b'{}'
        with self.assertRaises(ValueError):capture_sources.replay(original,lambda ref:capture_sources.read_original(db,'fixture',ref))

    def test_racing_publisher_cannot_overwrite_newer_whole_head(self):
        ns,db,_,_,_=captured_handler();original_publish=ns['publish'];newer=b'{"tickers":{"NEWER":{"symbol":"NEWER"}}}'
        def race(*args):db.raw[store.HEAD]=newer;return original_publish(*args)
        ns['publish']=race
        with self.assertRaises(StorageError):ns['lambda_handler']()
        self.assertEqual(db.raw[store.HEAD],newer)

    def test_archive_write_or_readback_failure_keeps_accepted_publication(self):
        for corrupt in (False,True):
            ns,db,_,_,_=captured_handler();prior=db.raw[store.HEAD];put=db.put_object
            def write(**kw):
                if kw['Key'].startswith(capture_sources.PREFIX):
                    if not corrupt:raise StorageError('AccessDenied')
                    kw=dict(kw,Body=b'{}')
                return put(**kw)
            db.put_object=write
            with self.assertRaises((StorageError,ValueError)):ns['lambda_handler']()
            self.assertEqual(db.raw[store.HEAD],prior)

    def test_malformed_or_denied_head_fails_before_provider_requests(self):
        for raw in (b'[]',b'{"tickers":[]}',b'{"tickers":{},"tickers":{}}'):
            ns,db,calls,_,_=captured_handler();db.raw[store.HEAD]=raw
            with self.assertRaises(ValueError):ns['lambda_handler']()
            self.assertEqual(calls,[]);self.assertEqual(db.puts,[])
        ns,db,calls,_,_=captured_handler()
        def denied(**kwargs):raise StorageError('AccessDenied')
        db.get_object=denied
        with self.assertRaises(StorageError):ns['lambda_handler']()
        self.assertEqual(calls,[]);self.assertEqual(db.puts,[])

    def test_source_scope_credentials_and_expanded_bounds_are_checked(self):
        for url in ('https://evil.invalid/a','https://financialmodelingprep.com/evil/profile?symbol=TEST',
                    'https://financialmodelingprep.com/stable/earnings-calendar?from=2026-01-01&to=2026-02-01&limit=3000'):
            with self.assertRaises(ValueError):capture_sources.endpoint(url)
        db=Memory();raw=b'{"error":"fixture-\\u0073ecret"}'
        class Response(BytesIO):pass
        def echo(*args,**kwargs):
            value=Response(raw);value.code=200;value.headers={};return value
        c=capture_sources.Capture(db,'fixture','fixture','fixture-secret',opener=echo)
        with self.assertRaises(PermissionError):c.acquire('https://financialmodelingprep.com/stable/profile?symbol=TEST&apikey=fixture-secret')
        self.assertEqual(db.puts,[])
        with patch.object(capture_sources,'DECODED_BOUND',8):
            with self.assertRaises(ValueError):capture_sources.expanded(b'123456789','identity')

    def test_excluded_provider_profile_keeps_its_exact_original_and_count(self):
        ns,db,calls,_,_=captured_handler(excluded=True);ns['lambda_handler']();p=json.loads(db.raw[store.HEAD])
        self.assertEqual(len(calls),12);self.assertEqual(p['n_excluded'],1)
        self.assertEqual(p['excluded'][0]['profile_response'][0]['whole_profile'],'retained')
        self.assertEqual(capture_sources.replay(p,lambda ref:capture_sources.read_original(db,'fixture',ref))['retained_responses'],12)
        p['excluded'][0]['provider_attempts']=None
        with self.assertRaises(ValueError):capture_sources.replay(p,lambda ref:capture_sources.read_original(db,'fixture',ref))


if __name__=='__main__':unittest.main()
