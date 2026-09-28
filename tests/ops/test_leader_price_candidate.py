"""Synthetic native/source/publication checks; never import or invoke a live engine."""
from pathlib import Path
from datetime import datetime,timezone,date,timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from unittest.mock import patch
import ast,json,math,sys,threading,time,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-momentum-leaders/source'
sys.path[:0]=[str(SRC),str(ROOT/'aws/shared')]
import leader_price_observations as m
from momentum_research_boundary import exclusion
AT='2026-09-28T01:00:00Z';WINDOW={'from':'2025-12-12','to':'2026-09-28'}


def rows(n=90):
    return [{'symbol':'TEST','date':(date(2026,9,25)-timedelta(days=n-i-1)).isoformat(),
             'open':100,'high':102,'low':98,'close':100,'volume':10000,'unknown_original':'keep'} for i in range(n)]


def attempt(raw,sources,ticker='TEST',status=200):
    ref=m.source_ref(raw);sources[ref['key']]=raw
    return {'status':'received','endpoint':m.endpoint(ticker),'http_status':status,'original_ref':ref,
            'request_window':WINDOW,'requested_at':AT,'received_at':AT}


def measure(values):
    sources={};return m.history(attempt(m.encode(values),sources),sources,'TEST',AT)


def inputs(radar=None,trends=None):
    radar=radar if radar is not None else {'momentum_research_exclusion':exclusion(),'pump_candidates':[{'ticker':'TEST'}],'tickers':[]}
    trends=trends if trends is not None else {'trends':[{'symbol':'TEST'},None]}
    sources={};attempts={}
    for key,doc in zip(m.INPUTS,(radar,trends)):
        raw=m.encode(doc);ref=m.source_ref(raw);sources[ref['key']]=raw
        attempts[key]={'endpoint':key,'status':'received','original_ref':ref,'requested_at':AT,'received_at':AT}
    return attempts,sources


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"all_scored":[{"ticker":"OLD","momentum_score":99}],"generated_at":"2026-09-27T11:00:00Z"}'
        a,s=inputs();self.data={m.HEAD:self.previous,**{key:s[value['original_ref']['key']] for key,value in a.items()}}
        self.reads=[];self.writes=[];self.corrupt=False;self.race=False;self.denied=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'bad' if self.corrupt and ('/history/' in key or '/sources/' in key) else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':m.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key==m.HEAD and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=m.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None):
    ns={'s3':memory or Memory(),'S3_BUCKET':'fixture','OUTPUT_KEY':m.HEAD,'MAX_UNIVERSE':60,'LOOKBACK_DAYS':90,
        'FMP_KEY':'SYNTHETIC_SECRET_ONLY','time':time,'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'Path':Path,
        '__file__':str(SRC/'lambda_function.py'),'urllib':urllib,'json':json,'threading':threading,
        'ThreadPoolExecutor':ThreadPoolExecutor,'momentum_exclusion':exclusion}
    ns.update({name:getattr(m,name) for name in ('CONTRACT','HEAD','FLAGS','INPUTS','sha','encode','strict','clock','symbol','endpoint','source_ref','validate_ref','content','universe','history','build')})
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_leaders_') or n.name in ('_LeadersSources','lambda_handler'))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated native leader functions>','exec'),ns);return ns


def fake_fetch(ticker,sources,remaining):
    if m.symbol(ticker) is None:return {'status':'invalid_symbol_not_requested'}
    a=sources.capture(m.encode([dict(r,symbol=ticker) for r in rows()]),m.endpoint(ticker),datetime.now(timezone.utc).isoformat())
    now=date.today();a.update(http_status=200,request_window={'from':(now-timedelta(days=290)).isoformat(),'to':now.isoformat()});return a


def publication(memory=None):
    memory=memory or Memory();ns=native(memory);ns['_leaders_fetch']=fake_fetch;ns['lambda_handler']()
    return memory,m.strict(memory.data[m.HEAD])


class Tests(unittest.TestCase):
    def test_complete_predecessor_is_preserved_and_original_breakout_reader_stays_guarded(self):
        raw=(SRC/'lambda_function.py').read_bytes().split(b'\n# Whole predecessor remains above.')[0]
        old=(ROOT/'tests/fixtures/pre-momentum-leaders-momentum-leaders.py.txt').read_bytes()
        self.assertEqual(raw.rstrip(b'\n').replace(b'def _legacy_lambda_handler(event, context):',b'def lambda_handler(event, context):',1),old.rstrip(b'\n'))
        self.assertIn('if key == MOMENTUM_SOURCE:',raw.decode())
    def test_universe_retains_occurrences_deduplicates_requests_and_keeps_original_list_limits(self):
        radar={'momentum_research_exclusion':exclusion(),'pump_candidates':[{'ticker':'TEST'},{'ticker':'TEST'}],
               'tickers':[{'ticker':'X'+str(i)} for i in range(40)]}
        trend={'trends':[{'symbol':'TEST'}]+[{'symbol':'T'+str(i)} for i in range(30)]}
        a,s=inputs(radar,trend);out=m.universe(a,s,60)
        self.assertEqual(len(out['occurrences']),73);self.assertEqual(len(out['selected']),55)
        test=next(r for r in out['selected'] if r['ticker']=='TEST');self.assertEqual(len(test['occurrence_indices']),3)
        self.assertEqual(test['categories'],['convergence-pump','ticker-trends'])
        self.assertNotIn('X30',[r['ticker'] for r in out['selected']]);self.assertNotIn('T24',[r['ticker'] for r in out['selected']])
        self.assertEqual(out['excluded_source']['key'],m.EXCLUDED)
    def test_original_priority_applies_only_above_cap_and_preboundary_composite_abstains(self):
        radar={'momentum_research_exclusion':exclusion(),'pump_candidates':[{'ticker':'P'+str(i)} for i in range(65)]}
        a,s=inputs(radar,{'trends':[{'symbol':'P64'}]});out=m.universe(a,s,60)
        self.assertEqual(out['selected'][0]['ticker'],'P64');self.assertEqual(len(out['selected']),60)
        radar.pop('momentum_research_exclusion');a,s=inputs(radar,{'trends':[{'symbol':'SAFE'}]});out=m.universe(a,s,60)
        self.assertEqual([r['ticker'] for r in out['selected']],['SAFE'])
        self.assertEqual(out['input_outcomes'][0]['status'],'excluded_pre_breakout_boundary')
    def test_current_ticker_trends_producer_schema_is_not_silently_dropped(self):
        producer=(ROOT/'aws/lambdas/justhodl-ticker-trends/source/lambda_function.py').read_text(encoding='utf-8')
        self.assertIn('"all_results":    results',producer)
        trend={'schema_version':'2.0','method':'wikipedia_primary_gtrends_fallback_v2',
               'all_results':[{'ticker':'T'+str(i),'score':99} for i in range(30)],'top_20':[{'ticker':'WRONG_PREVIEW'}]}
        a,s=inputs({'momentum_research_exclusion':exclusion(),'pump_candidates':[],'tickers':[]},trend)
        out=m.universe(a,s,60);self.assertEqual(len(out['selected']),25);self.assertEqual(len(out['occurrences']),30)
        self.assertEqual(out['selected'][-1]['ticker'],'T24');self.assertEqual(out['occurrences'][0]['field'],'all_results')
        self.assertNotIn('WRONG_PREVIEW',[r['ticker'] for r in out['selected']])
        trend['method']='unknown';a,s=inputs({},trend)
        self.assertEqual(m.universe(a,s,60)['selected'],[])
    def test_fifty_rows_are_available_window_not_a_52_week_high(self):
        out=measure(rows(50))['measurements'];self.assertEqual(out['status'],'descriptive_observations')
        high=out['close_vs_observed_window_high'];self.assertEqual(high['observations'],50);self.assertEqual(high['calendar_span_days'],49)
        self.assertFalse(high['annual_window_verified']);self.assertIsNone(out['fifty_two_week_high']['value'])
        self.assertIsNone(out['price_change_60']['value']);self.assertNotIn('momentum_score',out)
    def test_all_original_zero_volumes_remain_in_mean_and_true_zero_numerator_remains_zero(self):
        r=rows()
        for row in r[-21:]:row['volume']=0
        r[-2]['volume']=100;r[-1]['volume']=100
        self.assertEqual(measure(r)['measurements']['relative_volume_prior_20']['value'],20)
        r[-1]['volume']=0;self.assertEqual(measure(r)['measurements']['relative_volume_prior_20']['value'],0)
        r[-2]['volume']=0;self.assertIsNone(measure(r)['measurements']['relative_volume_prior_20']['value'])
    def test_missing_gap_values_boolean_prices_wrong_issuer_and_duplicate_dates_cannot_become_measurements(self):
        for change in ({'open':None},{'close':True},{'symbol':'WRONG'},{'high':95},{'volume':-1}):
            r=rows();r[-1].update(change);self.assertIsNone(measure(r)['measurements'])
        r=rows();r[-1]['date']=r[-2]['date'];self.assertIsNone(measure(r)['measurements'])
    def test_gap_pair_dates_values_and_strict_comparison_are_replayable(self):
        r=rows();r[-2]['open']=101;r[-1]['open']=100
        out=measure(r)['measurements']['gap_up_count_last_3_reported_pairs']
        self.assertEqual(out['value'],1);self.assertEqual(len(out['pairs']),3)
        self.assertEqual(out['pairs'][-2]['gap_percent_exact'],'1.00');self.assertEqual(out['pairs'][-1]['source_index'],89)
    def test_exact_benchmark_endpoints_missing_is_not_zero_or_shifted(self):
        stock=measure(rows());source={};spy=[dict(r,symbol='SPY') for r in rows()]
        spy[-1]['close']=101;spy[-1]['high']=102
        benchmark=m.history(attempt(m.encode(spy),source,'SPY'),source,'SPY',AT)
        self.assertEqual(m.comparisons(stock,benchmark)['20']['value'],-1)
        benchmark['selected_rows']=[r for r in benchmark['selected_rows'] if r['date']!=stock['measurements']['price_change_20']['start_date']]
        self.assertIsNone(m.comparisons(stock,benchmark)['20']['value'])
        self.assertIsNone(m.comparisons(stock,{})['20']['value'])
    def test_requested_span_and_completed_dates_are_enforced_before_quality_filtering(self):
        r=rows();r.append(dict(r[-1],date='2026-09-28',close=9999,high=9999))
        out=measure(r);self.assertEqual(out['measurements']['observations'],90);self.assertEqual(out['latest_observation_age_calendar_days'],3)
        sources={};a=attempt(m.encode(rows()),sources);a['request_window']={'from':'2025-01-01','to':'2026-09-28'}
        with self.assertRaises(ValueError):m.history(a,sources,'TEST',AT)
    def test_whole_source_integrity_strict_json_and_numeric_tokens(self):
        for raw in (b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":1e500}'):
            with self.assertRaises(ValueError):m.strict(raw)
        sources={};a=attempt(m.encode(rows()),sources);sources[a['original_ref']['key']]+=b' '
        with self.assertRaises(ValueError):m.history(a,sources,'TEST',AT)
        raw=m.encode(rows()).replace(b'"close":100,',b'"close":100.000000000000000000000000000001,')
        sources={};out=m.history(attempt(raw,sources),sources,'TEST',AT)
        self.assertIn('100.000000000000000000000000000001',out['selected_rows'][-1]['close'])
    def test_native_retains_whole_sources_and_previous_current_archives_and_never_reads_excluded_input(self):
        memory,p=publication()
        self.assertEqual(p['measurement_contract'],m.CONTRACT);self.assertEqual(len(p['request_records']),1)
        self.assertEqual(memory.data[p['previous_publication']['key']],memory.previous)
        self.assertEqual(memory.data['data/momentum-leaders/history/'+m.sha(memory.data[m.HEAD])+'.json'],memory.data[m.HEAD])
        self.assertNotIn(m.EXCLUDED,memory.reads)
        for k in m.FLAGS:self.assertIs(p[k],False)
        for key in ('leaders','pump_confirmed','all_scored'):self.assertEqual(p[key],[])
        self.assertIsNone(p['n_scored']);self.assertIsNone(p['call'])
        self.assertEqual(p['source_files']['leader_price_observations.py']['sha256'],m.sha((SRC/'leader_price_observations.py').read_bytes()))
    def test_two_warm_invocations_reacquire_prices_and_never_use_old_price_cache(self):
        memory=Memory();ns=native(memory);calls=[]
        def fetch(t,s,r):calls.append(t);return fake_fetch(t,s,r)
        ns['_leaders_fetch']=fetch;ns['PRICE_CACHE']={'TEST':[{'close':999999}]}
        ns['lambda_handler']();ns['lambda_handler']()
        self.assertEqual(calls,['SPY','TEST','SPY','TEST'])
    def test_failed_empty_selection_corrupt_storage_and_conditional_race_preserve_last_good(self):
        for attr in ('denied','corrupt','race'):
            memory=Memory();setattr(memory,attr,True)
            with self.assertRaises(Exception):publication(memory)
            self.assertEqual(memory.data[m.HEAD],memory.previous)
        memory=Memory();ns=native(memory);ns['_leaders_fetch']=lambda *a:{'status':'transport_unavailable'}
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(memory.data[m.HEAD],memory.previous)
    def test_reported_empty_selection_publishes_whole_coverage_without_any_provider_request(self):
        memory=Memory();memory.data[m.INPUTS[0]]=m.encode({'momentum_research_exclusion':exclusion(),'pump_candidates':[],'tickers':[]})
        memory.data[m.INPUTS[1]]=m.encode({'trends':[]});ns=native(memory)
        with patch.dict(ns,{'_leaders_fetch':lambda *a:(_ for _ in ()).throw(AssertionError('No provider request for empty selection'))}):ns['lambda_handler']()
        p=m.strict(memory.data[m.HEAD]);self.assertEqual(p['request_records'],[])
        self.assertEqual(p['quality']['selection_status'],'no_accepted_membership');self.assertIsNone(p['n_scored'])
        self.assertEqual(p['benchmark']['acquisition']['status'],'not_requested_no_selected_tickers')
        self.assertEqual(memory.data[p['previous_publication']['key']],memory.previous)
    def test_missing_list_shape_is_not_reported_empty_selection(self):
        memory=Memory()
        for key in m.INPUTS:memory.data[key]=b'{}'
        with self.assertRaises(ValueError):publication(memory)
        self.assertEqual(memory.data[m.HEAD],memory.previous)
    def test_provider_request_is_bounded_has_original_span_no_redirect_or_secret_in_source_identity(self):
        ns=native();sources=ns['_LeadersSources']();requests=[]
        class Response(BytesIO):
            status=200
        class Opener:
            def open(self,req,timeout):requests.append((req,timeout));return Response(m.encode(rows()))
        with patch.object(urllib.request,'build_opener',return_value=Opener()):out=ns['_leaders_fetch']('TEST',sources,lambda:200)
        self.assertEqual(out['status'],'received');self.assertEqual(requests[0][1],15)
        self.assertIn('&from=',requests[0][0].full_url);self.assertNotIn(ns['FMP_KEY'],requests[0][0].full_url)
        self.assertEqual((m.day(out['request_window']['to'])-m.day(out['request_window']['from'])).days,290)
        self.assertNotIn(ns['FMP_KEY'],json.dumps(out))
    def test_rate_limit_stops_further_requests_and_credential_echo_is_withheld(self):
        ns=native();sources=ns['_LeadersSources']()
        class Response(BytesIO):status=429
        with patch.object(urllib.request,'build_opener') as opener:
            opener.return_value.open.return_value=Response(b'{"error":"quota"}')
            self.assertEqual(ns['_leaders_fetch']('TEST',sources,lambda:200)['http_status'],429)
            self.assertEqual(ns['_leaders_fetch']('OTHER',sources,lambda:200)['status'],'not_attempted_runtime_rate_or_size_limit')
            self.assertEqual(opener.return_value.open.call_count,1)
        class Echo(BytesIO):status=200
        sources=ns['_LeadersSources']()
        with patch.object(urllib.request,'build_opener') as opener:
            opener.return_value.open.return_value=Echo(json.dumps({'error':ns['FMP_KEY']}).encode())
            self.assertEqual(ns['_leaders_fetch']('TEST',sources,lambda:200)['status'],'credential_echo_withheld');self.assertEqual(sources.raw,{})


if __name__=='__main__':unittest.main(verbosity=2)
