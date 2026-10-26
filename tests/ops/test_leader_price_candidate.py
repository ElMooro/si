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
import bank_originals
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
        'ThreadPoolExecutor':ThreadPoolExecutor,'momentum_exclusion':exclusion,'bank_originals':bank_originals}
    ns.update({name:getattr(m,name) for name in ('CONTRACT','HEAD','FLAGS','INPUTS','sha','encode','strict','clock','symbol','endpoint','source_ref','validate_ref','content','universe','history','build')})
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_leaders_') or n.name in ('_LeadersSources','lambda_handler'))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated native leader functions>','exec'),ns);return ns


def bank_answer(pulled_at='2026-10-26T02:00:00+00:00',n=90):
    """A fake warehouse-bank answer: FMP-shaped rows for the requested symbol with the bank's provenance."""
    def answer(url,now=None):
        ticker=url.split('symbol=')[1].split('&')[0]
        raw=m.encode([dict(r,symbol=ticker) for r in rows(n)])
        return {'raw':raw,'transport':'warehouse_bank','received_at':datetime.now(timezone.utc).isoformat(),
                'bank':{'object':'data/warm/fmp-walker/historical-price-eod-full/'+ticker+'.json.gz','pulled_at':pulled_at,'as_of':None}}
    return answer


def never_open(*a,**k):raise AssertionError('No live provider connection may be opened when the bank answers')


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
        with patch.object(urllib.request,'build_opener',return_value=Opener()),patch.object(bank_originals,'bank_original',lambda url,now=None:None):out=ns['_leaders_fetch']('TEST',sources,lambda:200)
        self.assertEqual(out['status'],'received');self.assertEqual(requests[0][1],15)
        self.assertIn('&from=',requests[0][0].full_url);self.assertNotIn(ns['FMP_KEY'],requests[0][0].full_url)
        self.assertEqual((m.day(out['request_window']['to'])-m.day(out['request_window']['from'])).days,290)
        self.assertNotIn(ns['FMP_KEY'],json.dumps(out))
    def test_rate_limit_stops_further_requests_and_credential_echo_is_withheld(self):
        ns=native();sources=ns['_LeadersSources']()
        class Response(BytesIO):status=429
        with patch.object(urllib.request,'build_opener') as opener,patch.object(bank_originals,'bank_original',lambda url,now=None:None):
            opener.return_value.open.return_value=Response(b'{"error":"quota"}')
            self.assertEqual(ns['_leaders_fetch']('TEST',sources,lambda:200)['http_status'],429)
            self.assertEqual(ns['_leaders_fetch']('OTHER',sources,lambda:200)['status'],'not_attempted_runtime_rate_or_size_limit')
            self.assertEqual(opener.return_value.open.call_count,1)
        class Echo(BytesIO):status=200
        sources=ns['_LeadersSources']()
        with patch.object(urllib.request,'build_opener') as opener,patch.object(bank_originals,'bank_original',lambda url,now=None:None):
            opener.return_value.open.return_value=Echo(json.dumps({'error':ns['FMP_KEY']}).encode())
            self.assertEqual(ns['_leaders_fetch']('TEST',sources,lambda:200)['status'],'credential_echo_withheld');self.assertEqual(sources.raw,{})
    # ── 2026-10-26 bank rung: the warehouse bank's copy of the provider response as a retained original ──
    def test_bank_rung_retains_warehouse_original_with_provenance_and_opens_no_live_connection(self):
        ns=native();sources=ns['_LeadersSources']()
        with patch.object(bank_originals,'bank_original',bank_answer()),patch.object(urllib.request,'build_opener',never_open):
            out=ns['_leaders_fetch']('TEST',sources,lambda:200)
        self.assertEqual((out['status'],out['transport'],out['network_attempted'],out['http_status']),('received','warehouse_bank',False,None))
        self.assertEqual(out['bank'],{'object':'data/warm/fmp-walker/historical-price-eod-full/TEST.json.gz','pulled_at':'2026-10-26T02:00:00+00:00','as_of':None})
        self.assertEqual((m.day(out['request_window']['to'])-m.day(out['request_window']['from'])).days,290)
        self.assertEqual(out['endpoint'],m.endpoint('TEST'));self.assertIn(out['original_ref']['key'],sources.raw)
        self.assertFalse(sources.stop.is_set());self.assertNotIn('bank_attempt',out);self.assertEqual(m.provenance(out),'warehouse_bank')
        obs=m.history(out,sources.raw,'TEST',datetime.now(timezone.utc).isoformat())
        self.assertEqual(obs['status'],'parsed_completed_observations');self.assertEqual(obs['transport'],'warehouse_bank');self.assertEqual(obs['bank'],out['bank'])
        self.assertEqual(obs['measurements']['status'],'descriptive_observations')
        # a bank record claiming an HTTP status, a network attempt or an incomplete bank block is a contract violation
        for bad in ({'http_status':200},{'network_attempted':True},{'bank':{'object':'x'}},{'transport':'mystery'}):
            with self.assertRaises(ValueError):m.history({**out,**bad},sources.raw,'TEST',datetime.now(timezone.utc).isoformat())
        with self.assertRaises(ValueError):m.provenance({'status':'received','bank':out['bank']})          # bank block on a provider record
        # every record retained before the rung is a provider record with its original shape
        sources2={};self.assertEqual(m.provenance(attempt(m.encode(rows()),sources2)),'provider')
    def test_bank_vintage_not_newer_than_previous_original_falls_back_to_live_and_records_it(self):
        ns=native();sources=ns['_LeadersSources']();sources.previous_vintages={'TEST':'2026-10-26T09:49:00+00:00'}
        class Response(BytesIO):status=200
        class Opener:
            def open(self,req,timeout):return Response(m.encode(rows()))
        with patch.object(bank_originals,'bank_original',bank_answer(pulled_at='2026-10-26T02:00:00+00:00')),patch.object(urllib.request,'build_opener',return_value=Opener()):
            out=ns['_leaders_fetch']('TEST',sources,lambda:200)
        self.assertEqual((out['status'],out['http_status']),('received',200));self.assertNotIn('transport',out);self.assertEqual(m.provenance(out),'provider')
        self.assertEqual(out['bank_attempt']['status'],'bank_vintage_not_newer');self.assertEqual(out['bank_attempt']['previous_vintage'],'2026-10-26T09:49:00+00:00')
        self.assertEqual(out['bank_attempt']['bank']['pulled_at'],'2026-10-26T02:00:00+00:00')
        # the same bank pull a day later is newer than yesterday's bank original -> admitted; equal -> refused
        sources.previous_vintages={'TEST':'2026-10-25T02:00:00+00:00'}
        with patch.object(bank_originals,'bank_original',bank_answer(pulled_at='2026-10-26T02:00:00+00:00')),patch.object(urllib.request,'build_opener',never_open):
            self.assertEqual(ns['_leaders_fetch']('TEST',sources,lambda:200)['transport'],'warehouse_bank')
        sources.previous_vintages={'TEST':'2026-10-26T02:00:00+00:00'}
        with patch.object(bank_originals,'bank_original',bank_answer(pulled_at='2026-10-26T02:00:00+00:00')),patch.object(urllib.request,'build_opener',return_value=Opener()):
            self.assertEqual(ns['_leaders_fetch']('TEST',sources,lambda:200)['bank_attempt']['status'],'bank_vintage_not_newer')
    def test_no_bank_answer_keeps_the_live_rung_exactly_and_a_bank_answer_never_sets_or_needs_the_provider_backoff(self):
        ns=native();sources=ns['_LeadersSources']()
        class Response(BytesIO):status=429
        with patch.object(bank_originals,'bank_original',lambda url,now=None:None),patch.object(urllib.request,'build_opener') as opener:
            opener.return_value.open.return_value=Response(b'{"error":"quota"}')
            out=ns['_leaders_fetch']('TEST',sources,lambda:200)
            self.assertEqual((out['http_status'],out['bank_attempt']),(429,{'status':'no_bank_answer'}));self.assertTrue(sources.stop.is_set())
            self.assertEqual(ns['_leaders_fetch']('OTHER',sources,lambda:200)['status'],'not_attempted_runtime_rate_or_size_limit')
            self.assertEqual(opener.return_value.open.call_count,1)
        # after the provider back-off the bank still answers (no provider request), and a bank fault is a typed attempt, not a failure
        with patch.object(bank_originals,'bank_original',bank_answer()),patch.object(urllib.request,'build_opener',never_open):
            self.assertEqual(ns['_leaders_fetch']('OTHER',sources,lambda:200)['transport'],'warehouse_bank')
        with patch.object(bank_originals,'bank_original',lambda url,now=None:(_ for _ in ()).throw(RuntimeError('bank down'))),patch.object(urllib.request,'build_opener',return_value=type('O',(),{'open':lambda s,r,timeout:type('R',(BytesIO,),{'status':200})(m.encode(rows()))})()):
            sources=ns['_LeadersSources']();out=ns['_leaders_fetch']('TEST',sources,lambda:200)
            self.assertEqual((out['status'],out['bank_attempt']),('received',{'status':'bank_error','error_type':'RuntimeError'}))
        # the retention budget and the runtime reserve gate the bank rung exactly as the live one
        sources=ns['_LeadersSources']();sources.bytes=96*1024*1024
        with patch.object(bank_originals,'bank_original',never_open):self.assertEqual(ns['_leaders_fetch']('TEST',sources,lambda:200)['status'],'not_attempted_runtime_rate_or_size_limit')
        with patch.object(bank_originals,'bank_original',never_open):self.assertEqual(ns['_leaders_fetch']('TEST',ns['_LeadersSources'](),lambda:10)['status'],'not_attempted_runtime_rate_or_size_limit')
    def test_previous_vintages_are_the_provider_time_of_each_retained_original(self):
        ns=native()
        live={'status':'received','endpoint':m.endpoint('LIVE'),'received_at':'2026-10-25T09:49:00+00:00','http_status':200}
        bank={'status':'received','endpoint':m.endpoint('BANK'),'received_at':'2026-10-25T09:49:00+00:00','transport':'warehouse_bank',
              'bank':{'object':'o','pulled_at':'2026-10-25T02:00:00+00:00','as_of':None}}
        failed={'status':'transport_unavailable','endpoint':m.endpoint('GONE'),'received_at':'2026-10-25T09:49:00+00:00'}
        previous={'measurement_contract':m.CONTRACT,'request_records':[{'acquisition':live},{'acquisition':bank},{'acquisition':failed},{'acquisition':{'status':'invalid_symbol_not_requested'}}],
                  'benchmark':{'acquisition':{**live,'endpoint':m.endpoint('SPY')}}}
        self.assertEqual(ns['_leaders_previous_vintages'](previous),{'LIVE':'2026-10-25T09:49:00+00:00','BANK':'2026-10-25T02:00:00+00:00','SPY':'2026-10-25T09:49:00+00:00'})
        self.assertEqual(ns['_leaders_previous_vintages']({'all_scored':[]}),{});self.assertEqual(ns['_leaders_previous_vintages'](None),{})
    def test_bank_sourced_publication_counts_the_rung_and_replays_through_the_acceptance_check(self):
        sys.path.insert(0,str(ROOT/'aws/ops/staged'));sys.path.insert(0,str(ROOT/'aws/ops'));sys.path.insert(0,str(ROOT/'aws/ops/checks'))
        import ops_6267_leader_price_acceptance as op
        memory=Memory();ns=native(memory)
        with patch.object(bank_originals,'bank_original',bank_answer()),patch.object(urllib.request,'build_opener',never_open):ns['lambda_handler']()
        raw=memory.data[m.HEAD];p=m.strict(raw)
        self.assertEqual(p['version'],'2.1.0');self.assertEqual(p['bank_rung']['module'],'bank_originals')
        self.assertEqual((p['bank_rung']['n_bank_originals'],p['bank_rung']['n_live_originals'],p['bank_rung']['n_vintage_refused']),(2,0,0))
        self.assertEqual(p['bank_rung']['module_identity']['sha256'],m.sha((ROOT/'aws/shared/bank_originals.py').read_bytes()))
        self.assertEqual(p['request_records'][0]['acquisition']['transport'],'warehouse_bank');self.assertEqual(p['benchmark']['acquisition']['transport'],'warehouse_bank')
        self.assertEqual(p['request_records'][0]['observations']['status'],'parsed_completed_observations')
        self.assertEqual(p['request_records'][0]['observations']['bank']['pulled_at'],'2026-10-26T02:00:00+00:00')
        self.assertTrue(any('warehouse-bank original' in c for c in p['caveats']));self.assertEqual(p['source_files'].keys(),{'lambda_function.py','leader_price_observations.py','momentum_research_boundary.py'})
        for k in m.FLAGS:self.assertIs(p[k],False)
        result=op.publication(raw,memory.previous,op.read_sources(memory,p))
        self.assertEqual(result['status'],'published_leader_originals_replayed');self.assertEqual(result['request_occurrences'],1)
        # the next run sees the bank's pull time as the previous vintage: the same pull is refused, live follows
        class Response(BytesIO):status=200
        class Opener:
            def open(self,req,timeout):return Response(m.encode([dict(r,symbol=req.full_url.split('symbol=')[1].split('&')[0]) for r in rows()]))
        with patch.object(bank_originals,'bank_original',bank_answer()),patch.object(urllib.request,'build_opener',return_value=Opener()):ns['lambda_handler']()
        p2=m.strict(memory.data[m.HEAD])
        self.assertEqual((p2['bank_rung']['n_bank_originals'],p2['bank_rung']['n_live_originals'],p2['bank_rung']['n_vintage_refused']),(0,2,2))
        self.assertEqual(p2['bank_rung']['previous_vintages_known'],2);self.assertEqual(p2['request_records'][0]['acquisition']['bank_attempt']['status'],'bank_vintage_not_newer')
    def test_engine_request_shape_is_answered_by_the_real_bank_reader(self):
        import gzip,fmp_bank,fmp_bankfirst
        class S3:
            def __init__(self):self.store={};self.meta={}
            def get_object(self,Bucket,Key):
                if Key not in self.store:raise KeyError(Key)
                return {'Body':BytesIO(self.store[Key]),'Metadata':self.meta.get(Key,{})}
        now=datetime(2026,10,22,23,0,tzinfo=timezone.utc)      # Thursday 23:00 UTC: Oct 22's session is complete
        class FixedDT(datetime):
            @classmethod
            def now(cls,tz=None):return now if tz else now.replace(tzinfo=None)
        s3=S3();W=fmp_bank.WALKER_ROOT
        banked=[dict(r,symbol='TEST',date=(date(2026,10,22)-timedelta(days=89-i)).isoformat()) for i,r in enumerate(rows())]
        s3.store[W+'historical-price-eod-full/TEST.json.gz']=gzip.compress(json.dumps(banked).encode());s3.meta[W+'historical-price-eod-full/TEST.json.gz']={'pulled_at':'2026-10-22T20:00:00Z'}
        saved=(fmp_bank._S3,fmp_bankfirst.datetime)
        fmp_bank._S3=s3;fmp_bank._cache.clear();fmp_bank._TABLES.clear();fmp_bank._MISSING.clear();fmp_bank._EVICTED.clear();fmp_bankfirst._reset_ctl(time.time());fmp_bankfirst.datetime=FixedDT
        try:
            url=m.endpoint('TEST')+'&from=2026-01-05&to=2026-10-22'
            out=bank_originals.bank_original(url,now=now)
            self.assertIsNotNone(out);rows_=json.loads(out['raw']);self.assertEqual(rows_[0]['symbol'],'TEST');self.assertEqual(len(rows_),90)
            self.assertEqual(out['bank']['object'],W+'historical-price-eod-full/TEST.json.gz');self.assertEqual(out['bank']['pulled_at'],'2026-10-22T20:00:00+00:00')
            self.assertIsNone(bank_originals.bank_original('https://financialmodelingprep.com/stable/quote?symbol=TEST',now=now))
            ns=native();sources=ns['_LeadersSources']()
            with patch.object(urllib.request,'build_opener',never_open):fetched=ns['_leaders_fetch']('TEST',sources,lambda:200)
            self.assertEqual(fetched['transport'],'warehouse_bank')
            obs=m.history(fetched,sources.raw,'TEST',datetime.now(timezone.utc).isoformat());self.assertEqual(obs['status'],'parsed_completed_observations')
        finally:
            fmp_bank._S3,fmp_bankfirst.datetime=saved;fmp_bank._cache.clear();fmp_bank._TABLES.clear();fmp_bank._MISSING.clear()


if __name__=='__main__':unittest.main(verbosity=2)
