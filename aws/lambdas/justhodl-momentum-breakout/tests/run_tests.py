"""Isolated deterministic price, acquisition and publication regressions."""
from pathlib import Path
from datetime import datetime,timezone,date,timedelta
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from io import BytesIO
from unittest.mock import patch
from copy import deepcopy
import ast,json,sys,time,threading,unittest,urllib.request,urllib.error,math
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-momentum-breakout/source'
sys.path.insert(0,str(SRC))
import momentum_observations as m
AT='2026-09-28T01:00:00Z'


def rows(n=90):
    return [{'symbol':'TEST','date':(date(2026,9,25)-timedelta(days=n-i-1)).isoformat(),
        'open':100,'high':102,'low':98,'close':100,'volume':10000,'unknown_original':'keep'} for i in range(n)]


def attempt(raw,sources,ticker='TEST',status=200):
    ref=m.source_ref(raw);sources[ref['key']]=raw
    return {'status':'received','endpoint':m.endpoint(ticker),'http_status':status,'original_ref':ref,
        'requested_at':'2026-09-28T00:00:00Z','received_at':'2026-09-28T00:00:01Z'}


def measure(values):
    sources={};a=attempt(m.encode(values),sources);return m.history(a,sources,'TEST',AT)


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"all_qualifying":[],"generated_at":"2026-09-27T11:00:00Z"}'
        self.data={m.HEAD:self.previous,'data/universe.json':m.encode({'stocks':[{'symbol':'TEST','cap_bucket':'small'},{'symbol':'TEST','cap_bucket':'small'},None]})}
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
    ns={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':m.HEAD,'MAX_TICKERS':600,'N_WORKERS':12,'TIMEOUT_BUDGET_S':260,'MIN_DOLLAR_VOL':5000000.0,
        'FMP_KEY':'SYNTHETIC_SECRET_ONLY','time':time,'datetime':datetime,'timezone':timezone,'Path':Path,
        '__file__':str(SRC/'lambda_function.py'),'urllib':urllib,'json':json,'threading':threading,'ThreadPoolExecutor':ThreadPoolExecutor}
    ns.update({name:getattr(m,name) for name in ('CONTRACT','HEAD','FLAGS','sha','encode','strict','clock','symbol','endpoint','source_ref','validate_ref','content','universe','history','build','acquisition_plan','acquisition_progress','validate_acquisition_progress','UNATTEMPTED')})
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_momentum_') or n.name in ('_MomentumSources','lambda_handler'))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated native price functions>','exec'),ns);return ns


def fake_fetch(ticker,sources,remaining):
    if m.symbol(ticker) is None:return {'status':'invalid_symbol_not_requested'}
    a=sources.capture(m.encode([dict(r,symbol=ticker) for r in rows()]),m.endpoint(ticker),datetime.now(timezone.utc).isoformat());a['http_status']=200;return a


def publication(memory=None):
    memory=memory or Memory();ns=native(memory);ns['_momentum_fetch']=fake_fetch;ns['lambda_handler']()
    return memory,m.strict(memory.data[m.HEAD])


def resumed_publication(memory=None,denied='SECOND',workers=1,executor=None):
    memory=memory or Memory();ns=native(memory);ns['N_WORKERS']=workers;calls=[]
    if executor is not None:ns['ThreadPoolExecutor']=executor
    class OrderedClock(datetime):
        current=max(datetime.now(timezone.utc),m.clock(m.strict(memory.data[m.HEAD]).get('generated_at')))
        @classmethod
        def now(cls,tz=None):
            cls.current+=timedelta(microseconds=1);return cls.current
    ns['datetime']=OrderedClock
    def fetch(ticker,sources,remaining):
        if sources.stop.is_set():return {'endpoint':m.endpoint(ticker),'status':m.UNATTEMPTED}
        calls.append(ticker)
        raw=m.encode([{**r,'symbol':ticker} for r in rows()]) if ticker!=denied else b'{"error":"rate"}'
        a=sources.capture(raw,m.endpoint(ticker),OrderedClock.now(timezone.utc).isoformat())
        a['http_status']=429 if ticker==denied else 200
        if ticker==denied:sources.stop.set()
        return a
    ns['_momentum_fetch']=fetch;ns['lambda_handler']()
    return m.strict(memory.data[m.HEAD]),calls


class Tests(unittest.TestCase):
    def test_invalid_window_cannot_be_cleaned_into_signal(self):
        for change in ({'close':True},{'open':None},{'volume':None},{'high':90},{'volume':-1},{'currency':[]},{'symbol':'OTHER'},{'date':'bad'}):
            with self.subTest(change=change):
                r=rows();r[-1].update(change);a=measure(r)
                self.assertEqual(a['status'],'unresolved_identity_or_window');self.assertIsNone(a['measurements']);self.assertTrue(a['row_issues'])
        r=rows();r[-1]['date']=r[-2]['date'];self.assertIsNone(measure(r)['measurements'])
    def test_future_dates_and_stale_observation_clock(self):
        r=rows()
        for x in r:x['date']='2030-01-01'
        self.assertEqual(measure(r)['status'],'no_completed_observations')
        r=rows()
        for x in r:x['date']=(date.fromisoformat(x['date'])-timedelta(days=60)).isoformat()
        p=measure(r);self.assertEqual(p['latest_observation_age_calendar_days'],63);self.assertFalse(p['age_policy']['within_five_calendar_days'])
    def test_exact_tokens_reject_boolean_and_impossible_rounded_order(self):
        sources={};raw=m.encode(rows()).replace(b'"high":102',b'"high":99.9999999999999999999999999')
        p=m.history(attempt(raw,sources),sources,'TEST',AT);self.assertIsNone(p['measurements'])
        with self.assertRaises(ValueError):m.strict(b'{"a":1,"a":2}')
        with self.assertRaises(ValueError):m.strict(b'[NaN]')
        with self.assertRaises(ValueError):m.strict(b'[1e400]')
    def test_empty_error_and_unattempted_are_distinct(self):
        self.assertEqual(measure([])['status'],'reported_empty_history')
        sources={};a=attempt(b'{"Error Message":"Denied"}',sources)
        self.assertEqual(m.history(a,sources,'TEST',AT)['status'],'provider_error_or_unexpected_shape')
        a=attempt(b'{"denied":true}',sources,status=403);self.assertEqual(m.history(a,sources,'TEST',AT)['status'],'http_unavailable')
        self.assertEqual(m.history({'status':'transport_unavailable'},sources,'TEST',AT)['status'],'transport_unavailable')
    def test_source_identity_tamper_and_private_pointer_rejected(self):
        sources={};a=attempt(b'[]',sources);sources[a['original_ref']['key']]=b'[1]'
        with self.assertRaises(ValueError):m.history(a,sources,'TEST',AT)
        a['original_ref']['key']='data/private.json'
        with self.assertRaises(ValueError):m.validate_ref(a['original_ref'])
    def test_publication_current_prior_and_replay(self):
        db,p=publication();self.assertEqual(p['measurement_contract'],m.CONTRACT);self.assertEqual(len(p['request_records']),2)
        self.assertEqual(p['previous_publication']['sha256'],m.sha(db.previous));self.assertEqual(db.data[p['previous_publication']['key']],db.previous)
        self.assertIn('data/momentum-breakout/history/'+m.sha(db.data[m.HEAD])+'.json',db.data)
        self.assertEqual(p['quality']['measured_occurrences'],2);self.assertTrue(all(p[k] is False for k in m.FLAGS))
        self.assertEqual(p['all_qualifying'],[]);self.assertEqual(p['summary']['top_25_overall'],[])
        replay=m.build(p['universe_acquisition'],[r['acquisition'] for r in p['request_records']],db.data,p['generated_at'],600,p['benchmark']['acquisition'],5000000)
        for k,v in replay.items():self.assertEqual(p[k],v)
        self.assertTrue(all(k==m.HEAD or k=='data/universe.json' or k.startswith('data/momentum-breakout/') for k in db.reads+db.writes))
    def test_publication_failure_preserves_previous(self):
        for attr in ('corrupt','race','denied'):
            db=Memory();setattr(db,attr,True)
            with self.assertRaises((Error,ValueError)):publication(db)
            self.assertEqual(db.data[m.HEAD],db.previous)
        db=Memory();ns=native(db);ns['_momentum_fetch']=lambda *args:{'status':'transport_unavailable'}
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(db.data[m.HEAD],db.previous)
    def test_http_rate_limit_stops_next_request_and_retains_full_error(self):
        ns=native();sources=ns['_MomentumSources']();response=urllib.error.HTTPError(m.endpoint('TEST'),429,'limited',{},BytesIO(b'{"error":"rate"}'))
        with patch.object(urllib.request,'build_opener') as opener:
            opener.return_value.open.side_effect=response;a=ns['_momentum_fetch']('TEST',sources,lambda:100)
            self.assertEqual(a['http_status'],429);self.assertEqual(m.content(a,sources.raw),b'{"error":"rate"}')
            b=ns['_momentum_fetch']('NEXT',sources,lambda:100);self.assertEqual(b['status'],'not_attempted_runtime_rate_or_size_limit');self.assertEqual(opener.return_value.open.call_count,1)
    def test_escaped_credential_echo_withheld_and_no_redirect(self):
        ns=native();sources=ns['_MomentumSources']()
        response=BytesIO(br'{"error":"SYNTHETIC_\u0053ECRET_ONLY"}');response.status=200
        with patch.object(urllib.request,'build_opener') as opener:
            opener.return_value.open.return_value=response;a=ns['_momentum_fetch']('TEST',sources,lambda:100)
            self.assertEqual(a['status'],'credential_echo_withheld');self.assertEqual(sources.raw,{})
            self.assertIsNone(opener.call_args.args[0].redirect_request(None,None,None,None,None,None))
    def test_whole_predecessor_original_limits_and_all_publication_sources(self):
        raw=(ROOT/'tests/fixtures/pre-momentum-momentum-breakout.py.txt').read_bytes()
        self.assertEqual(m.sha(raw),'cd69689147e8e2c0a697707f17cb32bf7265c304b49e7311cbf63ce5b2cdfdc1')
        self.assertTrue((SRC/'lambda_function.py').read_bytes().startswith(raw.replace(b'def lambda_handler(',b'def _legacy_lambda_handler(',1)))
        db,p=publication();self.assertEqual(p['acquisition_limits'],{'MAX_TICKERS':600,'N_WORKERS':12,'TIMEOUT_BUDGET_S':260,'active_workers':4,'MIN_DOLLAR_VOL':5000000.0})
        self.assertEqual(len(p['request_records']),2);self.assertEqual(p['benchmark']['ticker'],'SPY')
        self.assertEqual(len(p['source_files']),2);self.assertEqual(p['spy_returns'],{})
        self.assertTrue(all(p[k] is False for k in m.FLAGS));self.assertEqual(p['all_qualifying'],[])
        for ref in [p['universe_acquisition']['original_ref'],p['benchmark']['acquisition']['original_ref'],*[x['acquisition']['original_ref'] for x in p['request_records']]]:
            self.assertEqual(m.source_ref(db.data[ref['key']]),ref)
        self.assertEqual(db.data[p['previous_publication']['key']],db.previous)
    def test_preceding_highs_strict_equality_and_missing_sixty(self):
        a=measure(rows(30))['measurements'];self.assertIsNone(a['price_change_60']['value']);self.assertIsNone(a['close_vs_preceding_high_60']['value'])
        self.assertFalse(a['close_vs_preceding_high_20']['strict_new_closing_high']);self.assertTrue(a['close_vs_preceding_high_20']['equals_previous_closing_high'])
        r=rows();r[-1]['close']=99.6;a=measure(r)['measurements']
        self.assertAlmostEqual(a['close_vs_preceding_high_60']['value'],-.4);self.assertFalse(a['close_vs_preceding_high_60']['strict_new_closing_high'])
        r[-1].update(close=101);a=measure(r)['measurements'];self.assertTrue(a['close_vs_preceding_high_60']['strict_new_closing_high'])
        self.assertNotIn(89,a['close_vs_preceding_high_60']['reference_source_indices'])
    def test_volume_denominator_excludes_latest_and_zero_is_unavailable(self):
        r=rows();r[-1]['volume']=100000;a=measure(r)['measurements']
        self.assertEqual(a['relative_volume_prior_20']['value'],10)
        self.assertEqual(a['relative_volume_prior_20']['reference_source_indices'],list(range(69,89)))
        for item in r:item['volume']=0
        a=measure(r)['measurements'];self.assertIsNone(a['relative_volume_prior_20']['value']);self.assertEqual(a['up_price_high_volume_pairs_19']['value'],0)
    def test_exact_same_date_benchmark_not_naked_scalar_or_shifted_dates(self):
        r=rows();r[-1].update(close=110,high=111);stock=measure(r)
        b=rows();b[-1].update(close=105,high=106);benchmark=measure(b)
        c=m.comparisons(stock,benchmark);self.assertEqual(c['20']['value'],5);self.assertEqual(c['60']['value'],5)
        self.assertEqual(c['20']['benchmark_change']['observations_between_endpoints'],21)
        benchmark['selected_rows']=[x for x in benchmark['selected_rows'] if x['date']!=c['20']['start_date']]
        changed=m.comparisons(stock,benchmark);self.assertIsNone(changed['20']['value']);self.assertEqual(changed['60']['value'],5)
        self.assertIsNone(m.comparisons(stock,{'measurements':{'price_change_20':{'value':0}}})['20']['value'])
    def test_selected_window_before_validation_full_source_and_currency_context(self):
        r=rows(110);r[-1]['close']=True;p=measure(r);self.assertEqual(len(p['selected_indices']),90);self.assertIsNone(p['measurements'])
        r[0]['volume']=None;r[-1]['close']=100;p=measure(r);self.assertEqual(p['status'],'parsed_completed_observations');self.assertEqual(p['source_records'],110)
        self.assertEqual(p['selected_indices'],list(range(20,110)));self.assertEqual(p['row_issues'][0]['index'],0)
        for x in r:x['currency']='JPY'
        p=measure(r);self.assertEqual(p['currency'],'JPY');self.assertFalse(p['currency_verified'])
        self.assertEqual(p['measurements']['nominal_price_volume_20']['unit'],'provider_price_unit_times_provider_volume_unit')
    def test_no_cap_bucket_filter_duplicate_memberships_and_every_selected_outcome(self):
        sources={};raw=m.encode({'stocks':[{'symbol':' test ','cap_bucket':'unknown'}]*601+[None]});ref=m.source_ref(raw);sources[ref['key']]=raw
        a={'status':'received','endpoint':'data/universe.json','original_ref':ref};p=m.universe(a,sources,600)
        self.assertEqual(len(p['selected']),600);self.assertEqual(len(p['occurrences']),602);self.assertEqual(p['selected'][0]['ticker'],'TEST')
        attempts=[{'status':'not_attempted_runtime_rate_or_size_limit','endpoint':m.endpoint('TEST')}]*600
        benchmark={'status':'credential_unavailable','endpoint':m.endpoint('SPY')}
        out=m.build(a,attempts,sources,AT,600,benchmark,5000000)
        self.assertEqual(len(out['request_records']),600);self.assertEqual(out['quality']['measured_occurrences'],0)
        with self.assertRaises(ValueError):m.build(a,attempts[:-1],sources,AT,600,benchmark,5000000)


    def test_original_rate_limit_resumes_without_refreshing_unvisited_rows(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        first,calls=resumed_publication(db);old=db.data[m.HEAD]
        self.assertEqual(calls,['SPY','TEST','SECOND']);self.assertEqual(first['acquisition_progress']['pending_occurrences'],1)
        second,calls=resumed_publication(db)
        self.assertEqual(calls,['SPY','THIRD']);self.assertTrue(second['acquisition_progress']['cycle_complete'])
        self.assertEqual([r['acquisition']['status'] for r in second['request_records']],[m.UNATTEMPTED,m.UNATTEMPTED,'received'])
        self.assertEqual(db.data[second['previous_publication']['key']],old)
        third,calls=resumed_publication(db);self.assertEqual(calls,['SPY','TEST','SECOND'])
        self.assertEqual(third['acquisition_progress']['plan_reason'],'previous_cycle_complete_or_retired')
    def test_bootstrap_from_complete_preprogress_outcomes_and_changed_membership(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        old,_=resumed_publication(db);old.pop('acquisition_progress');old['version']='2.0.0';db.data[m.HEAD]=m.encode(old)
        p,calls=resumed_publication(db);self.assertEqual(calls,['SPY','THIRD'])
        self.assertEqual(p['acquisition_progress']['plan_reason'],'resume_prior_unattempted_occurrences')
        db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('THIRD','TEST','NEW','TEST')]})
        p,calls=resumed_publication(db)
        self.assertEqual(calls,['SPY','NEW','TEST']);self.assertEqual(p['acquisition_progress']['planned_request_indices'],[2,3])
        self.assertTrue(p['universe_membership']['duplicate_memberships_retained'])
    def test_inflight_gaps_remain_pending_instead_of_being_skipped(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        p,_=resumed_publication(db,denied=None);selected=p['universe_membership']['selected'];plan=m.acquisition_plan(selected,None)
        p['request_records'][1]['acquisition']={'endpoint':m.endpoint('SECOND'),'status':m.UNATTEMPTED}
        progress=m.acquisition_progress(plan,[0,2],'provider_denial_or_rate_limit',p['retained_unique_source_bytes'])
        p['acquisition_progress']=progress;m.validate_acquisition_progress(selected,p['request_records'],progress)
        self.assertEqual(m.acquisition_plan(selected,p)['planned_request_indices'],[1])
    def test_malformed_progress_and_outcome_coordinates_fail_closed(self):
        db=Memory();p,_=resumed_publication(db,denied=None);selected=p['universe_membership']['selected']
        edits=[lambda p:p['acquisition_progress'].update(visited_occurrences=True),
               lambda p:p['acquisition_progress'].update(pending_occurrences=0.0),
               lambda p:p['acquisition_progress'].update(cycle_complete=1),
               lambda p:p['acquisition_progress'].update(request_order_is_rank=0),
               lambda p:p['acquisition_progress'].update(occurrence_identity='legal issuer identity'),
               lambda p:p['acquisition_progress'].update(plan_reason='unreviewed'),
               lambda p:p['acquisition_progress'].update(planned_request_indices=[True,0]),
               lambda p:p['acquisition_progress'].update(planned_request_indices=[False,1]),
               lambda p:p['request_records'][0].update(request_index=False),
               lambda p:p['request_records'][0]['acquisition'].update(status=m.UNATTEMPTED)]
        for edit in edits:
            broken=deepcopy(p);edit(broken)
            with self.assertRaises(ValueError):m.acquisition_plan(selected,broken)
    def test_invalid_members_remain_visible_without_blocking_valid_queue(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('BAD!','TEST','TEST')]})
        p,calls=resumed_publication(db,denied=None);self.assertEqual(calls,['SPY','TEST','TEST'])
        self.assertEqual(len(p['request_records']),3);self.assertEqual(p['request_records'][0]['acquisition']['status'],'invalid_symbol_not_requested')
        self.assertEqual(p['acquisition_progress']['planned_request_indices'],[1,2])
    def test_all_failed_remaining_window_preserves_head_and_queue(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        resumed_publication(db);old=db.data[m.HEAD]
        with self.assertRaises(ValueError):resumed_publication(db,denied='THIRD')
        self.assertEqual(db.data[m.HEAD],old)
    def test_benchmark_denial_preserves_head_and_queue(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        resumed_publication(db);old=db.data[m.HEAD];ns=native(db);calls=[]
        class LaterClock(datetime):
            @classmethod
            def now(cls,tz=None):return m.clock(m.strict(old)['generated_at'])+timedelta(seconds=1)
        ns['datetime']=LaterClock
        def denied(ticker,sources,remaining):
            calls.append(ticker);a=sources.capture(b'{"error":"rate"}',m.endpoint(ticker),LaterClock.now(timezone.utc).isoformat())
            a['http_status']=429;sources.stop.set();return a
        ns['_momentum_fetch']=denied
        with self.assertRaisesRegex(ValueError,'No received market observations'):ns['lambda_handler']()
        self.assertEqual(calls,['SPY']);self.assertEqual(db.data[m.HEAD],old)
        p,calls=resumed_publication(db);self.assertEqual(calls,['SPY','THIRD'])
    def test_malformed_prior_queue_fails_before_benchmark_request(self):
        db=Memory();p,_=resumed_publication(db,denied=None);p['acquisition_progress']['visited_occurrences']=True
        db.data[m.HEAD]=m.encode(p);old=db.data[m.HEAD];ns=native(db);calls=[]
        class LaterClock(datetime):
            @classmethod
            def now(cls,tz=None):return m.clock(p['generated_at'])+timedelta(seconds=1)
        ns['datetime']=LaterClock
        ns['_momentum_fetch']=lambda *a:calls.append(a[0])
        with self.assertRaisesRegex(ValueError,'progress counters'):ns['lambda_handler']()
        self.assertEqual(calls,[]);self.assertEqual(db.data[m.HEAD],old)


if __name__=="__main__":unittest.main(verbosity=2)
