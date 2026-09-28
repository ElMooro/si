"""Isolated deterministic price, acquisition and publication regressions."""
from pathlib import Path
from datetime import datetime,timezone,date,timedelta
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from io import BytesIO
from unittest.mock import patch
from copy import deepcopy
import ast,json,sys,time,threading,unittest,urllib.request,urllib.error,math
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-volatility-squeeze-hunter/source'
sys.path.insert(0,str(SRC))
import price_observations as m
AT='2026-09-28T01:00:00Z'


def rows(n=300):
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
    ns={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':m.HEAD,'MAX_TICKERS':1500,'N_WORKERS':12,'TIMEOUT_BUDGET_S':550,
        'FMP_KEY':'SYNTHETIC_SECRET_ONLY','time':time,'datetime':datetime,'timezone':timezone,'Path':Path,
        '__file__':str(SRC/'lambda_function.py'),'urllib':urllib,'json':json,'threading':threading,'ThreadPoolExecutor':ThreadPoolExecutor}
    ns.update({name:getattr(m,name) for name in ('CONTRACT','HEAD','FLAGS','sha','encode','strict','clock','symbol','endpoint','source_ref','validate_ref','content','universe','history','build','acquisition_plan','acquisition_progress','validate_acquisition_progress','UNATTEMPTED')})
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_price_') or n.name in ('_PriceSources','lambda_handler'))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated native price functions>','exec'),ns);return ns


def fake_fetch(ticker,sources,remaining):
    if m.symbol(ticker) is None:return {'status':'invalid_symbol_not_requested'}
    a=sources.capture(m.encode(rows()),m.endpoint(ticker),datetime.now(timezone.utc).isoformat());a['http_status']=200;return a


def publication(memory=None):
    memory=memory or Memory();ns=native(memory);ns['_price_fetch']=fake_fetch;ns['lambda_handler']()
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
    ns['_price_fetch']=fetch;ns['lambda_handler']()
    return m.strict(memory.data[m.HEAD]),calls


class Tests(unittest.TestCase):
    def test_predecessor_whole_and_original_config_not_shrunk(self):
        raw=(ROOT/'tests/fixtures/pre-volatility-volatility-squeeze-hunter.py.txt').read_bytes()
        self.assertEqual(m.sha(raw),'01b674d9d396b1432be4a3624d606e46e983d326b79b18be68924bcf3db006de')
        self.assertTrue((SRC/'lambda_function.py').read_bytes().startswith(raw.replace(b'def lambda_handler(',b'def _legacy_lambda_handler(',1)))
        db,p=publication();self.assertEqual(p['acquisition_limits'],{'MAX_TICKERS':1500,'N_WORKERS':12,'TIMEOUT_BUDGET_S':550,'active_workers':4})
    def test_latest_close_included_and_analytic_population_sigma(self):
        r=rows()
        for i in range(20):r[-20+i].update(close=100+i,open=100+i,high=121,low=99)
        a=measure(r)['measurements'];expected=4*math.sqrt(33.25)/109.5*100
        self.assertAlmostEqual(a['bb_width_20']['value'],expected,places=11)
        r[-1].update(close=130,high=131)
        b=measure(r)['measurements'];self.assertNotEqual(a['bb_width_20'],b['bb_width_20'])
        self.assertEqual(b['bb_width_cdf']['reference_count'],252)
    def test_tie_definitions_zero_denominators_and_full_200_count(self):
        r=rows()
        for x in r:x.update(high=100,low=100,volume=0)
        a=measure(r)['measurements']
        self.assertEqual(a['bb_width_cdf']['value'],100);self.assertEqual(a['bb_width_cdf']['equal'],252)
        self.assertEqual(a['nr7_30']['strict_count'],0);self.assertEqual(a['nr7_30']['tied_minimum_count'],30)
        self.assertEqual(a['inside_30']['equal_range_count'],30);self.assertEqual(a['inside_30']['strictly_contained_count'],0)
        self.assertEqual(a['trailing_closes_within_15pct']['count'],200)
        self.assertIsNone(a['volume_mean_60_over_180']['value']);self.assertIsNone(a['close_range_position_60']['value'])
        self.assertFalse(a['nominal_price_volume_30']['original_nominal_gate_passed'])
    def test_missing_annual_window_is_null_not_zero(self):
        a=measure(rows(220))['measurements'];self.assertIsNone(a['nested_high_low_range']['252']['value'])
        self.assertEqual(a['bb_width_cdf']['reference_count'],201);self.assertEqual(a['atr_cdf']['reference_count'],200)
    def test_invalid_window_cannot_be_cleaned_into_signal(self):
        for change in ({'close':True},{'open':None},{'volume':None},{'high':90},{'volume':-1},{'currency':[]},{'symbol':'OTHER'},{'date':'bad'}):
            with self.subTest(change=change):
                r=rows();r[-1].update(change);a=measure(r)
                self.assertEqual(a['status'],'unresolved_identity_or_window');self.assertIsNone(a['measurements']);self.assertTrue(a['row_issues'])
        r=rows();r[-1]['date']=r[-2]['date'];self.assertIsNone(measure(r)['measurements'])
    def test_selected_window_before_validation_and_complete_original(self):
        r=rows(340);r[-1]['close']=True;sources={};raw=m.encode(r);a=attempt(raw,sources)
        p=m.history(a,sources,'TEST',AT);self.assertEqual(len(p['selected_indices']),300);self.assertIn(339,p['selected_indices'])
        self.assertIsNone(p['measurements']);self.assertEqual(m.content(a,sources),raw);self.assertEqual(p['source_records'],340)
        r[0]['close']=True;r[-1]['close']=100;p=measure(r)
        self.assertEqual(p['status'],'parsed_completed_observations');self.assertEqual(p['row_issues'][0]['index'],0)
    def test_future_dates_and_stale_observation_clock(self):
        r=rows()
        for x in r:x['date']='2030-01-01'
        self.assertEqual(measure(r)['status'],'no_completed_observations')
        r=rows()
        for x in r:x['date']=(date.fromisoformat(x['date'])-timedelta(days=60)).isoformat()
        p=measure(r);self.assertEqual(p['latest_observation_age_calendar_days'],63);self.assertFalse(p['age_policy']['within_five_calendar_days'])
    def test_currency_and_unverified_nominal_liquidity(self):
        r=rows()
        for x in r:x['currency']='JPY'
        p=measure(r);self.assertEqual(p['currency'],'JPY');self.assertFalse(p['currency_verified'])
        self.assertEqual(p['measurements']['nominal_price_volume_30']['unit'],'provider_price_unit_times_provider_volume_unit')
        r[-1]['currency']='USD';self.assertIsNone(measure(r)['measurements'])
    def test_exact_tokens_reject_boolean_and_impossible_rounded_order(self):
        sources={};raw=m.encode(rows()).replace(b'"high":102',b'"high":99.9999999999999999999999999')
        p=m.history(attempt(raw,sources),sources,'TEST',AT);self.assertIsNone(p['measurements'])
        with self.assertRaises(ValueError):m.strict(b'{"a":1,"a":2}')
        with self.assertRaises(ValueError):m.strict(b'[NaN]')
        with self.assertRaises(ValueError):m.strict(b'[1e400]')
    def test_full_universe_and_coverage_limit_identity(self):
        sources={};ref=m.source_ref(m.encode({'stocks':[{'symbol':'TEST','cap_bucket':'small'}]*1501+[None]}));sources[ref['key']]=m.encode({'stocks':[{'symbol':'TEST','cap_bucket':'small'}]*1501+[None]})
        a={'status':'received','endpoint':'data/universe.json','original_ref':ref};p=m.universe(a,sources,1500)
        self.assertEqual(len(p['selected']),1500);self.assertEqual(len(p['occurrences']),1502)
        attempts=[{'status':'not_attempted_runtime_rate_or_size_limit','endpoint':m.endpoint('TEST')}]*1500
        b=m.build(a,attempts,sources,AT,1500);self.assertEqual(len(b['request_records']),1500);self.assertEqual(b['quality']['measured_occurrences'],0)
        with self.assertRaises(ValueError):m.build(a,attempts[:-1],sources,AT,1500)
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
        self.assertIn('data/volatility-squeeze/history/'+m.sha(db.data[m.HEAD])+'.json',db.data)
        self.assertEqual(p['quality']['measured_occurrences'],2);self.assertTrue(all(p[k] is False for k in m.FLAGS))
        self.assertEqual(p['all_qualifying'],[]);self.assertEqual(p['summary']['top_25_overall'],[])
        replay=m.build(p['universe_acquisition'],[r['acquisition'] for r in p['request_records']],db.data,p['generated_at'],1500)
        for k,v in replay.items():self.assertEqual(p[k],v)
        self.assertTrue(all(k==m.HEAD or k=='data/universe.json' or k.startswith('data/volatility-squeeze/') for k in db.reads+db.writes))
    def test_publication_failure_preserves_previous(self):
        for attr in ('corrupt','race','denied'):
            db=Memory();setattr(db,attr,True)
            with self.assertRaises((Error,ValueError)):publication(db)
            self.assertEqual(db.data[m.HEAD],db.previous)
        db=Memory();ns=native(db);ns['_price_fetch']=lambda *args:{'status':'transport_unavailable'}
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(db.data[m.HEAD],db.previous)
    def test_http_rate_limit_stops_next_request_and_retains_full_error(self):
        ns=native();sources=ns['_PriceSources']();response=urllib.error.HTTPError(m.endpoint('TEST'),429,'limited',{},BytesIO(b'{"error":"rate"}'))
        with patch.object(urllib.request,'build_opener') as opener:
            opener.return_value.open.side_effect=response;a=ns['_price_fetch']('TEST',sources,lambda:100)
            self.assertEqual(a['http_status'],429);self.assertEqual(m.content(a,sources.raw),b'{"error":"rate"}')
            b=ns['_price_fetch']('NEXT',sources,lambda:100);self.assertEqual(b['status'],'not_attempted_runtime_rate_or_size_limit');self.assertEqual(opener.return_value.open.call_count,1)
    def test_escaped_credential_echo_withheld_and_no_redirect(self):
        ns=native();sources=ns['_PriceSources']()
        response=BytesIO(br'{"error":"SYNTHETIC_\u0053ECRET_ONLY"}');response.status=200
        with patch.object(urllib.request,'build_opener') as opener:
            opener.return_value.open.return_value=response;a=ns['_price_fetch']('TEST',sources,lambda:100)
            self.assertEqual(a['status'],'credential_echo_withheld');self.assertEqual(sources.raw,{})
            self.assertIsNone(opener.call_args.args[0].redirect_request(None,None,None,None,None,None))
    def test_original_rate_limit_resumes_without_refreshing_unvisited_rows(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        first,calls=resumed_publication(db);old=db.data[m.HEAD]
        self.assertEqual(calls,['TEST','SECOND']);self.assertEqual(first['acquisition_progress']['pending_occurrences'],1)
        second,calls=resumed_publication(db)
        self.assertEqual(calls,['THIRD']);self.assertTrue(second['acquisition_progress']['cycle_complete'])
        self.assertEqual([r['acquisition']['status'] for r in second['request_records']],[m.UNATTEMPTED,m.UNATTEMPTED,'received'])
        self.assertEqual(db.data[second['previous_publication']['key']],old)
        third,calls=resumed_publication(db);self.assertEqual(calls,['TEST','SECOND'])
        self.assertEqual(third['acquisition_progress']['plan_reason'],'previous_cycle_complete_or_retired')
    def test_bootstrap_from_complete_preprogress_outcomes_and_changed_membership(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        old,_=resumed_publication(db);old.pop('acquisition_progress');old['version']='2.0.0';db.data[m.HEAD]=m.encode(old)
        p,calls=resumed_publication(db);self.assertEqual(calls,['THIRD'])
        self.assertEqual(p['acquisition_progress']['plan_reason'],'resume_prior_unattempted_occurrences')
        db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('THIRD','TEST','NEW','TEST')]})
        p,calls=resumed_publication(db)
        self.assertEqual(calls,['NEW','TEST']);self.assertEqual(p['acquisition_progress']['planned_request_indices'],[2,3])
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
        p,calls=resumed_publication(db,denied=None);self.assertEqual(calls,['TEST','TEST'])
        self.assertEqual(len(p['request_records']),3);self.assertEqual(p['request_records'][0]['acquisition']['status'],'invalid_symbol_not_requested')
        self.assertEqual(p['acquisition_progress']['planned_request_indices'],[1,2])
    def test_all_failed_remaining_window_preserves_head_and_queue(self):
        db=Memory();db.data['data/universe.json']=m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        resumed_publication(db);old=db.data[m.HEAD]
        with self.assertRaises(ValueError):resumed_publication(db,denied='THIRD')
        self.assertEqual(db.data[m.HEAD],old)


if __name__=='__main__':unittest.main()
