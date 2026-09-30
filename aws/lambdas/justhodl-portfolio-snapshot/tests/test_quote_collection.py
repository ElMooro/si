"""Complete invented workloads; no provider requests or actual account reads."""
from pathlib import Path
from concurrent.futures import Future
from unittest.mock import patch
import ast,base64,contextlib,copy,gzip,hashlib,io,json,threading,types,unittest
from test_watchlist_sync import load_current
import test_quote_read as quote_support
ROOT=Path(__file__).resolve().parents[4]


def quote(symbol,size=600):
    value=quote_support.frame(symbol);value['complete_invented_padding']=''
    raw=json.dumps(value,separators=(',',':')).encode();assert len(raw)<=size
    raw+=b' '*(size-len(raw))
    return {'price':110,'open':100,'high':115,'low':99,'volume':0,'as_of_unix_ms':value['results'][0]['t'],
        'price_basis':'SPLIT_ADJUSTED_PREVIOUS_DAY_CLOSE','price_timestamp_basis':'AGGREGATE_WINDOW_START_UTC_MS','currency':None,
        'source_evidence':{'schema_version':'previous-close-source.v1','requested_symbol':symbol,'request_attempted':True,
            'status':'MEASURED_PREVIOUS_CLOSE','reason_code':None,'body_complete':True,'body_bytes':len(raw),
            'body_encoding':'base64','body_sha256':hashlib.sha256(raw).hexdigest(),'body':base64.b64encode(raw).decode(),
            'currency_verified':False,'instrument_binding_verified':False,'execution_quote':False}}


class ControlledCollection:
    def __init__(self,mod,fetch=None):
        self.mod=mod;self.clock=0.;self.submitted=[];self.ran=[];self.wait_sizes=[];self.pending=[];self.closed=False
        self.fetch=fetch or (lambda symbol,**kw:quote(symbol));self.input_options=[]
    def run_future(self,future):
        fn,args,kwargs=future.work;self.ran.append(args[0])
        try:future.set_result(fn(*args,**kwargs))
        except Exception as error:future.set_exception(error)
    def executor(self,**options):
        owner=self;owner.input_options.append(options)
        class Executor:
            def __enter__(self):return self
            def __exit__(self,*args):
                for future in owner.pending:
                    if not future.done():owner.run_future(future)
                owner.closed=True
            def submit(self,fn,*args,**kwargs):
                f=Future();f.work=(fn,args,kwargs);owner.pending.append(f);owner.submitted.append(args[0]);return f
        return Executor()
    def wait(self,futures,**kw):
        self.wait_sizes.append(len(futures));future=futures[-1];self.run_future(future)
        return {future},set(futures)-{future}
    def run(self,symbols,**kw):
        with patch.object(self.mod,'ThreadPoolExecutor',self.executor),patch.object(self.mod,'wait',self.wait),patch.object(self.mod,'fetch_polygon_latest',self.fetch),patch.object(self.mod.time,'monotonic',side_effect=lambda:self.clock):
            return self.mod.batch_fetch_prices(symbols,**kw)


class QuoteCollectionTests(unittest.TestCase):
    def setUp(self):self.mod=load_current();self.control=ControlledCollection(self.mod)
    def test_complete_original_predecessor_is_inert_and_hash_bound(self):
        audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-quote-collection.json').read_bytes())
        for row in audit['fixtures'].values():
            raw=(ROOT/row['path']).read_bytes();self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        data=json.loads(gzip.decompress((ROOT/'tests/fixtures/pre-snapshot-quote-collection/complete-synthetic.json.gz').read_bytes()))
        self.assertEqual(len(data['cases']),3);self.assertEqual(data['cases'][0]['submitted_before_first_completion'],[41]);self.assertGreater(data['cases'][0]['retained_complete_body_bytes'],4*1024*1024);self.assertEqual(data['cases'][1]['elapsed_mock_seconds'],552)
        self.assertTrue(all(value is None for value in data['cases'][2]['output'].values()))
    def test_only_collector_reader_and_handler_change_from_exact_predecessor(self):
        def functions(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
        before=functions(ROOT/'tests/fixtures/pre-snapshot-quote-collection/lambda_function.py.txt');after=functions(ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py')
        self.assertEqual(set(after),set(before));self.assertEqual({key for key in before if before[key]!=after[key]},{'batch_fetch_prices','fetch_polygon_latest','lambda_handler'})
    def test_empty_workload_has_explicit_complete_zero_without_executor(self):
        result=self.control.run([]);self.assertEqual(result,{});self.assertEqual(self.control.input_options,[])
        trace=result.collection_evidence;self.assertEqual(trace['status'],'COMPLETE_ATTEMPT_COVERAGE');self.assertEqual(trace['tasks_started'],0);self.assertEqual(trace['retained_complete_body_bytes'],0);self.assertFalse(trace['allows_sizing'])
    def test_unsorted_duplicates_deduplicate_requests_but_report_original_count(self):
        symbols=['CCC','AAA','BBB','AAA'];before=list(symbols);result=self.control.run(symbols)
        self.assertEqual(symbols,before);self.assertEqual(list(result),['AAA','BBB','CCC']);self.assertEqual(self.control.submitted,list(result));self.assertEqual(result.collection_evidence['requested_occurrences_count'],4);self.assertEqual(result.collection_evidence['unique_requested_count'],3)
    def test_pending_queue_never_exceeds_workers_and_all_symbols_survive(self):
        symbols=['S'+str(i).zfill(4) for i in range(105)];result=self.control.run(symbols,max_workers=3)
        self.assertEqual(list(result),symbols);self.assertEqual(self.control.wait_sizes[0],3);self.assertLessEqual(max(self.control.wait_sizes),3);self.assertEqual(self.control.submitted,symbols);self.assertTrue(self.control.closed);self.assertTrue(all(f.done() for f in self.control.pending));self.assertEqual(result.collection_evidence['tasks_started'],105)
    def test_exact_four_mib_retained_originals_and_unattempted_tail(self):
        self.control.fetch=lambda symbol,**kw:quote(symbol,self.mod.PREVIOUS_CLOSE_MAX_BYTES)
        symbols=['S'+str(i).zfill(4) for i in range(41)];result=self.control.run(symbols)
        self.assertEqual(list(result),symbols);self.assertEqual(len(self.control.submitted),32);trace=result.collection_evidence
        self.assertEqual(trace['retained_complete_body_bytes'],4*1024*1024);self.assertEqual(trace['unattempted_count'],9);self.assertEqual(trace['status'],'PARTIAL_ATTEMPT_COVERAGE')
        for symbol in symbols[:32]:
            source=result[symbol]['source_evidence'];raw=base64.b64decode(source['body']);self.assertEqual(len(raw),128*1024);self.assertEqual(hashlib.sha256(raw).hexdigest(),source['body_sha256']);self.assertEqual(json.loads(raw)['ticker'],symbol)
        for symbol in symbols[32:]:
            self.assertIsNone(result[symbol]['price']);self.assertEqual(result[symbol]['source_evidence']['reason_code'],'COLLECTION_BODY_RESERVATION_EXHAUSTED');self.assertFalse(result[symbol]['source_evidence']['request_attempted']);self.assertNotIn('body',result[symbol]['source_evidence'])
    def test_short_complete_bodies_release_unused_reservations(self):
        self.mod.QUOTE_COLLECTION_MAX_BODY_BYTES=2*self.mod.PREVIOUS_CLOSE_MAX_BYTES
        result=self.control.run(['A','B','C','D','E']);self.assertEqual(len(self.control.submitted),5);self.assertEqual(result.collection_evidence['retained_complete_body_bytes'],3000);self.assertLessEqual(max(self.control.wait_sizes),2)
    def test_insufficient_room_for_one_full_body_starts_no_requests(self):
        self.mod.QUOTE_COLLECTION_MAX_BODY_BYTES=self.mod.PREVIOUS_CLOSE_MAX_BYTES-1
        result=self.control.run(['AAA']);self.assertEqual(self.control.submitted,[]);self.assertEqual(result['AAA']['source_evidence']['reason_code'],'COLLECTION_BODY_RESERVATION_EXHAUSTED');self.assertTrue(self.control.closed)
    def test_deadline_stops_submission_drains_jobs_and_withholds_late_marks(self):
        def fetch(symbol,**kw):self.assertEqual(kw['collection_deadline'],45);self.control.clock=46;return quote(symbol)
        self.control.fetch=fetch;result=self.control.run(['A','B','C','D','E'],max_workers=2)
        self.assertEqual(self.control.submitted,['A','B']);self.assertEqual(set(self.control.ran),{'A','B'});self.assertTrue(self.control.closed)
        for symbol in ('A','B'):
            self.assertIsNone(result[symbol]['price']);source=result[symbol]['source_evidence'];self.assertTrue(source['body_complete']);self.assertTrue(source['collection_deadline_exceeded']);self.assertEqual(source['received_source_status'],'MEASURED_PREVIOUS_CLOSE')
        for symbol in ('C','D','E'):self.assertEqual(result[symbol]['source_evidence']['status'],'NOT_ATTEMPTED')
        self.assertEqual(result.collection_evidence['retained_complete_body_bytes'],1200);self.assertEqual(result.collection_evidence['measured_previous_close_count'],0)
    def test_native_remaining_time_reserves_thirty_seconds(self):
        context=types.SimpleNamespace(get_remaining_time_in_millis=lambda:35000)
        deadlines=[];self.control.fetch=lambda symbol,**kw:(deadlines.append(kw['collection_deadline']) or quote(symbol))
        result=self.control.run(['AAA'],context=context);self.assertEqual(deadlines,[5]);self.assertEqual(result.collection_evidence['acceptance_seconds'],5);self.assertEqual(result.collection_evidence['context_budget_status'],'REMAINING_TIME_RESERVE_APPLIED')
    def test_exhausted_native_budget_starts_nothing_even_for_full_identity_population(self):
        symbols=['S'+str(i).zfill(5) for i in range(2*self.mod.BOOK_MAX_ROWS)]
        result=self.control.run(symbols,context=types.SimpleNamespace(get_remaining_time_in_millis=lambda:30000))
        self.assertEqual(list(result),symbols);self.assertEqual(result.collection_evidence['unattempted_count'],len(symbols));self.assertEqual(self.control.input_options,[])
    def test_invalid_native_budget_fails_closed_without_provider_requests(self):
        for value in (None,True,'180000',float('nan'),float('inf'),-1,10**400):
            with self.subTest(value=type(value).__name__):
                result=self.control.run(['AAA'],context=types.SimpleNamespace(get_remaining_time_in_millis=lambda:value));self.assertEqual(result.collection_evidence['acceptance_seconds'],0);self.assertEqual(result.collection_evidence['context_budget_status'],'INVALID_REMAINING_TIME_NO_REQUESTS')
        self.assertEqual(self.control.submitted,[])
    def test_native_budget_does_not_increase_shared_maximum(self):
        result=self.control.run(['AAA'],context=types.SimpleNamespace(get_remaining_time_in_millis=lambda:180000));self.assertEqual(result.collection_evidence['acceptance_seconds'],45)
    def test_invalid_or_excess_identity_population_is_rejected_whole(self):
        for symbols in (None,('AAA',),['AAA',True],['AAA','aaa'],['AAA','A/B'],['AAA']*(2*self.mod.BOOK_MAX_ROWS+1)):
            with self.assertRaisesRegex(ValueError,'identity list'):self.control.run(symbols)
        self.assertEqual(self.control.submitted,[])
    def test_invalid_worker_settings_are_not_coerced(self):
        for value in (True,0,11,1.0,'2',None):
            with self.assertRaisesRegex(ValueError,'concurrency'):self.control.run(['AAA'],max_workers=value)
        self.assertEqual(self.control.submitted,[])
    def test_failed_task_preserves_identity_fixed_reason_and_no_exception_text(self):
        def fail(symbol,**kw):raise RuntimeError('INVENTED_SECRET_DETAIL')
        self.control.fetch=fail;result=self.control.run(['AAA','BBB']);self.assertEqual(result.collection_evidence['reason_counts'],{'COLLECTION_TASK_FAILED':2});self.assertNotIn('INVENTED_SECRET_DETAIL',json.dumps(result))
        for symbol,value in result.items():self.assertEqual(value['source_evidence']['requested_symbol'],symbol);self.assertIsNone(value['price']);self.assertIsNone(value['source_evidence']['request_attempted']);self.assertTrue(value['source_evidence']['collection_task_started'])
    def test_wrong_task_identity_refuses_complete_collection_and_drains(self):
        self.control.fetch=lambda symbol,**kw:quote('OTHER')
        with self.assertRaisesRegex(ValueError,'identity differs'):self.control.run(['AAA','BBB'])
        self.assertTrue(self.control.closed);self.assertTrue(all(f.done() for f in self.control.pending))
    def test_inconsistent_complete_body_reservation_is_not_silently_dropped(self):
        for change in ({'body_bytes':True},{'body_bytes':128*1024+1},{'body':'short'},{'body_bytes':-1}):
            def fetch(symbol,**kw):result=quote(symbol);result['source_evidence'].update(change);return result
            self.control.fetch=fetch
            with self.assertRaisesRegex(ValueError,'body reservation'):self.control.run(['AAA'])
    def test_current_reader_does_not_open_after_shared_deadline(self):
        self.mod.POLY_KEY='INVENTED_SECRET_CANARY'
        with patch.object(self.mod.time,'monotonic',return_value=45),patch.object(self.mod.urllib.request,'build_opener') as opener:result=self.mod.fetch_polygon_latest('AAA',collection_deadline=45)
        opener.assert_not_called();self.assertFalse(result['source_evidence']['request_attempted']);self.assertEqual(result['source_evidence']['reason_code'],'COLLECTION_ACCEPTANCE_DEADLINE')
    def test_current_reader_caps_socket_timeout_by_shared_remaining_time(self):
        self.mod.POLY_KEY='INVENTED_SECRET_CANARY';seen=[]
        def open_request(request,**kw):seen.append(kw);return quote_support.Response(json.dumps(quote_support.frame()).encode(),url=request.full_url)
        with patch.object(self.mod.time,'monotonic',return_value=0),patch.object(self.mod.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=open_request)):result=self.mod.fetch_polygon_latest('AAA',collection_deadline=2.5)
        self.assertEqual(seen,[{'timeout':2.5}]);self.assertEqual(result['price'],110)
    def test_deadline_during_parse_keeps_complete_original_without_usable_mark(self):
        self.mod.POLY_KEY='INVENTED_SECRET_CANARY';clock=[0];parse=self.mod.parse_previous_close
        def parse_late(raw,symbol):result=parse(raw,symbol);clock[0]=45;return result
        def open_request(request,**kw):return quote_support.Response(json.dumps(quote_support.frame()).encode(),url=request.full_url)
        with patch.object(self.mod.time,'monotonic',side_effect=lambda:clock[0]),patch.object(self.mod,'parse_previous_close',parse_late),patch.object(self.mod.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=open_request)):result=self.mod.fetch_polygon_latest('AAA',collection_deadline=45)
        self.assertIsNone(result['price']);self.assertTrue(result['source_evidence']['body_complete']);self.assertEqual(result['source_evidence']['reason_code'],'COLLECTION_ACCEPTANCE_DEADLINE')
    def test_real_thread_pool_respects_concurrency_and_finishes_every_started_task(self):
        barrier=threading.Barrier(3,timeout=2);lock=threading.Lock();active=[0];peak=[0];completed=[]
        def fetch(symbol,**kw):
            with lock:active[0]+=1;peak[0]=max(peak[0],active[0])
            barrier.wait()
            with lock:active[0]-=1;completed.append(symbol)
            return quote(symbol)
        with patch.object(self.mod,'fetch_polygon_latest',fetch):result=self.mod.batch_fetch_prices(['A','B','C','D','E','F'],max_workers=3)
        self.assertEqual(peak[0],3);self.assertEqual(active[0],0);self.assertEqual(set(completed),set(result));self.assertEqual(result.collection_evidence['measured_previous_close_count'],6)
    def test_current_handler_keeps_partial_collection_and_every_book_row(self):
        self.mod.load_s3_json=lambda key,default:copy.deepcopy(default)
        self.mod.sync_auto_watchlist=lambda _:{'added_S':[],'added_A':[],'removed_S':[],'removed_A':[]}
        positions=[{'symbol':'AAA','qty':10,'cost_basis_per_share':100}];watch=[{'symbol':'BBB','source':'MANUAL'},{'symbol':'AAA','source':'MANUAL'}]
        self.mod.query_pk=lambda pk:copy.deepcopy(positions if pk=='POSITION' else watch);writes=[]
        self.mod.publish_private=lambda kind,payload:writes.append(copy.deepcopy(payload));self.mod.s3.put_object=lambda **kw:None
        with patch.object(self.mod.urllib.request,'build_opener') as opener,contextlib.redirect_stdout(io.StringIO()):result=self.mod.lambda_handler({},types.SimpleNamespace(get_remaining_time_in_millis=lambda:1))
        opener.assert_not_called();self.assertEqual(result['statusCode'],200);payload=writes[0]
        self.assertEqual(len(payload['positions']),1);self.assertEqual(len(payload['watchlist']),2);self.assertEqual(set(payload['accounting']['source_prices']),{'AAA','BBB'});self.assertEqual(payload['accounting']['quote_collection']['unattempted_count'],2);self.assertIsNone(payload['positions'][0]['market_value']);self.assertFalse(payload['capital_book']['allows_new_entries'])


if __name__=='__main__':unittest.main()
