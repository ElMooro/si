"""Complete invented provider responses; current reader and handler, no HTTP I/O."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
import ast,base64,contextlib,copy,hashlib,io,json,types,unittest,urllib.error
from test_watchlist_sync import load_current
ROOT=Path(__file__).resolve().parents[4]
NOW=datetime(2026,9,30,8,tzinfo=timezone.utc)
def frame(symbol='AAA'):
    return {'ticker':symbol,'adjusted':True,'status':'OK','queryCount':1,'resultsCount':1,'request_id':'invented-request',
            'extra':{'keep':'complete original source'},'results':[{'T':symbol,'o':100,'h':115,'l':99,'c':110,'v':0,'t':int((NOW-timedelta(days=1)).timestamp()*1000)}]}
class Response(io.BytesIO):
    def __init__(self,raw,headers=None,status=200,url=None):
        super().__init__(raw);self.status=status;self.url=url;self.calls=[]
        self.headers={'Content-Type':'application/json','Content-Length':str(len(raw))} if headers is None else headers
    def read(self,*args):self.calls.append(args);return super().read(*args)
    def geturl(self):return self.url
    def __enter__(self):return self
    def __exit__(self,*args):self.close()
class QuoteReads(unittest.TestCase):
    def setUp(self):self.mod=load_current();self.mod.POLY_KEY='INVENTED_SECRET_CANARY';self.requests=[]
    def parsed(self,data=None,raw=None,symbol='AAA'):
        return self.mod.parse_previous_close(raw if raw is not None else json.dumps(frame() if data is None else data).encode(),symbol)
    def fetch(self,data=None,raw=None,headers=None,status=200,url=None,error=None,symbol='AAA',response=None):
        response=response or Response(raw if raw is not None else json.dumps(frame() if data is None else data).encode(),headers,status,url)
        self.response=response
        def open_request(request,**kw):
            self.requests.append({'url':request.full_url,'headers':dict(request.header_items()),'options':kw})
            if error:raise error
            if response.url is None:response.url=request.full_url
            return response
        self.output=io.StringIO()
        with patch.object(self.mod.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=open_request)) as build,contextlib.redirect_stdout(self.output):
            result=self.mod.fetch_polygon_latest(symbol)
        if self.requests:self.assertIsInstance(build.call_args.args[0],self.mod.NoQuoteRedirect)
        return result
    def test_complete_original_fixture_hashes_and_invented_canary(self):
        audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-quote-read-integrity.json').read_bytes())
        for row in audit['fixtures'].values():
            raw=(ROOT/row['path']).read_bytes();self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        data=json.loads((ROOT/'tests/fixtures/pre-snapshot-quote-read/complete-synthetic.json').read_bytes());self.assertEqual(len(data['cases']),8);self.assertIn('INVENTED_SECRET_CANARY',data['cases'][-1]['captured_log'])
    def test_only_quote_related_functions_change_from_predecessor(self):
        def functions(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
        before=functions(ROOT/'tests/fixtures/pre-snapshot-quote-read/lambda_function.py.txt');after=functions(ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py')
        self.assertEqual(set(after)-set(before),{'parse_previous_close','read_previous_close_body','research_text','research_join','research_value','validate_snapshot_publication'});self.assertEqual({k for k in before if before[k]!=after[k]},{'fetch_polygon_latest','enrich_symbol','lambda_handler','load_s3_json','index_by_symbol','batch_fetch_prices','build_holdings_accounting'})
    def test_one_adjusted_previous_day_bar_has_explicit_time_basis(self):
        result=self.parsed();self.assertEqual(result['price'],110);self.assertEqual(result['volume'],0);self.assertEqual(result['price_basis'],'SPLIT_ADJUSTED_PREVIOUS_DAY_CLOSE');self.assertEqual(result['price_timestamp_basis'],'AGGREGATE_WINDOW_START_UTC_MS');self.assertIsNone(result['currency'])
    def test_ticker_mismatch_in_envelope_or_bar_is_ineligible(self):
        for data in ({**frame(),'ticker':'BBB'},{**frame(),'ticker':True},{**frame(),'results':[{**frame()['results'][0],'T':'BBB'}]}):
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed(data)
    def test_unknown_or_unadjusted_price_basis_is_ineligible(self):
        for value in (False,None,'true',1):
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed({**frame(),'adjusted':value})
    def test_error_and_unknown_provider_status_cannot_supply_marks(self):
        for value in ('ERROR','NOT_AUTHORIZED','DELAYED',None,True):
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed({**frame(),'status':value})
    def test_counts_must_describe_exactly_one_complete_bar(self):
        for field in ('queryCount','resultsCount'):
            for value in (None,True,'1',1.0,0,2):
                with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed({**frame(),field:value})
        for rows in ([],None,{},[frame()['results'][0]]*2):
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed({**frame(),'results':rows})
    def test_duplicate_keys_at_every_depth_are_refused(self):
        for raw in (json.dumps(frame()).replace('"c": 110','"c": 110, "c": 999').encode(),json.dumps(frame()).replace('"ticker": "AAA"','"ticker": "AAA", "ticker": "AAA"').encode()):
            with self.assertRaisesRegex(self.mod.PreviousCloseUnavailable,'DUPLICATE_JSON_FIELD'):self.parsed(raw=raw)
    def test_complete_json_rejects_nonfinite_unicode_and_nonobjects(self):
        for raw in (b'[]',b'null',b'{"x":NaN}',b'{"x":1e999}',b'{"x":"\\ud800"}',b'\xff',b'{}junk'):
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed(raw=raw)
    def test_ohlc_and_volume_require_finite_typed_numbers(self):
        for field in ('c','o','h','l','v'):
            for value in (True,'100',None,float('inf'),10**400,-1):
                row={**frame()['results'][0],field:value}
                with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed({**frame(),'results':[row]})
        for field in ('c','o','h','l'):
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed({**frame(),'results':[{**frame()['results'][0],field:0}]})
    def test_incoherent_ohlc_ranges_are_not_accepted(self):
        for change in ({'l':111},{'h':109},{'h':95,'l':96}):
            with self.assertRaisesRegex(self.mod.PreviousCloseUnavailable,'INCOHERENT_OHLC_RANGE'):self.parsed({**frame(),'results':[{**frame()['results'][0],**change}]})
    def test_timestamp_is_exact_integer_window_start_not_acquisition_time(self):
        for value in (True,'1790640000000',1.5,None,-1,2**53):
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed({**frame(),'results':[{**frame()['results'][0],'t':value}]})
        zero={**frame(),'results':[{**frame()['results'][0],'t':0}]};self.assertEqual(self.parsed(zero)['as_of_unix_ms'],0)
    def test_optional_bar_ticker_can_use_identified_envelope(self):
        data=frame();data['results'][0].pop('T');self.assertEqual(self.parsed(data)['price'],110)
    def test_complete_body_is_retained_exactly_with_hash_and_fixed_origin(self):
        raw=json.dumps(frame(),indent=2).encode();result=self.fetch(raw=raw);source=result['source_evidence']
        self.assertEqual(result['price'],110);self.assertEqual(source['status'],'MEASURED_PREVIOUS_CLOSE');self.assertTrue(source['body_complete']);self.assertEqual(base64.b64decode(source['body']),raw);self.assertEqual(source['body_sha256'],hashlib.sha256(raw).hexdigest());self.assertEqual(source['body_bytes'],len(raw));self.assertTrue(self.response.closed)
        self.assertFalse(source['execution_quote']);self.assertFalse(source['instrument_binding_verified']);self.assertNotIn('INVENTED_SECRET_CANARY',json.dumps(result));self.assertEqual(self.requests[0]['headers']['Accept-encoding'],'identity')
    def test_wrong_length_never_claims_a_complete_body_or_mark(self):
        for length in ('1','5000','1, 1',True,'-1'):
            result=self.fetch(headers={'Content-Type':'application/json','Content-Length':length});self.assertIsNone(result['price']);self.assertFalse(result['source_evidence']['body_complete']);self.assertNotIn('body',result['source_evidence']);self.assertTrue(self.response.closed)
    def test_unknown_length_still_requires_actual_eof(self):
        result=self.fetch(headers={'Content-Type':'application/json'});self.assertEqual(result['price'],110);self.assertGreaterEqual(len(self.response.calls),2);self.assertTrue(all(len(c)==1 and c[0]>0 for c in self.response.calls))
    def test_byte_bound_withholds_instead_of_parsing_a_prefix(self):
        self.assertEqual(self.mod.PREVIOUS_CLOSE_MAX_BYTES,128*1024)
        raw=b' '*(self.mod.PREVIOUS_CLOSE_MAX_BYTES+1)
        result=self.fetch(raw=raw,headers={'Content-Type':'application/json'});self.assertEqual(result['source_evidence']['reason_code'],'RESPONSE_BYTE_BOUND');self.assertFalse(result['source_evidence']['body_complete']);self.assertIsNone(result['price']);self.assertTrue(self.response.closed)
    def test_http_status_content_type_and_encoding_are_checked(self):
        for args in ({'status':201},{'status':True},{'headers':{'Content-Type':'text/html'}},{'headers':{'Content-Type':'application/json','Content-Encoding':'gzip'}}):
            result=self.fetch(**args);self.assertIsNone(result['price']);self.assertFalse(result['source_evidence']['body_complete']);self.assertTrue(self.response.closed)
    def test_response_origin_path_and_query_are_bound(self):
        for url in ('https://invented-other.invalid/body','http://api.polygon.io/v2/aggs/ticker/AAA/prev','https://api.polygon.io/v2/aggs/ticker/BBB/prev','https://api.polygon.io/v2/aggs/ticker/AAA/prev?adjusted=false'):
            result=self.fetch(url=url);self.assertEqual(result['source_evidence']['reason_code'],'RESPONSE_URL_MISMATCH');self.assertTrue(self.response.closed);self.assertEqual(self.response.calls,[])
    def test_redirect_is_refused_before_another_request_and_closes_stream(self):
        body=io.BytesIO(b'invented redirect body');handler=self.mod.NoQuoteRedirect()
        with self.assertRaisesRegex(self.mod.PreviousCloseUnavailable,'REDIRECT_REFUSED'):handler.redirect_request(None,body,302,'Found',{},'https://invented-other.invalid')
        self.assertTrue(body.closed)
    def test_response_deadline_is_checked_after_read(self):
        raw=json.dumps(frame()).encode();response=Response(raw)
        with patch.object(self.mod.time,'monotonic',side_effect=[0,21]):
            with self.assertRaisesRegex(self.mod.PreviousCloseUnavailable,'READ_DEADLINE'):self.mod.read_previous_close_body(response,20)
    def test_complete_invalid_json_is_retained_but_cannot_supply_a_mark(self):
        raw=b'{"ticker":"AAA","ticker":"BBB"}';result=self.fetch(raw=raw);source=result['source_evidence']
        self.assertIsNone(result['price']);self.assertEqual(source['reason_code'],'DUPLICATE_JSON_FIELD');self.assertTrue(source['body_complete']);self.assertEqual(base64.b64decode(source['body']),raw)
    def test_transport_errors_do_not_echo_urls_secrets_or_body(self):
        for failure in (urllib.error.URLError('apiKey=INVENTED_SECRET_CANARY'),RuntimeError('invented private detail')):
            result=self.fetch(error=failure);self.assertIsNone(result['price']);self.assertEqual(self.output.getvalue(),'');self.assertNotIn('INVENTED_SECRET_CANARY',json.dumps(result));self.assertEqual(result['source_evidence']['reason_code'],'TRANSPORT_OR_SOURCE_UNAVAILABLE')
    def test_http_error_retains_status_only_and_closes_body(self):
        body=io.BytesIO(b'INVENTED_SECRET_CANARY');failure=urllib.error.HTTPError('https://api.polygon.io/?apiKey=INVENTED_SECRET_CANARY',429,'invented details',{},body)
        result=self.fetch(error=failure);self.assertEqual(result['source_evidence']['http_status'],429);self.assertEqual(result['source_evidence']['reason_code'],'HTTP_ERROR');self.assertTrue(body.closed);self.assertNotIn('INVENTED_SECRET_CANARY',json.dumps(result));self.assertEqual(self.output.getvalue(),'')
    def test_no_key_or_invalid_symbol_never_opens_a_connection(self):
        self.mod.POLY_KEY='';result=self.fetch();self.assertEqual(result['source_evidence']['reason_code'],'PROVIDER_UNCONFIGURED');self.assertEqual(self.requests,[])
        self.mod.POLY_KEY='INVENTED_SECRET_CANARY'
        for symbol in (None,True,'A/B','aaa','A A'):
            result=self.fetch(symbol=symbol);self.assertEqual(result['source_evidence']['reason_code'],'UNSUPPORTED_REQUEST_IDENTITY')
            with self.assertRaises(self.mod.PreviousCloseUnavailable):self.parsed(symbol=symbol)
        self.assertEqual(self.requests,[])
    def test_interrupted_body_is_closed_and_not_labeled_complete(self):
        class Interrupted(Response):
            def read(self,*args):raise OSError('invented incomplete body')
        response=Interrupted(json.dumps(frame()).encode());result=self.fetch(response=response);self.assertIsNone(result['price']);self.assertFalse(result['source_evidence']['body_complete']);self.assertTrue(response.closed)
    def test_current_enrichment_exposes_selected_source_trace_without_body_duplication(self):
        quote=self.fetch();result=self.mod.enrich_symbol('AAA',{'AAA':quote},{},{},{},{},{},{})
        self.assertEqual(result['price_basis'],quote['price_basis']);self.assertEqual(result['price_source']['body_sha256'],quote['source_evidence']['body_sha256']);self.assertEqual(result['price_source']['requested_symbol'],'AAA');self.assertNotIn('body',result['price_source'])
    def test_current_handler_retains_full_quote_source_and_excludes_invalid_mark(self):
        from datetime import datetime as RealDateTime
        class FrozenDateTime(RealDateTime):
            @classmethod
            def now(cls,tz=None):return NOW if tz else NOW.replace(tzinfo=None)
        self.mod.load_s3_json=lambda key,default:copy.deepcopy(default)
        self.mod.sync_auto_watchlist=lambda _:{'added_S':[],'added_A':[],'removed_S':[],'removed_A':[]}
        positions=[{'symbol':'AAA','qty':10,'cost_basis_per_share':100},{'symbol':'BBB','qty':5,'cost_basis_per_share':100}]
        self.mod.query_pk=lambda pk:copy.deepcopy(positions) if pk=='POSITION' else []
        writes=[];self.mod.publish_private=lambda kind,payload:writes.append(copy.deepcopy(payload));self.mod.s3.put_object=lambda **kw:None
        def open_request(request,**kw):
            symbol='AAA' if '/AAA/' in request.full_url else 'BBB';data=frame(symbol)
            if symbol=='BBB':data['ticker']='OTHER'
            return Response(json.dumps(data).encode(),url=request.full_url)
        with patch.object(self.mod.urllib.request,'build_opener',return_value=types.SimpleNamespace(open=open_request)),patch.object(self.mod,'datetime',FrozenDateTime),patch.object(self.mod.time,'time',return_value=NOW.timestamp()),contextlib.redirect_stdout(io.StringIO()):result=self.mod.lambda_handler({},None)
        self.assertEqual(result['statusCode'],200);payload=writes[0];sources=payload['accounting']['source_prices'];self.assertEqual(set(sources),{'AAA','BBB'});self.assertEqual(sources['BBB']['source_evidence']['reason_code'],'RESPONSE_TICKER_MISMATCH');self.assertEqual(payload['portfolio_summary']['priced_positions_count'],1);self.assertIsNone(payload['positions'][1]['market_value']);self.assertFalse(payload['capital_book']['allows_new_entries'])

if __name__=='__main__':unittest.main()
