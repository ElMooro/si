"""Synthetic-only statement, acquisition and conditional-publication regressions."""
from pathlib import Path
from datetime import datetime,timezone,date,timedelta
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote_plus
import ast,base64,hashlib,json,math,sys,time,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-earnings-pead/source';sys.path.insert(0,str(SRC))
from pead_observations import CONTRACT,strict,clock,number,symbol,envelope,original,universe,dossier,earnings_event,event_differences,price_coverage
TODAY='2026-09-27'


def events():
    return [{'symbol':'TEST','cik':'1','reportedCurrency':'JPY','date':announced,'fiscalDateEnding':end,'period':period,'fiscalYear':year,
        'epsBasis':'diluted_gaap','epsActual':actual,'epsEstimated':1,'revenueActual':200,'revenueEstimated':100,'unknown':'retain'}
        for announced,end,period,year,actual in [('2026-08-01','2026-06-30','Q2',2026,2),('2026-05-01','2026-03-31','Q1',2026,1),
        ('2026-02-01','2025-12-31','Q4',2025,-1),('2025-11-01','2025-09-30','Q3',2025,0)]]


def capture(values=None,endpoint='earnings',stamp=TODAY+'T01:00:00Z'):
    return envelope(json.dumps(events() if values is None else values).encode(),endpoint,stamp)


def member(ticker='TEST'):
    return {'source_index':0,'ticker':ticker,'raw':{'symbol':ticker,'cap_bucket':'large','market_cap':1e9}}


def model(values):return dossier(member(),[capture(values)],TODAY)


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"schema_version":1,"all_qualifying":[{"symbol":"OLD"}]}'
        self.data={'data/earnings-pead.json':self.previous,'data/universe.json':json.dumps({'stocks':[member()['raw'],member()['raw'],None,{'symbol':'NEXT','cap_bucket':'small'}]}).encode()}
        self.reads=[];self.writes=[];self.denied=False;self.corrupt=False;self.race=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'corrupt' if self.corrupt and '/history/' in key else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key=='data/earnings-pead.json' and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    scope={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':'data/earnings-pead.json','FMP_KEY':'synthetic-fixture-secret',
        'N_WORKERS':6,'MAX_TICKERS':3,'TIMEOUT_BUDGET_S':260,'CONTRACT':CONTRACT,'strict':strict,'clock':clock,'number':number,'symbol':symbol,
        'envelope':envelope,'original':original,'universe':universe,'dossier':dossier,'datetime':datetime,'timezone':timezone,
        'ThreadPoolExecutor':ThreadPoolExecutor,'urllib':urllib,'quote_plus':quote_plus,'hashlib':hashlib,'json':json,'time':time,'math':math,
        'Path':Path,'__file__':str(SRC/'lambda_function.py'),**extra}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());fns=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_pead_') or n.name=='lambda_handler')]
    exec(compile(ast.Module(body=fns,type_ignores=[]),'<isolated active revenue functions>','exec'),scope);return scope


def company(m):
    values=events()
    for row in values:row['symbol']=m['ticker']
    return [capture(values,stamp=datetime.now(timezone.utc).isoformat()),capture([{'symbol':m['ticker'],'date':'2026-08-01','close':10},{'symbol':m['ticker'],'date':'2026-09-25','close':20}], 'historical-price-eod/full',datetime.now(timezone.utc).isoformat())]


class Tests(unittest.TestCase):
    def test_complete_native_predecessor_is_retained_above_active_handler(self):
        original=(ROOT/'tests/fixtures/pre-earnings-pead-observations.py.txt').read_bytes()
        self.assertEqual(hashlib.sha256(original).hexdigest(),'c7d92d604f574f14e217a715bc9aff89606031461f6b31cca58fc8d0fce257a8')
        self.assertTrue((SRC/'lambda_function.py').read_bytes().startswith(original.replace(b'def lambda_handler(',b'def _legacy_lambda_handler(',1)))

    def test_descriptive_differences_are_not_surprises_streaks_or_returns(self):
        out=model(events());d=out['event_differences'][0]
        self.assertEqual(d['differences']['eps']['actual_minus_reported_estimate'],1)
        self.assertEqual(d['differences']['eps']['pct_of_absolute_estimate'],100)
        self.assertEqual(d['differences']['revenue']['pct_of_absolute_estimate'],100)
        for key in ('earnings_surprise','beat_streak','post_earnings_return_pct'):self.assertIsNone(d[key])
        self.assertFalse(out['calls_eligible']);self.assertFalse(out['event_observations'][0]['preannouncement_consensus_verified'])
    def test_duplicate_annual_wrong_issuer_and_future_records_never_qualify(self):
        for edit in [{},{'symbol':'OTHER'},{'period':'FY'},{'date':'2027-01-01'}]:
            values=[{**events()[0],**edit} for _ in range(4)];out=model(values)
            self.assertEqual(len(out['event_observations']),4)
            self.assertTrue(all(d['differences']['eps']['actual_minus_reported_estimate'] is None for d in out['event_differences']))
            self.assertTrue(all(d['beat_streak'] is None for d in out['event_differences']))
    def test_zero_missing_alias_conflicts_boolean_and_negative_baselines(self):
        for actual,extra,expected in [(0,{},-1),(None,{},None),(True,{},None),(0,{'actualEps':3},None),(0,{'actualEps':0},-1),(-2,{},-3),(2**54,{},None)]:
            values=events();values[0].update(epsActual=actual,**extra)
            self.assertEqual(model(values)['event_differences'][0]['differences']['eps']['actual_minus_reported_estimate'],expected)
        values=events();values[0]['epsEstimated']=0;d=model(values)['event_differences'][0]['differences']['eps']
        self.assertEqual(d['actual_minus_reported_estimate'],2);self.assertIsNone(d['pct_of_absolute_estimate'])
        values[0]['epsEstimated']=-1;d=model(values)['event_differences'][0]['differences']['eps'];self.assertEqual(d['pct_of_absolute_estimate'],300)
    def test_explicit_identity_currency_basis_and_completed_quarter_required(self):
        for edit in [{'cik':None},{'reportedCurrency':None},{'currency':'USD'},{'epsBasis':None},{'epsBasis':'guess'},
                     {'estimatedEpsBasis':'basic_gaap'},{'fiscalYear':True},{'period':None},{'fiscalDateEnding':'2027-01-01'}]:
            values=events();values[0].update(edit)
            self.assertIsNone(model(values)['event_differences'][0]['differences']['eps']['actual_minus_reported_estimate'],edit)
    def test_event_date_is_not_release_clock_and_receipt_is_not_vintage(self):
        values=events();values[0].update(announcementTime='2026-08-01 16:15:00',estimateAsOf='2026-07-31',lastUpdated='2026-08-02')
        e=model(values)['event_observations'][0];self.assertIsNone(e['reported_release_utc']);self.assertIsNone(e['reported_estimate_as_of_utc'])
        self.assertFalse(e['first_release_verified']);self.assertEqual(e['reported_last_updated'],'2026-08-02')
    def test_all_price_rows_preserved_with_dates_duplicates_and_missing_volume(self):
        rows=[{'symbol':'TEST','date':'2026-08-01','close':10},{'symbol':'TEST','date':'2026-09-25','close':20},
              {'symbol':'OTHER','date':'2027-01-01','close':True,'volume':0},None,{'date':'2026-08-01','close':0,'volume':False}]
        a=capture(rows,'historical-price-eod/full');out=price_coverage('TEST',a,TODAY)
        self.assertEqual(original(a),rows);self.assertEqual(out['records'],5);self.assertFalse(out['executable_return_qualified'])
        self.assertEqual(out['diagnostics']['repeated_date_occurrences'],2);self.assertEqual(out['diagnostics']['invalid_or_missing_volume'],4)
    def test_large_duplicate_population_is_retained_without_pairwise_expansion(self):
        out=model([events()[0]]*2000)
        self.assertEqual(len(out['event_observations']),2000);self.assertEqual(out['event_differences'][0]['same_date_occurrences'],2000)
        self.assertNotIn('same_date_source_indices',out['event_differences'][0])
    def test_all_whole_records_and_invalid_json_retained_with_integrity(self):
        values=events()+[None,{'unknown':'x'*70000}];a=capture(values)
        self.assertEqual(original(a),values);self.assertEqual(len(model(values)['event_observations']),6)
        for key,value in [('original_bytes',1),('original_sha256','bad'),('original_base64','!')]:
            with self.assertRaises(Exception):original({**a,key:value})
        for raw in [b'{"x":1,"x":2}',b'[NaN]',b'[1e999]',b'[1e-999]',b'\xff']:
            a=envelope(raw,'earnings',TODAY+'T00:00:00Z');self.assertEqual(a['status'],'invalid_original');self.assertIsNone(original(a));self.assertEqual(base64.b64decode(a['original_base64']),raw)
    def test_universe_retains_occurrences_duplicates_invalid_and_original_cap(self):
        rows=[member()['raw'],member()['raw'],{'symbol':'<BAD>','cap_bucket':'small'},member('LATER')['raw'],None]
        m=universe(capture({'stocks':rows},'data/universe.json'),3)
        self.assertEqual(len(m['occurrences']),5);self.assertEqual([r['ticker'] for r in m['selected']],['TEST','TEST',None])
        self.assertEqual(m['occurrences'][3]['status'],'outside_original_request_cap')
    def test_active_writer_keeps_all_selected_sources_history_and_code_identity(self):
        m=Memory();ns=native(m);ns['_pead_company']=company;ns['lambda_handler']();p=strict(m.data['data/earnings-pead.json'])
        self.assertEqual(len(p['request_records']),3);self.assertEqual(p['n_event_observations'],12);self.assertEqual(p['n_price_records'],6);self.assertEqual(p['summary']['top_25_overall'],[])
        self.assertEqual(p['source_files']['pead_observations.py']['sha256'],hashlib.sha256((SRC/'pead_observations.py').read_bytes()).hexdigest())
        self.assertEqual(len([k for k in m.data if '/history/' in k]),2);self.assertEqual(m.data[p['previous_publication']['key']],m.previous)
        self.assertTrue(all(k.startswith('data/earnings-pead') or k=='data/universe.json' for k in m.reads+m.writes))
    def test_denied_corrupt_racing_and_total_failure_preserve_current(self):
        for mode in ('denied','corrupt','race','failure'):
            m=Memory();setattr(m,mode,True);ns=native(m)
            ns['_pead_company']=(lambda _: [{'endpoint':'earnings','status':'unavailable'}]) if mode=='failure' else company
            with self.assertRaises(Exception):ns['lambda_handler']()
            self.assertEqual(m.data['data/earnings-pead.json'],m.previous)
    def test_rate_limit_and_time_budget_leave_unattempted_occurrences(self):
        m=Memory();ns=native(m,N_WORKERS=1);seen=[]
        def limited(row):
            seen.append(row['ticker']);return company(row) if len(seen)==1 else [{'endpoint':'earnings','status':'rate_limited'}]
        ns['_pead_company']=limited;ns['lambda_handler']();p=strict(m.data['data/earnings-pead.json'])
        self.assertEqual(len(seen),2);self.assertEqual(p['request_records'][2]['acquisitions'][0]['status'],'not_attempted_runtime_rate_or_size_limit')
        m=Memory();ns=native(m,TIMEOUT_BUDGET_S=1);ns['_pead_company']=lambda _:self.fail('No call in expired budget')
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(m.data['data/earnings-pead.json'],m.previous)
    def test_original_price_request_gate_and_whole_event_population(self):
        ns=native();seen=[]
        def fetch(ticker,endpoint):seen.append(endpoint);return capture(events()*3,endpoint)
        ns['_pead_fetch']=fetch;out=ns['_pead_company'](member());self.assertEqual(seen,['earnings','historical-price-eod/full']);self.assertEqual(len(original(out[0])),12)
        ns['_pead_fetch']=lambda *a:capture(events()[:2]);self.assertEqual(ns['_pead_company'](member())[-1]['status'],'not_requested_original_event_gate')
        values=events()
        for row in values:row['date']='invalid'
        ns['_pead_fetch']=lambda *a:capture(values);self.assertEqual(ns['_pead_company'](member())[-1]['status'],'not_requested_original_event_gate')
    def test_provider_redirect_rate_error_secret_echo_and_size_controls(self):
        ns=native();requests=[]
        def response(raw):return SimpleNamespace(__enter__=lambda s:s,__exit__=lambda *a:None,read=lambda n:raw)
        class Response:
            def __init__(self,raw):self.raw=raw
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def read(self,n):return self.raw[:n]
        def opener(*handlers):
            self.assertIsNone(handlers[0].redirect_request(None,None,302,'',{},'https://other.example'))
            def open(req,timeout):requests.append(req);return Response(b'[]')
            return SimpleNamespace(open=open)
        with patch.object(urllib.request,'build_opener',opener):
            self.assertEqual(ns['_pead_fetch']('TEST','earnings')['status'],'received')
        self.assertNotIn(ns['FMP_KEY'],requests[0].full_url);self.assertTrue(requests[0].full_url.endswith('/stable/earnings?symbol=TEST'))
        for raw,status in [(b'x'*(2*1024*1024+1),'response_exceeds_bound'),(ns['FMP_KEY'].encode(),'credential_echo_withheld')]:
            with patch.object(urllib.request,'build_opener',lambda *a:SimpleNamespace(open=lambda *a,**kw:Response(raw))):self.assertEqual(ns['_pead_fetch']('TEST','earnings')['status'],status)
        def rate(*a,**k):raise urllib.error.HTTPError('redacted',429,'slow',{},None)
        with patch.object(urllib.request,'build_opener',lambda *a:SimpleNamespace(open=rate)):
            self.assertEqual(ns['_pead_fetch']('TEST','earnings')['status'],'rate_limited')
    def test_future_previous_generation_cannot_be_replaced(self):
        m=Memory();m.data['data/earnings-pead.json']=json.dumps({'measurement_contract':CONTRACT,'generated_at':'2099-01-01T00:00:00Z'}).encode();ns=native(m)
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(m.writes,[])


if __name__=='__main__':unittest.main(verbosity=2)
