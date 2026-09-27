"""Synthetic-only statement, acquisition and conditional-publication regressions."""
from pathlib import Path
from datetime import datetime,timezone,date,timedelta
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote_plus
import ast,base64,hashlib,json,sys,time,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-revenue-acceleration/source';sys.path.insert(0,str(SRC))
from revenue_observations import CONTRACT,strict,clock,number,symbol,envelope,original,universe,dossier,statement,comparisons,shift
TODAY='2026-09-27'


def statements():
    out=[]
    for i,revenue in enumerate((200,180,160,140,100,100,100,100)):
        start=shift(date(2026,4,1),-3*i);end=shift(start,3)-timedelta(days=1)
        out.append({'symbol':'TEST','cik':'1','reportedCurrency':'JPY','date':end.isoformat(),'startDate':start.isoformat(),
            'period':'Q'+str((start.month-1)//3+1),'fiscalYear':str(start.year),'filingDate':(end+timedelta(days=20)).isoformat(),
            'acceptedDate':(end+timedelta(days=20)).isoformat()+' 12:00:00','revenue':revenue,'grossProfit':revenue/2,
            'operatingExpenses':40,'operatingIncome':20,'netIncome':10,'eps':2,'epsdiluted':1,'unknown':'retain'})
    return out


def capture(values=None,endpoint='income-statement',stamp=TODAY+'T01:00:00Z'):
    return envelope(json.dumps(statements() if values is None else values).encode(),endpoint,stamp)


def member(ticker='TEST'):
    return {'source_index':0,'ticker':ticker,'raw':{'symbol':ticker,'cap_bucket':'large','market_cap':1e9}}


def model(values):return dossier(member(),[capture(values)],TODAY)


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"schema_version":1,"all_qualifying":[{"symbol":"OLD"}]}'
        self.data={'data/revenue-acceleration.json':self.previous,'data/universe.json':json.dumps({'stocks':[member()['raw'],member()['raw'],None,{'symbol':'NEXT','cap_bucket':'small'}]}).encode()}
        self.reads=[];self.writes=[];self.denied=False;self.corrupt=False;self.race=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'corrupt' if self.corrupt and '/history/' in key else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key=='data/revenue-acceleration.json' and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    scope={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':'data/revenue-acceleration.json','FMP_KEY':'synthetic-fixture-secret',
        'N_WORKERS':6,'MAX_TICKERS':3,'TIMEOUT_BUDGET_S':260,'CONTRACT':CONTRACT,'strict':strict,'clock':clock,'number':number,'symbol':symbol,
        'envelope':envelope,'original':original,'universe':universe,'dossier':dossier,'datetime':datetime,'timezone':timezone,
        'ThreadPoolExecutor':ThreadPoolExecutor,'urllib':urllib,'quote_plus':quote_plus,'hashlib':hashlib,'json':json,'time':time,
        'Path':Path,'__file__':str(SRC/'lambda_function.py'),**extra}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());fns=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_revenue_') or n.name=='lambda_handler')]
    exec(compile(ast.Module(body=fns,type_ignores=[]),'<isolated active revenue functions>','exec'),scope);return scope


def company(m):
    values=statements()
    for row in values:row['symbol']=m['ticker']
    return [capture(values,stamp=datetime.now(timezone.utc).isoformat())]


class Tests(unittest.TestCase):
    def test_valid_quarters_reproduce_growth_acceleration_and_real_trailing_total(self):
        out=model(statements());current=out['period_comparisons'][0]
        self.assertEqual(current['yoy']['changes']['revenue']['pct_positive_base'],100)
        self.assertEqual(current['revenue_growth_acceleration_pp'],20);self.assertEqual(current['consecutive_acceleration_intervals'],3)
        self.assertEqual(current['ttm_revenue'],680);self.assertEqual(current['ttm_source_indices'],[0,1,2,3])
        self.assertEqual(out['statement_observations'][0]['gross_margin_pct'],50);self.assertFalse(out['sizing_eligible'])
        self.assertIsNone(out['statement_observations'][0]['accepted_utc'])
    def test_original_same_date_annual_wrong_issuer_defect_cannot_qualify(self):
        values=statements()
        for row in values:row.update(date='2026-06-30',period='FY',symbol='OTHER')
        out=model(values);self.assertEqual(len(out['statement_observations']),8)
        self.assertTrue(all(r['yoy'] is None and r['revenue_growth_acceleration_pp'] is None for r in out['period_comparisons']))
    def test_zero_missing_boolean_negative_and_overflow_are_distinct(self):
        for value,expected in [(0,-100),(None,None),(True,None),(-10,-110),(2**54,None)]:
            values=statements();values[0]['revenue']=value
            out=model(values);self.assertEqual(out['period_comparisons'][0]['yoy']['changes']['revenue']['pct_positive_base'],expected)
        values=statements();values[4]['revenue']=0;out=model(values)['period_comparisons'][0]['yoy']['changes']['revenue']
        self.assertEqual(out['absolute_change'],200);self.assertIsNone(out['pct_positive_base'])
    def test_zero_sequential_growth_and_absent_gross_profit_preserved(self):
        values=statements();values[0]['revenue']=180;values[0]['grossProfit']=None;out=model(values)
        self.assertEqual(out['period_comparisons'][0]['sequential']['changes']['revenue']['pct_positive_base'],0)
        self.assertIsNone(out['statement_observations'][0]['gross_margin_pct'])
    def test_basic_and_diluted_eps_are_separate_without_zero_alias_replacement(self):
        values=statements();values[0].update(epsdiluted=0,eps=9)
        out=model(values)['period_comparisons'][0]['yoy']['changes']
        self.assertEqual(out['epsdiluted']['absolute_change'],-1);self.assertEqual(out['eps']['absolute_change'],7)
    def test_units_dates_issuer_duration_and_fiscal_calendar_must_match(self):
        for edit in [{'reportedCurrency':'USD'},{'cik':'2'},{'startDate':None},{'date':'2027-06-30'},
                     {'period':'FY'},{'fiscalYear':True},{'filingDate':None},{'acceptedDate':'2099-01-01T00:00:00Z'}]:
            values=statements();values[0].update(edit);current=model(values)['period_comparisons'][0]
            self.assertIsNone(current['yoy'],edit);self.assertIsNone(current['ttm_revenue'],edit)
    def test_duplicate_and_missing_quarters_cannot_create_streak_or_trailing_total(self):
        values=statements();values.append(deepcopy(values[4]));current=model(values)['period_comparisons'][0]
        self.assertIsNone(current['yoy']);self.assertIsNone(current['revenue_growth_acceleration_pp'])
        values=statements();del values[1];current=model(values)['period_comparisons'][0]
        self.assertIsNotNone(current['yoy']);self.assertIsNone(current['sequential']);self.assertIsNone(current['ttm_revenue']);self.assertIsNone(current['consecutive_acceleration_intervals'])
    def test_non_calendar_year_quarters_and_leap_day_use_start_anchored_intervals(self):
        values=statements()
        for i,row in enumerate(values):
            start=shift(date(2026,3,1),-3*i);end=shift(start,3)-timedelta(days=1)
            row.update(startDate=start.isoformat(),date=end.isoformat(),filingDate=(end+timedelta(days=20)).isoformat(),acceptedDate=(end+timedelta(days=20)).isoformat()+'T12:00:00Z')
        out=model(values)['period_comparisons'][0];self.assertEqual(out['sequential']['prior_period_end'],'2026-02-28');self.assertEqual(out['ttm_revenue'],680)
        values=statements()
        for i,row in enumerate(values):
            start=shift(date(2024,12,1),-3*i);end=shift(start,3)-timedelta(days=1)
            row.update(startDate=start.isoformat(),date=end.isoformat(),filingDate=(end+timedelta(days=20)).isoformat(),acceptedDate=(end+timedelta(days=20)).isoformat()+'T12:00:00Z')
        out=model(values)['period_comparisons'][0];self.assertEqual(out['yoy']['prior_period_end'],'2024-02-29')
    def test_all_whole_records_and_invalid_json_retained_with_integrity(self):
        values=statements()+[None,{'unknown':'x'*70000}];a=capture(values)
        self.assertEqual(original(a),values);self.assertEqual(len(model(values)['statement_observations']),10)
        for key,value in [('original_bytes',1),('original_sha256','bad'),('original_base64','!')]:
            with self.assertRaises(Exception):original({**a,key:value})
        for raw in [b'{"x":1,"x":2}',b'[NaN]',b'[1e999]',b'[1e-999]',b'\xff']:
            a=envelope(raw,'income-statement',TODAY+'T00:00:00Z');self.assertEqual(a['status'],'invalid_original');self.assertIsNone(original(a));self.assertEqual(base64.b64decode(a['original_base64']),raw)
    def test_universe_retains_occurrences_duplicates_invalid_and_original_cap(self):
        rows=[member()['raw'],member()['raw'],{'symbol':'<BAD>','cap_bucket':'small'},member('LATER')['raw'],None]
        m=universe(capture({'stocks':rows},'data/universe.json'),3)
        self.assertEqual(len(m['occurrences']),5);self.assertEqual([r['ticker'] for r in m['selected']],['TEST','TEST',None])
        self.assertEqual(m['occurrences'][3]['status'],'outside_original_request_cap')
    def test_active_writer_keeps_all_selected_sources_history_and_code_identity(self):
        m=Memory();ns=native(m);ns['_revenue_company']=company;ns['lambda_handler']();p=strict(m.data['data/revenue-acceleration.json'])
        self.assertEqual(len(p['request_records']),3);self.assertEqual(p['n_statement_observations'],24);self.assertEqual(p['summary']['top_25_overall'],[])
        self.assertEqual(p['source_files']['revenue_observations.py']['sha256'],hashlib.sha256((SRC/'revenue_observations.py').read_bytes()).hexdigest())
        self.assertEqual(len([k for k in m.data if '/history/' in k]),2);self.assertEqual(m.data[p['previous_publication']['key']],m.previous)
        self.assertTrue(all(k.startswith('data/revenue-acceleration') or k=='data/universe.json' for k in m.reads+m.writes))
    def test_denied_corrupt_racing_and_total_failure_preserve_current(self):
        for mode in ('denied','corrupt','race','failure'):
            m=Memory();setattr(m,mode,True);ns=native(m)
            ns['_revenue_company']=(lambda _: [{'endpoint':'income-statement','status':'unavailable'}]) if mode=='failure' else company
            with self.assertRaises(Exception):ns['lambda_handler']()
            self.assertEqual(m.data['data/revenue-acceleration.json'],m.previous)
    def test_rate_limit_and_time_budget_leave_unattempted_occurrences(self):
        m=Memory();ns=native(m,N_WORKERS=1);seen=[]
        def limited(row):
            seen.append(row['ticker']);return company(row) if len(seen)==1 else [{'endpoint':'income-statement','status':'rate_limited'}]
        ns['_revenue_company']=limited;ns['lambda_handler']();p=strict(m.data['data/revenue-acceleration.json'])
        self.assertEqual(len(seen),2);self.assertEqual(p['request_records'][2]['acquisitions'][0]['status'],'not_attempted_runtime_rate_or_size_limit')
        m=Memory();ns=native(m,TIMEOUT_BUDGET_S=1);ns['_revenue_company']=lambda _:self.fail('No call in expired budget')
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(m.data['data/revenue-acceleration.json'],m.previous)
    def test_original_quote_budget_gate_and_endpoint_scope(self):
        ns=native();seen=[]
        def fetch(ticker,endpoint):seen.append(endpoint);return capture()
        ns['_revenue_fetch']=fetch;out=ns['_revenue_company'](member());self.assertEqual(seen,['income-statement']);self.assertEqual(out[-1]['status'],'not_requested_original_universe_cap')
        m=member();m['raw'].pop('market_cap');seen.clear();ns['_revenue_company'](m);self.assertEqual(seen,['income-statement','quote'])
        ns['_revenue_fetch']=lambda *a:capture(statements()[:5]);self.assertEqual(ns['_revenue_company'](m)[-1]['status'],'not_requested_original_statement_gate')
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
            self.assertEqual(ns['_revenue_fetch']('TEST','income-statement')['status'],'received')
        self.assertNotIn(ns['FMP_KEY'],requests[0].full_url);self.assertIn('period=quarter&limit=8',requests[0].full_url)
        for raw,status in [(b'x'*(512*1024+1),'response_exceeds_bound'),(ns['FMP_KEY'].encode(),'credential_echo_withheld')]:
            with patch.object(urllib.request,'build_opener',lambda *a:SimpleNamespace(open=lambda *a,**kw:Response(raw))):self.assertEqual(ns['_revenue_fetch']('TEST','income-statement')['status'],status)
        def rate(*a,**k):raise urllib.error.HTTPError('redacted',429,'slow',{},None)
        with patch.object(urllib.request,'build_opener',lambda *a:SimpleNamespace(open=rate)):
            self.assertEqual(ns['_revenue_fetch']('TEST','income-statement')['status'],'rate_limited')
    def test_future_previous_generation_cannot_be_replaced(self):
        m=Memory();m.data['data/revenue-acceleration.json']=json.dumps({'measurement_contract':CONTRACT,'generated_at':'2099-01-01T00:00:00Z'}).encode();ns=native(m)
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(m.writes,[])


if __name__=='__main__':unittest.main(verbosity=2)
