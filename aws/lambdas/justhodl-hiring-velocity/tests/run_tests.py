"""Synthetic statement/history regressions. No native import, network or AWS."""
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from urllib import request, error
from urllib.parse import quote_plus
import ast, base64, hashlib, json, re, sys, time, unittest
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-hiring-velocity/source';sys.path.insert(0,str(SRC))
from hiring_observations import CONTRACT, number, strict, day, clock, envelope, original, employee_rows, annual_changes, revenue_ratios, dossier
TODAY='2026-09-27'


def employees():
    return [{'symbol':'TEST','cik':'1','periodOfReport':f'{y}-12-31','filingDate':f'{y+1}-02-28',
             'formType':'10-K','employeeCount':n,'acceptanceTime':f'{y+1}-02-28 16:00:00','unknown':'retain'}
            for y,n in [(2025,120),(2024,100),(2023,80)]]


def captured(values=None,endpoint='historical-employee-count'):
    return envelope(json.dumps(employees() if values is None else values).encode(),TODAY+'T01:00:00Z',endpoint)


def income():
    return [{'symbol':'TEST','cik':'0000000001','date':'2025-12-31','period':'FY','reportedCurrency':'JPY','revenue':1200}]


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"version":"1.0","top_50":[{"symbol":"OLD"}]}'
        self.data={'data/hiring-velocity.json':self.previous,'data/universe.json':json.dumps({'stocks':[{'symbol':'TEST','cap_bucket':'small'},None,{'symbol':'LARGE','cap_bucket':'large'},{'symbol':'TEST','cap_bucket':'small'}]}).encode(),
                   'data/bagger-engine.json':b'{"top_100":[{"symbol":"TEST","bagger_score":99}]}'}
        self.reads=[];self.writes=[];self.race=False;self.corrupt=False;self.denied=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'wrong' if self.corrupt and '/history/' in key else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key=='data/hiring-velocity.json' and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    scope={'s3':memory or Memory(),'S3_BUCKET':'fixture','S3_KEY':'data/hiring-velocity.json',
           'UNIVERSE_KEY':'data/universe.json','BAGGER_KEY':'data/bagger-engine.json','MAX_WORKERS':8,
           'CAP_BUCKETS':{'nano','micro','small','mid'},'FMP_KEY':'synthetic-fixture-secret','CONTRACT':CONTRACT,
           'strict':strict,'clock':clock,'envelope':envelope,'original':original,'employee_rows':employee_rows,'dossier':dossier,
           'datetime':datetime,'timezone':timezone,'ThreadPoolExecutor':ThreadPoolExecutor,
           'hashlib':hashlib,'json':json,'time':time,'re':re,'request':request,'quote_plus':quote_plus,**extra}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_hiring_') or n.name=='lambda_handler')]
    exec(compile(ast.Module(body=functions,type_ignores=[]),'<isolated active hiring functions>','exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_original_daily_dates_do_not_become_annual_growth(self):
        values=employees()
        for r,d in zip(values,['2026-09-26','2026-09-25','2026-09-24']):r['periodOfReport']=d;r['filingDate']='2026-09-27';r.pop('acceptanceTime')
        rows=employee_rows('TEST',captured(values),TODAY)
        self.assertTrue(all(r['annual_interval_change_pct'] is None for r in annual_changes(rows)))
    def test_annual_join_uses_dates_identity_count_definition_and_form(self):
        rows=employee_rows('TEST',captured(),TODAY);result=annual_changes(rows)
        self.assertEqual(result[0]['annual_interval_change_pct'],20);self.assertEqual(result[0]['elapsed_days'],365)
        for field,value in [('reported_cik','2'),('count_field','fullTimeEmployees'),('annual_form_family',None),('measurement_status','unqualified_record')]:
            changed=deepcopy(rows);changed[1][field]=value
            self.assertIsNone(annual_changes(changed)[0]['reported_count_change'],field)
        self.assertIsNone(annual_changes(rows+[deepcopy(rows[1])])[0]['reported_count_change'])
    def test_report_period_never_inferred_from_filing_or_date_and_future_excluded(self):
        for change in [{'periodOfReport':None,'date':'2025-12-31'},{'periodOfReport':'2027-12-31'},{'filingDate':'2030-01-01'},{'acceptanceTime':'2030-01-01 00:00:00'},{'filingDate':'2025-01-01'},{'acceptanceTime':'2026-02-28 nonsense'},{'acceptanceTime':'2026-09-27T02:00:00Z'}]:
            values=employees();values[0].update(change);row=employee_rows('TEST',captured(values),TODAY)[0]
            self.assertEqual(row['measurement_status'],'unqualified_record',change)
        row=employee_rows('TEST',captured(),TODAY)[0]
        self.assertEqual(row['acceptance_timezone_status'],'unknown_not_assumed');self.assertNotEqual(row['report_period_end'],row['filing_date'])
    def test_zero_is_retained_boolean_or_ambiguous_count_never_coerced(self):
        for count,expected in [(0,0),(True,None),(False,None),(-1,None),(1.5,None),('100',None),(10**100,None)]:
            rows=employees();rows[1]['employeeCount']=count
            out=employee_rows('TEST',captured(rows),TODAY);self.assertEqual(out[1]['employee_count'],expected)
            if count==0 and type(count) is int:
                r=annual_changes(out)[0];self.assertEqual(r['reported_count_change'],120);self.assertIsNone(r['annual_interval_change_pct'])
        values=employees();values[0]['employees']=120
        self.assertIsNone(employee_rows('TEST',captured(values),TODAY)[0]['employee_count'])
    def test_full_original_unknown_fields_malformed_records_and_byte_tampering(self):
        values=employees()+[None,{'unknown':'x'*70000}];a=captured(values)
        self.assertEqual(original(a),values);self.assertEqual(len(employee_rows('TEST',a,TODAY)),5)
        for key,val in [('original_sha256','x'),('original_bytes',1),('original_base64','bad!')]:
            bad={**a,key:val}
            with self.assertRaises(Exception):original(bad)
        for raw in [b'{"x":1,"x":2}',b'[NaN]',b'[1e9999]',b'[1e-9999]',b'\xff']:
            a=envelope(raw,TODAY+'T01:00:00Z','historical-employee-count')
            self.assertEqual(a['status'],'invalid_original');self.assertIsNone(original(a));self.assertEqual(base64.b64decode(a['original_base64']),raw)
    def test_unaligned_income_and_currency_are_never_dollar_productivity(self):
        rows=employee_rows('TEST',captured(),TODAY)
        good=revenue_ratios('TEST',rows,captured(income(),'income-statement'),TODAY)[0]
        self.assertEqual(good['annual_revenue_per_ending_employee'],10);self.assertEqual(good['unit'],'JPY/reported_period_end_person')
        for change in [{'symbol':'OTHER'},{'cik':'2'},{'date':'2020-03-31'},{'period':'Q4'},{'reportedCurrency':None},{'revenue':True},{'filingDate':'2030-01-01'}]:
            values=income();values[0].update(change)
            self.assertIsNone(revenue_ratios('TEST',rows,captured(values,'income-statement'),TODAY)[0]['annual_revenue_per_ending_employee'],change)
        for values in [income()+income()]:
            self.assertIsNone(revenue_ratios('TEST',rows,captured(values,'income-statement'),TODAY)[0]['annual_revenue_per_ending_employee'])
        values=income();values[0]['revenue']=0
        self.assertEqual(revenue_ratios('TEST',rows,captured(values,'income-statement'),TODAY)[0]['annual_revenue_per_ending_employee'],0)
    def test_multiple_populations_not_silently_merged(self):
        d=dossier({'symbol':'TEST'},[captured(),captured(endpoint='employee-count')],TODAY)
        self.assertEqual(len(d['employee_observations']),6)
        self.assertTrue(all(x['reported_count_change'] is None for x in d['annual_comparisons']))
    def test_native_retains_whole_population_and_history_no_private_calls(self):
        m=Memory();ns=native(m);ns['_hiring_company']=lambda *a:[captured(),captured(income(),'income-statement')]
        ns['lambda_handler']({},None);p=strict(m.data['data/hiring-velocity.json'])
        self.assertEqual(len(p['request_records']),2);self.assertEqual(p['unselected_universe_indices'],[1,2]);self.assertEqual(p['n_employee_observations'],6)
        self.assertEqual(original(p['universe_acquisition']),strict(m.data['data/universe.json']))
        self.assertEqual(original(p['bagger_context_acquisition'])['top_100'][0]['bagger_score'],99)
        self.assertEqual(m.data[p['previous_publication']['key']],m.previous)
        self.assertTrue(all(k.startswith('data/hiring-velocity/history/') or k in ('data/hiring-velocity.json','data/universe.json','data/bagger-engine.json') for k in m.reads))
        self.assertEqual(p['notifications_sent'],0);self.assertEqual(p['top_50'],[]);self.assertEqual(p['double_confirmed'],[])
        for key in ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','private_state_read_or_written']:self.assertIs(p[key],False)
    def test_all_failed_denied_corrupt_archive_and_race_preserve_current(self):
        for flag in ['all_failed','denied','corrupt','race']:
            m=Memory();ns=native(m);setattr(m,flag,True)
            ns['_hiring_company']=lambda *a:[{'status':'unavailable','endpoint':'historical-employee-count'}] if flag=='all_failed' else [captured()]
            with self.assertRaises(Exception):ns['lambda_handler']({},None)
            self.assertEqual(m.data['data/hiring-velocity.json'],m.previous,flag)
    def test_time_reserve_rate_limit_and_event_limit_account_for_unattempted(self):
        m=Memory();ns=native(m,MAX_WORKERS=1);calls=[]
        def company(*a):
            calls.append(a);return [captured(),{'endpoint':'income-statement','status':'rate_limited'}]
        ns['_hiring_company']=company;ns['lambda_handler']({},None);p=strict(m.data['data/hiring-velocity.json'])
        self.assertEqual(len(calls),1);self.assertEqual(len(p['request_records']),2)
        self.assertEqual(p['request_records'][1]['acquisitions'][0]['status'],'not_attempted_runtime_rate_or_size_limit')
        m=Memory();ns=native(m);ns['_hiring_company']=lambda *a:self.fail('No request with exhausted runtime')
        with self.assertRaises(ValueError):ns['lambda_handler']({},SimpleNamespace(get_remaining_time_in_millis=lambda:1000))
        self.assertEqual(m.data['data/hiring-velocity.json'],m.previous)
        m=Memory();ns=native(m);ns['_hiring_company']=lambda *a:[captured()];ns['lambda_handler']({'limit':1},None)
        self.assertEqual(strict(m.data['data/hiring-velocity.json'])['unselected_universe_indices'],[1,2,3])
        for val in [-1,True,'5']:
            with self.assertRaises(ValueError):ns['lambda_handler']({'limit':val},None)
    def test_provider_error_no_retry_secret_echo_and_response_bound(self):
        ns=native();calls=[]
        def fail(req,**kw):calls.append(req);raise error.HTTPError(req.full_url,429,'fixture',{},None)
        with patch.object(request,'build_opener',lambda *a:SimpleNamespace(open=fail)):out=ns['_hiring_fetch']('TEST','historical-employee-count',16)
        self.assertEqual(out['status'],'rate_limited');self.assertEqual(len(calls),1)
        self.assertNotIn(ns['FMP_KEY'],calls[0].full_url)
        for raw,status in [(ns['FMP_KEY'].encode(),'credential_echo_withheld'),(b'x'*(256*1024+1),'response_exceeds_bound'),(b'[{"x":1,"x":2}]','invalid_original')]:
            with patch.object(request,'build_opener',lambda *a:SimpleNamespace(open=lambda *args,**kw:BytesIO(raw))):out=ns['_hiring_fetch']('TEST','historical-employee-count',16)
            self.assertEqual(out['status'],status)
        def redirect_check(handler):
            self.assertIsNone(handler.redirect_request(None,None,302,'fixture',{},'https://unrelated.test'))
            return SimpleNamespace(open=lambda *a,**kw:BytesIO(b'[]'))
        with patch.object(request,'build_opener',redirect_check):ns['_hiring_fetch']('TEST','historical-employee-count',16)
    def test_company_keeps_error_acquisitions_and_never_requests_income_without_history(self):
        ns=native();calls=[]
        def fetch(symbol,endpoint,limit):calls.append(endpoint);return captured([],endpoint)
        ns['_hiring_fetch']=fetch;out=ns['_hiring_company']({'symbol':'TEST'},TODAY)
        self.assertEqual(calls,['historical-employee-count','employee-count']);self.assertEqual(len(out),3)
        calls=[];ns['_hiring_fetch']=lambda *a:{'status':'rate_limited','endpoint':a[1]}
        self.assertEqual(len(ns['_hiring_company']({'symbol':'TEST'},TODAY)),1)
    def test_active_handler_has_no_notification_or_legacy_call(self):
        tree=ast.parse((SRC/'lambda_function.py').read_bytes());active=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_hiring_') or n.name=='lambda_handler')]
        names={n.func.id for f in active for n in ast.walk(f) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
        self.assertTrue(names.isdisjoint({'maybe_telegram','analyze','fmp','_legacy_lambda_handler'}))


if __name__=='__main__':unittest.main(verbosity=2)
