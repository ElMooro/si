"""Synthetic-only target, source and publication tests; no AWS/native/provider calls."""
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote_plus
import ast,base64,hashlib,json,sys,time,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-eps-revision-velocity/source';sys.path.insert(0,str(SRC))
from eps_observations import CONTRACT,strict,clock,number,symbol,envelope,original,universe,dossier,estimates,compare,ratings
TODAY='2026-09-27'


def forecasts(eps=2,date='2027-12-31'):
    return [{'symbol':'TEST','date':date,'epsAvg':eps,'epsLow':-10,'epsHigh':20,'numAnalystsEps':5,
             'reportedCurrency':'USD','cik':'1','epsBasis':'reported_adjusted','unknown':'retain'}]


def capture(values=None,endpoint='analyst-estimates',stamp=TODAY+'T01:00:00Z'):
    return envelope(json.dumps(forecasts() if values is None else values).encode(),endpoint,stamp)


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"schema_version":1,"all_qualifying":[{"symbol":"OLD"}]}'
        self.data={'data/eps-revision-velocity.json':self.previous,
                   'data/universe.json':b'{"stocks":[{"symbol":"TEST"},{"symbol":"TEST"},null,{"symbol":"SECOND"}]}',
                   'screener/data.json':b'{"rows":[{"ticker":"TEST"},{"ticker":"THIRD"}]}'}
        self.reads=[];self.writes=[];self.denied=False;self.corrupt=False;self.race=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'corrupt' if self.corrupt and '/history/' in key else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key=='data/eps-revision-velocity.json' and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    scope={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':'data/eps-revision-velocity.json','FMP_KEY':'synthetic-fixture-secret',
           'N_WORKERS':10,'MIN_MCAP':300000000,'MAX_TICKERS':2,'TIMEOUT_BUDGET_S':240,'SP500_BACKUP':['TEST','BACKUP'],
           'CONTRACT':CONTRACT,'strict':strict,'clock':clock,'number':number,'symbol':symbol,'envelope':envelope,'original':original,'universe':universe,'dossier':dossier,
           'datetime':datetime,'timezone':timezone,'ThreadPoolExecutor':ThreadPoolExecutor,'urllib':urllib,'quote_plus':quote_plus,'hashlib':hashlib,'json':json,'time':time,'Path':Path,'__file__':str(SRC/'lambda_function.py'),**extra}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_eps_') or n.name=='lambda_handler')]
    exec(compile(ast.Module(body=functions,type_ignores=[]),'<isolated active EPS functions>','exec'),scope);return scope


class Tests(unittest.TestCase):
    def test_reported_quarter_cannot_be_relabelled_as_an_annual_target(self):
        values=forecasts();values[0]['period']='Q1'
        row=estimates('TEST',capture(values),TODAY)[0]
        self.assertEqual(row['measurement_status'],'unqualified_record')
        self.assertIn('reported_period_conflicts_with_annual_request',row['issues'])
        self.assertEqual(row['raw']['period'],'Q1')
    def test_same_snapshot_different_targets_are_not_revisions(self):
        p=dossier('TEST',[capture(forecasts(1)+forecasts(2,'2028-12-31'))],TODAY)
        self.assertEqual(len(p['estimate_observations']),2);self.assertTrue(all(r['eps_change'] is None for r in p['same_target_comparisons']))
        self.assertIsNone(p['revision_velocity_60d']);self.assertFalse(p['independent_evidence_eligible'])
    def test_only_unique_target_identity_ordered_time_units_basis_permits_change(self):
        first=estimates('TEST',capture(stamp=TODAY+'T00:00:00Z'),TODAY);last=estimates('TEST',capture(forecasts(3)),TODAY)
        self.assertEqual(compare(last,first)[0]['eps_change'],1);self.assertEqual(compare(last,first)[0]['eps_change_pct_positive_base'],50)
        for field,value in [('target_period_end','2028-12-31'),('reported_cik','2'),('reported_currency','JPY'),('eps_basis',None),('received_at',TODAY+'T00:00:00Z')]:
            changed=deepcopy(last);changed[0][field]=value;self.assertIsNone(compare(changed,first)[0]['eps_change'],field)
        self.assertTrue(all(x['eps_change'] is None for x in compare(last+last,first)))
        self.assertIsNone(compare(last,first+first)[0]['eps_change'])
    def test_zero_negative_and_bool_counts_remain_distinct(self):
        for old in [0,-1]:
            first=estimates('TEST',capture(forecasts(old),stamp=TODAY+'T00:00:00Z'),TODAY);last=estimates('TEST',capture(forecasts(1)),TODAY)
            out=compare(last,first)[0];self.assertEqual(out['eps_change'],1-old);self.assertIsNone(out['eps_change_pct_positive_base'])
        for value in [True,False,'',float('inf'),10**100]:self.assertIsNone(number(value))
        for value in [True,-1,1.5]:
            values=forecasts();values[0]['numAnalystsEps']=value
            self.assertEqual(estimates('TEST',capture(values),TODAY)[0]['measurement_status'],'unqualified_record')
    def test_calendar_year_no_longer_overwrites_dates_and_past_target_stays_past(self):
        rows=forecasts(1,'2026-01-31')+forecasts(2,'2026-12-31');out=estimates('TEST',capture(rows),TODAY)
        self.assertEqual(len(out),2);self.assertEqual(out[0]['target_status'],'past_target');self.assertEqual(out[1]['values']['epsAvg'],2)
        rows[0]['epsAvg']=0;self.assertEqual(estimates('TEST',capture(rows),TODAY)[0]['values']['epsAvg'],0)
        rows[0]['estimatedEpsAvg']=1;self.assertIsNone(estimates('TEST',capture(rows),TODAY)[0]['values']['epsAvg'])
    def test_ranges_unknown_currency_and_alias_conflicts_do_not_qualify(self):
        for changes in [{'epsLow':5,'epsHigh':1},{'epsAvg':True},{'currency':'JPY'},{'symbol':'OTHER'},{'date':'not-a-date'}]:
            rows=forecasts();rows[0].update(changes);out=estimates('TEST',capture(rows),TODAY)[0]
            self.assertEqual(out['measurement_status'],'unqualified_record',changes)
    def test_whole_original_bytes_unknown_fields_invalid_json_and_tampering(self):
        values=forecasts()+[None,{'extra':'x'*70000}];a=capture(values)
        self.assertEqual(original(a),values);self.assertEqual(len(estimates('TEST',a,TODAY)),3)
        for key,val in [('original_sha256','bad'),('original_bytes',1),('original_base64','!')]:
            with self.assertRaises(Exception):original({**a,key:val})
        for raw in [b'{"a":1,"a":2}',b'[NaN]',b'[1e999]',b'[1e-999]',b'\xff']:
            a=envelope(raw,'analyst-estimates',TODAY+'T00:00:00Z');self.assertEqual(a['status'],'invalid_original');self.assertIsNone(original(a));self.assertEqual(base64.b64decode(a['original_base64']),raw)
    def test_ratings_are_separate_full_records_not_word_inferred_revision_breadth(self):
        values=[{'symbol':'TEST','date':'2027-01-01','newGrade':'Buy'},{'symbol':'TEST','date':'2026-09-26','gradingCompany':'Buy Research','previousGrade':'Sell','newGrade':'Sell','action':'maintain'}]+[None]*58
        out=ratings('TEST',capture(values,'grades'),TODAY);self.assertEqual(len(out),60)
        self.assertEqual(out[0]['status'],'unqualified_record');self.assertEqual(out[1]['reported_action'],'maintain');self.assertTrue(all(r['earnings_revision_breadth'] is None for r in out))
    def test_universe_preserves_every_source_occurrence_duplicate_and_original_cap(self):
        m=Memory();caps=[envelope(m.data[k],k,TODAY+'T00:00:00Z') for k in ['data/universe.json','screener/data.json']]
        out=universe(caps,['TEST','BACKUP'],2);self.assertEqual(len(out['occurrences']),8);self.assertEqual(out['selected_symbols'],['TEST','SECOND']);self.assertEqual(len(out['distinct_request_symbols']),4)
        self.assertEqual(out['occurrences'][0]['request_index'],out['occurrences'][1]['request_index']);self.assertEqual(out['occurrences'][2]['status'],'invalid_symbol');self.assertEqual(out['occurrences'][-1]['status'],'outside_original_request_cap')
        self.assertEqual(universe([capture({'wrong':[]},'data/universe.json')],['TEST'],1)['source_populations'][0]['status'],'missing_expected_population')
    def test_active_publication_archives_complete_source_and_no_private_reads(self):
        m=Memory();ns=native(m);ns['_eps_company']=lambda *a:[capture()];ns['lambda_handler']();p=strict(m.data['data/eps-revision-velocity.json'])
        self.assertEqual(len(p['request_records']),2);self.assertEqual(p['n_estimate_observations'],2);self.assertEqual(m.data[p['previous_publication']['key']],m.previous)
        self.assertEqual(p['all_qualifying'],[]);self.assertEqual(p['summary']['top_25_overall'],[]);self.assertEqual(p['notifications_sent'],0)
        self.assertEqual(p['source_files']['eps_observations.py']['sha256'],hashlib.sha256((SRC/'eps_observations.py').read_bytes()).hexdigest())
        for k in ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','independent_evidence_eligible','private_state_read_or_written']:self.assertIs(p[k],False)
        self.assertTrue(all(k in ('data/eps-revision-velocity.json','data/universe.json','screener/data.json') or k.startswith('data/eps-revision-velocity/history/') for k in m.reads))
    def test_failure_archive_corruption_and_race_preserve_previous_current(self):
        for flag in ['all_failed','denied','corrupt','race']:
            m=Memory();ns=native(m);setattr(m,flag,True);ns['_eps_company']=lambda *a:[{'endpoint':'quote','status':'unavailable'}] if flag=='all_failed' else [capture()]
            with self.assertRaises(Exception):ns['lambda_handler']()
            self.assertEqual(m.data['data/eps-revision-velocity.json'],m.previous,flag)
    def test_rate_and_time_limits_preserve_unattempted_occurrences(self):
        m=Memory();ns=native(m,N_WORKERS=1);calls=[]
        def fetch(*a):calls.append(a);return [capture(),{'endpoint':'grades','status':'rate_limited'}]
        ns['_eps_company']=fetch;ns['lambda_handler']();p=strict(m.data['data/eps-revision-velocity.json'])
        self.assertEqual(len(calls),1);self.assertEqual(len(p['request_records']),2);self.assertEqual(p['request_records'][1]['acquisitions'][0]['status'],'not_attempted_runtime_rate_or_size_limit')
        m=Memory();ns=native(m);ns['_eps_company']=lambda *a:self.fail('Provider request after deadline')
        with self.assertRaises(ValueError):ns['lambda_handler'](context=SimpleNamespace(get_remaining_time_in_millis=lambda:1000))
        self.assertEqual(m.data['data/eps-revision-velocity.json'],m.previous)
    def test_provider_error_scope_redirect_secret_echo_and_byte_limits(self):
        ns=native();seen=[]
        def fail(req,**kw):seen.append(req);raise urllib.error.HTTPError(req.full_url,429,'fixture',{},None)
        def opener(handler):
            self.assertIsNone(handler.redirect_request(None,None,302,'fixture',{},'https://unrelated.test'));return SimpleNamespace(open=fail)
        with patch.object(urllib.request,'build_opener',opener):out=ns['_eps_fetch']('TEST','analyst-estimates')
        self.assertEqual(out['status'],'rate_limited');self.assertEqual(len(seen),1);self.assertIn('period=annual&limit=5',seen[0].full_url);self.assertNotIn(ns['FMP_KEY'],seen[0].full_url)
        for raw,status in [(ns['FMP_KEY'].encode(),'credential_echo_withheld'),(b'x'*(256*1024+1),'response_exceeds_bound')]:
            with patch.object(urllib.request,'build_opener',lambda *a:SimpleNamespace(open=lambda *a,**kw:BytesIO(raw))):out=ns['_eps_fetch']('TEST','grades')
            self.assertEqual(out['status'],status)
    def test_original_acquisition_gate_and_endpoint_scope_without_partial_grade_caps(self):
        ns=native();calls=[]
        def fetch(ticker,endpoint):
            calls.append(endpoint);return capture([{'symbol':'TEST','marketCap':1e9}],endpoint) if endpoint=='quote' else capture(forecasts()+forecasts(3,'2028-12-31'),endpoint)
        ns['_eps_fetch']=fetch;out=ns['_eps_company']('TEST');self.assertEqual(calls,['quote','analyst-estimates','grades'])
        calls=[];ns['_eps_fetch']=lambda *a:capture([{'symbol':'OTHER','marketCap':1e9}],a[1])
        self.assertEqual(ns['_eps_company']('TEST')[-1]['status'],'not_requested_original_quote_gate')
    def test_incompatible_or_future_previous_capture_never_creates_observed_revision(self):
        first=[capture(stamp=TODAY+'T02:00:00Z')];out=dossier('TEST',[capture(forecasts(3))],TODAY,first)
        self.assertIsNone(out['same_target_comparisons'][0]['eps_change'])


if __name__=='__main__':unittest.main(verbosity=2)
