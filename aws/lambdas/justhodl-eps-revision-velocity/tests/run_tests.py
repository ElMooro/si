"""Synthetic-only target, source and publication tests; no AWS/native/provider calls."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote_plus
import ast,base64,hashlib,json,sys,time,tempfile,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-eps-revision-velocity/source';sys.path.insert(0,str(SRC))
from eps_observations import (CONTRACT,strict,clock,number,symbol,envelope,original,universe,dossier,estimates,compare,ratings,
                             estimate_descriptor,validate_estimate_descriptor,estimate_source_envelope,
                             baseline_catalogue,previous_estimate_baselines,merge_estimate_baselines)
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
        self.reads=[];self.writes=[];self.bodies=[];self.denied=False;self.corrupt=False;self.race=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'corrupt' if self.corrupt and '/history/' in key else self.data[key]
        body=BytesIO(raw);self.bodies.append(body)
        return {'Body':body,'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key=='data/eps-revision-velocity.json' and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    memory=memory or Memory()
    scope={'S3':memory,'SOURCE_S3':memory,'BUCKET':'fixture','S3_KEY':'data/eps-revision-velocity.json','FMP_KEY':'synthetic-fixture-secret',
           'N_WORKERS':10,'MIN_MCAP':300000000,'MAX_TICKERS':2,'TIMEOUT_BUDGET_S':240,'SP500_BACKUP':['TEST','BACKUP'],
           'CONTRACT':CONTRACT,'strict':strict,'clock':clock,'number':number,'symbol':symbol,'envelope':envelope,'original':original,'universe':universe,'dossier':dossier,
           'estimate_descriptor':estimate_descriptor,'validate_estimate_descriptor':validate_estimate_descriptor,'estimate_source_envelope':estimate_source_envelope,
           'previous_estimate_baselines':previous_estimate_baselines,'merge_estimate_baselines':merge_estimate_baselines,
           'datetime':datetime,'timezone':timezone,'ThreadPoolExecutor':ThreadPoolExecutor,'urllib':urllib,'quote_plus':quote_plus,'hashlib':hashlib,'json':json,'time':time,'Path':Path,'__file__':str(SRC/'lambda_function.py'),**extra}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_eps_') or n.name=='lambda_handler')]
    exec(compile(ast.Module(body=functions,type_ignores=[]),'<isolated active EPS functions>','exec'),scope)
    scope['_eps_import_identity']=lambda:{'managed_secret.py':{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()} for raw in [(ROOT/'aws/shared/managed_secret.py').read_bytes()]}
    return scope


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
        m=Memory();ns=native(m);ns['_eps_company']=lambda *a:[capture(stamp=datetime.now(timezone.utc).isoformat())];ns['lambda_handler']();p=strict(m.data['data/eps-revision-velocity.json'])
        self.assertEqual(len(p['request_records']),2);self.assertEqual(p['n_estimate_observations'],2);self.assertEqual(m.data[p['previous_publication']['key']],m.previous)
        self.assertEqual(p['all_qualifying'],[]);self.assertEqual(p['summary']['top_25_overall'],[]);self.assertEqual(p['notifications_sent'],0)
        self.assertEqual(p['source_files']['eps_observations.py']['sha256'],hashlib.sha256((SRC/'eps_observations.py').read_bytes()).hexdigest())
        for k in ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','independent_evidence_eligible','private_state_read_or_written']:self.assertIs(p[k],False)
        self.assertTrue(all(k in ('data/eps-revision-velocity.json','data/universe.json','screener/data.json') or k.startswith(('data/eps-revision-velocity/history/','data/eps-revision-velocity/sources/')) for k in m.reads))
    def test_failure_archive_corruption_and_race_preserve_previous_current(self):
        for flag in ['all_failed','denied','corrupt','race']:
            m=Memory();ns=native(m);setattr(m,flag,True);ns['_eps_company']=lambda *a:[{'endpoint':'quote','status':'unavailable'}] if flag=='all_failed' else [capture(stamp=datetime.now(timezone.utc).isoformat())]
            with self.assertRaises(Exception):ns['lambda_handler']()
            self.assertEqual(m.data['data/eps-revision-velocity.json'],m.previous,flag)
    def test_rate_and_time_limits_preserve_unattempted_occurrences(self):
        m=Memory();ns=native(m,N_WORKERS=1);calls=[]
        def fetch(*a):calls.append(a);return [capture(stamp=datetime.now(timezone.utc).isoformat()),{'endpoint':'grades','status':'rate_limited'}]
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

    def test_original_auth_error_is_generic_and_later_windows_are_attempted(self):
        raw=(ROOT/'tests/fixtures/pre-eps-transport-lambda_function.py.txt').read_bytes();tree=ast.parse(raw)
        m=Memory();ns=native(m,N_WORKERS=1)
        functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_eps_') or n.name=='lambda_handler')]
        exec(compile(ast.Module(body=functions,type_ignores=[]),'<complete original EPS functions>','exec'),ns)
        error=urllib.error.HTTPError('https://invalid/?secret',401,'never expose',{},None)
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:(_ for _ in ()).throw(error))):out=ns['_eps_fetch']('TEST','quote')
        self.assertEqual(out['status'],'unavailable');calls=[]
        ns['_eps_company']=lambda symbol:(calls.append(symbol) or [out] if symbol=='TEST' else calls.append(symbol) or [capture()])
        ns['lambda_handler']();self.assertEqual(len(calls),2);self.assertTrue(any(not b.closed for b in m.bodies))
        for body in m.bodies:body.close()

    def test_authorization_stops_later_windows_and_all_failed_run_keeps_original(self):
        for code in (401,403):
            ns=native();error=urllib.error.HTTPError('https://invalid/?secret',code,'never expose',{},None)
            with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:(_ for _ in ()).throw(error))) as mock:
                out=ns['_eps_fetch']('TEST','quote');self.assertEqual(mock.call_count,1)
            self.assertEqual(out['status'],'authorization_unavailable');self.assertNotIn('secret',json.dumps(out));m=Memory();ns=native(m,N_WORKERS=1);calls=[]
            ns['_eps_company']=lambda ticker:(calls.append(ticker) or [out])
            with self.assertRaises(ValueError):ns['lambda_handler']()
            self.assertEqual(len(calls),1);self.assertEqual(m.data['data/eps-revision-velocity.json'],m.previous);self.assertEqual(m.writes,[]);self.assertTrue(all(b.closed for b in m.bodies))

    def test_mixed_first_window_is_preserved_and_remaining_occurrences_are_unattempted(self):
        m=Memory();ns=native(m,N_WORKERS=2,MAX_TICKERS=4);calls=[]
        def company(ticker):
            calls.append(ticker);return [capture([{'symbol':ticker,'marketCap':1e9}],'quote'),{'endpoint':'analyst-estimates','status':'authorization_unavailable','http_status':403}]
        ns['_eps_company']=company;ns['lambda_handler']();p=strict(m.data['data/eps-revision-velocity.json'])
        self.assertEqual(len(calls),2);self.assertEqual(len(p['request_records']),4)
        self.assertEqual([r['acquisitions'][0]['status'] for r in p['request_records']],['received','received','not_attempted_runtime_rate_or_size_limit','not_attempted_runtime_rate_or_size_limit'])
        self.assertEqual(p['version'],'1.2.0');self.assertTrue(p['transport']['stop_after_authorization_error']);self.assertEqual(p['source_imports'],ns['_eps_import_identity']());self.assertTrue(all(b.closed for b in m.bodies))

    def test_every_s3_body_closes_on_success_bounds_metadata_and_read_failure(self):
        for kind in ('ok','oversize','metadata','read_failure'):
            class Body(BytesIO):
                def read(self,*a):
                    if kind=='read_failure':raise OSError('synthetic read failure')
                    return super().read(*a)
            body=Body(b'whole');obj={'Body':body,'ContentLength':4 if kind=='metadata' else 5,'ETag':'identity'}
            ns=native(S3=SimpleNamespace(get_object=lambda **k:obj))
            if kind=='ok':self.assertEqual(ns['_eps_object']('declared',5),(b'whole','identity'))
            else:
                with self.assertRaises((ValueError,OSError)):ns['_eps_object']('declared',2 if kind=='oversize' else 5)
            self.assertTrue(body.closed)

    def test_actual_bundled_import_identity_is_not_a_source_tree_assumption(self):
        fn=next(n for n in ast.parse((SRC/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='_eps_import_identity')
        with tempfile.TemporaryDirectory() as td:
            folder=Path(td);raw=b'complete bundled helper';(folder/'managed_secret.py').write_bytes(raw)
            ns={'Path':Path,'hashlib':hashlib,'__file__':str(folder/'lambda_function.py')};exec(compile(ast.Module(body=[fn],type_ignores=[]),'<actual import identity>','exec'),ns)
            self.assertEqual(ns['_eps_import_identity'](),{'managed_secret.py':{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}})

    def baseline_run(self,memory,eps,**options):
        # Windows wall-clock resolution can give consecutive synthetic runs the
        # same timestamp. Advance an isolated clock; retain production ordering.
        prior=clock(strict(memory.data['data/eps-revision-velocity.json']).get('generated_at'))
        class RunClock:
            value=(prior or clock(TODAY+'T01:00:00Z'))+timedelta(seconds=1)
            @classmethod
            def now(cls,tz):
                cls.value+=timedelta(microseconds=1);return cls.value
        run_context=options.pop('run_context',None)
        ns=native(memory,N_WORKERS=1,datetime=RunClock,**options);calls=[]
        def company(ticker):
            calls.append(ticker);group=[capture([{'symbol':ticker,'marketCap':1e9}],'quote',RunClock.now(timezone.utc).isoformat())]
            if eps is None:group.append({'endpoint':'analyst-estimates','status':'rate_limited'})
            else:
                values=forecasts(eps);values[0]['symbol']=ticker
                group.append(capture(values,'analyst-estimates',RunClock.now(timezone.utc).isoformat()))
                group.append({'endpoint':'grades','status':'rate_limited'})
            return group
        ns['_eps_company']=company;ns['lambda_handler'](context=run_context)
        return strict(memory.data['data/eps-revision-velocity.json']),ns,calls

    def test_retained_baseline_recovers_across_a_partial_failed_run_without_refreshing_old_values(self):
        m=Memory();first,_,_=self.baseline_run(m,2);descriptor=first['estimate_baselines']['entries'][0]
        second,_,_=self.baseline_run(m,None)
        self.assertEqual(second['estimate_baselines'],first['estimate_baselines']);self.assertEqual(second['request_records'][0]['estimate_observations'],[])
        self.assertEqual(second['request_records'][0]['comparison_source']['status'],'not_needed_no_current_estimate')
        third,_,_=self.baseline_run(m,3);row=third['request_records'][0];change=row['same_target_comparisons'][0]
        self.assertEqual(change['eps_change'],1);self.assertEqual(change['eps_change_pct_positive_base'],50)
        self.assertEqual(change['prior_received_at'],descriptor['received_at']);self.assertEqual(row['comparison_source']['baseline'],descriptor)
        self.assertEqual(third['request_records'][1]['acquisitions'][0]['status'],'not_attempted_runtime_rate_or_size_limit')
        self.assertEqual(sum('/history/' in k for k in m.data),4);self.assertEqual(sum('/sources/' in k for k in m.data),2)
        self.assertTrue(all(b.closed for b in m.bodies));self.assertFalse(third['calls_eligible'])

    def test_catalogue_preserves_retired_tickers_and_reuses_their_own_original_on_return(self):
        m=Memory();first,_,_=self.baseline_run(m,2);test_ref=first['estimate_baselines']['entries'][0]
        m.data['data/universe.json']=b'{"stocks":[{"symbol":"SECOND"}]}';m.data['screener/data.json']=b'{"rows":[]}'
        second,_,_=self.baseline_run(m,10,SP500_BACKUP=[])
        self.assertEqual([e['ticker'] for e in second['estimate_baselines']['entries']],['SECOND','TEST'])
        self.assertEqual(second['estimate_baselines']['entries'][1],test_ref)
        m.data['data/universe.json']=b'{"stocks":[{"symbol":"TEST"}]}'
        third,_,_=self.baseline_run(m,3,SP500_BACKUP=[])
        self.assertEqual(third['request_records'][0]['same_target_comparisons'][0]['eps_change'],1)
        self.assertEqual(third['request_records'][0]['comparison_source']['baseline'],test_ref)

    def test_failed_prior_source_read_withholds_change_and_keeps_the_expected_reference(self):
        m=Memory();first,_,_=self.baseline_run(m,2);descriptor=first['estimate_baselines']['entries'][0];key=descriptor['original_ref']['key'];real_get=m.get_object
        def get(**kw):
            if kw['Key']==key:raise Error('AccessDenied')
            return real_get(**kw)
        m.get_object=get;third,_,_=self.baseline_run(m,3);row=third['request_records'][0]
        self.assertEqual(row['estimate_observations'][0]['values']['epsAvg'],3);self.assertIsNone(row['same_target_comparisons'][0]['eps_change'])
        self.assertEqual(row['comparison_source'],{'status':'unavailable','baseline':descriptor,'error_type':'Error'})
        self.assertNotEqual(third['estimate_baselines']['entries'][0]['original_ref'],descriptor['original_ref'])

    def test_malformed_reference_or_future_catalogue_clock_fails_before_any_unrelated_read(self):
        m=Memory();first,_,_=self.baseline_run(m,2)
        for mutate in [lambda p:p['estimate_baselines']['entries'][0]['original_ref'].update(key='private/account.json'),
            lambda p:p['estimate_baselines']['entries'][0]['original_ref'].update(bytes=True),
            lambda p:p['estimate_baselines']['entries'][0].update(received_at='2099-01-01T00:00:00Z'),
            lambda p:p['estimate_baselines']['entries'].append(deepcopy(p['estimate_baselines']['entries'][0]))]:
            q=deepcopy(first);mutate(q);m.data['data/eps-revision-velocity.json']=json.dumps(q).encode();m.reads=[];m.writes=[]
            ns=native(m);ns['_eps_company']=lambda *a:self.fail('No provider request after invalid prior reference')
            with self.assertRaises(ValueError):ns['lambda_handler']()
            self.assertEqual(m.reads,['data/eps-revision-velocity.json']);self.assertEqual(m.writes,[])

    def test_immutable_source_collision_or_corruption_never_overwrites_current_head(self):
        for failure in ('corrupt','write'):
            m=Memory();before=m.previous;get=m.get_object;put=m.put_object
            def get_source(**kw):
                obj=get(**kw)
                if '/sources/' in kw['Key']:
                    obj['Body'].close();body=BytesIO(b'changed');m.bodies.append(body);obj.update(Body=body,ContentLength=7)
                return obj
            def put_source(**kw):
                if '/sources/' in kw['Key']:raise Error('AccessDenied')
                return put(**kw)
            if failure=='corrupt':m.get_object=get_source
            else:m.put_object=put_source
            with self.assertRaises((ValueError,Error)):self.baseline_run(m,2)
            self.assertEqual(m.data['data/eps-revision-velocity.json'],before);self.assertTrue(all(b.closed for b in m.bodies))

    def test_source_reader_rejects_hash_length_and_path_before_or_after_bounded_read_as_appropriate(self):
        a=capture();d=estimate_descriptor('TEST',a);raw=base64.b64decode(a['original_base64']);self.assertEqual(estimate_source_envelope(d,raw),a)
        for body in (raw[:-1],b'x'*len(raw)):
            with self.assertRaises(ValueError):estimate_source_envelope(d,body)
        m=Memory();ns=native(m);bad=deepcopy(d);bad['original_ref']['key']='data/another-engine.json'
        with self.assertRaises(ValueError):ns['_eps_read_estimate_source'](bad)
        self.assertEqual(m.reads,[])

    def test_received_empty_array_replaces_baseline_without_fabricating_a_zero_change(self):
        m=Memory();first,_,_=self.baseline_run(m,2);ns=native(m,N_WORKERS=1)
        ns['_eps_company']=lambda ticker:[capture([{'symbol':ticker,'marketCap':1e9}],'quote',datetime.now(timezone.utc).isoformat()),
            capture([],'analyst-estimates',datetime.now(timezone.utc).isoformat()),{'endpoint':'grades','status':'rate_limited'}]
        ns['lambda_handler']();empty=strict(m.data['data/eps-revision-velocity.json']);d=empty['estimate_baselines']['entries'][0]
        self.assertEqual(d['original_ref']['bytes'],2);self.assertEqual(m.data[d['original_ref']['key']],b'[]')
        third,_,_=self.baseline_run(m,3);self.assertIsNone(third['request_records'][0]['same_target_comparisons'][0]['eps_change'])

    def test_original_inline_prior_can_bootstrap_without_scanning_history(self):
        m=Memory();first,_,_=self.baseline_run(m,2);first.pop('estimate_baselines');first['version']='1.1.1'
        for row in first['request_records']:row.pop('comparison_source')
        m.data['data/eps-revision-velocity.json']=json.dumps(first).encode();owned=[k for k in m.data if '/sources/' in k]
        for key in owned:del m.data[key]
        m.reads=[];third,_,_=self.baseline_run(m,3)
        self.assertEqual(third['request_records'][0]['same_target_comparisons'][0]['eps_change'],1)
        self.assertEqual(sum('/sources/' in k for k in m.data),2)
        self.assertTrue(all(k in ('data/eps-revision-velocity.json','data/universe.json','screener/data.json') or k.startswith(('data/eps-revision-velocity/history/','data/eps-revision-velocity/sources/')) for k in m.reads))

    def test_complete_predecessor_reproduces_comparison_loss_after_failed_run(self):
        original_native=native
        tree=ast.parse((ROOT/'tests/fixtures/pre-eps-baselines-lambda_function.py.txt').read_bytes())
        functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_eps_') or n.name=='lambda_handler')]
        def predecessor(*args,**kw):
            ns=original_native(*args,**kw);identity=ns['_eps_import_identity']
            exec(compile(ast.Module(body=functions,type_ignores=[]),'<complete predecessor>','exec'),ns)
            ns['_eps_import_identity']=identity;return ns
        m=Memory()
        with patch.object(sys.modules[__name__],'native',side_effect=predecessor):
            self.baseline_run(m,2);self.baseline_run(m,None);third,_,_=self.baseline_run(m,3)
        self.assertIsNone(third['request_records'][0]['same_target_comparisons'][0]['eps_change'])
        self.assertEqual(sum('/history/' in k for k in m.data),4)

    def test_retained_baseline_does_not_bypass_issuer_currency_basis_or_target_gates(self):
        original_forecasts=forecasts
        for changes in ({'cik':'2'},{'reportedCurrency':'JPY'},{'epsBasis':'different'},{'date':'2028-12-31'}):
            m=Memory();self.baseline_run(m,2);self.baseline_run(m,None)
            def changed(*args,**kw):
                values=original_forecasts(*args,**kw);values[0].update(changes);return values
            with patch.object(sys.modules[__name__],'forecasts',side_effect=changed):third,_,_=self.baseline_run(m,3)
            self.assertEqual(third['request_records'][0]['comparison_source']['status'],'received')
            self.assertIsNone(third['request_records'][0]['same_target_comparisons'][0]['eps_change'])

    def test_archive_reserve_preserves_head_and_comparison_reserve_withholds_only_change(self):
        m=Memory();self.baseline_run(m,2);before=m.data['data/eps-revision-velocity.json'];calls=[]
        def remaining():calls.append(1);return 60000 if len(calls)==1 else 20000
        with self.assertRaisesRegex(ValueError,'archival exceeds original reserve'):
            self.baseline_run(m,3,run_context=SimpleNamespace(get_remaining_time_in_millis=remaining))
        self.assertEqual(m.data['data/eps-revision-velocity.json'],before)
        remaining_ms=[60000];put=m.put_object
        def put_then_reserve(**kw):
            put(**kw)
            if '/sources/' in kw['Key']:remaining_ms[0]=20000
        m.put_object=put_then_reserve
        third,_,_=self.baseline_run(m,3,run_context=SimpleNamespace(get_remaining_time_in_millis=lambda:remaining_ms[0]))
        row=third['request_records'][0];self.assertEqual(row['comparison_source']['status'],'not_read_runtime_reserve')
        self.assertIsNone(row['same_target_comparisons'][0]['eps_change']);self.assertEqual(row['estimate_observations'][0]['values']['epsAvg'],3)

    def test_catalogue_bound_never_silently_drops_retired_entries(self):
        m=Memory();self.baseline_run(m,2);before=m.data['data/eps-revision-velocity.json']
        m.data['data/universe.json']=b'{"stocks":[{"symbol":"SECOND"}]}';m.data['screener/data.json']=b'{"rows":[]}'
        with patch.object(sys.modules['eps_observations'],'BASELINE_LIMIT',1):
            # Use a prior catalogue with the matching declared capacity, so the
            # failure is at the new complete merge, not prior metadata parsing.
            p=strict(before);p['estimate_baselines']['maximum_entries']=1;m.data['data/eps-revision-velocity.json']=json.dumps(p).encode();saved=m.data['data/eps-revision-velocity.json']
            with self.assertRaisesRegex(ValueError,'exceeds declared bound'):self.baseline_run(m,3,SP500_BACKUP=[])
            self.assertEqual(m.data['data/eps-revision-velocity.json'],saved)


if __name__=='__main__':unittest.main(verbosity=2)
