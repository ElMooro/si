"""Native observation/archive regressions; synthetic public provider bytes, no network/AWS."""
from pathlib import Path
from datetime import datetime,timezone,timedelta,date
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
import ast,base64,hashlib,json,sys,time,tempfile,unittest,urllib.request,urllib.error,urllib.parse
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-estimate-revisions/source';sys.path.insert(0,str(SRC))
from estimate_observations import CONTRACT,number,strict,day,clock,envelope,original,projection,compare,dossier,calendar_capture,calendar_original,calendar_projection
NOW=datetime.now(timezone.utc).date().isoformat()


def rows(eps=2,target='2030-12-31',currency='USD'):
    return [{'symbol':'TEST','date':target,'cik':'0001','reportedCurrency':currency,'epsBasis':'reported_adjusted',
             'epsAvg':eps,'epsLow':0,'epsHigh':20,'numAnalystsEps':10}]


def acquisition(values=None,stamp=None):
    return envelope(json.dumps(rows() if values is None else values).encode(),stamp or NOW+'T01:00:00Z')


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self,previous=b'{"version":"3.1.0","top_picks":[]}'):
        self.data={} if previous is None else {'data/estimate-revisions.json':previous};self.reads=[];self.writes=[];self.bodies=[];self.denied=False;self.race=False;self.corrupt=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and '/history/' in key:raw=b'wrong'
        body=BytesIO(raw);self.bodies.append(body)
        return {'Body':body,'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if key=='data/estimate-revisions.json' and self.race:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    scope={'S3':memory or Memory(),'BUCKET':'fixture','OUT_KEY':'data/estimate-revisions.json','FMP_KEY':'fixture-secret-only',
           'HORIZON_DAYS':75,'MIN_IMPORTANCE':2,'FMP_SEED_CAP':280,'CONTRACT':CONTRACT,'strict':strict,'number':number,'day':day,'clock':clock,'envelope':envelope,'dossier':dossier,
           'calendar_capture':calendar_capture,'calendar_projection':calendar_projection,'_calendar_key':lambda:'calendar-fixture-secret',
           'datetime':datetime,'timezone':timezone,'ThreadPoolExecutor':ThreadPoolExecutor,'hashlib':hashlib,'json':json,'time':time,'urllib':urllib,
           'fetch_calendar':lambda **kw:[{'ticker':'TEST','date':NOW,'importance':2,'fiscal_period':'Q1'}],**extra}
    tree=ast.Module(body=[n for n in ast.parse((SRC/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_estimate_') or n.name=='lambda_handler')],type_ignores=[])
    exec(compile(tree,'<actual native observation functions>','exec'),scope)
    scope['_actual_calendar_fetch']=scope['_estimate_calendar_fetch']
    scope['_estimate_calendar_fetch']=lambda:calendar_capture(json.dumps({'status':'OK','results':scope['fetch_calendar']()}).encode(),datetime.now(timezone.utc).isoformat(),NOW,(date.fromisoformat(NOW)+timedelta(days=75)).isoformat(),2)
    scope['_estimate_source_identity']=lambda:{p.name:{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()} for p in
        (SRC/'lambda_function.py',SRC/'estimate_observations.py',ROOT/'aws/shared/benzinga.py',ROOT/'aws/shared/managed_secret.py') for raw in [p.read_bytes()]}
    return scope


class Tests(unittest.TestCase):
    def test_all_bytes_unknown_fields_and_every_occurrence_are_retained(self):
        a=acquisition(rows()+[None,{'unknown':'whole'}]+rows());p=projection('TEST',a,NOW)
        self.assertEqual(len(p),4);self.assertEqual([r['raw'] for r in p],original(a));self.assertEqual(p[1]['measurement_status'],'unqualified_record')
    def test_zero_missing_boolean_nonfinite_and_huge_numbers_remain_distinct(self):
        self.assertEqual(number(0),0)
        for v in [True,False,'0','',float('inf'),10**1000,None]:self.assertIsNone(number(v))
    def test_past_targets_stay_past_and_calendar_period_is_not_inferred(self):
        p=projection('TEST',acquisition(rows(target='2020-12-31')),NOW)[0]
        self.assertEqual(p['target_status'],'past_target');self.assertEqual(p['target_period_end'],'2020-12-31');self.assertEqual(p['requested_period'],'annual');self.assertIsNone(p['estimate_strength'])
    def test_only_same_target_issuer_unit_basis_ordered_snapshot_can_compare(self):
        old=projection('TEST',acquisition(rows(2),NOW+'T00:00:00Z'),NOW);current=projection('TEST',acquisition(rows(3)),NOW)
        result=compare(current,old)[0];self.assertEqual(result['eps_change'],1);self.assertEqual(result['eps_change_pct_positive_base'],50)
        for field,value in [('target_period_end','2031-12-31'),('reported_currency','JPY'),('eps_basis','GAAP'),('reported_cik','2'),('reported_currency',None),('received_at',NOW+'T00:00:00Z')]:
            changed=deepcopy(current);changed[0][field]=value;self.assertIsNone(compare(changed,old)[0]['eps_change'],field)
    def test_zero_and_negative_baselines_have_absolute_change_not_infinite_percent(self):
        for value in [0,-2]:
            a=rows(value);a[0]['epsLow']=-5
            old=projection('TEST',acquisition(a,NOW+'T00:00:00Z'),NOW);new=projection('TEST',acquisition(rows(2)),NOW)
            result=compare(new,old)[0];self.assertEqual(result['eps_change'],2-value);self.assertIsNone(result['eps_change_pct_positive_base'])
    def test_duplicate_targets_do_not_select_one_convenient_baseline(self):
        old=projection('TEST',acquisition(rows(2)*2,NOW+'T00:00:00Z'),NOW);new=projection('TEST',acquisition(rows(3)),NOW)
        self.assertIsNone(compare(new,old)[0]['eps_change']);self.assertIsNone(compare(new*2,old[:1])[0]['eps_change'])
    def test_wrong_symbols_invalid_dates_counts_ranges_and_clocks_do_not_qualify(self):
        for field,value in [('symbol','OTHER'),('date','2030-02-30'),('numAnalystsEps',-1),('numAnalystsEps',1.5),('epsLow',3),('epsHigh',1)]:
            a=rows();a[0][field]=value;p=projection('TEST',acquisition(a),NOW)[0];self.assertEqual(p['measurement_status'],'unqualified_record',field)
        with self.assertRaises(ValueError):envelope(b'[]','2026-01-01')
    def test_whole_original_tampering_and_unsafe_json_fail(self):
        a=acquisition();a['original_bytes']+=1
        with self.assertRaises(ValueError):original(a)
        for raw in [b'{"x":1,"x":2}',b'{"x":NaN}',b'[1e999]',b'[1e-999]',b'\xff']:
            with self.assertRaises((ValueError,UnicodeError)):strict(raw)
        with self.assertRaises(ValueError):acquisition(rows()*7)
    def test_original_byte_budget_declares_unattempted_requests_without_truncating_responses(self):
        calendar=[{'ticker':'TEST','date':NOW,'importance':2} for _ in range(30)]
        values=rows();values[0]['complete_extra_field']='x'*500000
        a=acquisition(values);n=native(fetch_calendar=lambda **kw:calendar);calls=[]
        n['_estimate_fetch']=lambda symbol:(calls.append(symbol) or a)
        n['lambda_handler']();p=strict(n['S3'].data['data/estimate-revisions.json'])
        self.assertEqual(len(calls),10);self.assertEqual(len(p['request_records']),30)
        self.assertEqual(len(p['request_records'][0]['observations'][0]['raw']['complete_extra_field']),500000)
    def test_native_keeps_calendar_and_duplicate_requests_without_scores_private_state_or_notifications(self):
        memory=Memory();calendar=[{'ticker':'TEST','date':NOW,'importance':2,'fiscal_period':'Q1'},{'ticker':'TEST','date':NOW,'importance':2,'fiscal_period':'Q2'},None]
        n=native(memory,fetch_calendar=lambda **kw:calendar);n['_estimate_fetch']=lambda symbol:acquisition() if symbol else {'status':'invalid_symbol_not_requested'}
        n['lambda_handler']();p=strict(memory.data['data/estimate-revisions.json'])
        self.assertEqual(calendar_original(p['calendar_acquisition'])['results'],calendar);self.assertEqual(len(p['request_records']),2);self.assertEqual(p['n_estimate_observations'],2)
        self.assertEqual(p['calendar_evidence']['excluded'][0]['source_index'],2)
        self.assertEqual(p['direction_map'],{});self.assertEqual(p['top_picks'],[]);self.assertIsNone(p['call']);self.assertFalse(p['private_state_read_or_written'])
        self.assertTrue(all(k=='data/estimate-revisions.json' or k.startswith('data/estimate-revisions/history/') for k in memory.reads+memory.writes))
        self.assertEqual(sum('/history/' in k for k in memory.data),2)
    def test_cap_and_budget_retain_every_unselected_and_unattempted_occurrence(self):
        calendar=[{'ticker':'TEST','date':NOW,'importance':2} for _ in range(300)];n=native(fetch_calendar=lambda **kw:calendar);calls=[]
        n['_estimate_fetch']=lambda symbol:(calls.append(symbol) or acquisition())
        times=iter([30000,10000]);n['lambda_handler'](context=SimpleNamespace(get_remaining_time_in_millis=lambda:next(times,10000)))
        p=strict(n['S3'].data['data/estimate-revisions.json']);self.assertEqual(len(calls),10);self.assertEqual(len(p['request_records']),280);self.assertEqual(len(p['not_selected_calendar_indices']),20)
        self.assertEqual(sum(r['acquisition']['status'].startswith('not_attempted') for r in p['request_records']),270)
    def test_rate_limit_stops_remaining_windows_and_never_retries(self):
        calendar=[{'ticker':'TEST','date':NOW,'importance':2} for _ in range(30)];n=native(fetch_calendar=lambda **kw:calendar);calls=[]
        def acquire(symbol):
            calls.append(symbol);return {'status':'rate_limited'} if len(calls)==1 else acquisition()
        n['_estimate_fetch']=acquire;n['lambda_handler']();self.assertEqual(len(calls),10)
    def test_failed_calendar_or_all_failed_acquisitions_preserve_original(self):
        for calendar,status in [([],None),([{'ticker':'TEST','date':NOW,'importance':2}],'unavailable')]:
            memory=Memory();before=dict(memory.data);n=native(memory,fetch_calendar=lambda **kw:calendar);n['_estimate_fetch']=lambda s:{'status':status}
            with self.assertRaises(ValueError):n['lambda_handler']()
            self.assertEqual(memory.data,before);self.assertEqual(memory.writes,[])
    def test_archive_corruption_or_concurrent_publish_preserves_current(self):
        for field in ['corrupt','race']:
            memory=Memory();setattr(memory,field,True);before=memory.data['data/estimate-revisions.json'];n=native(memory);n['_estimate_fetch']=lambda s:acquisition()
            with self.assertRaises((ValueError,Error)):n['lambda_handler']()
            self.assertEqual(memory.data['data/estimate-revisions.json'],before)
    def test_missing_prior_allowed_but_access_denied_and_malformed_prior_fail(self):
        n=native(Memory(None));self.assertEqual(n['_estimate_previous'](),(None,None,None))
        memory=Memory();memory.denied=True
        with self.assertRaises(Error):native(memory)['_estimate_previous']()
        with self.assertRaises(ValueError):native(Memory(b'{bad'))['_estimate_previous']()
    def test_native_endpoint_scope_zero_originals_and_error_redaction(self):
        n=native();raw=json.dumps(rows(0)).encode()
        opener=SimpleNamespace(open=lambda *args,**kw:BytesIO(raw))
        with patch.object(urllib.request,'build_opener',return_value=opener),patch.object(opener,'open',return_value=BytesIO(raw)) as mock:
            result=n['_estimate_fetch']('TEST')
        self.assertEqual(original(result)[0]['epsAvg'],0);self.assertEqual(mock.call_count,1)
        request=mock.call_args.args[0];self.assertEqual(request.full_url,'https://financialmodelingprep.com/stable/analyst-estimates?symbol=TEST&period=annual&limit=6')
        self.assertNotIn(n['FMP_KEY'],request.full_url);self.assertEqual(request.get_header('Apikey'),n['FMP_KEY'])
        error=urllib.error.HTTPError('https://invalid/?secret=DO_NOT_LEAK',429,'secret text',{},None)
        with patch.object(urllib.request,'build_opener',return_value=opener),patch.object(opener,'open',side_effect=error) as mock:result=n['_estimate_fetch']('TEST')
        self.assertEqual(mock.call_count,1);self.assertEqual(result['status'],'rate_limited');self.assertNotIn('secret',json.dumps(result))
        with patch.object(urllib.request,'build_opener',return_value=opener),patch.object(opener,'open',return_value=BytesIO(b'[{"echo":"fixture-secret-only"}]')):result=n['_estimate_fetch']('TEST')
        self.assertEqual(result['status'],'unavailable');self.assertNotIn('original_base64',result)

    def test_preserved_original_exposes_credential_query_and_short_echo_gap(self):
        raw=(ROOT/'tests/fixtures/pre-estimate-transport-lambda_function.py.txt').read_bytes()
        audit=json.loads((ROOT/'docs/audit/2026-09-28/estimate-transport-audit.json').read_bytes())
        self.assertEqual(len(raw),audit['original_native_bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),audit['original_native_sha256'])
        fn=next(x for x in ast.parse(raw).body if isinstance(x,ast.FunctionDef) and x.name=='_estimate_fetch');scope=native(FMP_KEY='abc')
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'<original FMP transport>','exec'),scope)
        with patch.object(urllib.request,'urlopen',return_value=BytesIO(b'[{"unknown":"abc"}]')) as mock:out=scope['_estimate_fetch']('TEST')
        self.assertIn('apikey=abc',mock.call_args.args[0].full_url);self.assertEqual(original(out)[0]['unknown'],'abc')

    def test_redirect_auth_short_echo_size_and_invalid_symbol_never_retry(self):
        ns=native(FMP_KEY='abc');seen=[]
        def factory(*handlers):
            handler=handlers[0]
            self.assertIsNone(handler.redirect_request(None,None,302,'',{},'https://outside.invalid/collect'))
            def open(req,timeout):seen.append(req);return BytesIO(b'[{"unknown":"abc"}]')
            return SimpleNamespace(open=open)
        with patch.object(urllib.request,'build_opener',factory):out=ns['_estimate_fetch']('TEST')
        self.assertEqual(len(seen),1);self.assertEqual(out['status'],'unavailable');self.assertNotIn('original_base64',out)
        for code in (401,403,429):
            error=urllib.error.HTTPError('redacted',code,'never expose',{},None)
            with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:(_ for _ in ()).throw(error))) as mock:
                out=ns['_estimate_fetch']('TEST');self.assertEqual(mock.call_count,1)
            self.assertEqual(out['http_status'],code);self.assertEqual(out['status'],'rate_limited' if code==429 else 'authorization_unavailable')
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:BytesIO(b'x'*(1024*1024+1)))):
            self.assertEqual(ns['_estimate_fetch']('TEST')['status'],'unavailable')
        with patch.object(urllib.request,'build_opener',side_effect=AssertionError('No network setup')):
            self.assertEqual(ns['_estimate_fetch']('BAD?credential=x')['status'],'invalid_symbol_not_requested')

    def test_auth_failure_stops_later_windows_and_complete_native_compiler_is_bound(self):
        calendar=[{'ticker':'TEST','date':NOW,'importance':2} for _ in range(30)];ns=native(fetch_calendar=lambda **kw:calendar);calls=[]
        def acquire(symbol):
            calls.append(symbol);return {'status':'authorization_unavailable','http_status':403} if len(calls)==1 else acquisition()
        ns['_estimate_fetch']=acquire;ns['lambda_handler']();p=strict(ns['S3'].data['data/estimate-revisions.json'])
        self.assertEqual(len(calls),10);self.assertEqual(p['source_files'],ns['_estimate_source_identity']());self.assertEqual(len(p['source_files']),4)
        self.assertEqual(p['version'],'3.3.0');self.assertFalse(p['estimate_transport']['follow_redirects'])
        self.assertTrue(ns['S3'].bodies);self.assertTrue(all(body.closed for body in ns['S3'].bodies))

    def test_compiler_identity_uses_all_four_actual_package_files(self):
        fn=next(n for n in ast.parse((SRC/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='_estimate_source_identity')
        with tempfile.TemporaryDirectory() as td:
            folder=Path(td);names=('lambda_function.py','estimate_observations.py','benzinga.py','managed_secret.py')
            for name in names:(folder/name).write_bytes(name.encode())
            scope={'Path':Path,'hashlib':hashlib,'__file__':str(folder/'lambda_function.py')};exec(compile(ast.Module(body=[fn],type_ignores=[]),'<package identity>','exec'),scope)
            out=scope['_estimate_source_identity']();self.assertEqual(set(out),set(names))
            for name in names:self.assertEqual(out[name],{'bytes':len(name),'sha256':hashlib.sha256(name.encode()).hexdigest()})

    def test_original_calendar_loses_fields_and_invents_a_session_for_ten_am(self):
        source=(ROOT/'tests/fixtures/pre-estimate-calendar-benzinga.py.txt').read_bytes()
        fn=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='fetch_calendar')
        row={'ticker':'TEST','date':NOW,'time':'10:00:00','importance':2,'currency':'JPY','unknown':'retained only in original'}
        scope={'date':date,'_get':lambda *a,**k:{'status':'OK','results':[row]}}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'<complete original calendar helper>','exec'),scope)
        old=scope['fetch_calendar']();self.assertEqual(old[0]['session'],'BMO');self.assertNotIn('time',old[0]);self.assertNotIn('unknown',old[0])
        a=calendar_capture(json.dumps({'status':'OK','results':[row]}).encode(),NOW+'T12:00:00Z',NOW,NOW,2)
        new=calendar_projection(a)['rows'][0];self.assertEqual(new['time'],'10:00:00');self.assertEqual(new['session'],'—');self.assertEqual(new['reported_currency'],'JPY')
        self.assertEqual(calendar_original(a)['results'],[row])

    def test_calendar_every_occurrence_exclusion_zero_and_continuation_are_replayable(self):
        row={'ticker':'TEST','date':NOW,'time':'16:00:00','importance':2,'estimated_eps':0,'actual_eps':0,'currency':'USD','unknown':'whole'}
        values=[row,deepcopy(row),None,{**row,'importance':0},{**row,'importance':False},{**row,'importance':'2'},
                {**row,'date':'2000-01-01'},{**row,'date':'not a date'},{**row,'importance':6}]
        raw=json.dumps({'status':'OK','results':values,'next_url':'https://outside.invalid/unrequested','request_id':'literal'}).encode()
        a=calendar_capture(raw,NOW+'T12:00:00Z',NOW,NOW,2);p=calendar_projection(a)
        self.assertEqual(base64.b64decode(a['original_base64']),raw);self.assertEqual(calendar_original(a)['results'],values)
        self.assertEqual([r['source_index'] for r in p['rows']],[0,1]);self.assertEqual([r['source_index'] for r in p['excluded']],list(range(2,9)))
        self.assertEqual(p['rows'][0]['estimated_eps'],0);self.assertEqual(p['pagination_status'],'next_page_unrequested');self.assertEqual(p['additional_requests'],0)
        self.assertFalse(p['provider_universe_complete']);self.assertFalse(p['market_session_qualified'])
        zero=calendar_capture(json.dumps({'status':'OK','results':[{**row,'importance':0}]}).encode(),NOW+'T12:00:00Z',NOW,NOW,0)
        self.assertEqual(calendar_projection(zero)['rows'][0]['importance'],0)

    def test_calendar_malformed_incomplete_clock_and_hash_are_rejected(self):
        valid={'status':'OK','results':[{'ticker':'TEST','date':NOW,'importance':2}]}
        a=calendar_capture(json.dumps(valid).encode(),NOW+'T12:00:00Z',NOW,NOW,2)
        for field,value in [('original_bytes',False),('original_sha256','bad'),('received_at',NOW),('requested_limit',True),('sort','importance.desc'),('min_importance',True),('end_date','2000-01-01')]:
            q=deepcopy(a);q[field]=value
            with self.assertRaises(ValueError,msg=field):calendar_projection(q)
        for raw in [b'{"status":"OK","status":"OK","results":[]}',b'{"status":"ERROR","results":[]}',b'{"status":"OK","results":null}',
                    json.dumps({**valid,'next_url':False}).encode(),json.dumps({**valid,'results':valid['results']*1001}).encode(),b'\xff']:
            with self.assertRaises((ValueError,UnicodeError)):calendar_capture(raw,NOW+'T12:00:00Z',NOW,NOW,2)

    def test_calendar_actual_transport_is_single_bounded_header_request_and_never_follows_next(self):
        ns=native();body=json.dumps({'status':'OK','results':[{'ticker':'TEST','date':NOW,'importance':2}],
             'next_url':'https://outside.invalid/never-request'}).encode();calls=[]
        def factory(*handlers):
            self.assertIsNone(handlers[0].redirect_request(None,None,302,'',{},'https://outside.invalid/'))
            def open(req,timeout):calls.append((req,timeout));return BytesIO(body)
            return SimpleNamespace(open=open)
        with patch.object(urllib.request,'build_opener',factory):a=ns['_actual_calendar_fetch']()
        self.assertEqual(len(calls),1);req,timeout=calls[0];self.assertEqual(timeout,15)
        url=urllib.parse.urlsplit(req.full_url);self.assertEqual(url.netloc,'api.polygon.io');self.assertEqual(url.path,'/benzinga/v1/earnings')
        query=urllib.parse.parse_qs(url.query);self.assertEqual(query['sort'],['date.asc']);self.assertEqual(query['limit'],['1000']);self.assertNotIn('apiKey',query)
        self.assertEqual(req.get_header('Authorization'),'Bearer calendar-fixture-secret');self.assertEqual(calendar_projection(a)['pagination_status'],'next_page_unrequested')

    def test_calendar_errors_echoes_oversize_and_missing_key_cannot_publish_or_retry(self):
        ns=native();ns['_calendar_key']=lambda:'abc'
        for body in [b'{"status":"OK","results":[],"echo":"abc"}',b'x'*(8*1024*1024+1)]:
            with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:BytesIO(body))) as mock:
                result=ns['_actual_calendar_fetch']();self.assertEqual(mock.call_count,1)
            self.assertEqual(result['status'],'unavailable');self.assertNotIn('original_base64',result);self.assertNotIn('abc',json.dumps(result))
        for code in [301,302,401,403,429,500]:
            err=urllib.error.HTTPError('https://bad/?abc',code,'secret abc',{},None)
            with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:(_ for _ in ()).throw(err))):result=ns['_actual_calendar_fetch']()
            self.assertEqual(result['http_status'],code);self.assertNotIn('abc',json.dumps(result))
            memory=Memory();before=dict(memory.data);writer=native(memory);writer['_estimate_calendar_fetch']=lambda:result
            writer['_estimate_fetch']=lambda s:self.fail('No estimate call after failed calendar')
            with self.assertRaises(ValueError):writer['lambda_handler']()
            self.assertEqual(memory.data,before);self.assertEqual(memory.writes,[])
        ns['_calendar_key']=lambda:''
        with patch.object(urllib.request,'build_opener',side_effect=AssertionError('No request without key')):
            self.assertEqual(ns['_actual_calendar_fetch']()['status'],'credential_unavailable')

    def test_calendar_source_filtering_and_full_bytes_survive_real_writer_and_archives(self):
        memory=Memory();values=[{'ticker':'TEST','date':NOW,'importance':2,'time':'12:30:00','actual_eps':0},None,
            {'ticker':'LOW','date':NOW,'importance':1}];ns=native(memory,fetch_calendar=lambda:values)
        ns['_estimate_fetch']=lambda s:acquisition();ns['lambda_handler']();p=strict(memory.data['data/estimate-revisions.json'])
        self.assertEqual(calendar_original(p['calendar_acquisition'])['results'],values);self.assertTrue(p['calendar_original_http_retained'])
        self.assertEqual(p['calendar_rows'],calendar_projection(p['calendar_acquisition'])['rows']);self.assertEqual(len(p['calendar_evidence']['excluded']),2)
        self.assertEqual(p['calendar_rows'][0]['session'],'—');self.assertEqual(len(p['request_records']),1)
        self.assertEqual(memory.data['data/estimate-revisions/history/'+hashlib.sha256(memory.data['data/estimate-revisions.json']).hexdigest()+'.json'],memory.data['data/estimate-revisions.json'])


if __name__=='__main__':unittest.main(verbosity=2)
