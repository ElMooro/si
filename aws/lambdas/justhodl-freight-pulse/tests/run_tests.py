"""Full native freight fixtures. No provider, AWS, account or production call."""
from pathlib import Path
from datetime import datetime,timedelta,timezone
from io import BytesIO,StringIO
from unittest.mock import patch
from copy import deepcopy
import ast,contextlib,hashlib,importlib.util,json,sys,unittest,urllib.parse,urllib.error
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
import freight_store as store
import freight_measurements as m
NOW=datetime(2026,9,27,11,50,tzinfo=timezone.utc)
with patch('boto3.client'),patch('managed_secret.managed_secret',return_value='fixture-fred-key'):
    spec=importlib.util.spec_from_file_location('freight_native_test',SOURCE/'lambda_function.py')
    native=importlib.util.module_from_spec(spec);sys.modules[spec.name]=native;spec.loader.exec_module(native)

class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
class Denied(Exception):response={'Error':{'Code':'AccessDenied'}}
class Conflict(Exception):response={'Error':{'Code':'PreconditionFailed'}}

class Memory:
    def __init__(self):self.data={};self.writes=[];self.reads=[];self.denied=self.truncated=self.conflict=self.corrupt=None
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key==self.denied:raise Denied()
        if key not in self.data:raise Missing()
        raw=self.data[key]
        if key==self.corrupt:raw+=b'!'
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+int(key==self.truncated),'ETag':store.sha(raw),'LastModified':NOW}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body']
        if key==self.denied:raise Denied()
        if key==self.conflict:self.data[key]=b'newer-writer';raise Conflict()
        if key.startswith(store.PRIVATE) or key.startswith(store.ARCHIVE):
            assert kw['IfNoneMatch']=='*'
            if key in self.data:raise Conflict()
        else:
            assert key==store.HEAD and kw['IfMatch']==store.sha(self.data[key])
        self.data[key]=raw;self.writes.append(key);return {}
    def list_objects_v2(self,**kw):
        assert kw['Prefix']==store.ARCHIVE and kw['MaxKeys']==1000
        keys=sorted(k for k in self.data if k.startswith(store.ARCHIVE));offset=int(kw.get('ContinuationToken',0));rows=keys[offset:offset+1000]
        result={'IsTruncated':offset+len(rows)<len(keys),'Contents':[{'Key':k} for k in rows]}
        if result['IsTruncated']:result['NextContinuationToken']=str(offset+len(rows))
        return result

def fixture(archives=48):
    mem=Memory();mem.data={store.HEAD:store.encode({'generated_at':'2026-09-26T11:50:00Z','composite':0,'all_old_fields':list(range(50))}),
       store.GRAPH:store.encode({'sector_etf_proxy':{'Industrials':'XLI'}}),store.BETAS:store.encode({'betas':{}})}
    for i in range(archives):mem.data[store.ARCHIVE+(NOW-timedelta(days=i+1)).date().isoformat()+'.json']=store.encode({'date':(NOW-timedelta(days=i+1)).date().isoformat(),'legacy_unknown':[i,0,None]})
    data={};metas={};calls=[]
    for sid,(key,unit,seasonal,name) in m.PROFILES.items():
        metas[sid]={'seriess':[{'id':sid,'title':name,'units':unit,'frequency_short':'M','seasonal_adjustment_short':seasonal,'last_updated':'2026-09-01 09:00:00-05','observation_start':'2015-01-01','observation_end':'2026-08-01'}]}
        rows=[]
        for i in range(140):
            date=m.month_shift(m.day('2015-01-01'),i).isoformat()
            value=str(100+i) if 'cass' not in key else str((1000+i*3)/1000 if key=='cass_shipments' else (3000+i*7)/1000)
            rows.append({'date':date,'value':value,'realtime_start':'2026-09-27','realtime_end':'2026-09-27'})
        data[sid]={'count':len(rows),'offset':0,'limit':100000,'units':'lin','output_type':1,'observations':rows}
    weekly={'response':{'data':[{'period':(NOW-timedelta(days=9+7*(199-i))).date().isoformat(),'value':str(3000+i),'units':'MBBL/D'} for i in range(200)]}}
    def opener(req,timeout=None):
        url=req if isinstance(req,str) else req.full_url;calls.append(url);u=urllib.parse.urlsplit(url);p=dict(urllib.parse.parse_qsl(u.query))
        packet=(metas if u.path=='/fred/series' else data)[p['series_id']] if u.netloc=='api.stlouisfed.org' else weekly
        raw=store.encode(packet);return store.Response(raw,headers={'Content-Length':str(len(raw))})
    opener.data=data;opener.metas=metas;opener.weekly=weekly
    return mem,calls,opener

class Tests(unittest.TestCase):
    def execute(self,mem,opener,key='fixture-eia-key'):
        native.S3=mem
        with patch.object(native,'_eia_key',return_value=key),contextlib.redirect_stdout(StringIO()):return store.run(native,at=NOW.isoformat(),opener=opener)
    def test_complete_native_sources_input_bytes_history_and_calendar_replay(self):
        mem,calls,opener=fixture();before=dict(mem.data);out=self.execute(mem,opener);self.assertTrue(out['published']);self.assertEqual(len(calls),13)
        p=store.decode(mem.data[store.HEAD]);self.assertEqual(p['portfolio_action'],'WAIT');self.assertFalse(p['sizing_eligible']);self.assertEqual(p['measurement_review']['series_count'],6)
        self.assertEqual(sum(r['returned_rows'] for r in p['measurement_review']['series'].values()),840)
        manifest=store.decode(store.retained(mem,native.BUCKET,p['publication_context']['manifest']))
        for key in store.KEYS:self.assertEqual(store.retained(mem,native.BUCKET,manifest['inputs'][key]['original']),before[key])
        for key,raw in before.items():
            if key!=store.HEAD:self.assertEqual(mem.data[key],raw)
        with contextlib.redirect_stdout(StringIO()):proof=store.replay(native,mem,native.BUCKET,p)
        self.assertEqual(proof['monthly_observations'],840);self.assertEqual(proof['provider_responses'],13);self.assertEqual(len(calls),13)
        self.assertNotIn(b'fixture-fred-key',store.encode(manifest));self.assertNotIn(b'fixture-eia-key',store.encode(manifest))
    def test_offline_replay_requires_no_live_provider_credentials(self):
        mem,calls,opener=fixture();self.execute(mem,opener);p=store.decode(mem.data[store.HEAD])
        with patch.object(native,'FRED_KEY',''),patch.object(native,'_EIA',{}),contextlib.redirect_stdout(StringIO()):
            proof=store.replay(native,mem,native.BUCKET,p)
            self.assertEqual(proof['provider_requests'],0);self.assertEqual(native.FRED_KEY,'')
        self.assertEqual(len(calls),13)

    def test_archive_enumeration_exceeds_500_and_every_old_record_remains(self):
        mem,calls,opener=fixture(1204);old={k:v for k,v in mem.data.items() if k.startswith(store.ARCHIVE)};self.execute(mem,opener);p=store.decode(mem.data[store.HEAD])
        self.assertEqual(p['archive_review']['complete_prior_membership_count'],1204);self.assertEqual(p['lead_vs_port']['n_archived'],500)
        self.assertTrue(all(mem.data[k]==v for k,v in old.items()));self.assertEqual(len([k for k in mem.data if k.startswith(store.ARCHIVE)]),1205)
        with contextlib.redirect_stdout(StringIO()):self.assertEqual(store.replay(native,mem,native.BUCKET,p)['prior_archive_membership'],1204)
    def test_predecessor_missing_denied_malformed_or_truncated_does_not_publish(self):
        for case in ('missing','denied','malformed','duplicate','truncated','future'):
            mem,calls,opener=fixture();old=mem.data[store.HEAD]
            if case=='missing':del mem.data[store.GRAPH]
            if case=='denied':mem.denied=store.BETAS
            if case=='malformed':mem.data[store.GRAPH]=b'{bad'
            if case=='duplicate':mem.data[store.GRAPH]=b'{"x":0,"x":1}'
            if case=='truncated':mem.truncated=store.HEAD
            if case=='future':mem.data[store.HEAD]=store.encode({'generated_at':'2027-01-01T00:00:00Z'});old=mem.data[store.HEAD]
            with self.assertRaises(Exception):self.execute(mem,opener)
            self.assertEqual(calls,[]);self.assertEqual(mem.data[store.HEAD],old);self.assertTrue(all(k.startswith(store.PRIVATE) for k in mem.writes))
    def test_existing_daily_record_is_retained_and_no_provider_is_called(self):
        mem,calls,opener=fixture();key=store.ARCHIVE+NOW.date().isoformat()+'.json';mem.data[key]=b'{"already":0}'
        with self.assertRaisesRegex(store.CaptureError,'Existing research date'):self.execute(mem,opener)
        self.assertEqual(calls,[]);self.assertEqual(mem.data[key],b'{"already":0}')
    def test_http_error_malformed_duplicate_and_length_mismatch_are_retained_not_published(self):
        for raw,code,length in [(b'full rate limit body',429,None),(b'{bad',200,None),(b'{"x":0,"x":1}',200,None),(b'{}',200,'999')]:
            mem,calls,opener=fixture();old=mem.data[store.HEAD]
            def fail(*args,**kw):return store.Response(raw,code,{'Content-Length':length} if length else {})
            with self.assertRaises(Exception):self.execute(mem,fail)
            self.assertIn(raw,mem.data.values());self.assertEqual(mem.data[store.HEAD],old);self.assertTrue(all(k.startswith(store.PRIVATE) for k in mem.writes))
    def test_transport_failure_is_recorded_without_a_retry(self):
        mem,_,_=fixture();calls=[]
        def fail(*args,**kw):calls.append(1);raise urllib.error.URLError('fixture')
        with self.assertRaises(store.CaptureError):self.execute(mem,fail)
        self.assertEqual(calls,[1]);self.assertTrue(any(b'transport_error' in raw for raw in mem.data.values()))
    def test_source_failure_swallowed_by_legacy_function_still_blocks_publication(self):
        mem,calls,opener=fixture();old=mem.data[store.HEAD]
        def fail(req,**kw):
            url=req if isinstance(req,str) else req.full_url
            return store.Response(b'{"error_code":400}',400) if '/observations?' in url else opener(req,**kw)
        with self.assertRaises(store.CaptureError):self.execute(mem,fail)
        self.assertEqual(mem.data[store.HEAD],old);self.assertTrue(all(k.startswith(store.PRIVATE) for k in mem.writes))
    def test_provider_identity_is_credential_free_and_blocks_foreign_targets(self):
        request='https://api.stlouisfed.org/fred/series/observations?series_id=TSIFRGHT&api_key=private&file_type=json&observation_start=2015-01-01'
        self.assertNotIn('private',json.dumps(store.identity(request,25)))
        for url in [request.replace('api.stlouisfed.org','example.com'),request+'&series_id=OTHER',request.replace('TSIFRGHT','UNREVIEWED'),request.replace('https:','http:'),request+'&token=secret']:
            with self.assertRaises(store.CaptureError):store.identity(url,25)
    def test_missing_weekly_credential_stays_explicit_and_does_not_invent_values(self):
        mem,calls,opener=fixture();self.execute(mem,opener,key='');p=store.decode(mem.data[store.HEAD]);self.assertEqual(len(calls),12);self.assertEqual(p['fast_leg']['status'],'UNAVAILABLE')
        with contextlib.redirect_stdout(StringIO()):self.assertEqual(store.replay(native,mem,native.BUCKET,p)['provider_responses'],12)
    def test_conditional_writers_preserve_foreign_values_and_never_rollback(self):
        for where in ('archive','head'):
            mem,calls,opener=fixture();archive=store.ARCHIVE+NOW.date().isoformat()+'.json';target=archive if where=='archive' else store.HEAD;mem.conflict=target
            out=self.execute(mem,opener);self.assertFalse(out['published']);self.assertEqual(mem.data[target],b'newer-writer');self.assertEqual(out['completed_paths'],[] if where=='archive' else [archive])
            if where=='head':self.assertIn(archive,mem.data)
    def test_private_retention_failure_stops_before_publication(self):
        mem,calls,opener=fixture();put=mem.put_object
        def deny(**kw):
            if kw['Key'].startswith(store.PRIVATE):raise Denied()
            return put(**kw)
        mem.put_object=deny
        with self.assertRaises(Denied):self.execute(mem,opener)
        self.assertEqual(calls,[]);self.assertEqual(mem.writes,[])
    def test_warm_impact_cache_and_native_globals_restored(self):
        mem,calls,opener=fixture();impact=native.impact_mapper;old=impact._CACHE;impact._CACHE={'graph':{'wrong':True},'betas':{'wrong':True}};wrong=impact._CACHE
        original={k:getattr(native,k) for k in ('datetime','urllib','_EIA')}
        try:self.execute(mem,opener);self.assertIs(impact._CACHE,wrong);self.assertTrue(all(getattr(native,k) is v for k,v in original.items()))
        finally:impact._CACHE=old
    def test_replay_rejects_tampered_output_and_corrupted_original_body(self):
        mem,calls,opener=fixture();self.execute(mem,opener);p=store.decode(mem.data[store.HEAD]);p['measurement_review']['series']['TSIFRGHT']['yoy']['percent']=123
        with contextlib.redirect_stdout(StringIO()),self.assertRaises(store.CaptureError):store.replay(native,mem,native.BUCKET,p)
        p=store.decode(mem.data[store.HEAD]);manifest=store.decode(store.retained(mem,native.BUCKET,p['publication_context']['manifest']));ref=manifest['http_attempts'][0]['original'];mem.data[ref['key']]+=b'!'
        with contextlib.redirect_stdout(StringIO()),self.assertRaises(store.CaptureError):store.replay(native,mem,native.BUCKET,p)
    def test_exact_calendar_comparisons_do_not_shift_missing_months(self):
        _,_,opener=fixture();sid='TSIFRGHT';p=deepcopy(opener.data[sid]);p['observations']=[r for r in p['observations'] if r['date']!='2025-08-01'];p['count']=len(p['observations'])
        out=m.measure(sid,opener.metas[sid],p,NOW.isoformat());self.assertEqual(out['yoy']['status'],'missing_comparison');self.assertIsNone(out['yoy']['percent']);self.assertEqual(out['yoy']['previous_date'],'2025-08-01')
        p['observations'][-1]['value']='.';out=m.measure(sid,opener.metas[sid],p,NOW.isoformat());self.assertEqual(out['status'],'latest_missing');self.assertEqual(out['latest_date'],'2026-08-01');self.assertIsNone(out['level'])
    def test_source_units_seasonality_and_ambiguous_rows_cannot_gain_arithmetic(self):
        _,_,opener=fixture();sid='TSIFRGHT';meta=deepcopy(opener.metas[sid]);meta['seriess'][0]['units']='Percent';self.assertEqual(m.measure(sid,meta,opener.data[sid],NOW.isoformat())['status'],'metadata_definition_changed')
        for case in ('duplicate','future','count','transformed'):
            p=deepcopy(opener.data[sid])
            if case=='duplicate':p['observations'][1]['date']=p['observations'][0]['date']
            if case=='future':p['observations'][-1]['date']='2027-01-01'
            if case=='count':p['count']+=1
            if case=='transformed':p['units']='pc1'
            self.assertNotEqual(m.measure(sid,opener.metas[sid],p,NOW.isoformat())['status'],'measured')
    def test_nsa_annualization_withheld_zero_denominator_and_zero_variance_explicit(self):
        _,_,opener=fixture();sid='FRGSHPUSM649NCIS';out=m.measure(sid,opener.metas[sid],opener.data[sid],NOW.isoformat());self.assertIsNone(out['six_month']['annualized_percent']);self.assertEqual(out['six_month']['annualization_status'],'not_seasonally_adjusted')
        sid='TSIFRGHT';p=deepcopy(opener.data[sid])
        for row in p['observations']:row['value']='0'
        out=m.measure(sid,opener.metas[sid],p,NOW.isoformat());self.assertEqual(out['level'],0);self.assertEqual(out['yoy']['status'],'zero_denominator');self.assertEqual(out['prior_60_months']['status'],'zero_variance');self.assertIsNone(out['prior_60_months']['z'])
    def test_prior_60_exact_calendar_months_exclude_current_and_refuse_holes(self):
        _,_,opener=fixture();sid='TSIFRGHT';out=m.measure(sid,opener.metas[sid],opener.data[sid],NOW.isoformat());b=out['prior_60_months'];self.assertEqual(b['start'],'2021-08-01');self.assertEqual(b['end'],'2026-07-01');self.assertEqual(b['mean'],208.5)
        p=deepcopy(opener.data[sid]);p['observations']=[r for r in p['observations'] if r['date']!='2024-12-01'];p['count']=len(p['observations']);b=m.measure(sid,opener.metas[sid],p,NOW.isoformat())['prior_60_months'];self.assertEqual(b['missing_or_invalid_dates'],['2024-12-01']);self.assertIsNone(b['z'])
    def test_cass_ratio_uses_unrounded_same_month_inputs_and_does_not_claim_dollars(self):
        _,_,opener=fixture();responses={(sid,endpoint):(opener.metas if endpoint=='series' else opener.data)[sid] for sid in m.PROFILES for endpoint in ('series','series/observations')}
        review=m.build(responses,NOW.isoformat());r=review['cass_index_ratio'];self.assertAlmostEqual(r['current_ratio'],3.973/1.417);self.assertEqual(r['unit'],'dimensionless_index_ratio');self.assertEqual(len(r['observations']),140)
        responses[('FRGSHPUSM649NCIS','series/observations')]['observations'][-1]['value']='.';r=m.build(responses,NOW.isoformat())['cass_index_ratio'];self.assertIsNone(r['current_ratio']);self.assertEqual(r['current_date'],'2026-08-01')
    def test_numeric_underflow_overflow_and_boolean_schema_do_not_fake_measurements(self):
        self.assertIsNone(m.number('1e-999'));self.assertIsNone(m.number(True))
        _,_,opener=fixture();sid='TSIFRGHT';p=deepcopy(opener.data[sid])
        for row in p['observations']:row['value']='1e-300'
        p['observations'][-1]['value']='1e30'
        out=m.measure(sid,opener.metas[sid],p,NOW.isoformat());self.assertEqual(out['yoy']['status'],'outside_numeric_range');self.assertIsNone(out['yoy']['percent']);store.encode(out)
        p['offset']=False;self.assertEqual(m.measure(sid,opener.metas[sid],p,NOW.isoformat())['status'],'incomplete_or_transformed_response')

    def test_original_native_functions_and_verified_runtime_are_preserved(self):
        raw=(ROOT/'tests/fixtures/pre-freight-research-freight-pulse.py.txt').read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),'8c39ed0209c0103b0e1faf9b650d063c156caf398beb0ef4ad512c93df6a845a')
        previous={n.name:n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef)};current={n.name:n for n in ast.parse((SOURCE/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef)}
        for name,node in previous.items():
            actual=deepcopy(current['_legacy_calculation' if name=='lambda_handler' else name]);actual.name=name;self.assertEqual(ast.dump(actual),ast.dump(node),name)
        config=json.loads((SOURCE.parent/'config.json').read_bytes());self.assertEqual((config['memory'],config['timeout'],config['architectures']),(512,300,['x86_64']));self.assertNotIn('schedule',config)

if __name__=='__main__':unittest.main(verbosity=2)
