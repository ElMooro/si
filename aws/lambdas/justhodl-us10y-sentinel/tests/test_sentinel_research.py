from copy import deepcopy
from datetime import datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
from unittest.mock import patch
import hashlib,io,json,sys,types,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'source'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
import sentinel_model as model
import sentinel_sources as sources
import sentinel_store as store


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class S3:
    def __init__(self):
        self.docs={model.CURRENT:model.encoded({'schema':'2.1','tier':'BENIGN','generated_at':'2026-01-01T00:00:00Z'}),
            'data/indicator-bus.json':model.encoded({'indicators':{'US10Y':{'v':4.3,'asof':'2026-09-24','src':'FRED'}}})}
        self.writes=[];self.reject_current=False
    def get_object(self,**kw):
        if kw['Key'] not in self.docs:raise Error('NoSuchKey')
        raw=self.docs[kw['Key']]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(kw)
        if key==model.CURRENT and self.reject_current:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.docs:raise Error('PreconditionFailed')
        if kw.get('IfMatch') and kw['IfMatch']!=hashlib.sha256(self.docs[key]).hexdigest():raise Error('PreconditionFailed')
        self.docs[key]=kw['Body']


def responses():
    last=datetime.now(timezone.utc).date()-timedelta(days=1);dates=[]
    while len(dates)<1300:
        if last.weekday()<5:dates.append(last.isoformat())
        last-=timedelta(days=1)
    dates.reverse();out={}
    for sid,row in model.SERIES.items():
        out[sid,'definition']={'seriess':[{'id':sid,'units':row['unit'],'frequency':row['frequency'],
            'seasonal_adjustment':'Not Seasonally Adjusted','notes':'Original fixture definition'}]}
        out[sid,'observations']={'count':len(dates),'offset':0,'units':'lin','output_type':1,
            'sort_order':'asc','order_by':'observation_date','observations':[{'date':d,'value':
                str(5.1 if i>=250 and (i-250)%300==0 else 4.2) if sid=='DGS10' else str(2+i/10000) if sid=='DFII10' else str(100+i/10)} for i,d in enumerate(dates)]}
    return out


class Tests(unittest.TestCase):
    def setUp(self):self.s3=S3();self.responses=responses();self.requests=[]
    def fetch(self,request,timeout):
        self.assertEqual(timeout,25);query=parse_qs(urlsplit(request.full_url).query)
        self.assertEqual(query.pop('api_key'),['fixture-secret'])
        sid=query['series_id'][0];kind='observations' if '/observations?' in request.full_url else 'definition'
        self.requests.append((sid,kind));return io.BytesIO(model.encoded(self.responses[sid,kind]))
    def run_native(self):
        import lambda_function as entry
        with patch.object(entry.boto3,'client',return_value=self.s3),patch.dict(entry.os.environ,{'FRED_API_KEY':'fixture-secret'}), \
             patch.object(sources,'acquire',side_effect=lambda client,bucket,key: self.acquire(client,bucket,key)):
            return json.loads(entry.lambda_handler({})['body'])
    def acquire(self,client,bucket,key):return self.real_acquire(client,bucket,key,self.fetch)
    real_acquire=staticmethod(sources.acquire)
    def retained(self):
        original=sources.acquire(self.s3,'test','fixture-secret',self.fetch)
        inputs={'contract':'us10y-sentinel-inputs.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'sources':original,'predecessor':store.retain_bytes(self.s3,'test',self.s3.docs[model.CURRENT],'snapshots'),
            'bus':store.retain_bytes(self.s3,'test',self.s3.docs['data/indicator-bus.json'],'snapshots')}
        return store.retain(self.s3,'test',inputs)
    def test_real_native_entrypoint_whole_replay_and_private_history(self):
        result=self.run_native();packet=sources.strict(self.s3.docs[model.CURRENT])
        full,view=store.replay(packet['replay'],store.reader(self.s3,'test'))
        self.assertTrue(result['published']);self.assertEqual(len(self.requests),6)
        self.assertEqual(result['original_rows'],3900)
        self.assertEqual(sum(len(rows) for rows in full['histories'].values()),3900)
        self.assertTrue(store.same({k:v for k,v in packet.items() if k!='replay'},view))
        self.assertEqual(len(full['correlation_trace']['observations']),60)
        self.assertNotIn('observations',view['correlation_trace']);self.assertNotIn('histories',packet)
        self.assertFalse(packet['calls_eligible']);self.assertFalse(packet['sizing_eligible'])
        for key,raw in self.s3.docs.items():
            self.assertNotIn(b'fixture-secret',raw)
            if key.startswith('data/'):
                self.assertNotIn(b'"histories":',raw)
        self.assertEqual(sum(w['Key']==model.CURRENT for w in self.s3.writes),1)
        self.assertIn('IfMatch',next(w for w in self.s3.writes if w['Key']==model.CURRENT))
        from sentinel_original_arithmetic import verify
        proof=verify(full,{sid:self.responses[sid,'observations'] for sid in model.SERIES})
        self.assertEqual(proof['original_rows'],3900);self.assertGreater(proof['independent_scalar_checks'],3900)
        full['velocity']['comparisons']['60_observations']['value']=999
        with self.assertRaises(ValueError):verify(full,{sid:self.responses[sid,'observations'] for sid in model.SERIES})
    def test_optional_bus_failure_does_not_erase_independent_original_measurements(self):
        for raw in (None,b'not-json',b'[]',b'{"indicators":[]}'):
            self.s3=S3()
            if raw is None:self.s3.docs.pop('data/indicator-bus.json')
            else:self.s3.docs['data/indicator-bus.json']=raw
            self.assertTrue(self.run_native()['published'])
            packet=sources.strict(self.s3.docs[model.CURRENT])
            self.assertIn(packet['bus_cross']['status'],('missing','malformed_retained_context'))
            self.assertEqual(packet['bus_cross']['independent_votes'],0)
    def test_complete_original_population_rejects_wrong_identity_and_truncation(self):
        definition=self.responses['DGS10','definition'];obs=self.responses['DGS10','observations'];stamp=datetime.now(timezone.utc).isoformat()
        for change in ({'count':1301},{'offset':1},{'units':'pch'},{'sort_order':'desc'},{'output_type':2}):
            with self.assertRaises(ValueError):sources.daily('DGS10',definition,{**obs,**change},stamp)
        for change in ({'id':'DGS2'},{'units':'Index'},{'frequency':'Monthly'},{'seasonal_adjustment':'Seasonally Adjusted'}):
            with self.assertRaises(ValueError):sources.daily('DGS10',{'seriess':[{**definition['seriess'][0],**change}]},obs,stamp)
    def test_dates_and_numeric_values_cannot_be_silently_repaired(self):
        for field,value in (('date','2099-01-01'),('date','2026-02-30'),('date','2000-01-01'),('value','NaN'),('value',0)):
            obs=deepcopy(self.responses['DFII10','observations']);obs['observations'][3][field]=value
            with self.assertRaises(ValueError):sources.daily('DFII10',self.responses['DFII10','definition'],obs,datetime.now(timezone.utc).isoformat())
        obs=deepcopy(self.responses['DGS10','observations']);obs['observations'][1]=obs['observations'][0]
        with self.assertRaises(ValueError):sources.daily('DGS10',self.responses['DGS10','definition'],obs,datetime.now(timezone.utc).isoformat())
    def test_missing_values_are_conserved_as_source_gaps_and_yield_zero_is_valid(self):
        obs=deepcopy(self.responses['DGS10','observations']);obs['observations'][0]['value']='.';obs['observations'][1]['value']='0'
        rows,audit=sources.daily('DGS10',self.responses['DGS10','definition'],obs,datetime.now(timezone.utc).isoformat())
        self.assertEqual(audit['missing_rows'],1);self.assertEqual(len(rows),1299);self.assertEqual(rows[0][1],0)
    def test_whole_output_tampering_or_authority_change_cannot_borrow_replay(self):
        packet=self.retained();before=self.s3.docs[model.CURRENT]
        for change in ({'level':99},{'calls_eligible':0},{'execution_eligible':True},{'extra':0}):
            with self.assertRaises(ValueError):store.publish(self.s3,'test',{**packet,**change})
        self.assertEqual(self.s3.docs[model.CURRENT],before)
    def test_whole_replay_rejects_altered_original_compiler_and_reference(self):
        packet=self.retained();read=store.reader(self.s3,'test');ref=packet['replay'];manifest=sources.strict(read(ref['manifest_key']))
        source=packet['original_sources']['DGS10']['sources']['observations']['evidence']['key']
        for key in (source,manifest['compilers']['sentinel_model.py']['key']):
            original=self.s3.docs[key];self.s3.docs[key]=original+b' '
            with self.assertRaises(ValueError):store.replay(ref,read)
            self.s3.docs[key]=original
        with self.assertRaises(ValueError):store.replay({**ref,'view_sha256':'0'*64},read)
    def test_source_failure_never_publishes_a_partial_or_empty_head(self):
        before=self.s3.docs[model.CURRENT]
        def fail(*a,**kw):raise TimeoutError('Request with secret')
        with self.assertRaisesRegex(RuntimeError,'FRED acquisition failed') as exc:store.run(self.s3,'test','fixture-secret',fail)
        self.assertNotIn('secret',str(exc.exception));self.assertEqual(self.s3.docs[model.CURRENT],before)
        self.assertFalse(any(w['Key']==model.CURRENT for w in self.s3.writes))
    def test_conditional_collision_does_not_retry_or_overwrite(self):
        packet=self.retained();before=self.s3.docs[model.CURRENT];self.s3.reject_current=True
        self.assertFalse(store.publish(self.s3,'test',packet));self.assertEqual(self.s3.docs[model.CURRENT],before)
        self.assertEqual(sum(w['Key']==model.CURRENT for w in self.s3.writes),1)
    def test_later_or_same_clock_head_is_not_overwritten(self):
        packet=self.retained();self.assertTrue(store.publish(self.s3,'test',packet))
        self.assertTrue(store.publish(self.s3,'test',packet))
        corrupt={**packet,'level':999};self.s3.docs[model.CURRENT]=model.encoded(corrupt)
        with self.assertRaisesRegex(ValueError,'Same-clock'):store.publish(self.s3,'test',packet)
    def test_restricted_paths_and_ambiguous_json_fail(self):
        self.assertTrue(sources.PRIVATE.startswith('audit-private/20260909-originals/'))
        worker=(ROOT/'cloudflare/workers/justhodl-data-proxy/src/index.js').read_text(encoding='utf-8')
        self.assertIn("'audit-private/'",worker)
        for key in ('private/credentials','data/../private/secrets','data/other.json'):
            self.assertFalse(store.allowed(key))
        for raw in (b'{"v":1,"v":2}',b'{"v":NaN}',b'{"v":1e999}'):
            with self.assertRaises(ValueError):sources.strict(raw)
    def test_complete_predecessor_source_and_tests_are_preserved(self):
        path=Path(__file__).parent/'legacy_before_source_replay.py.txt';raw=path.read_bytes()
        self.assertEqual(len(raw),16601);self.assertEqual(hashlib.sha256(raw).hexdigest(),'93ceac5a7f731cb245cecbfe2437c24996754fb48023429d8e2fd324a6d88d89')
        self.assertGreater((Path(__file__).parent/'legacy_before_source_replay_tests.py.txt').stat().st_size,8000)


if __name__=='__main__':unittest.main(verbosity=2)
