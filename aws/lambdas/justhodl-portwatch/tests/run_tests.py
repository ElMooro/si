from pathlib import Path
from datetime import datetime,timedelta,timezone
from io import BytesIO
from unittest.mock import patch
from copy import deepcopy
import ast,contextlib,gzip,hashlib,importlib.util,json,sys,unittest,urllib.error,urllib.parse
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0,str(SOURCE));import portwatch_store as store
NOW=datetime(2026,9,27,11,20,tzinfo=timezone.utc)
with patch('boto3.client'):
    spec=importlib.util.spec_from_file_location('portwatch_native_test',SOURCE/'lambda_function.py')
    native=importlib.util.module_from_spec(spec);sys.modules[spec.name]=native;spec.loader.exec_module(native)
class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
class Denied(Exception):response={'Error':{'Code':'AccessDenied'}}
class Conflict(Exception):response={'Error':{'Code':'PreconditionFailed'}}


def body(packet):return json.dumps(packet,sort_keys=True).encode()
def compress(packet):return gzip.compress(body(packet),mtime=0)


class Memory:
    def __init__(self):self.data={};self.writes=[];self.reads=[];self.error=None;self.truncated=None;self.conflict=None;self.corrupt=None
    def seed(self,k,v):self.data[k]=v
    def get_object(self,**kw):
        k=kw['Key'];self.reads.append(k)
        if k==self.error:raise Denied()
        if k not in self.data:raise Missing()
        raw=self.data[k]
        if k==self.corrupt:raw=raw+b'!'
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+(k==self.truncated),'ETag':hashlib.sha256(raw).hexdigest(),'LastModified':NOW}
    def put_object(self,**kw):
        k=kw['Key'];raw=kw['Body']
        if k.startswith(store.PRIVATE):
            assert kw['IfNoneMatch']=='*'
            if k in self.data:raise Conflict()
        else:
            assert k in (store.HEAD,store.HISTORY) and 'IfMatch' in kw
            if k==self.conflict:self.data[k]=b'foreign-newer-publication';raise Conflict()
            if kw['IfMatch']!=hashlib.sha256(self.data[k]).hexdigest():raise Conflict()
        self.data[k]=raw;self.writes.append(k)
        return {}


def fixture():
    m=Memory();hist={'choke':{},'ports':{},'through':{},'version':'1.6.0'}
    # Full 600-day predecessor. Preserve every date, including the 450-day
    # deletion boundary; keep all configured examples rather than top-k.
    for kind,ids,field in (('choke',[f'chokepoint{i}' for i in range(1,7)],'n_total'),('ports',['port1','port2'],'portcalls')):
        for ident in ids:
            for i in range(600):
                day=(NOW-timedelta(days=601-i)).date().isoformat()
                row={'portid':ident,'date':day,field:10+i%19}
                hist[kind][ident+'|'+day]=row
    m.seed(store.HISTORY,compress(hist));m.seed(store.HEAD,body({'version':'1.6.5','generated_at':'2026-09-26T11:20:00Z'}))
    m.seed(store.IMPORT,body({'generated_at':'2026-09-26T13:00:00Z','lines':[]}))
    calls=[]
    def opener(req,timeout=None):
        calls.append(store.identity(req,timeout))
        layer=req.full_url.split('/services/',1)[1].split('/')[0]
        if layer=='PortWatch_chokepoints_database':rows=[{'portid':f'chokepoint{i}','portname':f'Channel {i}'} for i in range(1,7)]
        elif layer=='PortWatch_ports_database':rows=[{'portid':'port1','portname':'Shanghai','country':'China'},{'portid':'port2','portname':'Busan','country':'Korea'}]
        elif layer=='portwatch_disruptions_database':rows=[]
        else:
            kind='choke' if layer=='Daily_Chokepoints_Data' else 'ports'
            rows=[deepcopy(v) for v in hist[kind].values() if v['date']>='2026-09-23']
        raw=body({'features':[{'attributes':x} for x in rows],'exceededTransferLimit':False})
        return store.Response(raw,headers={'Content-Length':str(len(raw))})
    return m,hist,calls,opener


class Tests(unittest.TestCase):
    def execute(self,m,opener):
        native.S3=m
        with contextlib.redirect_stdout(BytesIOText()):return store.run(native,opener=opener,at=NOW.isoformat())
    def test_full_native_output_history_and_replay(self):
        m,old,calls,opener=fixture();result=self.execute(m,opener)
        self.assertTrue(result['published']);self.assertEqual(len(calls),5)
        packet=store.strict(m.data[store.HEAD]);history=store.decode(m.data[store.HISTORY],store.HISTORY)
        self.assertEqual(history,old);self.assertEqual(len(history['choke']),3600);self.assertEqual(len(history['ports']),1200)
        self.assertEqual(packet['contract'],store.CONTRACT);self.assertFalse(packet['sizing_eligible']);self.assertEqual(packet['portfolio_action'],'WAIT')
        with contextlib.redirect_stdout(BytesIOText()):replayed=store.replay(native,m,native.BUCKET,packet)
        self.assertEqual(replayed['history_rows'],{'choke':3600,'ports':1200});self.assertEqual(replayed['provider_requests'],0)
        self.assertEqual(m.writes[-2:],[store.HISTORY,store.HEAD]);self.assertEqual(len(calls),5)
        self.assertIs(native.S3,m)
    def test_denied_missing_corrupt_and_truncated_stop_before_provider(self):
        for case in ('denied','missing','corrupt','truncated'):
            m,old,calls,opener=fixture();original=m.data[store.HEAD]
            if case=='denied':m.error=store.HISTORY
            elif case=='missing':del m.data[store.HISTORY]
            elif case=='corrupt':m.data[store.HISTORY]=b'bad gzip'
            else:m.truncated=store.HISTORY
            with self.assertRaises(Exception):self.execute(m,opener)
            self.assertEqual(calls,[]);self.assertEqual(m.data[store.HEAD],original)
            self.assertTrue(all(k.startswith(store.PRIVATE) for k in m.writes))
    def test_whole_failures_and_429_are_retained_with_no_followup_request(self):
        for case in ('429','transport','api_error','truncated'):
            m,old,calls,opener=fixture();original=deepcopy(m.data);attempts=[]
            def fail(req,timeout=None):
                attempts.append(req.full_url)
                if case=='transport':raise urllib.error.URLError('unavailable')
                raw=b'{"error":{"code":429}}' if case=='api_error' else b'{"message":"limited"}'
                return store.Response(raw,429 if case=='429' else 200,{'Content-Length':str(len(raw)+(case=='truncated'))})
            with self.assertRaises(Exception):self.execute(m,fail)
            self.assertEqual(len(attempts),1)
            self.assertEqual({k:m.data[k] for k in original},original)
            self.assertTrue(all(k.startswith(store.PRIVATE) for k in m.writes))
            if case!='truncated':self.assertTrue(any(b'http_response' in raw or b'transport_error' in raw for k,raw in m.data.items() if k.startswith(store.PRIVATE)))
    def test_concurrent_newer_writer_preserved_without_rollback(self):
        for key in (store.HISTORY,store.HEAD):
            m,old,calls,opener=fixture();m.conflict=key;result=self.execute(m,opener)
            self.assertFalse(result['published']);self.assertEqual(m.data[key],b'foreign-newer-publication')
            self.assertEqual(result['completed_paths'],[store.HISTORY] if key==store.HEAD else [])
    def test_original_hash_and_exact_json_types_are_replayed(self):
        m,old,calls,opener=fixture();self.execute(m,opener);packet=store.strict(m.data[store.HEAD])
        manifest=store.strict(store.retained(m,native.BUCKET,packet['publication_context']['manifest']))
        ref=manifest['http_attempts'][0]['original'];m.corrupt=ref['key']
        with contextlib.redirect_stdout(BytesIOText()),self.assertRaises(Exception):store.replay(native,m,native.BUCKET,packet)
        m.corrupt=None;packet['sizing_eligible']=0
        with contextlib.redirect_stdout(BytesIOText()),self.assertRaises(store.CaptureError):store.replay(native,m,native.BUCKET,packet)
    def test_no_legacy_observation_or_metadata_deletion(self):
        old={'choke':{'a':{'n_total':0}},'ports':{},'metadata':{'x':1}}
        with self.assertRaises(store.CaptureError):store.history_preserved(old,{**old,'choke':{}})
        with self.assertRaises(store.CaptureError):store.history_preserved(old,{**old,'metadata':{}})
        store.history_preserved(old,{**old,'choke':{'a':{'n_total':1},'b':{'n_total':0}}})
        rows={'p|2020-01-01':{'portid':'p','date':'2020-01-01','n_total':0}}
        native._merge_rows(rows,[{'portid':'p','date':'2026-09-26','n_total':7}])
        self.assertEqual(len(rows),2);self.assertEqual(rows['p|2020-01-01']['n_total'],0)
    def test_all_original_calculations_preserved(self):
        original=ast.parse((ROOT/'tests/fixtures/pre-research-portwatch.py.txt').read_text(encoding='utf-8'))
        current=ast.parse((SOURCE/'lambda_function.py').read_text(encoding='utf-8'))
        before={n.name:n for n in original.body if isinstance(n,ast.FunctionDef)}
        after={n.name:n for n in current.body if isinstance(n,ast.FunctionDef)}
        for name,node in before.items():
            if name=='_merge_rows':continue
            candidate=deepcopy(after['_native_calculation' if name=='lambda_handler' else name]);candidate.name=name
            self.assertEqual(ast.dump(node,include_attributes=False),ast.dump(candidate,include_attributes=False),name)
    def test_public_provider_identity_rejects_credentials_and_unreviewed_hosts(self):
        for url in ('https://example.test/query?f=json','https://services9.arcgis.com/x?token=secret',native.CHOKE_REF+'?f=json&api_key=secret'):
            with self.assertRaises(store.CaptureError):store.identity(urllib.request.Request(url),30)
        with self.assertRaises(store.CaptureError):store.identity(urllib.request.Request(native.CHOKE_REF+'?f=json'),True)
    def test_nonfinite_and_duplicate_json_are_rejected(self):
        for raw in (b'{"n":NaN}',b'{"n":1e400}',b'{"n":1,"n":2}'):
            with self.assertRaises(store.CaptureError):store.strict(raw)


class BytesIOText:
    def write(self,text):return len(text)
    def flush(self):pass


if __name__=='__main__':unittest.main()
