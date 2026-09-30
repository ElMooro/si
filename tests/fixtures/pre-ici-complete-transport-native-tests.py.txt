"""Native ICI publication regressions, with no real AWS or provider requests."""
from pathlib import Path
from io import BytesIO
from unittest.mock import patch
import copy,hashlib,json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-ici-flows/source'))
import ici_store as store
import lambda_function as handler
from test_ici_research_candidate import release,GENERATED,ACQUIRED

class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Memory:
    def __init__(self):self.data={};self.writes=[];self.cas_conflict=False
    def get_object(self,Bucket,Key):
        if Key not in self.data:raise StorageError('NoSuchKey')
        raw=self.data[Key];return {'Body':BytesIO(raw),'ETag':hashlib.md5(raw).hexdigest()}
    def put_object(self,Bucket,Key,Body,**kw):
        raw=bytes(Body)
        if kw.get('IfNoneMatch')=='*' and Key in self.data:raise StorageError('PreconditionFailed')
        if 'IfMatch' in kw and (Key not in self.data or kw['IfMatch']!=hashlib.md5(self.data[Key]).hexdigest()):raise StorageError('PreconditionFailed')
        if Key==store.CURRENT and self.cas_conflict:raise StorageError('PreconditionFailed')
        self.data[Key]=raw;self.writes.append((Key,kw))

def acquire(kind):return release(kind),{'url':store.model.SOURCES[kind]['url'],'http_status':200,'acquired_at':ACQUIRED,'response_headers':{}}

def inputs(mem,when=GENERATED):
    sources={}
    for kind in store.model.SOURCES:
        raw,source=acquire(kind);source['original']=store.retain(mem,'bucket',raw,private=True);sources[kind]=source
    return {'contract':'ici-inputs.v1','generated_at':when,'sources':sources,'predecessors':{}}

class Tests(unittest.TestCase):
    def test_accepted_arithmetic_is_byte_identical_and_complete_closure_is_retained(self):
        store.qualified()
        self.assertEqual((store.ROOT/'ici_research.py').read_bytes(),(ROOT/'aws/ops/checks/ici_research_candidate.py').read_bytes())
        self.assertEqual((store.ROOT/'verify_ici_research.py').read_bytes(),(ROOT/'aws/ops/checks/verify_ici_research.py').read_bytes())
        mem=Memory();packet=store.seal(mem,'bucket',inputs(mem));out=store.replay(packet,store.reader(mem,'bucket'))
        self.assertEqual(out['original_arithmetic_checks']['observation_checks'],81)
        self.assertEqual(out['original_arithmetic_checks']['independent_rational_reconciliations'],48)
        self.assertEqual(out['portfolio_consequences']['target_weights'],None)
        manifest=store.binding(packet,store.reader(mem,'bucket'));self.assertEqual(len(manifest['compilers']),5)
        for key,raw in mem.data.items():
            if key.startswith(store.PREFIX):self.assertNotIn(b'<html>',raw)

    def test_normal_run_claims_once_preserves_whole_histories_and_replays(self):
        mem=Memory();old=b'{"legacy": [1, 2, 3]}';mem.data['data/history/ici-mmf.json']=old
        mem.data['data/history/ici-flows.json']=old+b'\n'
        with patch.object(store,'acquire',side_effect=acquire) as fetch,patch.object(store,'now',return_value=GENERATED):
            result=store.run(mem,'bucket','scheduled-event');duplicate=store.run(mem,'bucket','scheduled-event')
        self.assertTrue(result['published']);self.assertEqual(fetch.call_count,2)
        self.assertEqual(duplicate,{'published':False,'status':'duplicate_request','provider_requests':0})
        self.assertEqual(mem.data['data/history/ici-mmf.json'],old)
        self.assertFalse(any(k.startswith('data/history/') for k,_ in mem.writes))
        packet=store.strict(mem.data[store.CURRENT]);store.replay(packet,store.reader(mem,'bucket'))
        self.assertEqual(len(packet['predecessors']),2)
        for ref in packet['predecessors'].values():self.assertIn(mem.data[ref['key']],(old,old+b'\n'))

    def test_complete_error_retained_without_repainting_last_good_or_retries(self):
        mem=Memory();mem.data[store.CURRENT]=b'{"legacy":"unchanged"}'
        error=b'<html>Provider unavailable</html>'
        def denied(kind):return error,{'url':store.model.SOURCES[kind]['url'],'http_status':403,'acquired_at':ACQUIRED}
        with patch.object(store,'acquire',side_effect=denied) as fetch,patch.object(store,'now',return_value=GENERATED):
            with self.assertRaises(ValueError):store.run(mem,'bucket','denied')
        self.assertEqual(fetch.call_count,1);self.assertEqual(mem.data[store.CURRENT],b'{"legacy":"unchanged"}')
        self.assertEqual(mem.data[store.PRIVATE+store.sha(error)+'.bin'],error)
        self.assertFalse(any(k.startswith(store.PREFIX) for k,_ in mem.writes))

    def test_tampered_original_output_manifest_compiler_or_authority_cannot_publish(self):
        for kind in ('original','output','compiler','authority','manifest'):
            mem=Memory();packet=store.seal(mem,'bucket',inputs(mem));read=store.reader(mem,'bucket');manifest=store.binding(packet,read)
            if kind=='original':key=next(k for k in mem.data if k.endswith('.bin'));mem.data[key]+=b' '
            if kind=='output':mem.data[manifest['output']['key']]=b'{}'
            if kind=='compiler':mem.data[manifest['compilers']['ici_research.py']['key']]+=b'\n'
            if kind=='authority':packet['calls_eligible']=True
            if kind=='manifest':packet['replay']['output_sha256']='0'*64
            with self.subTest(kind=kind),self.assertRaises(ValueError):store.publish(mem,'bucket',packet)
            self.assertNotIn(store.CURRENT,mem.data)

    def test_head_uses_conditional_create_and_conditional_replace_with_readback(self):
        mem=Memory();packet=store.seal(mem,'bucket',inputs(mem));self.assertTrue(store.publish(mem,'bucket',packet))
        self.assertEqual([kw for k,kw in mem.writes if k==store.CURRENT][-1]['IfNoneMatch'],'*')
        self.assertTrue(store.publish(mem,'bucket',packet))
        later=store.seal(mem,'bucket',inputs(mem,'2026-09-26T18:52:20Z'))
        self.assertTrue(store.publish(mem,'bucket',later));self.assertIn('IfMatch',[kw for k,kw in mem.writes if k==store.CURRENT][-1])
        self.assertFalse(store.publish(mem,'bucket',packet))
        mem.cas_conflict=True;newer=store.seal(mem,'bucket',inputs(mem,'2026-09-26T18:52:30Z'))
        with self.assertRaises(RuntimeError):store.publish(mem,'bucket',newer)
        self.assertEqual(store.strict(mem.data[store.CURRENT]),later)

    def test_same_clock_conflict_and_backdated_release_cannot_replace_head(self):
        mem=Memory();packet=store.seal(mem,'bucket',inputs(mem));store.publish(mem,'bucket',packet)
        inp=inputs(mem);ref=store.retain(mem,'bucket',b'legacy',private=True);inp['predecessors']={'legacy_mmf':ref}
        other=store.seal(mem,'bucket',inp)
        with self.assertRaises(ValueError):store.publish(mem,'bucket',other)
        inp=inputs(mem,'2026-09-26T18:52:30Z')
        for kind,row in inp['sources'].items():
            raw=release(kind).replace(b'September 24, 2026',b'September 23, 2026').replace(b'September 23, 2026',b'September 22, 2026')
            # Shift every printed observation back a week as well.
            import re
            from datetime import datetime,timedelta
            raw=re.sub(rb'\d\d/\d\d/2026',lambda m:(datetime.strptime(m[0].decode(),'%m/%d/%Y')-timedelta(days=7)).strftime('%m/%d/%Y').encode(),raw)
            row['original']=store.retain(mem,'bucket',raw,private=True)
        regressed=store.seal(mem,'bucket',inp);self.assertFalse(store.publish(mem,'bucket',regressed))

    def test_storage_denial_corruption_or_partial_source_stops_publication(self):
        mem=Memory()
        with patch.object(mem,'get_object',side_effect=StorageError('AccessDenied')):
            with self.assertRaises(StorageError):store.predecessors(mem,'bucket')
        raw=b'whole';key=store.PRIVATE+store.sha(raw)+'.bin';mem.data[key]=b'corrupt'
        with self.assertRaises(ValueError):store.retain(mem,'bucket',raw,private=True)
        with patch.object(store,'acquire',return_value=(b'<html>truncated',acquire('mmf')[1])),patch.object(store,'now',return_value=GENERATED):
            with self.assertRaises(ValueError):store.run(mem,'bucket','partial')
        self.assertNotIn(store.CURRENT,mem.data)
        for raw in (b'{"x":1,"x":2}',b'{"x":NaN}',b'{"x":1e999}'):
            with self.assertRaises(ValueError):store.strict(raw)
        for key in ('data/portfolio.json',store.PRIVATE+'requests/x.json',store.PREFIX+'../secret'):
            with self.assertRaises(ValueError):store.reader(mem,'bucket')(key)

    def test_http_validate_only_and_missing_context_never_create_clients(self):
        with patch.object(handler.boto3,'client',side_effect=AssertionError('AWS client forbidden')):
            for event in ({'httpMethod':'GET'},{'requestContext':{'http':{'method':'GET'}},'validate_only':True}):
                out=handler.lambda_handler(event);self.assertEqual(out['statusCode'],307);self.assertIn('exact=1&nogen=1',out['headers']['Location'])
            self.assertFalse(json.loads(handler.lambda_handler({'validate_only':True})['body'])['published'])
            with self.assertRaises(ValueError):handler.lambda_handler({})

    def test_source_transport_is_bounded_and_blocks_redirect_following(self):
        class Response(BytesIO):
            status=200
            headers={'Content-Length':'4','Content-Type':'text/html'}
            def geturl(self):return store.model.SOURCES['mmf']['url']
        class Opener:
            def open(self,request,timeout):self.request=request;return Response(b'body')
        op=Opener()
        with patch.object(store.urllib.request,'build_opener',return_value=op):
            raw,meta=store.acquire('mmf');self.assertEqual(raw,b'body');self.assertEqual(meta['http_status'],200)
        self.assertNotIn('Authorization',op.request.headers);self.assertNotIn('Cookie',op.request.headers)
        self.assertIsNone(store.NoRedirect().redirect_request(None,None,302,'',{},'https://unreviewed.invalid'))
        Response.headers={'Content-Length':'5'}
        with patch.object(store.urllib.request,'build_opener',return_value=op),self.assertRaises(ValueError):store.acquire('mmf')

if __name__=='__main__':unittest.main()
