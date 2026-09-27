"""Offline full-handler, privacy, history and deterministic replay regressions."""
from pathlib import Path
from io import BytesIO
from copy import deepcopy
from types import SimpleNamespace,ModuleType
from unittest.mock import patch
import ast,hashlib,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4]
SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0,str(SOURCE))
import page_explanation_store as store


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class S3:
    def __init__(self,objects):
        self.objects=dict(objects);self.reads=[];self.writes=[];self.denied=set();self.before_write=None;self.bad_length=set()
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key in self.denied:raise Error('AccessDenied')
        if Key not in self.objects:raise Error('NoSuchKey')
        raw=self.objects[Key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+(Key in self.bad_length),'ETag':'"'+store.sha(raw)+'"'}
    def put_object(self,Bucket,Key,Body,**kw):
        if self.before_write:self.before_write(Key)
        if Key in self.denied:raise Error('AccessDenied')
        prev=self.objects.get(Key)
        if kw.get('IfNoneMatch')=='*' and prev is not None:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (prev is None or kw['IfMatch']!='"'+store.sha(prev)+'"'):raise Error('PreconditionFailed')
        self.writes.append((Key,dict(kw)));self.objects[Key]=Body


def native(client):
    boto=ModuleType('boto3');boto.client=lambda *a,**kw:client
    spec=importlib.util.spec_from_file_location('tested_page_ai',SOURCE/'lambda_function.py')
    module=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'boto3':boto}):spec.loader.exec_module(module)
    module.boto3=boto
    return module


class Tests(unittest.TestCase):
    def fixture(self):
        meta={'title':'Macro Leads','engines':['justhodl-macro-leads'],'data_files':['macro-leads','data/freight-pulse.json']}
        self.at='2026-09-27T09:00:00+00:00';self.head='data/page-ai/macro-leads.json'
        self.old=store.encode({'page':'macro-leads','generated_at':'2026-09-26T07:15:00Z','what_it_is':None,'analysis':None,'unknown':{'zero':0,'null':None,'false':False}})
        self.large=store.encode({'generated_at':'2026-09-26T11:50:17Z','rows':[{'month':i,'value':None if i%7==0 else 0,'unknown':False} for i in range(10000)]})
        c=S3({store.MANIFEST:store.encode({'macro-leads':meta}),self.head:self.old,'data/macro-leads.json':b'{"generated_at":"2026-09-26T12:20:00Z","zero":0,"big":9007199254740993}', 'data/freight-pulse.json':self.large})
        p_raw,p=store.policy_read();pr=store.retain(c,'b',p_raw);mr=store.retain(c,'b',c.objects[store.MANIFEST])
        return c,meta,p,pr,mr
    def publish(self,c,meta,p,pr,mr):return store.publish(c,'b','macro-leads',meta,mr,p,pr,self.at)
    def test_complete_large_inputs_native_entry_and_offline_replay_no_models(self):
        c,meta,p,pr,mr=self.fixture();m=native(c)
        poison=ModuleType('llm_router')
        def fail(*a,**k):raise AssertionError('Model call prohibited')
        poison.complete=fail
        with patch.dict(sys.modules,{'llm_router':poison}):result=m.lambda_handler({},SimpleNamespace(get_remaining_time_in_millis=lambda:300000))
        self.assertEqual(result['statusCode'],200);out=store.strict(c.objects[self.head])
        self.assertEqual(out['source_coverage']['declared'],2);self.assertEqual(out['source_inventory'][1]['bytes'],len(self.large));self.assertGreater(len(self.large),40000)
        self.assertTrue(all(isinstance(out[k],str) and out[k] for k in ('what_it_is','what_it_does','analysis','pick_read')))
        self.assertFalse(out['calls_eligible']);self.assertIsNone(out['model']);self.assertEqual(out['model_api_calls'],0)
        proof=store.replay(c,'b',out);self.assertEqual(proof['whole_input_bodies'],2)
        self.assertIn(self.old,c.objects.values());self.assertIn(self.large,c.objects.values())
        self.assertNotIn('data/signal-scorecard.json',c.reads);self.assertNotIn('data/_cache/page-ai-explain.json',c.reads)
    def test_replay_rejects_changed_projection_qualification_or_original(self):
        c,meta,p,pr,mr=self.fixture();out=self.publish(c,meta,p,pr,mr)
        for key,value in [('analysis','invented'),('calls_eligible',True),('page','freight-pulse')]:
            bad=deepcopy(out);bad[key]=value
            with self.assertRaises(store.EvidenceError):store.replay(c,'b',bad)
        bad=deepcopy(out);bad['publication_context']['original_vintage_verified']=True
        with self.assertRaises(store.EvidenceError):store.replay(c,'b',bad)
        plan=store.strict(store.retained(c,'b',out['publication_context']['manifest']));c.objects[plan['inputs'][1]['key']]=b'{}'
        with self.assertRaises(store.EvidenceError):store.replay(c,'b',out)
    def test_whole_predecessor_preserved_before_failed_read_and_no_public_write(self):
        c,meta,p,pr,mr=self.fixture();c.denied.add('data/freight-pulse.json')
        with self.assertRaises(store.EvidenceError):self.publish(c,meta,p,pr,mr)
        self.assertEqual(c.objects[self.head],self.old);self.assertIn(self.old,c.objects.values());self.assertFalse(any(k==self.head for k,_ in c.writes))
    def test_missing_invalid_and_undated_inputs_are_distinct(self):
        for raw,status in [(None,'missing'),(b'{"x":1,"x":2}','invalid_json'),(b'\xff','invalid_json'),(b'{"x":NaN}','invalid_json'),(b'{"x":1e999}','invalid_json'),(b'{}','empty_json'),(b'[]','empty_json'),(b'null','unsupported_json_shape')]:
            with self.subTest(status=status,raw=raw):
                c,meta,p,pr,mr=self.fixture()
                if raw is None:del c.objects['data/freight-pulse.json']
                else:c.objects['data/freight-pulse.json']=raw
                out=self.publish(c,meta,p,pr,mr);self.assertEqual(out['source_inventory'][1]['status'],status)
                store.replay(c,'b',out)
                if raw is not None:self.assertIn(raw,c.objects.values())
    def test_missing_clock_does_not_borrow_capture_time(self):
        for value,status in [(None,'absent'),('2026-09-27','invalid'),('2026-09-27T10:00:00Z','future'),('2026-09-26T12:00:00Z','valid')]:
            c,meta,p,pr,mr=self.fixture();c.objects['data/freight-pulse.json']=store.encode({'generated_at':value,'as_of':'2020-01-01'})
            out=self.publish(c,meta,p,pr,mr);row=out['source_inventory'][1]
            self.assertEqual(row['publication_clock_status'],status);self.assertEqual(row['observation_clock_status'],'not_inferred')
    def test_private_unregistered_and_traversal_bindings_are_refused_before_head(self):
        for key in ('data/ai-brief.json','portfolio/snapshot.json','data/validation-log.json','data/unknown.json','../macro-leads','https://evil/a','data/macro-leads.json?x=1'):
            c,meta,p,pr,mr=self.fixture();meta['data_files'].append(key);c.reads=[]
            with self.assertRaises(store.EvidenceError):self.publish(c,meta,p,pr,mr)
            self.assertEqual(c.reads,[]);self.assertEqual(c.objects[self.head],self.old)
        c,meta,p,pr,mr=self.fixture();c.reads=[]
        with self.assertRaises(store.EvidenceError):store.publish(c,'b','portfolio',meta,mr,p,pr,self.at)
        self.assertEqual(c.reads,[])
    def test_truncated_and_denied_predecessors_never_initialize_new_head(self):
        for failure in ('denied','truncated','wrong_page','future','malformed'):
            c,meta,p,pr,mr=self.fixture()
            if failure=='denied':c.denied.add(self.head)
            if failure=='truncated':c.bad_length.add(self.head)
            if failure=='wrong_page':c.objects[self.head]=store.encode({'page':'calls'})
            if failure=='future':c.objects[self.head]=store.encode({'page':'macro-leads','generated_at':'2030-01-01T00:00:00Z'})
            if failure=='malformed':c.objects[self.head]=b'{broken'
            previous=c.objects[self.head]
            with self.assertRaises((ValueError,UnicodeError)):self.publish(c,meta,p,pr,mr)
            self.assertEqual(c.objects[self.head],previous)
    def test_competing_head_write_and_private_retention_failure_preserve_foreign_head(self):
        c,meta,p,pr,mr=self.fixture();foreign=b'{"foreign":true}'
        def race(key):
            if key==self.head:c.objects[key]=foreign
        c.before_write=race
        with self.assertRaises(Error):self.publish(c,meta,p,pr,mr)
        self.assertEqual(c.objects[self.head],foreign)
        c,meta,p,pr,mr=self.fixture();c.denied.add(store.PRIVATE+store.sha(self.old)+'.bin')
        with self.assertRaises(Error):self.publish(c,meta,p,pr,mr)
        self.assertEqual(c.objects[self.head],self.old)
    def test_no_head_create_without_verified_missing_and_no_existing_same_time_replace(self):
        c,meta,p,pr,mr=self.fixture();del c.objects[self.head]
        out=self.publish(c,meta,p,pr,mr);store.replay(c,'b',out)
        public=[kwargs for key,kwargs in c.writes if key==self.head];self.assertEqual(public[0]['IfNoneMatch'],'*')
        with self.assertRaises(store.EvidenceError):self.publish(c,meta,p,pr,mr)
    def test_cursor_advances_attempts_not_success_and_failed_page_remains_unchanged(self):
        c,meta,p,pr,mr=self.fixture();bad=deepcopy(meta);bad['data_files']=['data/private.json']
        c.objects[store.MANIFEST]=store.encode({'macro-leads':bad,'freight-pulse':meta})
        # Second page has a reviewed freight binding; the first failure cannot skip it.
        c.objects[store.MANIFEST]=store.encode({'macro-leads':bad,'freight-pulse':{'title':'Freight','data_files':['freight-pulse']},'portfolio':{'data_files':['portfolio/snapshot']}})
        result=native(c).lambda_handler({},None);body=json.loads(result['body'])
        self.assertEqual(body['pages_attempted'],3);self.assertEqual(body['pages_published'],1);self.assertEqual(body['cursor'],0)
        self.assertEqual(c.objects[self.head],self.old);self.assertEqual(body['results'][2]['status'],'unreviewed_page_not_read')
        self.assertNotIn('portfolio/snapshot',c.reads)
    def test_http_cannot_generate_or_read_and_options_is_harmless(self):
        c,meta,p,pr,mr=self.fixture();c.reads=[];c.writes=[];m=native(c)
        for method,status in [('GET',409),('OPTIONS',204),('POST',409)]:
            out=m.lambda_handler({'requestContext':{'http':{'method':method}},'queryStringParameters':{'mode':'live','page':'portfolio','feeds':'portfolio/snapshot'}},None)
            self.assertEqual(out['statusCode'],status)
        self.assertEqual(c.reads,[]);self.assertEqual(c.writes,[])
    def test_manifest_cursor_corruption_and_population_bound_fail_closed(self):
        c,meta,p,pr,mr=self.fixture();c.objects[store.MANIFEST]=b'{"a":{},"a":{}}'
        with self.assertRaises(store.EvidenceError):native(c).lambda_handler({},None)
        c,meta,p,pr,mr=self.fixture();c.objects[store.CURSOR]=b'{"i":true}'
        with self.assertRaises(store.EvidenceError):native(c).lambda_handler({},None)
        c,meta,p,pr,mr=self.fixture();meta['data_files']=['macro-leads']*201;c.reads=[]
        with self.assertRaises(store.EvidenceError):self.publish(c,meta,p,pr,mr)
        self.assertEqual(c.reads,[])
    def test_deadline_stops_before_any_operation_and_partial_errors_are_explicit(self):
        c,meta,p,pr,mr=self.fixture();c.reads=[];c.writes=[]
        bounded=store.DeadlineClient(c,store.time.monotonic()+30)
        with self.assertRaises(store.EvidenceError):bounded.get_object(Bucket='b',Key=self.head)
        with self.assertRaises(store.EvidenceError):bounded.put_object(Bucket='b',Key=self.head,Body=b'{}')
        self.assertEqual(c.reads,[]);self.assertEqual(c.writes,[])
        c.denied.add('data/freight-pulse.json')
        result=native(c).lambda_handler({},None);self.assertEqual(result['statusCode'],503)
        self.assertEqual(json.loads(result['body'])['pages_failed'],1);self.assertEqual(c.objects[self.head],self.old)
    def test_full_source_preserved_policy_twins_and_no_model_in_new_entry_path(self):
        old=(ROOT/'tests/fixtures/pre-explanation-research-page-ai.py.txt').read_bytes()
        self.assertEqual(store.sha(old),'c01267c87d5607f57001676f9ddb6eb74f691805bc5b40426223a1c5b7cd3da8')
        tree=ast.parse((SOURCE/'lambda_function.py').read_bytes());before=ast.parse(old)
        functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
        for fn in before.body:
            if isinstance(fn,ast.FunctionDef):
                name=fn.name
                if name=='lambda_handler':fn.name='_legacy_lambda_handler'
                self.assertEqual(ast.dump(fn),ast.dump(functions[fn.name]),name)
        self.assertEqual((ROOT/'config'/store.POLICY).read_bytes(),(SOURCE/store.POLICY).read_bytes())
        text=(SOURCE/'page_explanation_store.py').read_text()
        for needle in ('urlopen','llm_router','get_parameter','invoke('):self.assertNotIn(needle,text)


if __name__=='__main__':unittest.main(verbosity=2)
