"""Synthetic whole-input, public-boundary and failure-preservation tests."""
from pathlib import Path
from io import BytesIO
from copy import deepcopy
from types import ModuleType,SimpleNamespace
from datetime import datetime,timezone
from unittest.mock import patch
import ast,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
import commentary_store as store


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class S3:
    def __init__(self,objects):
        self.objects=dict(objects);self.reads=[];self.writes=[];self.denied=set();self.bad_length=set();self.before_write=None
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key in self.denied:raise Error('AccessDenied')
        if Key not in self.objects:raise Error('NoSuchKey')
        raw=self.objects[Key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+(Key in self.bad_length),'ETag':'"'+store.sha(raw)+'"'}
    def put_object(self,Bucket,Key,Body,**kw):
        if self.before_write:self.before_write(Key)
        if Key in self.denied:raise Error('AccessDenied')
        old=self.objects.get(Key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or kw['IfMatch']!='"'+store.sha(old)+'"'):raise Error('PreconditionFailed')
        self.objects[Key]=Body;self.writes.append((Key,kw))
    def get_parameter(self,**kw):raise AssertionError('No credential reads')


def native(client):
    boto=ModuleType('boto3');boto.client=lambda *a,**kw:client
    spec=importlib.util.spec_from_file_location('tested_commentary',SOURCE/'lambda_function.py');m=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'boto3':boto,'anthropic_shim':ModuleType('anthropic_shim')}):spec.loader.exec_module(m)
    m.boto3=boto
    def forbidden(*a,**kw):raise AssertionError('Legacy/model path prohibited')
    for key in ('call_claude','_get_anthropic_key','generate_commentary','gather_page_context','_legacy_lambda_handler'):setattr(m,key,forbidden)
    return m


class Tests(unittest.TestCase):
    def setUp(self):
        class FixtureClock(datetime):
            @classmethod
            def now(cls,tz=None):
                fixed=datetime(2026,9,27,10,tzinfo=timezone.utc)
                return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
        clock=patch.object(store,'datetime',FixtureClock);clock.start();self.addCleanup(clock.stop)

    def fixture(self):
        self.at='2026-09-27T10:00:00+00:00';self.page='13f';self.head='data/ai-commentary/13f.json';self.archive='data/ai-commentary/history/13f/2026-09-27.json'
        self.old=store.encode({'page':self.page,'generated_at':'2026-09-26T14:00:00Z','commentary':{'headline':'OLD EXPLICITLY SYNTHETIC'},'unknown':[None,0,False]})
        raw,p=store.policy_read();self.large=store.encode({'generated_at':'2026-09-26T12:00:00Z','holdings':[{'n':i,'value':None if i%7==0 else 0} for i in range(12000)]})
        objects={self.head:self.old}
        for key in p['pages'][self.page]['allowed']:objects[key]=self.large
        c=S3(objects);ref=store.retain(c,'b',raw)
        return c,p,ref
    def publish(self,c,p,ref):return store.publish(c,'b',self.page,p,ref,self.at)
    def test_native_large_complete_inputs_and_replay_never_call_models(self):
        c,p,ref=self.fixture();m=native(c)
        with patch('urllib.request.urlopen',side_effect=AssertionError('No HTTP/model requests')):result=m.lambda_handler({'page':'13f'},None)
        self.assertEqual(result['statusCode'],200);out=store.strict(c.objects[self.head]);self.assertEqual(out['model_api_calls'],0)
        self.assertEqual(store.replay(c,'b',out)['whole_input_bodies'],4)
        self.assertGreater(len(self.large),40000);self.assertIn(self.large,c.objects.values());self.assertIn(self.old,c.objects.values())
        self.assertEqual(len(out['source_inventory']),4);self.assertFalse(out['calls_eligible']);self.assertIsNone(out['commentary']['conviction_score'])
        self.assertEqual(c.objects[self.archive],c.objects[self.head]);self.assertTrue(all(out['commentary'][k] for k in store.FIELDS['13f'][:3]))
    def test_excluded_pages_http_and_unknown_event_never_read_or_write(self):
        c,p,ref=self.fixture();m=native(c);c.reads=[];c.writes=[]
        for page in ('portfolio','pre-pump-radar','signals','screener','../13f',{},['13f']):
            self.assertEqual(m.lambda_handler({'page':page},None)['statusCode'],422)
        self.assertEqual(m.lambda_handler({'httpMethod':'POST','page':'13f'},None)['statusCode'],409)
        self.assertEqual(c.reads,[]);self.assertEqual(c.writes,[])
    def test_unknown_bindings_are_not_turned_into_empty_observations(self):
        c,p,ref=self.fixture();out=store.publish(c,'b','risk-desk',p,ref,self.at)
        self.assertEqual(out['source_inventory'][0],{'key':'data/risk-composite.json','status':'unreviewed_binding_not_read'})
        self.assertNotIn('data/risk-composite.json',c.reads);self.assertFalse(out['source_coverage']['complete_for_declared_inputs'])
        self.assertEqual(store.replay(c,'b',out)['declared_inputs'],5)
    def test_missing_invalid_empty_and_full_inputs_remain_distinct(self):
        for raw,status in [(None,'missing'),(b'{"x":1,"x":2}','invalid_json'),(b'{"x":NaN}','invalid_json'),(b'{"x":1e999}','invalid_json'),(b'\xff','invalid_json'),(b'{}','empty_json'),(b'null','unsupported_json_shape')]:
            c,p,ref=self.fixture();key=p['pages']['13f']['allowed'][0]
            if raw is None:del c.objects[key]
            else:c.objects[key]=raw
            out=self.publish(c,p,ref);self.assertEqual(out['source_inventory'][0]['status'],status);store.replay(c,'b',out)
    def test_source_clock_is_not_restamped_or_used_as_observation_clock(self):
        for clock,state in [(None,'absent'),('2026-09-27','invalid'),('2030-01-01T00:00:00Z','future'),('2026-09-26T00:00:00Z','valid')]:
            c,p,ref=self.fixture();c.objects[p['pages']['13f']['allowed'][0]]=store.encode({'generated_at':clock,'as_of':'2000-01-01','zero':0})
            out=self.publish(c,p,ref);self.assertEqual(out['source_inventory'][0]['publication_clock_status'],state);self.assertEqual(out['source_inventory'][0]['observation_clock_status'],'not_inferred')
    def test_failures_preserve_original_head_without_new_timestamp_or_error_stub(self):
        for kind in ('denied_head','truncated_head','denied_input','truncated_input','retention'):
            c,p,ref=self.fixture();key=p['pages']['13f']['allowed'][0]
            if kind=='denied_head':c.denied.add(self.head)
            if kind=='truncated_head':c.bad_length.add(self.head)
            if kind=='denied_input':c.denied.add(key)
            if kind=='truncated_input':c.bad_length.add(key)
            if kind=='retention':c.denied.add(store.PRIVATE+store.sha(self.old)+'.bin')
            result=native(c).lambda_handler({'page':'13f'},None)
            self.assertEqual(result['statusCode'],503);self.assertEqual(c.objects[self.head],self.old)
            self.assertNotIn(self.archive,c.objects)
    def test_corrupt_future_or_wrong_predecessor_refuses_publication(self):
        for old in (b'{bad',store.encode({'page':'crisis','generated_at':'2026-09-26T00:00:00Z'}),store.encode({'page':'13f','generated_at':'2030-01-01T00:00:00Z'}),store.encode({'page':'13f','generated_at':'2026-09-27T10:00:00Z'})):
            c,p,ref=self.fixture();c.objects[self.head]=old
            with self.assertRaises(ValueError):self.publish(c,p,ref)
            self.assertEqual(c.objects[self.head],old)
    def test_same_day_history_remains_byte_identical_and_predecessor_chain_complete(self):
        c,p,ref=self.fixture();first=self.publish(c,p,ref);original=c.objects[self.archive]
        second=store.publish(c,'b',self.page,p,ref,'2026-09-27T11:00:00Z')
        self.assertEqual(c.objects[self.archive],original);self.assertEqual(second['publication_context']['dated_history'],'prior_daily_file_retained')
        plan=store.strict(store.retained(c,'b',second['publication_context']['manifest']))
        self.assertEqual(store.retained(c,'b',plan['predecessor']),store.encode(first));self.assertEqual(store.retained(c,'b',plan['dated_predecessor']),original)
        store.replay(c,'b',second)
    def test_concurrent_head_or_archive_survives_without_rollback(self):
        for key_kind in ('head','archive'):
            c,p,ref=self.fixture();key=self.head if key_kind=='head' else self.archive;foreign=b'{"concurrent":true}'
            def race(k):
                if k==key:c.objects[k]=foreign
            c.before_write=race
            with self.assertRaises(Error):self.publish(c,p,ref)
            self.assertEqual(c.objects[key],foreign)
            if key_kind=='archive':self.assertEqual(c.objects[self.head],self.old)
    def test_replay_rejects_changes_to_text_permission_provenance_or_input(self):
        c,p,ref=self.fixture();out=self.publish(c,p,ref)
        for key,value in [('calls_eligible',True),('page','crisis'),('model','claude')]:
            bad=deepcopy(out);bad[key]=value
            with self.assertRaises(ValueError):store.replay(c,'b',bad)
        bad=deepcopy(out);bad['commentary']['headline']='invented'
        with self.assertRaises(ValueError):store.replay(c,'b',bad)
        bad=deepcopy(out);bad['publication_context']['original_vintage_verified']=True
        with self.assertRaises(ValueError):store.replay(c,'b',bad)
        plan=store.strict(store.retained(c,'b',out['publication_context']['manifest']));c.objects[plan['inputs'][0]['original']['key']]=b'{}'
        with self.assertRaises(ValueError):store.replay(c,'b',out)
    def test_deadline_prevents_partial_replacement_and_other_pages_continue(self):
        c,p,ref=self.fixture();c.denied.add('data/13f-positions.json')
        result=native(c).lambda_handler({},None);body=json.loads(result['body'])
        self.assertEqual(result['statusCode'],207);self.assertEqual(body['pages_published'],5);self.assertEqual(body['pages_failed'],1)
        self.assertEqual(c.objects[self.head],self.old)
        c,p,ref=self.fixture();c.reads=[];c.writes=[]
        with self.assertRaises(ValueError):native(c).lambda_handler({},SimpleNamespace(get_remaining_time_in_millis=lambda:10000))
        self.assertEqual(c.reads,[]);self.assertEqual(c.writes,[])
    def test_whole_body_bound_and_changed_source_policy_fail_before_replacement(self):
        c,p,ref=self.fixture()
        with patch.object(store,'LIMIT',len(self.large)-1):
            with self.assertRaises(ValueError):self.publish(c,p,ref)
        self.assertEqual(c.objects[self.head],self.old);self.assertNotIn(self.archive,c.objects)
        raw,p=store.policy_read();p['pages']['13f']['allowed'].append('data/private.json')
        with patch.object(Path,'read_bytes',return_value=store.encode(p)):
            with self.assertRaises(ValueError):store.policy_read()
    def test_full_predecessor_functions_policy_twins_and_no_model_in_native_helper(self):
        old=(ROOT/'tests/fixtures/pre-explanation-research-page-ai-commentary.py.txt').read_bytes()
        self.assertEqual(store.sha(old),'40d7312e88234218a76b475d3ca5bcdbcf29f6998562ca358d6ed6681df24e72')
        before=ast.parse(old);tree=ast.parse((SOURCE/'lambda_function.py').read_bytes());functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
        for fn in before.body:
            if isinstance(fn,ast.FunctionDef):
                if fn.name=='lambda_handler':fn.name='_legacy_lambda_handler'
                self.assertEqual(ast.dump(fn),ast.dump(functions[fn.name]))
        self.assertEqual((ROOT/'config'/store.POLICY).read_bytes(),(SOURCE/store.POLICY).read_bytes())
        text=(SOURCE/'commentary_store.py').read_text()
        for needle in ('urlopen','llm_router','get_parameter','invoke('):self.assertNotIn(needle,text)


if __name__=='__main__':unittest.main(verbosity=2)
