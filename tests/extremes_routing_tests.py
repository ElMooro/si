"""Explicit publisher identity and exact retained native compiler compatibility."""
from pathlib import Path
from unittest.mock import patch
import base64,copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tests'))
import extremes_native_test_support as support
store=support.store;model=support.model

def frozen():
    doc=json.loads((ROOT/'tests/fixtures/pre-extremes-routing-native.json').read_bytes())
    return support.Storage({k:base64.b64decode(v,validate=True) for k,v in doc['objects'].items()}),doc['packet']

class Routing(unittest.TestCase):
    def test_cross_engine_missing_or_nonobject_packet_rejected_before_any_storage(self):
        for value in ({'engine':'market-extremes','generated_at':support.STAMP},{'generated_at':support.STAMP},None,[],0):
            s=support.Storage()
            with self.assertRaisesRegex(ValueError,'engine identity'):store.publish(s,'synthetic','capitulation',value)
            self.assertEqual(s.reads,[]);self.assertEqual(s.writes,[])
    def test_unknown_destination_is_rejected_before_any_storage(self):
        s=support.Storage()
        with self.assertRaisesRegex(ValueError,'Reviewed engine'):store.publish(s,'synthetic','unknown',{'engine':'unknown','generated_at':support.STAMP})
        self.assertEqual(s.reads,[]);self.assertEqual(s.writes,[])
    def test_matching_packet_writes_only_its_explicit_destination(self):
        for engine in ('capitulation','market-extremes'):
            s,i,p=support.packet(engine);before=set(s.objects);self.assertTrue(store.publish(s,'synthetic',engine,p))
            self.assertEqual(set(s.objects)-before,{'data/'+engine+'.json'})
            self.assertEqual(json.loads(s.objects['data/'+engine+'.json']),p)
    def test_run_cannot_publish_a_compiler_result_routed_to_the_other_engine(self):
        s,_=support.fixture();real=store.compile_output
        def altered(*args,**kwargs):return {**real(*args,**kwargs),'engine':'market-extremes'}
        with patch.object(store,'compile_output',side_effect=altered),patch.object(store,'now',return_value=support.STAMP):
            with self.assertRaisesRegex(RuntimeError,'publication not verified'):
                store.run(s,'synthetic','capitulation','isolated-routing','isolated-execution',60)
        result=json.loads(s.objects[store.request_key('capitulation','isolated-routing')])
        self.assertEqual(result['status'],'failed');self.assertEqual(result['phase'],'publish')
        self.assertNotIn('data/capitulation.json',s.objects);self.assertNotIn('data/market-extremes.json',s.objects)
    def test_genuine_pre_edit_run_reproduces_with_current_code_without_mutation(self):
        s,p=frozen();before=dict(s.objects);out=store.replay(p['replay'],store.reader(s,'synthetic'))
        self.assertEqual(out,{k:v for k,v in p.items() if k!='replay'});self.assertEqual(s.objects,before);self.assertEqual(s.writes,[])
        self.assertIsNone(out['call']);self.assertTrue(all(out[k] is False for k in model.PERMISSIONS))
    def test_retained_predecessor_code_must_match_every_exact_byte(self):
        s,p=frozen();m=json.loads(s.objects[p['replay']['manifest_key']]);ref=m['compilers']['extremes_native_store'];s.objects[ref['key']]+=b'\n'
        with self.assertRaisesRegex(ValueError,'compiler'):store.replay(p['replay'],store.reader(s,'synthetic'))
        self.assertEqual(s.writes,[])
    def test_unknown_revision_or_changed_calculation_compiler_is_not_reviewed(self):
        for name in ('extremes_native_store','extremes_native_model'):
            s,p=frozen();m=json.loads(s.objects[p['replay']['manifest_key']]);code=s.objects[m['compilers'][name]['key']]+b'\n';digest=model.sha(code);key=model.PREFIX+'compilers/'+digest+'.py'
            s.objects[key]=code;m['compilers'][name]={'key':key,'sha256':digest};raw=model.encoded(m);key=model.PREFIX+'runs/'+model.sha(raw)+'.json';s.objects[key]=raw
            with self.assertRaisesRegex(ValueError,'compiler'):store.replay({**p['replay'],'manifest_key':key},store.reader(s,'synthetic'))
            self.assertEqual(s.writes,[])

def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Routing))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
