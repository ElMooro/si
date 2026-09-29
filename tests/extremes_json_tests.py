"""Unambiguous JSON across native capture, retained replay and HTTP reads.

Only reviewed current modules and synthetic storage are executed.
"""
import importlib.util,json,math,sys,time,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import extremes_native_store as store
from extremes_native_test_support import Storage,fixture,packet,STAMP


def duplicate(raw,key='generated_at',value='2000-01-01T00:00:00Z'):
    assert raw.startswith(b'{')
    return b'{'+json.dumps(key).encode()+b':'+json.dumps(value).encode()+b','+raw[1:]


def corrupt_upstream(s,kind):
    """Rehash ambiguous runs; corrupt output bytes under their existing identity."""
    key=store.model.SOURCES['crisis'][0];p=json.loads(s.objects[key])
    run=json.loads(s.objects[p['replay']['manifest_key']]);out_raw=s.objects[run['output']['key']]
    if kind=='output':
        # A rehashed noncanonical output already fails model.identity; retain
        # that protection and exercise corrupt bytes at the declared output.
        out_raw=duplicate(out_raw)
        s.objects[run['output']['key']]=out_raw
    run_raw=store.model.encoded(run)
    if kind=='run':run_raw=duplicate(run_raw)
    runkey='data/crisis-research/runs/'+store.model.sha(run_raw)+'.json'
    p['replay']['manifest_key']=runkey;s.objects[runkey]=run_raw;s.objects[key]=store.model.encoded(p)
    return run_raw if kind=='run' else out_raw


class StrictJSON(unittest.TestCase):
    def test_duplicate_keys_at_every_depth_and_escaped_equivalents_are_rejected(self):
        for raw in (b'{"clock":1,"clock":2}',b'{"nested":[{"x":1,"x":1}]}',b'{"clock":1,"clo\\u0063k":1}'):
            with self.subTest(raw=raw),self.assertRaises(ValueError):store.strict_json(raw)
    def test_nonfinite_constants_and_numeric_overflow_are_rejected(self):
        for token in ('NaN','Infinity','-Infinity','1e999','-1e999'):
            with self.subTest(token=token),self.assertRaises(ValueError):store.strict_json(('{"nested":['+token+']}').encode())
    def test_finite_types_null_false_zero_and_text_are_preserved(self):
        raw=b'{"integer":0,"float":-0.0,"finite":1e308,"large":9007199254740993,"flag":false,"missing":null,"text":"NaN","nested":[{},[]]}'
        expected=json.loads(raw);actual=store.strict_json(raw)
        self.assertEqual(actual,expected);self.assertIs(type(actual['integer']),int);self.assertIs(type(actual['float']),float)
        self.assertEqual(math.copysign(1,actual['float']),-1);self.assertIs(actual['flag'],False);self.assertIsNone(actual['missing'])
    def test_malformed_and_trailing_json_are_rejected(self):
        for raw in (b'',b'{',b'{}{}',b'{"x":01}',b'{"x":true,}',b'\xff'):
            with self.subTest(raw=raw),self.assertRaises((ValueError,UnicodeDecodeError)):store.strict_json(raw)
    def test_source_duplicate_clock_is_retained_whole_but_withheld_and_replays(self):
        s,i=fixture();key=store.model.SOURCES['crisis'][0];raw=duplicate(s.objects[key]);s.objects[key]=raw
        with patch.object(store,'now',return_value=STAMP):i['sources']=store.collect(s,'synthetic','capitulation',time.monotonic()+30)
        entry=i['sources']['crisis'];self.assertEqual(entry['status'],'malformed_source_json');self.assertIs(entry['upstream_identity_verified'],False)
        self.assertEqual(entry['packet'],{'key':store.PRIVATE+store.model.sha(raw)+'.bin','sha256':store.model.sha(raw),'bytes':len(raw)})
        self.assertEqual(s.objects[entry['packet']['key']],raw)
        out=store.compile_output(i,store.reader(s,'synthetic'))
        self.assertFalse(out['eligibility']['crisis']['research_context_available']);self.assertEqual(out['contexts']['crisis'],{})
        self.assertEqual(out['decision']['verb'],'WAIT');self.assertEqual(out['decision']['eligible_votes'],0)
        ref=store.retain(s,'synthetic',i,out);self.assertEqual(store.replay(ref,store.reader(s,'synthetic')),out)
    def test_nested_duplicate_and_nonfinite_source_diagnostics_cannot_bypass_capture(self):
        for field in (b'"diagnostic":{"x":0,"x":1}',b'"diagnostic":NaN',b'"diagnostic":1e999'):
            s,i=fixture();key=store.model.SOURCES['crisis'][0];raw=b'{'+field+b','+s.objects[key][1:];s.objects[key]=raw
            with patch.object(store,'now',return_value=STAMP):i['sources']=store.collect(s,'synthetic','capitulation',time.monotonic()+30)
            e=i['sources']['crisis'];self.assertEqual(e['status'],'malformed_source_json');self.assertEqual(s.objects[e['packet']['key']],raw)
            self.assertFalse(store.compile_output(i,store.reader(s,'synthetic'))['eligibility']['crisis']['research_context_available'])
    def test_malformed_retained_original_cannot_claim_success_or_verification(self):
        for status,verified in (('retained',False),('retained',True),('malformed_source_json',True)):
            s,i=fixture();e=i['sources']['crisis'];raw=duplicate(s.objects[e['packet']['key']]);e.update(status=status,upstream_identity_verified=verified,packet=store.retain_original(s,'synthetic',raw))
            with self.subTest(status=status,verified=verified),self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'synthetic'))
    def test_parseable_original_cannot_claim_malformed_status(self):
        s,i=fixture();i['sources']['crisis'].update(status='malformed_source_json',upstream_identity_verified=False)
        with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'synthetic'))
    def test_ambiguous_upstream_run_and_output_are_retained_then_rejected(self):
        for kind in ('run','output'):
            s,_=fixture();raw=corrupt_upstream(s,kind)
            with self.subTest(kind=kind),self.assertRaises(ValueError):store.collect(s,'synthetic','capitulation',time.monotonic()+30)
            self.assertEqual(s.objects[store.PRIVATE+store.model.sha(raw)+'.bin'],raw)
    def test_capture_bound_is_enforced_before_retaining_upstream_proofs(self):
        s,_=fixture();key=store.model.SOURCES['crisis'][0];p=json.loads(s.objects[key]);run_raw=s.objects[p['replay']['manifest_key']]
        first=store.model.INPUTS['capitulation'][0];self.assertEqual(first,'crisis')
        # Remove fixture-generated private copies, keeping the synthetic source
        # objects and original output/run identities only.
        s.objects={k:v for k,v in s.objects.items() if not k.startswith(store.PRIVATE)};s.writes=[]
        with patch.object(store,'TOTAL',len(s.objects[key])+len(run_raw)-1):
            with self.assertRaisesRegex(ValueError,'Total capture bound'):store.collect(s,'synthetic','capitulation',time.monotonic()+30)
        self.assertEqual([w['Body'] for w in s.writes],[s.objects[key]])
        self.assertNotIn(store.PRIVATE+store.model.sha(run_raw)+'.bin',s.objects)
    def test_hash_consistent_ambiguous_retained_artifact_is_rejected(self):
        s=Storage();raw=b'{"engine":"a","engine":"b"}';digest=store.model.sha(raw)
        for category in ('inputs','outputs'):
            key=store.PREFIX+category+'/'+digest+'.json';s.objects[key]=raw
            with self.subTest(category=category),self.assertRaises(ValueError):store.checked({'key':key,'sha256':digest,'bytes':len(raw)},category,store.reader(s,'synthetic'))
    def test_hash_consistent_ambiguous_run_manifest_is_rejected(self):
        s,_,p=packet();raw=duplicate(s.objects[p['replay']['manifest_key']]);key=store.PREFIX+'runs/'+store.model.sha(raw)+'.json';s.objects[key]=raw
        ref={**p['replay'],'manifest_key':key}
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s,'synthetic'))
    def test_current_predecessor_ambiguity_preserves_whole_object_and_no_write(self):
        s,_,p=packet();key=store.current('capitulation');raw=duplicate(store.model.encoded(p));s.objects[key]=raw;before=dict(s.objects);writes=len(s.writes)
        with self.assertRaises(ValueError):store.publish(s,'synthetic',p)
        self.assertEqual(s.objects,before);self.assertEqual(len(s.writes),writes)
    def test_ambiguous_publication_readback_is_never_success(self):
        s,_,p=packet();read=store.reader(s,'synthetic');key=store.current('capitulation')
        def altered(k):return duplicate(read(k)) if k==key else read(k)
        with patch.object(store,'reader',return_value=altered):
            with self.assertRaises(ValueError):store.publish(s,'synthetic',p)
        self.assertEqual(s.objects[key],store.model.encoded(p))
    def test_ambiguous_existing_request_never_becomes_complete_or_republishes(self):
        s=Storage();key=store.request_key('capitulation','same');raw=b'{"status":"failed","status":"complete"}';s.objects[key]=raw
        with self.assertRaises(ValueError):store.run(s,'synthetic','capitulation','same','exec')
        self.assertEqual(s.objects,{key:raw});self.assertEqual(s.writes,[])
    def test_failed_upstream_proof_keeps_current_and_records_failure(self):
        s,_=fixture();raw=corrupt_upstream(s,'run');key=store.current('capitulation');s.objects[key]=b'{"existing":true}';before=s.objects[key]
        with patch.object(store,'now',return_value=STAMP):
            with self.assertRaises(RuntimeError):store.run(s,'synthetic','capitulation','bad-proof','exec')
        self.assertEqual(s.objects[key],before);self.assertEqual(s.objects[store.PRIVATE+store.model.sha(raw)+'.bin'],raw)
        status=json.loads(s.objects[store.request_key('capitulation','bad-proof')]);self.assertEqual(status['status'],'failed');self.assertEqual(status['publication_status'],'not_attempted')
    def test_failed_readback_records_uncertainty_and_never_claims_rollback(self):
        s,_=fixture();key=store.current('capitulation');factory=store.reader
        def altered(client,bucket):
            read=factory(client,bucket)
            return lambda k:duplicate(read(k)) if k==key else read(k)
        with patch.object(store,'reader',side_effect=altered),patch.object(store,'now',return_value=STAMP):
            with self.assertRaisesRegex(RuntimeError,'publication not verified') as error:store.run(s,'synthetic','capitulation','readback','exec')
        self.assertNotIn('preserved',str(error.exception));self.assertEqual(json.loads(s.objects[key])['contract'],store.model.CONTRACT)
        status=json.loads(s.objects[store.request_key('capitulation','readback')]);self.assertEqual(status['status'],'failed');self.assertEqual(status['phase'],'publish');self.assertEqual(status['publication_status'],'unverified')
        self.assertEqual(len([w for w in s.writes if w['Key']==key]),1)
    def test_http_paths_reject_ambiguous_packets_without_writes(self):
        for engine in store.model.INPUTS:
            spec=importlib.util.spec_from_file_location('reviewed_'+engine,ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py')
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            s=Storage();key=store.current(engine);valid={'contract':store.model.CONTRACT,'engine':engine,'generated_at':STAMP,'call':None};raw=store.model.encoded(valid)
            for bad in (duplicate(raw),b'{"unused":NaN,'+raw[1:],b'{"unused":1e999,'+raw[1:],b'[]',b'null'):
                s.objects[key]=bad
                with patch('boto3.client',return_value=s):response=module.lambda_handler({'httpMethod':'GET'})
                self.assertEqual(response['statusCode'],503);self.assertEqual(json.loads(response['body']),{'reason':'native_research_publication_unavailable'})
                self.assertEqual(s.objects[key],bad);self.assertEqual(s.writes,[])
            s.objects[key]=raw
            with patch('boto3.client',return_value=s):response=module.lambda_handler({'requestContext':{'http':{'method':'GET'}}})
            self.assertEqual(response['statusCode'],200);self.assertEqual(json.loads(response['body']),valid);self.assertEqual(s.writes,[])
    def test_valid_complete_capture_replay_and_request_idempotency_stay_exact(self):
        for engine in store.model.INPUTS:
            s,_=fixture(engine)
            with patch.object(store,'now',return_value=STAMP):
                result=store.run(s,'synthetic',engine,'valid','exec1');count=len(s.writes);again=store.run(s,'synthetic',engine,'valid','exec2')
            self.assertEqual(result,again);self.assertEqual(count,len(s.writes));self.assertTrue(result['published'])
            public=json.loads(s.objects[store.current(engine)]);self.assertEqual(store.replay(public['replay'],store.reader(s,'synthetic')),{k:v for k,v in public.items() if k!='replay'})


def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(StrictJSON))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
