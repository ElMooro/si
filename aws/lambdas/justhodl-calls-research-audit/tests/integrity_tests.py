"""Actual current auditor with complete synthetic storage and SDK responses."""
import copy,hashlib,io,json,unittest
from datetime import datetime,timezone
from botocore.response import StreamingBody
from botocore.exceptions import IncompleteReadError

KEY='data/ai-brief-public.json'
HISTORY='data/decisive-call-history.json'
AT=datetime(2026,9,18,17,5,tzinfo=timezone.utc)


class Integrity(unittest.TestCase):
    def setUp(self):
        self.mod,self.store,self.public,self.row=self.factory()
        self.bodies=[];get=self.store.get_object
        def tracked(**kw):
            response=get(**kw);self.bodies.append(response['Body']);return response
        self.store.get_object=tracked
    def verify(self):return self.mod.verify_current(self.store,'fixture',AT)
    def rejected(self):
        with self.assertRaises((ValueError,TypeError,UnicodeError)):self.verify()
        self.assertTrue(self.bodies and all(b.closed for b in self.bodies))
    def test_exact_received_bytes_and_snapshot_are_bound(self):
        raw=b' \n'+json.dumps(self.public,ensure_ascii=True,indent=2).encode()+b'\n'
        self.store.objects[KEY]=raw;proof=self.verify()
        self.assertEqual(proof['public_object'],{'key':KEY,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
        self.assertEqual(proof['snapshot_id'],self.public['snapshot_id'])
        self.assertEqual(proof['publication_generated_at'],self.public['generated_at'])
        self.assertIs(proof['decision_eligible'],False)
        self.assertTrue(all(b.closed for b in self.bodies))
    def test_native_producer_decision_metadata_remains_supported(self):
        self.public.update(decision_status=self.row['decision_status'],decision_reason=self.row['decision_reason'])
        self.store.objects[KEY]=self.mod.canonical(self.public)
        self.assertEqual(self.verify()['status'],'reproduced')
    def test_duplicate_current_clocks_are_not_silently_selected(self):
        self.store.objects[KEY]=b'{"generated_at":"2000-01-01T00:00:00Z",'+self.store.objects[KEY][1:]
        self.rejected()
    def test_escaped_and_nested_duplicate_keys_fail(self):
        for raw in (b'{"x":0,"\\u0078":1}',b'{"rows":[{"value":0,"value":1}]}'):
            self.store.objects[KEY]=raw;self.rejected()
    def test_nonfinite_invalid_unicode_and_nonobject_documents_fail(self):
        for raw in (b'{"v":NaN}',b'{"v":Infinity}',b'{"v":-Infinity}',b'{"v":1e999}',b'{"v":"\xff"}',b'{"v":"\\ud800"}',b'\xef\xbb\xbf{}',b'[]',b'null',b'0',b'{}{}'):
            self.store.objects[KEY]=raw;self.rejected()
    def test_hash_consistent_bundle_with_duplicate_identity_fails(self):
        key=self.public['research_replay']['bundle_key']
        raw=b'{"payload_sha256":"' + b'0'*64 + b'",'+self.store.objects[key][1:]
        self.store.objects[key]=raw
        self.public['research_replay']['bundle_sha256']=hashlib.sha256(raw).hexdigest()
        self.store.objects[KEY]=self.mod.canonical(self.public);self.rejected()
    def test_history_duplicate_keys_are_not_silently_selected(self):
        self.store.objects[HISTORY]=b'{"snapshots":[], '+self.store.objects[HISTORY][1:]
        self.rejected()
    def test_history_requires_one_matching_snapshot(self):
        for rows in ([self.row,self.row],[None],{},None,[]):
            self.store.objects[HISTORY]=self.mod.canonical({'snapshots':rows});self.rejected()
    def test_fragmented_body_is_read_to_eof_and_closed(self):
        class Fragmented(io.BytesIO):
            def read(self,n=-1):return super().read(min(n,13))
        bodies=[]
        def get(**kw):
            raw=self.store.objects[kw['Key']];body=Fragmented(raw);bodies.append(body)
            return {'Body':body,'ContentLength':len(raw)}
        self.store.get_object=get
        self.assertEqual(self.verify()['status'],'reproduced');self.assertTrue(all(b.closed for b in bodies))
    def test_sdk_truncated_body_does_not_become_a_valid_empty_object(self):
        raw=io.BytesIO(b'{}');body=StreamingBody(raw,100)
        self.store.get_object=lambda **kw:{'Body':body}
        with self.assertRaises(IncompleteReadError):self.verify()
        self.assertTrue(raw.closed)
    def test_reported_length_and_nonbyte_chunks_fail_and_close(self):
        for body,length in [(io.BytesIO(b'{}'),3),(io.BytesIO(b'{}'),True),(io.StringIO('{}'),2)]:
            self.store.get_object=lambda **kw:{'Body':body,'ContentLength':length}
            with self.assertRaises(ValueError):self.verify()
            self.assertTrue(body.closed)
    def test_failed_audit_retires_same_run_and_closes_failure_path_reads(self):
        self.mod.BUCKET='fixture';self.mod.lambda_handler({},None)
        self.store.objects[self.public['research_replay']['bundle_key']]+=b' '
        with self.assertRaises(RuntimeError):self.mod.lambda_handler({},None)
        key='data/calls-research-proofs/'+self.public['research_replay']['payload_sha256']+'.json'
        self.assertEqual(json.loads(self.store.objects[key])['status'],'failed')
        self.assertTrue(all(b.closed for b in self.bodies))
    def test_ambiguous_failure_pointer_cannot_retire_arbitrary_run(self):
        self.mod.BUCKET='fixture'
        self.store.objects[KEY]=b'{"generated_at":"bad",'+self.store.objects[KEY][1:]
        with self.assertRaises(RuntimeError):self.mod.lambda_handler({},None)
        self.assertFalse(any(k.startswith('data/calls-research-proofs/') for k in self.store.objects))
        self.assertEqual(json.loads(self.store.objects['data/calls-research-audit.json'])['status'],'failed')
        self.assertTrue(all(b.closed for b in self.bodies))
    def test_wrong_output_authority_hash_and_snapshot_still_fail(self):
        for field,value in [('brief_md','altered'),('snapshot_id','other'),('call_verb','LONG'),('sizing_eligible',True),('decision_eligible',True)]:
            candidate=copy.deepcopy(self.public);candidate[field]=value
            self.store.objects[KEY]=self.mod.canonical(candidate);self.rejected()
    def test_naive_audit_clock_is_rejected_before_read(self):
        with self.assertRaises(ValueError):self.mod.verify_current(self.store,'fixture',AT.replace(tzinfo=None))
        self.assertEqual(self.bodies,[])
    def test_unrecorded_lineage_and_decision_explanations_cannot_earn_proof(self):
        for field,value in [('original_source_lineage',{'ciss':{'status':'verified'}}),('decision_reason','made up'),('unrecorded',42)]:
            candidate=copy.deepcopy(self.public);candidate[field]=value
            self.store.objects[KEY]=self.mod.canonical(candidate);self.rejected()
    def test_missing_null_output_is_not_identical_to_present_null(self):
        self.assertIsNone(self.public['model']);del self.public['model']
        self.store.objects[KEY]=self.mod.canonical(self.public);self.rejected()


def run(factory):
    Integrity.factory=staticmethod(factory)
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Integrity))
    if not result.wasSuccessful():raise SystemExit(1)
