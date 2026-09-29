"""Complete scenario publication integrity with synthetic objects only."""
from pathlib import Path
import base64,copy,hashlib,io,json,unittest
from unittest.mock import patch
from botocore.response import StreamingBody
from botocore.exceptions import IncompleteReadError
import scenario_publication as p
from scenario_test_support import S3

ROOT=Path(__file__).resolve().parents[4]
FROZEN=ROOT/'tests/fixtures/pre-scenario-publication-native.json'
AT='2026-09-29T13:00:00Z'

def frozen():
    doc=json.loads(FROZEN.read_bytes());client=S3()
    client.docs={key:base64.b64decode(raw) for key,raw in doc['objects'].items()}
    client.reads.clear();client.writes.clear()
    return client,doc

class Client:
    def __init__(self,body,length=None,etag='synthetic'):
        self.body,self.length,self.etag=body,length,etag
    def get_object(self,**kw):return {'Body':self.body,'ContentLength':self.length,'ETag':self.etag}

class Chunks(io.BytesIO):
    def read(self,n=-1):return super().read(min(n,3) if n>=0 else 3)

class Integrity(unittest.TestCase):
    def test_complete_short_chunks_and_exact_bound(self):
        raw=b'complete retained evidence';body=Chunks(raw)
        self.assertEqual(p.read(Client(body,len(raw)),'synthetic',p.CURRENT),(raw,'synthetic'));self.assertTrue(body.closed)
        body=io.BytesIO(b'x'*p.LIMIT);self.assertEqual(len(p.read(Client(body,p.LIMIT),'synthetic',p.CURRENT)[0]),p.LIMIT);self.assertTrue(body.closed)

    def test_sdk_incomplete_declared_transfer_is_rejected_and_closed(self):
        body=io.BytesIO(b'{"synthetic":1}');sdk=StreamingBody(body,99)
        with self.assertRaises(IncompleteReadError):p.read(Client(sdk,99),'synthetic',p.CURRENT)
        self.assertTrue(body.closed)

    def test_declared_length_types_and_mismatch_close_every_body(self):
        for length in (True,1.0,-1,'2',p.LIMIT+1,1,3):
            body=io.BytesIO(b'{}')
            with self.assertRaises(ValueError):p.read(Client(body,length),'synthetic',p.CURRENT)
            self.assertTrue(body.closed)

    def test_oversized_malformed_and_failed_streams_are_closed(self):
        body=io.BytesIO(b'x'*(p.LIMIT+1))
        with self.assertRaises(ValueError):p.read(Client(body),'synthetic',p.CURRENT)
        self.assertTrue(body.closed)
        class Broken(io.BytesIO):
            def read(self,n):raise RuntimeError('synthetic failure')
        body=Broken(b'x')
        with self.assertRaises(RuntimeError):p.read(Client(body),'synthetic',p.CURRENT)
        self.assertTrue(body.closed)
        class Wrong(io.BytesIO):
            def read(self,n):return 'not bytes'
        body=Wrong()
        with self.assertRaises(ValueError):p.read(Client(body),'synthetic',p.CURRENT)
        self.assertTrue(body.closed)

    def test_strict_json_rejects_ambiguous_identifiers_and_invalid_numbers(self):
        for raw in (b'{"call":"LONG","call":"WAIT"}',b'{"a":{"x":1,"\\u0078":2}}',b'{"x":NaN}',b'{"x":1e999}',b'{"x":"\\ud800"}',b'\xef\xbb\xbf{}',b'{"x":"\xc3("}'):
            with self.assertRaises(ValueError):p.decoded(raw)
        self.assertEqual(p.decoded(b'{"x":"\\ud83c\\udfe6","z":0}'),{'x':'🏦','z':0})
        with self.assertRaises(ValueError):p.decoded(b'['*140+b'0'+b']'*140)

    def test_reference_rejects_float_bool_path_and_hash_before_read(self):
        raw=b'{}';valid=p.reference(p.PREFIX+'outputs/'+p.digest(raw)+'.json',raw)
        calls=[]
        for key,value in [('bytes',2.0),('bytes',True),('bytes',0),('bytes',p.LIMIT+1),('sha256','a'*63),('key',p.CURRENT)]:
            bad=dict(valid);bad[key]=value
            with self.assertRaises(ValueError):p.verified(bad,lambda key:calls.append(key),'outputs')
        self.assertEqual(calls,[]);self.assertEqual(p.verified(valid,lambda key:raw,'outputs'),{})
        for value in (b'[]',b'{}suffix',bytearray(raw)):
            with self.assertRaises(ValueError):p.verified(valid,lambda key:value,'outputs')

    def test_invalid_publication_clocks_fail_before_any_read_or_write(self):
        for stamp in ('2026-02-30T12:00:00Z','2026-09-29T24:00:00Z','2026-09-29T12:00:00+00:60','2026-09-29T12:00:00','20260929T120000Z','',True):
            s3=S3()
            with self.assertRaises(ValueError):p.run(s3,'synthetic',stamp)
            self.assertEqual(s3.reads,[]);self.assertEqual(s3.writes,[])
        self.assertEqual(p.clock('2026-09-29T14:00:00+01:00'),p.clock(AT))

    def test_exact_predecessor_replays_with_current_code_and_no_private_reads(self):
        s3,doc=frozen();manifest=json.loads(s3.docs[doc['result']['replay']['key']]);calls=[]
        def reader(key):
            self.assertTrue(key.startswith(p.PREFIX));calls.append(key);return s3.docs[key]
        out=p.replay(manifest,reader)
        self.assertEqual(out,{k:v for k,v in doc['packet'].items() if k!='replay'})
        self.assertEqual(set(calls),{ref['key'] for ref in manifest['compilers'].values()}|{manifest['output']['key']});self.assertEqual(s3.writes,[])

    def test_exact_predecessor_migration_preserves_every_field_and_retained_source(self):
        s3,doc=frozen();before=s3.docs[p.CURRENT];result=p.run(s3,'synthetic',AT);self.assertTrue(result['published'])
        current=p.decoded(s3.docs[p.CURRENT]);manifest=p.verified(current['replay'],lambda key:s3.docs[key],'runs')
        self.assertEqual(p.replay(manifest,lambda key:s3.docs[key]),{k:v for k,v in current.items() if k!='replay'})
        self.assertEqual(s3.docs[p.PRIVATE+p.digest(before)+'.bin'],before)
        self.assertEqual(current['scenario_model'],doc['packet']['scenario_model']);self.assertEqual(current['whole_preceding_product'],doc['packet']['whole_preceding_product'])
        for key in doc['packet']:
            if key not in ('generated_at','compilers','replay'):self.assertEqual(current[key],doc['packet'][key],key)
        docs=dict(s3.docs);writes=list(s3.writes);self.assertFalse(p.run(s3,'synthetic','2026-09-30T13:00:00Z')['published']);self.assertEqual(s3.docs,docs);self.assertEqual(s3.writes,writes)

    def test_unknown_or_forged_preceding_current_cannot_migrate(self):
        for kind in ('compilers','sized_positions','extra','permissions'):
            s3,doc=frozen();current=doc['packet']
            if kind=='compilers':current['compilers']={}
            elif kind=='sized_positions':current['sized_positions']=[{'ticker':'SYNTHETIC_FORGED','suggested_size_pct':123}]
            elif kind=='extra':current['extra']='unreviewed'
            else:current['permissions']['sizing_eligible']=True
            s3.docs[p.CURRENT]=p.encoded(current);before=dict(s3.docs)
            with self.assertRaises(ValueError):p.run(s3,'synthetic',AT)
            self.assertEqual(s3.docs,before);self.assertEqual(s3.writes,[])

    def test_duplicate_current_fields_cannot_enter_migration_or_unchanged_branch(self):
        for current_generation in (False,True):
            s3,doc=frozen()
            if current_generation:p.run(s3,'synthetic',AT)
            s3.docs[p.CURRENT]=s3.docs[p.CURRENT].replace(b'"call":"WAIT"',b'"call":"LONG","call":"WAIT"');writes=list(s3.writes);before=dict(s3.docs)
            with self.assertRaises(ValueError):p.run(s3,'synthetic','2026-09-30T13:00:00Z')
            self.assertEqual(s3.docs,before);self.assertEqual(s3.writes,writes)

    def test_retained_predecessor_code_corruption_is_not_executed_or_migrated(self):
        s3,doc=frozen();key=doc['packet']['compilers']['scenario_publication.py']['key'];s3.docs[key]=b'raise RuntimeError("DO NOT EXECUTE ARCHIVED CODE")'
        with self.assertRaises(ValueError):p.run(s3,'synthetic',AT)
        self.assertEqual(s3.writes,[])

    def test_replay_manifest_extra_fields_and_typed_predecessor_are_rejected(self):
        s3,doc=frozen();manifest=json.loads(s3.docs[doc['result']['replay']['key']])
        for mutate in (lambda v:v.update(extra=0),lambda v:v.update(generated_at='2026-02-30T12:00:00Z'),lambda v:v['whole_preceding_product'].update(bytes=1.0),lambda v:v['whole_preceding_product'].update(qualification='validated'),lambda v:v['compilers']['scenario_model.js'].update(bytes=7387.0)):
            bad=copy.deepcopy(manifest);mutate(bad)
            with self.assertRaises(ValueError):p.replay(bad,lambda key:s3.docs[key])
        self.assertEqual(s3.writes,[])

    def test_hash_matching_duplicate_output_is_rejected(self):
        s3,doc=frozen();manifest=json.loads(s3.docs[doc['result']['replay']['key']]);raw=s3.docs[manifest['output']['key']].replace(b'"call":"WAIT"',b'"call":"LONG","call":"WAIT"');key=p.PREFIX+'outputs/'+p.digest(raw)+'.json';s3.docs[key]=raw;manifest['output']=p.reference(key,raw)
        with self.assertRaises(ValueError):p.replay(manifest,lambda key:s3.docs[key])

    def test_migration_refuses_earlier_publication_before_any_write(self):
        s3,doc=frozen()
        with self.assertRaises(ValueError):p.run(s3,'synthetic','2026-09-29T11:59:59Z')
        self.assertEqual(s3.writes,[])

    def test_migration_preserves_concurrent_pointer_and_complete_old_bytes(self):
        s3,doc=frozen();s3.race=True;before=s3.docs[p.CURRENT]
        with self.assertRaises(Exception):p.run(s3,'synthetic',AT)
        self.assertEqual(s3.docs[p.CURRENT],before);self.assertEqual(s3.docs[p.PRIVATE+p.digest(before)+'.bin'],before)

    def test_empty_or_nonstring_etag_cannot_write(self):
        for etag in ('',None,True,123):
            s3,doc=frozen();body=io.BytesIO(s3.docs[p.CURRENT])
            with patch.object(s3,'get_object',return_value={'Body':body,'ETag':etag}):
                with self.assertRaises(ValueError):p.run(s3,'synthetic',AT)
            self.assertTrue(body.closed);self.assertEqual(s3.writes,[])

if __name__=='__main__':unittest.main()
