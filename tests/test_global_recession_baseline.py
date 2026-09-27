from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from unittest.mock import patch
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6189_global_recession_baseline as op

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Memory:
    def __init__(self):self.data={};self.reads=[]
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key not in self.data:raise Error('NoSuchKey')
        return {'Body':BytesIO(self.data[Key]),'ETag':'version','LastModified':datetime(2026,9,26,tzinfo=timezone.utc),'ContentLength':len(self.data[Key])}
    def put_object(self,Bucket,Key,Body,**kw):
        if kw.get('IfNoneMatch')=='*' and Key in self.data:raise Error('PreconditionFailed')
        self.data[Key]=Body

class Tests(unittest.TestCase):
    def test_partial_source_fails_before_retaining_a_prefix(self):
        m=Memory();m.data['data/global-recession.json']=b'{}';get=m.get_object
        m.get_object=lambda **kw:{**get(**kw),'ContentLength':99}
        with self.assertRaises(ValueError):op.capture(m,'data/global-recession.json')
        self.assertFalse(any(k.startswith(op.PRIVATE) for k in m.data))

    def test_every_preserved_predecessor_read_is_reviewed_without_importing_engine(self):
        source=(ROOT/'tests/fixtures/pre-research-global-recession.py.txt').read_text(encoding='utf-8')
        keys=op.source_reads(source);self.assertEqual(keys['fixed_derived_inputs'],list(op.INPUTS));self.assertEqual(len(op.KEYS),7)
        self.assertEqual(keys['native_writer_stages'],2);self.assertFalse(keys['original_provider_responses_retained'])
        for invalid in ('S3.get_object(Bucket=BUCKET,Key="data/portfolio.json")','get_bytes(dynamic)','get_json("data/portfolio.json")','get_bytes("data/new-input.json")'):
            with self.assertRaises(ValueError):op.source_reads(source+'\n'+invalid)

    def test_whole_packet_and_empty_predecessors_remain_distinct_and_unqualified(self):
        m=Memory();raw=b'{"generated_at":"old","rows":[0,null,1,2,3],"call":"LONG"}';m.data['data/global-recession.json']=raw
        row=op.capture(m,'data/global-recession.json');self.assertEqual(m.data[row['original']['key']],raw)
        self.assertIs(row['metadata']['forecast_qualified'],False);self.assertIs(row['metadata']['original_provider_verified'],False)
        self.assertEqual(op.retain(m,b'')['bytes'],0)
        self.assertEqual(op.capture(m,'data/oecd-cli.json')['status'],'missing')

    def test_denied_private_paths_corrupt_retention_and_access_denials_fail(self):
        m=Memory()
        with self.assertRaises(ValueError):op.capture(m,'data/pm-decision.json')
        self.assertEqual(m.reads,[])
        raw=b'whole';m.data[op.PRIVATE+op.sha(raw)+'.bin']=b'broken'
        with self.assertRaises(ValueError):op.retain(m,raw)
        with patch.object(m,'get_object',side_effect=Error('AccessDenied')),self.assertRaises(Error):op.capture(m,'data/global-recession.json')

if __name__=='__main__':unittest.main()
