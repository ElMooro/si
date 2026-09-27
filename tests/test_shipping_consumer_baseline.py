from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import Mock
import sys, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6204_shipping_consumer_baseline as op


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):self.rows={};self.reads=[]
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key not in self.rows:raise Error('NoSuchKey')
        raw=self.rows[Key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':'whole-version','LastModified':datetime(2026,9,26,tzinfo=timezone.utc)}
    def put_object(self,Bucket,Key,Body,**kwargs):
        if kwargs.get('IfNoneMatch')=='*' and Key in self.rows:raise Error('PreconditionFailed')
        self.rows[Key]=Body


class Tests(unittest.TestCase):
    def test_all_complete_consumer_sources_are_pinned_without_importing_producers(self):
        for fn in op.PINS:
            raw=(ROOT/'tests/fixtures'/('pre-shipping-qualification-'+fn.removeprefix('justhodl-')+'.py.txt')).read_bytes()
            self.assertEqual(op.source_check(fn,raw)['sha256'],op.PINS[fn])
            with self.assertRaises(ValueError):op.source_check(fn,raw+b'\n')

    def test_only_one_shipping_input_can_be_read_never_accounts_or_learning_logs(self):
        memory=Memory()
        for key in ('data/katlin.json','data/pm-decision.json','learning/morning_run_log.json','data/allocator.json'):
            with self.assertRaises(ValueError):op.capture(memory,key)
        self.assertEqual(memory.reads,[])
        raw=b'{"complete":[0,null,1],"generated_at":"old"}';memory.rows[op.KEYS[0]]=raw
        out=op.capture(memory,op.KEYS[0]);self.assertEqual(memory.rows[out['original']['key']],raw)

    def test_truncation_denied_access_and_corrupt_retention_fail(self):
        memory=Memory();memory.rows[op.KEYS[0]]=b'{}';get=memory.get_object
        memory.get_object=lambda **kw:{**get(**kw),'ContentLength':123}
        with self.assertRaises(ValueError):op.capture(memory,op.KEYS[0])
        self.assertFalse(any(k.startswith(op.PRIVATE) for k in memory.rows))
        memory=Memory();memory.rows[op.PRIVATE+op.sha(b'complete')+'.bin']=b'wrong'
        with self.assertRaises(ValueError):op.retain(memory,b'complete')
        denied=Mock();denied.get_object.side_effect=Error('AccessDenied')
        with self.assertRaises(Error):op.capture(denied,op.KEYS[0])

    def test_absent_native_function_is_reported_without_creating_or_invoking_it(self):
        lam=Mock();lam.get_function.side_effect=Error('ResourceNotFoundException')
        self.assertEqual(op.runtime(lam,Mock(),Mock(),Mock(),'justhodl-boom-stage')['status'],'function_not_deployed')
        lam.create_function.assert_not_called();lam.invoke.assert_not_called();lam.update_function_code.assert_not_called()
        lam.get_function.side_effect=Error('AccessDenied')
        with self.assertRaises(Error):op.runtime(lam,Mock(),Mock(),Mock(),'justhodl-boom-stage')


if __name__=='__main__':unittest.main(verbosity=2)
