from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6198_geopolitical_original_baseline as op


class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
class Denied(Exception):response={'Error':{'Code':'AccessDenied'}}


class Memory:
    def __init__(self,raw=None):self.data={op.KEYS[0]:raw} if raw is not None else {};self.reads=[];self.writes=[];self.error=None;self.truncate=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.error:raise self.error()
        if key not in self.data:raise Missing()
        raw=self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+(1 if self.truncate else 0),'ETag':'complete-version','LastModified':datetime.now(timezone.utc)}
    def put_object(self,**kw):
        assert kw['Key'].startswith(op.PRIVATE) and kw['IfNoneMatch']=='*'
        self.writes.append(kw['Key']);self.data[kw['Key']]=kw['Body']


class Tests(unittest.TestCase):
    def test_whole_pinned_source_enumerates_both_native_inputs(self):
        raw=(ROOT/'tests/fixtures/pre-research-geopolitical-risk.py.txt').read_bytes()
        self.assertEqual(op.source_reads(raw),sorted(op.INPUTS))
        with self.assertRaises(ValueError):op.source_reads(raw+b'\n# changed source')

    def test_every_corpus_entry_is_accounted_for_even_beyond_native_cap(self):
        rows=[{'name':str(i),'url':'https://example.test/'+str(i)} for i in range(164)]
        inv=op.corpus_inventory(json.dumps({'geopolitics':rows,'analysis':rows[:5]}).encode())
        self.assertEqual(inv['unique_configured'],164)
        self.assertEqual(inv['native_selected']+inv['native_omitted_by_cap'],rows)
        self.assertEqual(len(inv['native_selected']),120);self.assertEqual(len(inv['native_omitted_by_cap']),44)
        with self.assertRaises(ValueError):op.corpus_inventory(b'{"analysis":[{"name":"missing URL"}]}')

    def test_complete_corrupt_and_empty_originals_are_preserved_without_replacement(self):
        for raw in (b'{"days":{"2026-09-25":{"US":{"v":0}}}}',b'{bad',b''):
            mem=Memory(raw);row=op.capture(mem,op.KEYS[0]);ref=row['original']
            self.assertEqual(mem.data[ref['key']],raw);self.assertEqual(mem.data[op.KEYS[0]],raw)
            self.assertEqual(row['status'],'whole_object_retained')
            self.assertTrue(all(k.startswith(op.PRIVATE) for k in mem.writes))

    def test_missing_denied_truncated_and_private_reads_are_distinct(self):
        self.assertEqual(op.capture(Memory(),op.KEYS[0]),{'status':'missing'})
        mem=Memory(b'{}');mem.error=Denied
        with self.assertRaises(Denied):op.capture(mem,op.KEYS[0])
        mem=Memory(b'{}');mem.truncate=True
        with self.assertRaises(ValueError):op.capture(mem,op.KEYS[0])
        self.assertEqual(mem.writes,[])
        mem=Memory()
        with self.assertRaises(ValueError):op.capture(mem,'private/account.json')
        self.assertEqual(mem.reads,[])


if __name__=='__main__':unittest.main()
