from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from unittest.mock import Mock
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6213_freight_original_baseline as op

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Memory:
    def __init__(self):self.rows={};self.reads=[];self.writes=[];self.truncate=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.rows:raise Error('NoSuchKey')
        raw=self.rows[key];return {'Body':BytesIO(raw),'ContentLength':len(raw)+int(self.truncate),'ETag':'whole-version','LastModified':datetime(2026,9,27,tzinfo=timezone.utc)}
    def put_object(self,**kw):
        assert kw['Key'].startswith(op.PRIVATE) and kw['IfNoneMatch']=='*'
        if kw['Key'] in self.rows:raise Error('PreconditionFailed')
        self.rows[kw['Key']]=kw['Body'];self.writes.append(kw['Key'])

class Tests(unittest.TestCase):
    def test_all_original_producer_sources_pinned_without_executing(self):
        for fn in op.PINS:
            raw=(ROOT/'tests/fixtures'/('pre-freight-research-'+fn.removeprefix('justhodl-')+'.py.txt')).read_bytes()
            self.assertEqual(op.source_check(fn,raw)['sha256'],op.PINS[fn])
            with self.assertRaises(ValueError):op.source_check(fn,raw+b'\n')
    def test_private_or_foreign_paths_never_read(self):
        memory=Memory()
        for key in ('data/pm-decision.json','learning/morning_run_log.json','private/account.json',op.ARCHIVE+'2026-02-30.json',op.ARCHIVE+'../account.json'):
            with self.assertRaises(ValueError):op.capture(memory,key)
        self.assertEqual(memory.reads,[])
    def test_originals_including_malformed_empty_and_zero_are_retained_whole(self):
        for raw in (b'{"zero":0,"missing":null}',b'{bad',b''):
            memory=Memory();memory.rows[op.KEYS[0]]=raw;out=op.capture(memory,op.KEYS[0]);self.assertEqual(memory.rows[out['original']['key']],raw);self.assertEqual(memory.rows[op.KEYS[0]],raw)
            self.assertFalse(out['metadata']['original_provider_verified']);self.assertEqual(op.capture(memory,op.KEYS[0]),out)
    def test_denial_truncation_corrupt_retention_and_disappearing_history_fail(self):
        self.assertEqual(op.capture(Memory(),op.KEYS[0]),{'status':'missing'})
        with self.assertRaises(Error):op.capture(Memory(),op.ARCHIVE+'2026-09-01.json',required=True)
        memory=Memory();memory.rows[op.KEYS[0]]=b'{}';memory.truncate=True
        with self.assertRaises(ValueError):op.capture(memory,op.KEYS[0])
        self.assertEqual(memory.writes,[])
        memory=Memory();memory.rows[op.PRIVATE+op.sha(b'whole')+'.bin']=b'corrupt'
        with self.assertRaises(ValueError):op.retain(memory,b'whole')
        s3=Mock();s3.get_object.side_effect=Error('AccessDenied')
        with self.assertRaises(Error):op.capture(s3,op.KEYS[0])
    def test_all_archive_pages_and_empty_history_are_distinct(self):
        s3=Mock();s3.list_objects_v2.side_effect=[{'IsTruncated':True,'Contents':[{'Key':op.ARCHIVE+'2026-09-01.json'}],'NextContinuationToken':'next'},{'IsTruncated':False,'Contents':[{'Key':op.ARCHIVE+'2026-09-02.json'}]}]
        self.assertEqual(len(op.archive_keys(s3)),2);self.assertEqual(s3.list_objects_v2.call_args.kwargs['ContinuationToken'],'next')
        s3=Mock();s3.list_objects_v2.return_value={'IsTruncated':False};self.assertEqual(op.archive_keys(s3),[])
    def test_incomplete_duplicate_or_foreign_archive_enumeration_refuses(self):
        bad=[{}, {'IsTruncated':True}, {'IsTruncated':False,'Contents':[{'Key':'private/account'}]}, {'IsTruncated':False,'Contents':[{'Key':op.ARCHIVE+'2026-09-01.json'}]*2}]
        for page in bad:
            s3=Mock();s3.list_objects_v2.return_value=page
            with self.assertRaises(ValueError):op.archive_keys(s3)
        s3=Mock();s3.list_objects_v2.return_value={'IsTruncated':True,'NextContinuationToken':'same'}
        with self.assertRaises(ValueError):op.archive_keys(s3)

if __name__=='__main__':unittest.main(verbosity=2)
