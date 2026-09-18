import ast
import gzip
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared/tests'), str(ROOT/'aws/shared'), str(Path(__file__).resolve().parents[1]/'source')]
import report_source_store as store
from test_report_observations import inputs
from evidence_store import capture


class StorageError(Exception):
    def __init__(self, value):self.response={'Error':{'Code':value}}


class Storage:
    def __init__(self):self.objects={};self.current_writes=0;self.race=False
    def get_object(self, Bucket, Key):
        if Key not in self.objects:raise StorageError('NoSuchKey')
        body,meta=self.objects[Key]
        return {'Body':io.BytesIO(body),'Metadata':meta,'ETag':str(hash(body))}
    def put_object(self, Bucket, Key, Body, **kwargs):
        if Key==store.CURRENT:
            self.current_writes+=1
            if self.race:
                self.race=False
                self.objects[Key]=(b'{"generated_at":"2099-01-01T00:00:00+00:00"}',{})
                raise StorageError('PreconditionFailed')
        if kwargs.get('IfNoneMatch')=='*' and Key in self.objects:raise StorageError('PreconditionFailed')
        self.objects[Key]=(Body,kwargs.get('Metadata',{}))


def captured(client):
    item=inputs()
    item['acquired_at']=store.now()
    for part in ('definition','observations'):
        raw=json.dumps(item[part]).encode()
        url=item['evidence'][part]['source_url']
        item['evidence'][part]=capture(client,'b','fred',url,raw)
    return item


class ReportStoreTests(unittest.TestCase):
    def test_stored_originals_replay_before_current_publication(self):
        s=Storage();item=captured(s)
        result=store.publish(s,'b',{'TEST':{}},{'TEST':item},{})
        self.assertTrue(result['published'])
        packet=json.loads(s.objects[store.CURRENT][0]);self.assertEqual(packet['measurements']['TEST']['month_pct'],10)
        self.assertFalse(packet['sizing_eligible'])

    def test_corrupt_stored_original_never_publishes_current(self):
        s=Storage();item=captured(s)
        s.objects[item['evidence']['observations']['key']]=(gzip.compress(b'{}'),{})
        with self.assertRaises(ValueError):store.publish(s,'b',{'TEST':{}},{'TEST':item},{})
        self.assertEqual(s.current_writes,0)

    def test_concurrent_newer_publication_wins(self):
        s=Storage();item=captured(s);s.race=True
        result=store.publish(s,'b',{'TEST':{}},{'TEST':item},{})
        self.assertFalse(result['published']);self.assertEqual(s.current_writes,1)

    def test_storage_denial_is_not_empty_cache(self):
        class Denied:
            def get_object(self,**kwargs):raise StorageError('AccessDenied')
        with self.assertRaises(StorageError):store.acquire(Denied(),'b','TEST','hidden',10)

    def test_real_research_handler_does_not_enter_legacy_path(self):
        path=ROOT/'aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py'
        node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        calls=[]
        env={'time':store.time,'json':json,'track_errors':lambda fn:fn,'s3':object(),'S3_BUCKET':'b','FRED_KEY':'private',
             'FRED_SERIES':{'ICSA':('macro','Initial Claims')},'run_source_research':lambda *a,**kw:calls.append((a[2],kw)) or {'published':True}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        out=env['lambda_handler']({'action':'research_measurements'},None)
        self.assertEqual(out['statusCode'],200);self.assertEqual(calls[0][0],{'ICSA':{'category':'macro','display_name':'Initial Claims'}})


if __name__=='__main__':unittest.main()
