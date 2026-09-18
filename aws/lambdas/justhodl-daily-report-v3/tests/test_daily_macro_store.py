import ast
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(Path(__file__).resolve().parents[1]/'source')]
import daily_macro_store as store
import report_observations
from daily_macro_model import build,digest,encoded
from test_research_brief_model import source_packet,NOW
from test_daily_macro_model import auxiliary


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class S3:
    def __init__(self):self.objects={};self.puts=[];self.races=0;self.race_update=None
    def get_object(self,Bucket,Key):
        if Key not in self.objects:raise Error('NoSuchKey')
        raw=self.objects[Key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,Bucket,Key,Body,**kwargs):
        if Key==store.CURRENT and self.races:
            self.races-=1
            if self.race_update:self.objects[Key]=encoded(self.race_update)
            raise Error('PreconditionFailed')
        previous=self.objects.get(Key)
        if kwargs.get('IfNoneMatch') and previous is not None:raise Error('PreconditionFailed')
        if kwargs.get('IfMatch') and (previous is None or hashlib.sha256(previous).hexdigest()!=kwargs['IfMatch']):raise Error('PreconditionFailed')
        self.objects[Key]=Body;self.puts.append(Key)


def client():
    s3=S3();p=source_packet();p.pop('replay')
    compiler=Path(report_observations.__file__).read_bytes();sha=hashlib.sha256(compiler).hexdigest()
    manifest={'compiler':{'key':'data/report-research/compilers/'+sha+'.py','sha256':sha},'output_sha256':digest(p)}
    key='data/report-research/runs/'+digest(manifest)+'.json'
    p['replay']={'manifest_key':key,'compiler_sha256':sha,'output_sha256':digest(p)}
    s3.objects.update({store.SOURCE:encoded(p),key:encoded(manifest),manifest['compiler']['key']:compiler})
    return s3


class DailyStoreTests(unittest.TestCase):
    def setUp(self):
        patcher=patch.object(store,'datetime',SimpleNamespace(now=lambda *_:datetime.fromisoformat(NOW)))
        patcher.start();self.addCleanup(patcher.stop)

    def test_replay_base_and_preserve_newer_augmentation_on_cas_retry(self):
        s3=client();previous={'version':'V10','generated_at':'2026-09-17T00:00:00Z','defi_tvl':{'total_tvl':10},'enriched_at':'2026-09-17T01:00:00Z'}
        old=encoded(previous);s3.objects[store.CURRENT]=old;s3.races=1
        s3.race_update={**previous,'defi_tvl':{'total_tvl':20},'enriched_at':'2026-09-18T00:00:00Z'}
        result=store.run(s3,'test',auxiliary);self.assertTrue(result['published'])
        current=json.loads(s3.objects[store.CURRENT]);manifest=json.loads(s3.objects[result['replay']['manifest_key']])
        retained=json.loads(s3.objects[manifest['input']['key']])
        self.assertEqual(current['defi_tvl']['total_tvl'],20)
        self.assertEqual(digest({key:current[key] for key in manifest['base_fields']}),manifest['output_sha256'])
        self.assertEqual(digest(build(retained['macro'],retained['auxiliary'],NOW)),manifest['output_sha256'])
        self.assertEqual(s3.objects[store.PREFIX+'legacy-unvalidated/'+hashlib.sha256(old).hexdigest()+'.json'],old)
        self.assertIsNone(current['market_intelligence']['risk_score'])

    def test_corrupt_source_prevents_market_collection_and_current_write(self):
        s3=client();p=json.loads(s3.objects[store.SOURCE]);p['measurements']['ICSA']['current']=1;s3.objects[store.SOURCE]=encoded(p)
        with self.assertRaises(ValueError):store.run(s3,'test',lambda:self.fail('unverified source reached collection'))
        self.assertNotIn(store.CURRENT,s3.puts)

    def test_newer_report_and_repeated_races_do_not_overwrite(self):
        s3=client();s3.objects[store.CURRENT]=encoded({'generated_at':'2099-01-01T00:00:00Z'})
        self.assertFalse(store.run(s3,'test',auxiliary)['published']);self.assertNotIn(store.CURRENT,s3.puts)
        s3=client();s3.races=9
        with self.assertRaises(RuntimeError):store.run(s3,'test',auxiliary)
        self.assertEqual(s3.races,5)

    def test_actual_handler_has_no_legacy_scoring_or_notification_route(self):
        path=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler');node.decorator_list=[]
        calls=[];collect=object()
        env={'json':json,'s3':object(),'S3_BUCKET':'test','collect_auxiliary_observations':collect,
             'publish_daily_macro':lambda *args:calls.append(args) or {'published':True}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        self.assertEqual(env['lambda_handler']({},None)['statusCode'],200)
        self.assertIs(calls[0][-1],collect)
        env['publish_daily_macro']=lambda *args:(_ for _ in ()).throw(ValueError('private error text'))
        self.assertNotIn('private error text',env['lambda_handler']({},None)['body'])


if __name__=='__main__':unittest.main()
