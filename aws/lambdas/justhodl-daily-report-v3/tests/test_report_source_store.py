import ast
import gzip
import io
import json
from pathlib import Path
import sys
import unittest
from datetime import datetime, timedelta, timezone
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
    def test_failure_label_keeps_http_status_without_authenticated_url(self):
        exc=store.urllib.error.HTTPError('https://provider.invalid/?api_key=DO_NOT_PUBLISH',429,'private request',{},None)
        self.assertEqual(store.error_label(exc),'HTTP_429')

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

    def test_slow_old_acquisition_cannot_regress_a_newer_source(self):
        s=Storage();item=captured(s)
        stamp=datetime.now(timezone.utc)
        item['acquired_at']=(stamp-timedelta(hours=1)).isoformat()
        old={'contract':'report-observations.v1','generated_at':(stamp-timedelta(seconds=1)).isoformat(),
             'measurements':{'TEST':{'acquired_at':stamp.isoformat()}}}
        s.objects[store.CURRENT]=(json.dumps(old).encode(),{})
        result=store.publish(s,'b',{'TEST':{}},{'TEST':item},{})
        self.assertFalse(result['published']);self.assertEqual(s.current_writes,0)
        self.assertIn('source acquisition is newer',result['reason'])

    def test_storage_denial_is_not_empty_cache(self):
        class Denied:
            def get_object(self,**kwargs):raise StorageError('AccessDenied')
        with self.assertRaises(StorageError):store.acquire(Denied(),'b','TEST','hidden',10)

    def test_original_request_retains_extended_query_and_exact_bytes_without_secret(self):
        from urllib.parse import parse_qs,urlsplit
        s=Storage();payload=inputs()['observations'];payload['limit']=4000
        blob=json.dumps(payload).encode();requests=[]
        class Response(io.BytesIO):
            pass
        class Opener:
            def open(self,request,timeout):requests.append(request);return Response(blob)
        with patch.object(store.urllib.request,'build_opener',return_value=Opener()),patch.object(store.time,'sleep'):
            result,receipt,acquired=store.original(s,'b','TEST','observations','SECRET_TOKEN',store.time.monotonic()+120)
        query=parse_qs(urlsplit(requests[0].full_url).query)
        self.assertEqual(query['limit'],['4000']);self.assertEqual(query['units'],['lin'])
        self.assertNotIn('observation_start',query);self.assertNotIn('observation_end',query)
        self.assertNotIn('SECRET_TOKEN',json.dumps(receipt))
        self.assertEqual(store.read_verified(s,'b',receipt),blob)
        self.assertEqual(result,payload)

    def test_short_recent_cache_is_expanded_but_definition_is_reused(self):
        s=Storage();item=captured(s)
        descriptor={'contract':'report-source-cache.v1','series_id':'TEST','acquired_at':item['acquired_at'],
                    'definition_acquired_at':item['acquired_at'],'evidence':item['evidence']}
        s.objects['data/report-research/cache/TEST.json']=(json.dumps(descriptor).encode(),{})
        def original(client,bucket,sid,part,key,deadline):
            self.assertEqual(part,'observations')
            return item[part],item['evidence'][part],item['acquired_at']
        with patch.object(store,'original',side_effect=original) as fetch:
            out,error=store.acquire(s,'b','TEST','hidden',999999999)
            self.assertIsNone(error);self.assertEqual(fetch.call_count,1)
        with patch.object(store,'original',side_effect=AssertionError('should use expanded cache')):
            replay,error=store.acquire(s,'b','TEST','hidden',999999999)
        self.assertEqual(out,replay)
        self.assertEqual(json.loads(s.objects['data/report-research/cache/TEST.json'][0])['history_policy'],store.HISTORY_POLICY)

    def test_expansion_failure_retains_old_evidence_clock_and_error(self):
        s=Storage();item=captured(s)
        descriptor={'contract':'report-source-cache.v1','series_id':'TEST','acquired_at':item['acquired_at'],
                    'definition_acquired_at':item['acquired_at'],'evidence':item['evidence']}
        s.objects['data/report-research/cache/TEST.json']=(json.dumps(descriptor).encode(),{})
        with patch.object(store,'original',side_effect=TimeoutError('secret request url')):
            out,error=store.acquire(s,'b','TEST','hidden',999999999)
        self.assertEqual(error,'TimeoutError');self.assertEqual(out['acquired_at'],item['acquired_at'])
        self.assertEqual(out['evidence'],item['evidence'])
        self.assertNotIn('history_policy',json.loads(s.objects['data/report-research/cache/TEST.json'][0]))

    def test_real_research_handler_does_not_enter_legacy_path(self):
        from lce_research_catalog import extend_catalog,SERIES
        from risk_gate_research_catalog import extend_catalog as risk_catalog,SERIES as RISK_SERIES
        path=ROOT/'aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py'
        node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        calls=[]
        env={'time':store.time,'json':json,'track_errors':lambda fn:fn,'s3':object(),'S3_BUCKET':'b','FRED_KEY':'private',
             'include_lce_series':extend_catalog,'include_risk_gate_series':risk_catalog,
             'FRED_SERIES':{'ICSA':('macro','Initial Claims')},'run_source_research':lambda *a,**kw:calls.append((a[2],kw)) or {'published':True}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        out=env['lambda_handler']({'action':'research_measurements'},None)
        self.assertEqual(out['statusCode'],200)
        self.assertEqual(calls[0][0]['ICSA'],{'category':'macro','display_name':'Initial Claims'})
        self.assertTrue(set(SERIES)<=set(calls[0][0]))
        self.assertTrue(set(RISK_SERIES)<=set(calls[0][0]))


if __name__=='__main__':unittest.main()
