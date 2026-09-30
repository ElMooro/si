"""Whole invented native workbook replay with a read-only public archive adapter."""
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import importlib.util
import io
import json
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/lambdas/justhodl-term-premium/tests'),str(ROOT/'aws/lambdas/justhodl-term-premium/source'),str(ROOT/'tests')]
import term_premium_archive_acceptance as archive
import test_term_native as native


class PublicClient:
    def __init__(self,objects):
        self.objects=objects;self.reads=[];self.listings=0;self.pages=None;self.change=False
    def get_object(self,**request):
        assert set(request)=={'Bucket','Key'} and request['Bucket']=='invented'
        key=request['Key'];assert key.startswith(native.model.PREFIX) and key!=native.model.CURRENT
        self.reads.append(key);return {'Body':io.BytesIO(self.objects[key]),'ContentLength':len(self.objects[key])}
    def get_paginator(self,name):
        assert name=='list_objects_v2';return self
    def paginate(self,**request):
        assert request=={'Bucket':'invented','Prefix':native.model.PREFIX+'runs/'}
        self.listings+=1
        if self.pages is not None:yield from self.pages;return
        rows=[{'Key':k,'Size':len(v),'ETag':'"invented"','LastModified':datetime(2026,9,25,tzinfo=timezone.utc)} for k,v in sorted(self.objects.items()) if k.startswith(request['Prefix'])]
        if self.change and self.listings==2:rows[0]['ETag']='"changed"'
        yield {'Name':'invented','Prefix':request['Prefix'],'IsTruncated':False,'KeyCount':len(rows),'Contents':rows,'ResponseMetadata':{'RequestId':'invented-'+str(self.listings)}}


def replay(client,cutoff='2026-09-01T00:00:00Z'):
    return archive.inspect(client,'invented',native.store,native.model,cutoff=cutoff,checked_at='2026-09-30T00:00:00Z')


def rejected(fn):
    try:fn()
    except (ValueError,TypeError,KeyError):return
    raise AssertionError('Invalid archive evidence received acceptance')


def complete_case(test):
    case=native.Tests();case.setUp()
    try:test(case,case.packet())
    finally:case.doCleanups()


def test_complete_original_workbook_replays_all_rows_without_current_or_private_reads():
    def check(case,packet):
        client=PublicClient(deepcopy(case.client.objects));before=deepcopy(client.objects)
        result=replay(client)
        assert result['status']=='complete_original_archive_replayed' and result['replayed']
        assert result['series']==60 and sum(t['rows'] for t in result['tables'].values())==320
        assert not result['current_head_read'] and not result['current_head_publication_verified'] and not result['investment_authority']
        assert result['inventory_before']['entries']==result['inventory_after']['entries']
        assert result['inventory_before']['pages']!=result['inventory_after']['pages'] # complete SDK request metadata is retained
        assert client.objects==before and len(result['complete_artifacts_read'])==23
    complete_case(check)


def test_empty_or_pre_repair_archive_is_explicitly_pending_not_published():
    empty=replay(PublicClient({}));assert empty['status']=='pending_original_post_repair_archive' and not empty['replayed']
    def check(case,packet):
        client=PublicClient(deepcopy(case.client.objects));result=replay(client,'2026-09-29T00:00:00Z')
        assert not result['replayed'] and not result['current_head_publication_verified']
        assert len(result['manifests'])==1 and len(client.reads)==1
    complete_case(check)


def test_same_clock_conflict_future_timestamp_and_changed_inventory_refuse():
    def check(case,packet):
        client=PublicClient(deepcopy(case.client.objects));key=packet['replay']['manifest_key'];base=native.store.strict(client.objects[key])
        conflict=deepcopy(base);conflict['invented_conflict']=True;raw=native.model.encoded(conflict)
        client.objects[native.model.PREFIX+'runs/'+native.store.sha(raw)+'.json']=raw
        rejected(lambda:replay(client))
        client=PublicClient(deepcopy(case.client.objects));future=deepcopy(base);future['generated_at']='2099-01-01T00:00:00Z';raw=native.model.encoded(future)
        del client.objects[key];client.objects[native.model.PREFIX+'runs/'+native.store.sha(raw)+'.json']=raw
        rejected(lambda:replay(client))
        client=PublicClient(deepcopy(case.client.objects));client.change=True;rejected(lambda:replay(client))
    complete_case(check)


def test_whole_manifest_workbook_and_compiler_corruption_cannot_pass():
    def check(case,packet):
        manifest=native.store.strict(case.client.objects[packet['replay']['manifest_key']])
        for key in (packet['replay']['manifest_key'],case.inputs['workbook']['key'],manifest['compilers']['xlrd/book.py']['key']):
            client=PublicClient(deepcopy(case.client.objects));client.objects[key]=b'whole invented corruption'
            rejected(lambda:replay(client))
    complete_case(check)


def test_reader_rejects_private_current_external_and_traversal_before_get():
    client=PublicClient({});read=archive.PublicArchiveReader(client,'invented',native.store,native.model)
    for key in (native.model.CURRENT,native.store.PRIVATE+'requests/x.json','portfolio/snapshot.json','data/prospective-outcomes.json',native.model.PREFIX+'../secrets.json','https://example.invalid/data.json'):
        rejected(lambda key=key:read(key))
    assert client.reads==[]


def test_complete_listing_requires_exact_scope_and_final_page():
    good={'Name':'invented','Prefix':native.model.PREFIX+'runs/','IsTruncated':False,'KeyCount':0,'Contents':[]}
    for page in ({**good,'Name':'wrong'},{**good,'KeyCount':True},{**good,'IsTruncated':True},{**good,'IsTruncated':True,'NextContinuationToken':'invented'},{**good,'Prefix':'portfolio/'}):
        client=PublicClient({});client.pages=[page];rejected(lambda:replay(client))
    client=PublicClient({});client.pages=[];rejected(lambda:replay(client))
    client=PublicClient({});client.pages=[good,good];rejected(lambda:replay(client))


def test_duplicate_unexpected_and_over_bound_inventory_refuses_without_truncation():
    def check(case,packet):
        client=PublicClient(deepcopy(case.client.objects));page=next(client.paginate(Bucket='invented',Prefix=native.model.PREFIX+'runs/'))
        for rows in (page['Contents']*2,[{**page['Contents'][0],'Key':native.model.PREFIX+'runs/not-content-addressed.json'}],[{**page['Contents'][0],'Size':True}]):
            client.pages=[{**page,'Contents':rows,'KeyCount':len(rows)}];rejected(lambda:replay(client))
        old=archive.MAX_RUNS;archive.MAX_RUNS=0
        try:client.pages=[page];rejected(lambda:replay(client))
        finally:archive.MAX_RUNS=old
    complete_case(check)


def operation():
    path=ROOT/'aws/ops/staged/ops_6376_term_premium_public_archive_acceptance.py'
    spec=importlib.util.spec_from_file_location('term_public_archive_operation',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def test_operation_receipt_scope_runtime_type_identity_and_sdk_timestamp_retention():
    op=operation();base=json.loads(op.BASELINE.read_bytes())['code_and_runtime']['actual_runtime'];op.exact_runtime(base)
    for changed in ({**base,'timeout':301},{**base,'code_sha256':'wrong'},{**base,'receipt':{'status':'matched','commit':'0'*40}}):rejected(lambda:op.exact_runtime(changed))
    class Client:
        def get_object(self,**request):return request
    receipt=op.ReceiptOnly(Client());assert receipt.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')['Bucket']==op.BUCKET
    rejected(lambda:receipt.get_object(Bucket=op.BUCKET,Key=native.model.CURRENT))
    stamp=datetime(2026,9,25,tzinfo=timezone.utc);result=op.jsonable({'whole':[stamp,False,1,1.0,None]})
    assert result=={'whole':[{'sdk_type':'datetime','iso8601':stamp.isoformat()},False,1,1.0,None]}
    assert type(result['whole'][2]) is int and type(result['whole'][3]) is float


def test_original_bucket_is_verified_without_returning_other_environment_values():
    op=operation()
    class Client:
        def __init__(self,bucket):self.bucket=bucket
        def get_function_configuration(self,**request):
            assert request=={'FunctionName':op.FN}
            return {'FunctionName':op.FN,'CodeSha256':op.CODE_SHA,'Environment':{'Variables':{'S3_BUCKET':self.bucket,'UNRELATED':'invented_do_not_emit'}}}
    result=op.check_origin(Client(op.BUCKET));assert 'invented_do_not_emit' not in json.dumps(result)
    rejected(lambda:op.check_origin(Client('wrong-bucket')))
