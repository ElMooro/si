"""Native transport acceptance scope against complete invented control records."""
from pathlib import Path
from copy import deepcopy
import importlib.util,json
ROOT=Path(__file__).resolve().parents[2]


def operation():
    path=ROOT/'aws/ops/staged/ops_6379_fifx_transport_acceptance.py'
    spec=importlib.util.spec_from_file_location('fifx_transport_op',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def rejected(fn):
    try:fn()
    except (ValueError,TypeError,KeyError):return
    raise AssertionError('Invalid runtime received acceptance')


def test_exact_source_receipt_and_original_whole_native_settings_remain_required():
    op=operation();base=json.loads(op.BASELINE.read_bytes())['evidence']['actual_runtime']
    expected='a'*40;base['receipt']={'status':'matched','commit':expected};op.validate(base,expected)
    changes=[{'timeout':900.0},{'memory_mb':2049},{'source_files_checked':14.0},{'receipt':{'status':'matched','commit':'b'*40}},{'code_sha256':'not-a-package-hash'}]
    for change in changes:rejected(lambda change=change:op.validate({**base,**change},expected))
    changed=deepcopy(base);changed['schedules'][0]['expression']='rate(1 minute)'
    rejected(lambda:op.validate(changed,expected))
    rejected(lambda:op.validate(base,'a'*7))


def test_only_release_receipt_and_minimized_origin_identity_are_exposed():
    op=operation()
    class Client:
        def get_object(self,**request):return request
        def get_function_configuration(self,**request):
            assert request=={'FunctionName':op.FN}
            return {'FunctionName':op.FN,'CodeSha256':'invented','Environment':{'Variables':{'S3_BUCKET':op.BUCKET,'UNRELATED':'invented-do-not-emit'}}}
    client=Client();read=op.ReceiptOnly(client)
    assert read.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')['Bucket']==op.BUCKET
    for key in ('data/fifx-vol.json','audit-private/original.json','portfolio/snapshot.json'):
        rejected(lambda key=key:read.get_object(Bucket=op.BUCKET,Key=key))
    result=op.origin(client,{'code_sha256':'invented'})
    assert result['configured_bucket_matches_reviewed_source'] and 'invented-do-not-emit' not in json.dumps(result)
    rejected(lambda:op.origin(client,{'code_sha256':'changed'}))
