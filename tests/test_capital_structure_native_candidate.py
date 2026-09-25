"""Prepared handler: pure validation, read-only HTTP, least-scope S3 access."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/ops/checks'), str(ROOT/'aws/shared'), str(ROOT/'tests')]
import capital_structure_native_candidate as native
import capital_structure_producer as producer
import capital_structure_source as source
from test_capital_structure_producer import fixture, run as publish


class Tests(unittest.TestCase):
    def test_validation_never_creates_an_aws_client(self):
        with patch.object(native.boto3, 'client') as factory:
            response = native.lambda_handler({'validate_only':True})
        factory.assert_not_called(); self.assertFalse(json.loads(response['body'])['published'])

    def test_storage_refuses_accounts_other_engines_other_buckets_and_public_immutable_writes(self):
        raw = Mock(); adapter = native.EvidenceStorage(raw, native.publish_current)
        for key in ('data/accounts.json','data/portfolio.json','data/calls.json','../data/share-flows.json',source.PRIVATE+'unreviewed.json'):
            with self.assertRaises(ValueError):adapter.get_object(Bucket=native.BUCKET, Key=key)
            with self.assertRaises(ValueError):adapter.put_object(Bucket=native.BUCKET, Key=key, Body=b'{}')
        with self.assertRaises(ValueError):adapter.get_object(Bucket='other', Key=producer.CURRENT)
        with self.assertRaises(ValueError):adapter.head_object(Bucket=native.BUCKET, Key=producer.CURRENT)
        with self.assertRaises(ValueError):adapter.put_object(Bucket=native.BUCKET, Key=source.PREFIX+'outputs/'+'a'*64+'.json', Body=b'{}')
        with self.assertRaises(ValueError):adapter.put_object(Bucket=native.BUCKET, Key=producer.READY, Body=b'{}')
        self.assertEqual(raw.mock_calls, [])
        for key in (producer.CURRENT, producer.READY, source.PRIVATE+'a'*64+'.bin',producer.request_key('checked')):
            adapter.get_object(Bucket=native.BUCKET, Key=key)
        self.assertEqual(raw.get_object.call_count,4)

    def test_current_write_requires_exact_key_and_conditional_guard(self):
        raw = Mock(); request = dict(Bucket=native.BUCKET,Key=producer.CURRENT,Body=b'{}',IfMatch='etag',CacheControl='no-store',ContentType='application/json')
        for field in ('IfMatch','CacheControl','ContentType'):
            changed = dict(request);changed.pop(field)
            with self.assertRaises(ValueError):native.publish_current(raw,changed)
        raw.put_object.assert_not_called(); native.publish_current(raw,request)
        raw.put_object.assert_called_once_with(**request)

    def test_http_never_publishes_and_legacy_or_missing_current_returns_503(self):
        f,s3,reference = fixture()
        for event in ({'httpMethod':'POST','request_id':'force'}, {'requestContext':{'http':{'method':'GET'}},'action':'generate'}, {'action':'current_state'}):
            with patch.object(native.boto3,'client',return_value=s3),patch.object(producer,'run') as called:
                response = native.lambda_handler(event)
            self.assertEqual(response['statusCode'],503); called.assert_not_called()
        del s3.files[producer.CURRENT]
        with patch.object(native.boto3,'client',return_value=s3):
            self.assertEqual(native.lambda_handler({'httpMethod':'GET'})['statusCode'],503)
        self.assertEqual(s3.writes,[])

    def test_qualified_current_redirect_has_no_body_or_source_fetch_and_no_publication(self):
        f,s3,reference = fixture();publish(s3); s3.writes=[]
        with patch.object(native.boto3,'client',return_value=s3),patch.object(producer,'run') as called:
            response=native.lambda_handler({'httpMethod':'GET'})
            status=native.lambda_handler({'action':'current_state'})
        called.assert_not_called();self.assertEqual(s3.writes,[])
        self.assertEqual(response,{'statusCode':307,'headers':{'Location':'https://justhodl.ai/data/share-flows.json','Cache-Control':'no-store'},'body':''})
        detail=json.loads(status['body']);self.assertEqual(detail['replay'],reference);self.assertFalse(detail['calls_eligible'])

    def test_real_publisher_through_scoped_adapter_requires_aws_execution(self):
        f,s3,reference=fixture();old=s3.files[producer.CURRENT]
        context=SimpleNamespace(aws_request_id='synthetic-execution',get_remaining_time_in_millis=lambda:900000)
        actual=producer.run
        def fixed_clock(*args,**kwargs):
            return actual(*args,**kwargs,clock=lambda:'2026-09-25T15:00:00Z')
        with patch.object(native.boto3,'client',return_value=s3),patch.object(producer,'run',side_effect=fixed_clock):
            with self.assertRaisesRegex(ValueError,'execution'):native.lambda_handler({'request_id':'unique'})
            self.assertEqual(s3.files[producer.CURRENT],old)
            response=native.lambda_handler({'request_id':'native-test'},context)
        result=json.loads(response['body']);self.assertTrue(result['published']);self.assertEqual(result['replay'],reference)
        state=json.loads(s3.files[producer.request_key('native-test')])
        self.assertEqual(s3.files[state['predecessor']['key']],old)


if __name__ == '__main__':unittest.main(verbosity=2)
