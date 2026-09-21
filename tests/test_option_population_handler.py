from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
spec=importlib.util.spec_from_file_location('population_handler',ROOT/'aws/lambdas/justhodl-dealer-gex/source/lambda_function.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class Tests(unittest.TestCase):
    def test_validation_never_constructs_a_client(self):
        with patch.object(m.boto3,'client',side_effect=AssertionError('No AWS access')):
            out=m.lambda_handler({'validate_only':True});self.assertEqual(out['statusCode'],200)
            self.assertFalse(json.loads(out['body'])['published'])
    def test_http_and_current_state_never_compute(self):
        packet={'contract':m.CONTRACT,'source_capture_completed_at':'kept'}
        for event in ({'action':'current_state'},{'httpMethod':'GET'},{'requestContext':{'http':{'method':'GET'}}}):
            with patch.object(m.boto3,'client'),patch.object(m,'reader',return_value=lambda key:json.dumps(packet).encode()),patch.object(m,'run',side_effect=AssertionError('No execution')):
                self.assertEqual(json.loads(m.lambda_handler(event)['body']),packet)
    def test_missing_publication_is_503_not_a_legacy_fallback(self):
        with patch.object(m.boto3,'client'),patch.object(m,'reader',return_value=lambda key:b'{}'),patch.object(m,'run') as run:
            self.assertEqual(m.lambda_handler({'action':'current_state'})['statusCode'],503);run.assert_not_called()
    def test_explicit_recovery_passes_durable_execution_identity(self):
        with patch.object(m.boto3,'client') as client,patch.object(m,'run',return_value={'status':'complete'}) as run:
            out=m.lambda_handler({'request_id':'exact','recover_run':{'manifest_key':'qualified'}},SimpleNamespace(aws_request_id='execution'))
            self.assertEqual(out['statusCode'],200);self.assertEqual(run.call_args.args[2:],('exact','execution'))
            self.assertEqual(run.call_args.kwargs['recover_run'],{'manifest_key':'qualified'})
            self.assertEqual(client.call_count,1);self.assertEqual(client.call_args.args[0],'s3')
    def test_execution_identity_required(self):
        with patch.object(m.boto3,'client'),patch.object(m,'run') as run:
            with self.assertRaises(ValueError):m.lambda_handler({})
            run.assert_not_called()
    def test_publication_callback_cannot_write_other_key(self):
        with self.assertRaises(ValueError):m.publish_current(None,'bucket','data/trade-tickets.json',b'{}',{'IfMatch':'etag'})


if __name__=='__main__':unittest.main(verbosity=2)
