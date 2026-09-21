from pathlib import Path
from unittest import mock
import importlib.util,json,sys,types,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
path=ROOT/'aws/lambdas/justhodl-polygon-options-flow/source/lambda_function.py'
spec=importlib.util.spec_from_file_location('option_handler_test',path);handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)


class Tests(unittest.TestCase):
    def test_validation_does_not_read_credentials_or_create_clients(self):
        with mock.patch.object(handler.boto3,'client') as client,mock.patch.object(handler,'managed_secret') as secret,mock.patch.object(handler,'run') as run:
            out=handler.lambda_handler({'validate_only':True})
        client.assert_not_called();secret.assert_not_called();run.assert_not_called();self.assertFalse(json.loads(out['body'])['published'])
    def test_http_request_cannot_trigger_recollection_or_recovery(self):
        for event in ({'httpMethod':'POST','recover_run':{'fake':True}}, {'requestContext':{'http':{'method':'GET'}}}, {'action':'current_state'}):
            with mock.patch.object(handler.boto3,'client'),mock.patch.object(handler,'reader',return_value=lambda _:json.dumps({'contract':handler.CONTRACT})),mock.patch.object(handler,'managed_secret') as secret,mock.patch.object(handler,'run') as run:
                out=handler.lambda_handler(event)
            self.assertEqual(out['statusCode'],200);secret.assert_not_called();run.assert_not_called()
    def test_missing_native_contract_returns_unavailable(self):
        with mock.patch.object(handler.boto3,'client'),mock.patch.object(handler,'reader',return_value=lambda _:b'{}'),mock.patch.object(handler,'run') as run:
            out=handler.lambda_handler({'httpMethod':'GET'})
        self.assertEqual(out['statusCode'],503);run.assert_not_called()
    def test_scheduled_retry_keeps_event_identity(self):
        context=types.SimpleNamespace(aws_request_id='execution',get_remaining_time_in_millis=lambda:890000)
        with mock.patch.object(handler.boto3,'client'),mock.patch.object(handler,'managed_secret',return_value='configured'),mock.patch.object(handler,'run',return_value={'status':'complete'}) as run:
            out=handler.lambda_handler({'id':'scheduled-id'},context)
        self.assertEqual(run.call_args.args[2:4],('scheduled-id','execution'));self.assertEqual(run.call_args.kwargs['remaining_seconds'],890)
        self.assertIs(run.call_args.kwargs['publish'],handler.publish_current);self.assertEqual(out['statusCode'],200)
    def test_retained_recovery_never_resolves_provider_credentials(self):
        context=types.SimpleNamespace(aws_request_id='execution',get_remaining_time_in_millis=lambda:890000);ref={'manifest_key':'reviewed','output_sha256':'reviewed'}
        with mock.patch.object(handler.boto3,'client'),mock.patch.object(handler,'managed_secret') as secret,mock.patch.object(handler,'run',return_value={'status':'complete'}) as run:
            handler.lambda_handler({'request_id':'recovery','recover_run':ref},context)
        secret.assert_not_called();self.assertEqual(run.call_args.kwargs['credential'],'');self.assertEqual(run.call_args.kwargs['recover_run'],ref)
    def test_publisher_does_not_accept_other_targets_or_unconditional_writes(self):
        client=mock.Mock()
        for key,condition in [('data/portfolio.json',{'IfMatch':'etag'}),(handler.PUBLISHED_KEY,{})]:
            with self.assertRaises(ValueError):handler.publish_current(client,'bucket',key,b'{}',condition)
        client.put_object.assert_not_called()
        handler.publish_current(client,'bucket',handler.PUBLISHED_KEY,b'{}',{'IfMatch':'etag'})
        self.assertEqual(client.put_object.call_args.kwargs['Key'],handler.PUBLISHED_KEY)


if __name__=='__main__':unittest.main(verbosity=2)
