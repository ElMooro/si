"""HTTP is read-only; scheduled identities and conditional publication remain bound."""
from pathlib import Path
from unittest import mock
import importlib.util,json,sys,types,unittest
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/shared'))
path=ROOT/'aws/lambdas/justhodl-etf-global-desk/source/lambda_function.py'
spec=importlib.util.spec_from_file_location('desk_handler_test',path);handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)


class DeskHandler(unittest.TestCase):
    def test_validate_does_not_create_an_aws_client_or_collect(self):
        with mock.patch.object(handler.boto3,'client') as client,mock.patch.object(handler,'run') as run:
            result=handler.lambda_handler({'validate_only':True})
        client.assert_not_called();run.assert_not_called();self.assertEqual(json.loads(result['body'])['published'],False)
    def test_public_http_reads_never_trigger_collection(self):
        packet={'contract':handler.CONTRACT,'generated_at':'2026-09-21T10:00:00Z'}
        with mock.patch.object(handler.boto3,'client'),mock.patch.object(handler,'reader',return_value=lambda _:json.dumps(packet)),mock.patch.object(handler,'run') as run:
            out=handler.lambda_handler({'httpMethod':'GET','request_id':'cannot-trigger'})
        run.assert_not_called();self.assertEqual(out['statusCode'],200);self.assertEqual(json.loads(out['body']),packet)
    def test_old_packet_has_explicit_unavailable_response(self):
        with mock.patch.object(handler.boto3,'client'),mock.patch.object(handler,'reader',return_value=lambda _:'{}'),mock.patch.object(handler,'run') as run:
            out=handler.lambda_handler({'action':'current_state'})
        self.assertEqual(out['statusCode'],503);run.assert_not_called()
    def test_schedule_retries_keep_event_identity_and_recovery_is_explicit(self):
        context=types.SimpleNamespace(aws_request_id='execution',get_remaining_time_in_millis=lambda:890000)
        recovery={'manifest_key':'reviewed-candidate','output_sha256':'reviewed'}
        with mock.patch.object(handler.boto3,'client'),mock.patch.object(handler,'run',return_value={'status':'complete'}) as run:
            out=handler.lambda_handler({'id':'scheduled-event-id','recover_run':recovery},context)
        self.assertEqual(run.call_args.args[2:4],('scheduled-event-id','execution'))
        self.assertEqual(run.call_args.kwargs['recover_run'],recovery);self.assertEqual(run.call_args.kwargs['remaining_seconds'],890)
        self.assertIs(run.call_args.kwargs['publish'],handler.publish_current);self.assertEqual(out['statusCode'],200)
    def test_publisher_cannot_write_other_targets_or_drop_compare_and_swap(self):
        client=mock.Mock()
        for key,condition in [('data/portfolio.json',{'IfMatch':'etag'}),(handler.PUBLISHED_KEY,{})]:
            with self.assertRaises(ValueError):handler.publish_current(client,'bucket',key,b'{}',condition)
        client.put_object.assert_not_called()
        handler.publish_current(client,'bucket',handler.PUBLISHED_KEY,b'{}',{'IfMatch':'etag'})
        self.assertEqual(client.put_object.call_args.kwargs['IfMatch'],'etag')
        self.assertEqual(client.put_object.call_args.kwargs['Key'],handler.PUBLISHED_KEY)


if __name__=='__main__':unittest.main(verbosity=2)
