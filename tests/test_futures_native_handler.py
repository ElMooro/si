from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch,Mock
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
PATH=ROOT/'aws/lambdas/justhodl-polygon-futures-curves/source/lambda_function.py'
spec=importlib.util.spec_from_file_location('futures_native_handler_test',PATH);handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)


class Tests(unittest.TestCase):
    def test_validate_only_makes_no_aws_or_provider_request(self):
        with patch.object(handler.boto3,'client',side_effect=AssertionError('No AWS')):
            self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
    def test_http_only_reads_published_body(self):
        client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps({'contract':handler.CONTRACT}).encode())}
        with patch.object(handler.boto3,'client',return_value=client),patch.object(handler,'get_massive_key',side_effect=AssertionError('No provider key')),patch.object(handler,'run',side_effect=AssertionError('No publication')):
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'})['statusCode'],200)
        client.get_object.assert_called_once_with(Bucket='justhodl-dashboard-live',Key='data/futures-research.json');client.put_object.assert_not_called()
    def test_recovery_skips_provider_credential_and_preserves_identity(self):
        context=SimpleNamespace(aws_request_id='execution',get_remaining_time_in_millis=lambda:59000)
        with patch.object(handler.boto3,'client'),patch.object(handler,'get_massive_key',side_effect=AssertionError('No provider key')),patch.object(handler,'run',return_value={'status':'complete'}) as run:
            handler.lambda_handler({'request_id':'durable','recover_run':{'recorded':'identity'}},context)
        self.assertEqual(run.call_args.args[2:4],('durable','execution'))
        self.assertEqual(run.call_args.kwargs['credential'],'');self.assertEqual(run.call_args.kwargs['remaining_seconds'],59)
    def test_ordinary_execution_uses_existing_managed_key_and_schedule_id(self):
        context=SimpleNamespace(aws_request_id='execution',get_remaining_time_in_millis=lambda:58000)
        with patch.object(handler.boto3,'client'),patch.object(handler,'get_massive_key',return_value='synthetic-key') as key,patch.object(handler,'run',return_value={'status':'complete'}) as run:
            handler.lambda_handler({'id':'scheduled-id'},context)
        key.assert_called_once_with();self.assertEqual(run.call_args.args[2:4],('scheduled-id','execution'));self.assertEqual(run.call_args.kwargs['credential'],'synthetic-key')


if __name__=='__main__':unittest.main(verbosity=2)
