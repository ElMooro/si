from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from io import BytesIO
import hashlib,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from test_dollar_research_consumers import native
path=ROOT/'aws/lambdas/justhodl-dollar-radar/source/lambda_function.py'
spec=importlib.util.spec_from_file_location('dollar_native_handler',path);handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)


class Tests(unittest.TestCase):
    def test_validation_never_constructs_aws_client(self):
        with patch.object(handler.boto3,'client',side_effect=AssertionError('Unexpected client')):
            self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
    def test_http_reads_exact_native_publication_without_capture_or_write(self):
        packet=native();client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps(packet).encode())}
        with patch.object(handler.boto3,'client',return_value=client),patch.object(handler,'run',side_effect=AssertionError('HTTP invocation mutated')):
            result=handler.lambda_handler({'httpMethod':'GET'});self.assertEqual(result['statusCode'],200)
            self.assertEqual(json.loads(result['body']),packet);self.assertEqual(result['headers']['Cache-Control'],'no-store')
        client.put_object.assert_not_called();client.get_object.assert_called_once_with(Bucket=handler.BUCKET,Key=handler.CURRENT)
    def test_old_and_tampered_http_packets_are_unavailable(self):
        for packet in ({'regime':'PUMP'},{**native(),'score':99}):
            client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps(packet).encode())}
            with patch.object(handler.boto3,'client',return_value=client):self.assertEqual(handler.lambda_handler({'action':'current_state'})['statusCode'],503)
    def test_run_requires_execution_identity_and_preserves_durable_recovery_request(self):
        client=Mock();record={'status':'complete','published':True};ref={'manifest_key':'recorded'}
        with patch.object(handler.boto3,'client',return_value=client),patch.object(handler,'run',return_value=record) as run:
            with self.assertRaises(ValueError):handler.lambda_handler({})
            result=handler.lambda_handler({'request_id':'recover-once','recover_run':ref},SimpleNamespace(aws_request_id='execution'))
            self.assertEqual(result['statusCode'],200);self.assertEqual(run.call_args.args[1:],(handler.BUCKET,'recover-once','execution'))
            self.assertEqual(run.call_args.kwargs,{'recover_run':ref})
    def test_writer_scope_cannot_modify_history_or_accounts_or_unconditionally_publish(self):
        client=Mock();storage=handler.EvidenceStorage(client,handler.publish_current)
        for key in ('data/dollar-radar-history.json','data/trade-tickets.json','data/portfolio.json','data/other.json'):
            with self.assertRaises(ValueError):storage.put_object(Bucket=handler.BUCKET,Key=key,Body=b'{}')
        with self.assertRaises(ValueError):storage.put_object(Bucket=handler.BUCKET,Key=handler.CURRENT,Body=b'{}')
        with self.assertRaises(ValueError):storage.put_object(Bucket='other',Key=handler.CURRENT,Body=b'{}')
        client.put_object.assert_not_called()
        storage.put_object(Bucket=handler.BUCKET,Key=handler.CURRENT,Body=b'{}',IfMatch='etag',CacheControl='no-store',ContentType='application/json')
        self.assertEqual(client.put_object.call_args.kwargs['Key'],handler.CURRENT)
    def test_handler_injects_the_source_bound_publication_writer(self):
        client=Mock()
        def publish(storage,*args,**kwargs):
            self.assertIs(storage.publisher,handler.publish_current)
            storage.put_object(Bucket=handler.BUCKET,Key=handler.CURRENT,Body=b'{}',IfMatch='observed-etag',CacheControl='no-store',ContentType='application/json')
            return {'status':'complete'}
        with patch.object(handler.boto3,'client',return_value=client),patch.object(handler,'run',side_effect=publish):
            self.assertEqual(handler.lambda_handler({'request_id':'once'},SimpleNamespace(aws_request_id='execution'))['statusCode'],200)
        client.put_object.assert_called_once_with(Bucket=handler.BUCKET,Key=handler.CURRENT,Body=b'{}',IfMatch='observed-etag',CacheControl='no-store',ContentType='application/json')
        sys.path.insert(0,str(ROOT/'scripts'))
        from gen_engine_manifest import scan_code
        scan=scan_code(path.read_text(encoding='utf-8'),entrypoint='lambda_handler')
        self.assertEqual(scan.writes,{handler.CURRENT})
        self.assertTrue(scan.proofs[handler.CURRENT])
    def test_complete_original_handler_is_preserved_in_inactive_fixture(self):
        raw=(ROOT/'tests/fixtures/legacy-dollar-radar-handler.py.txt').read_bytes();self.assertEqual(len(raw),58697)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),'1f9ddd0ce8ed458567959a4951f34f380c4ddfbc2de982462b16e86b87981699')
        self.assertNotIn('telegram',path.read_text(encoding='utf-8').lower())


if __name__=='__main__':unittest.main(verbosity=2)
