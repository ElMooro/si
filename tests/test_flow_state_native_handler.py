from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from io import BytesIO
import hashlib,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from test_flow_state_model import fixture
import flow_state_model as model
path=ROOT/'aws/lambdas/justhodl-cross-asset-flow-state/source/lambda_function.py'
spec=importlib.util.spec_from_file_location('flow_state_native_handler',path);handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)
def packet():
    inputs,blobs,_=fixture();out=model.compile_output(inputs,blobs.__getitem__)
    return {**out,'replay':{'manifest_key':model.PREFIX+'runs/'+'a'*64+'.json','output_sha256':model.digest(out)}}

class Tests(unittest.TestCase):
    def test_validation_never_constructs_aws_client(self):
        with patch.object(handler.boto3,'client',side_effect=AssertionError('Unexpected client')):
            self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
    def test_http_reads_only_current_native_packet_without_mutation(self):
        p=packet();client=Mock();client.get_object.return_value={'Body':BytesIO(model.encoded(p))}
        with patch.object(handler.boto3,'client',return_value=client),patch.object(handler.store,'run',side_effect=AssertionError('HTTP cannot publish')):
            out=handler.lambda_handler({'httpMethod':'GET'});self.assertEqual(out['statusCode'],200)
            self.assertEqual(json.loads(out['body']),p);self.assertEqual(out['headers']['Cache-Control'],'no-store')
        client.get_object.assert_called_once_with(Bucket=handler.BUCKET,Key=handler.PUBLISHED_KEY);client.put_object.assert_not_called()
    def test_legacy_tampered_or_self_promoted_publication_is_unavailable(self):
        for p in ({'engine':'cross-asset-flow-state','version':'1.0'},{**packet(),'score':99},{**packet(),'calls_eligible':True}):
            client=Mock();client.get_object.return_value={'Body':BytesIO(model.encoded(p))}
            with patch.object(handler.boto3,'client',return_value=client):self.assertEqual(handler.lambda_handler({'action':'current_state'})['statusCode'],503)
    def test_native_requests_keep_real_execution_identity_and_inject_actual_writer(self):
        client=Mock()
        def run(storage,bucket,request,execution):
            self.assertIs(storage.publisher,handler.publish_current);self.assertEqual((request,execution),('once','aws-execution'))
            storage.put_object(Bucket=bucket,Key=handler.PUBLISHED_KEY,Body=b'{}',IfMatch='observed',CacheControl='no-store',ContentType='application/json')
            return {'status':'complete','published':True}
        with patch.object(handler.boto3,'client',return_value=client),patch.object(handler.store,'run',side_effect=run):
            with self.assertRaises(ValueError):handler.lambda_handler({})
            self.assertEqual(handler.lambda_handler({'request_id':'once'},SimpleNamespace(aws_request_id='aws-execution'))['statusCode'],200)
        client.put_object.assert_called_once_with(Key=handler.PUBLISHED_KEY,Bucket=handler.BUCKET,Body=b'{}',IfMatch='observed',CacheControl='no-store',ContentType='application/json')
        sys.path.insert(0,str(ROOT/'scripts'))
        from gen_engine_manifest import scan_code
        scan=scan_code(path.read_text(encoding='utf-8'),entrypoint='lambda_handler');self.assertEqual(scan.writes,{handler.PUBLISHED_KEY});self.assertTrue(scan.proofs[handler.PUBLISHED_KEY])
    def test_storage_refuses_accounts_other_buckets_parent_writes_and_unconditional_head(self):
        client=Mock();storage=handler.EvidenceStorage(client,handler.publish_current)
        for key in ('data/trade-tickets.json','portfolio/current.json','data/unreviewed.json'):
            with self.assertRaises(ValueError):storage.get_object(Bucket=handler.BUCKET,Key=key)
            with self.assertRaises(ValueError):storage.put_object(Bucket=handler.BUCKET,Key=key,Body=b'{}')
        with self.assertRaises(ValueError):storage.get_object(Bucket='other',Key=handler.PUBLISHED_KEY)
        with self.assertRaises(ValueError):storage.put_object(Bucket=handler.BUCKET,Key=handler.PUBLISHED_KEY,Body=b'{}')
        with self.assertRaises(ValueError):storage.put_object(Bucket=handler.BUCKET,Key='data/etf-research/outputs/'+'a'*64+'.json',Body=b'{}')
        client.get_object.assert_not_called();client.put_object.assert_not_called()
    def test_whole_original_handler_is_preserved(self):
        raw=(ROOT/'tests/fixtures/legacy-cross-asset-flow-state-handler.py.txt').read_bytes()
        self.assertEqual(len(raw),5168);self.assertEqual(hashlib.sha256(raw).hexdigest(),'5e8706203ffbd851a5134c6452d4a1563fa01d6144ecbe25833ec21abaa22558')

if __name__=='__main__':unittest.main(verbosity=2)
