from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from io import BytesIO
import hashlib,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from test_gold_rotation_model import fixture
import gold_rotation_model as m
path=ROOT/'aws/lambdas/justhodl-gold-equity-rotation/source/lambda_function.py'
spec=importlib.util.spec_from_file_location('gold_native_handler',path);h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
def packet():
    inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__)
    return {**out,'replay':{'manifest_key':m.PREFIX+'runs/'+'a'*64+'.json','output_sha256':m.digest(out)}}
class Tests(unittest.TestCase):
    def test_validation_does_not_construct_aws_or_resolve_provider_auth(self):
        with patch.object(h.boto3,'client',side_effect=AssertionError('No AWS')),patch.object(h,'managed_secret',side_effect=AssertionError('No credential')):
            self.assertEqual(h.lambda_handler({'validate_only':True})['statusCode'],200)
    def test_http_only_reads_current_and_never_fetches_provider_or_publishes(self):
        p=packet();client=Mock();client.get_object.return_value={'Body':BytesIO(m.encoded(p))}
        with (patch.object(h.boto3,'client',return_value=client),patch.object(h,'managed_secret',side_effect=AssertionError('No credential')),
              patch.object(h.store,'run',side_effect=AssertionError('No publication'))):
            self.assertEqual(json.loads(h.lambda_handler({'httpMethod':'GET'})['body']),p)
        client.get_object.assert_called_once_with(Bucket=h.BUCKET,Key=h.PUBLISHED_KEY);client.put_object.assert_not_called()
    def test_legacy_and_tampered_current_packet_do_not_serve_as_native(self):
        for p in ({'engine':'gold-equity-rotation','state':'GOLD_BREAKOUT_RICH'},{**packet(),'calls_eligible':True},{**packet(),'state':'GOLD_BREAKOUT_RICH'}):
            client=Mock();client.get_object.return_value={'Body':BytesIO(m.encoded(p))}
            with patch.object(h.boto3,'client',return_value=client):self.assertEqual(h.lambda_handler({'action':'current_state'})['statusCode'],503)
    def test_real_request_identity_and_existing_credential_reach_the_scoped_writer(self):
        client=Mock()
        def run(storage,bucket,request,execution,credential):
            self.assertEqual((request,execution,credential),('once','aws-execution','existing-secret'));self.assertIs(storage.publisher,h.publish_current)
            storage.put_object(Bucket=bucket,Key=h.PUBLISHED_KEY,Body=b'{}',IfMatch='observed',CacheControl='no-store',ContentType='application/json')
            return {'status':'complete','published':True}
        with patch.object(h.boto3,'client',return_value=client),patch.object(h.store,'run',side_effect=run),patch.dict(h.os.environ,{'FMP_KEY':'existing-secret'}):
            with self.assertRaises(ValueError):h.lambda_handler({})
            self.assertEqual(h.lambda_handler({'request_id':'once'},SimpleNamespace(aws_request_id='aws-execution'))['statusCode'],200)
        self.assertEqual(client.put_object.call_count,1)
        sys.path.insert(0,str(ROOT/'scripts'));from gen_engine_manifest import scan_code
        scan=scan_code(path.read_text(encoding='utf-8'),entrypoint='lambda_handler');self.assertEqual(scan.writes,{h.PUBLISHED_KEY});self.assertTrue(scan.proofs[h.PUBLISHED_KEY])
    def test_storage_denies_accounts_other_buckets_and_unconditional_head_writes(self):
        client=Mock();storage=h.EvidenceStorage(client,h.publish_current)
        for key in ('data/trade-tickets.json','portfolio/current.json','data/unreviewed.json'):
            with self.assertRaises(ValueError):storage.get_object(Bucket=h.BUCKET,Key=key)
            with self.assertRaises(ValueError):storage.put_object(Bucket=h.BUCKET,Key=key,Body=b'{}')
        with self.assertRaises(ValueError):storage.get_object(Bucket='other',Key=h.PUBLISHED_KEY)
        with self.assertRaises(ValueError):storage.put_object(Bucket=h.BUCKET,Key=h.PUBLISHED_KEY,Body=b'{}')
        client.get_object.assert_not_called();client.put_object.assert_not_called()
    def test_whole_original_handler_is_preserved_inactive(self):
        raw=(ROOT/'tests/fixtures/legacy-gold-rotation-handler.py.txt').read_bytes()
        self.assertEqual(len(raw),15374);self.assertEqual(hashlib.sha256(raw).hexdigest(),'a99b7f585d12d4439419b148d57f9033441f0c65ed74b9cdac07bbebcc983bfb')

if __name__=='__main__':unittest.main(verbosity=2)
