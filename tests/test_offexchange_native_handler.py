from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from io import BytesIO
import hashlib,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import offexchange_research_model as m
path=ROOT/'aws/lambdas/justhodl-dark-pool/source/lambda_function.py'
spec=importlib.util.spec_from_file_location('offexchange_native_handler',path);h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)

def packet():return json.loads(json.loads((ROOT/'tests/fixtures/offexchange-native.json').read_text(encoding='utf-8'))['objects'][m.CURRENT])
class Tests(unittest.TestCase):
    def test_validation_never_constructs_aws(self):
        with patch.object(h.boto3,'client',side_effect=AssertionError('No AWS')):self.assertEqual(h.lambda_handler({'validate_only':True})['statusCode'],200)
    def test_http_reads_only_current_and_never_runs_acquisition(self):
        p=packet();client=Mock();client.get_object.return_value={'Body':BytesIO(m.encoded(p))}
        with patch.object(h.boto3,'client',return_value=client),patch.object(h.producer,'run',side_effect=AssertionError('No acquisition')):
            self.assertEqual(json.loads(h.lambda_handler({'httpMethod':'GET'})['body']),p)
        client.get_object.assert_called_once_with(Bucket=h.BUCKET,Key=h.PUBLISHED_KEY);client.put_object.assert_not_called()
    def test_legacy_and_tampered_heads_are_unavailable(self):
        for p in ({'board':[{'ticker':'AAPL','score':99}]},{**packet(),'calls_eligible':True},{**packet(),'state':'ACCUMULATION'}):
            client=Mock();client.get_object.return_value={'Body':BytesIO(m.encoded(p))}
            with patch.object(h.boto3,'client',return_value=client):self.assertEqual(h.lambda_handler({'action':'current_state'})['statusCode'],503)
    def test_actual_identity_budget_and_conditional_head_reach_scoped_producer(self):
        client=Mock()
        def run(storage,bucket,request,execution,remaining_seconds):
            self.assertEqual((request,execution,remaining_seconds),('once','aws-execution',299));self.assertIs(storage.publisher,h.publish_current)
            storage.put_object(Bucket=bucket,Key=h.PUBLISHED_KEY,Body=b'{}',IfMatch='observed',CacheControl='no-store',ContentType='application/json')
            return {'published':True}
        with patch.object(h.boto3,'client',return_value=client),patch.object(h.producer,'run',side_effect=run):
            with self.assertRaises(ValueError):h.lambda_handler({})
            self.assertEqual(h.lambda_handler({'request_id':'once'},SimpleNamespace(aws_request_id='aws-execution',get_remaining_time_in_millis=lambda:299000))['statusCode'],200)
        self.assertEqual(client.put_object.call_count,1)
        sys.path.insert(0,str(ROOT/'scripts'));from gen_engine_manifest import scan_code
        scan=scan_code(path.read_text(encoding='utf-8'),entrypoint='lambda_handler');self.assertEqual(scan.writes,{h.PUBLISHED_KEY});self.assertTrue(scan.proofs[h.PUBLISHED_KEY])
    def test_storage_rejects_accounts_other_buckets_and_unconditional_head(self):
        client=Mock();storage=h.EvidenceStorage(client,h.publish_current)
        for key in ('data/trade-tickets.json','portfolio/current.json','data/unreviewed.json'):
            with self.assertRaises(ValueError):storage.get_object(Bucket=h.BUCKET,Key=key)
            with self.assertRaises(ValueError):storage.put_object(Bucket=h.BUCKET,Key=key,Body=b'{}')
        with self.assertRaises(ValueError):storage.get_object(Bucket='other',Key=h.PUBLISHED_KEY)
        with self.assertRaises(ValueError):storage.put_object(Bucket=h.BUCKET,Key=h.PUBLISHED_KEY,Body=b'{}')
        client.get_object.assert_not_called();client.put_object.assert_not_called()
    def test_all_complete_predecessors_are_preserved_inactive(self):
        manifest=json.loads((ROOT/'tests/fixtures/offexchange-consumer-migration.json').read_text(encoding='utf-8'))
        for entry in manifest['archives'].values():
            raw=(ROOT/entry['file']).read_bytes();self.assertEqual(len(raw),entry['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),entry['sha256'])
        self.assertEqual(manifest['archives']['dark-pool']['bytes'],32512)
    def test_invalid_legacy_resilience_cadence_is_retained_without_schedule_mutation(self):
        sys.path.insert(0,str(ROOT/'scripts'));from normalize_lambda_config import normalize_config
        config=json.loads((ROOT/'aws/lambdas/justhodl-resilience/config.json').read_text())
        self.assertEqual(config['retained_legacy_schedule']['cron'],'cron(45 22 * * 1-5)')
        self.assertNotIn('schedule',normalize_config(config));self.assertLessEqual(len(config['description']),256)
if __name__=='__main__':unittest.main(verbosity=2)
