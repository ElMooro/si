from pathlib import Path
from io import BytesIO
from unittest.mock import Mock, patch
import ast, hashlib, importlib.util, json, sys, time, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import statement_context as gate
import statement_producer as producer
from test_statement_context import current
MIGRATION=json.loads((ROOT/'tests/fixtures/statement-consumer-migration.json').read_bytes())
LEGACY={'version':'2.2.0','all_results':[{'symbol':'ABC','m_score':5,'concern_score':100,'strength_grade':'A+'}],
    'sector_valuation_medians':{'Technology':{'pe_ttm':1}},'score':100,'call':'LONG'}


def native():
    path=ROOT/'aws/lambdas/justhodl-forensic-screen/source/lambda_function.py'
    spec=importlib.util.spec_from_file_location('statement_native_test',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def tree(name):return ast.parse((ROOT/'aws/lambdas'/name/'source/lambda_function.py').read_text(encoding='utf-8'))


class Tests(unittest.TestCase):
    def test_native_runtime_uses_deployer_keys_and_has_measured_replay_capacity(self):
        retained=json.loads((ROOT/'tests/fixtures/statement-runtime-repair.json').read_bytes())
        raw=(ROOT/retained['predecessor']).read_bytes()
        self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(retained['bytes'],retained['sha256']))
        self.assertEqual(json.loads(raw)['timeout_s'],840)
        config=json.loads((ROOT/'aws/lambdas/justhodl-forensic-screen/config.json').read_bytes())
        self.assertEqual(config['memory'],1024)
        self.assertEqual(config['timeout'],840)
        self.assertNotIn('memory_mb',config)
        self.assertNotIn('timeout_s',config)
        # The actual release script reads these keys; metadata-only aliases
        # previously left AWS at 512 MB / 300 seconds and prevented publication.
        deploy=(ROOT/'scripts/deploy_lambdas.sh').read_text(encoding='utf-8')
        self.assertIn("jq -r '.timeout // 300'",deploy)
        self.assertIn("jq -r '.memory // 512'",deploy)

    def test_whole_predecessors_and_accepted_compilers_remain_exact(self):
        for entry in MIGRATION['files']:
            body=(ROOT/entry['predecessor']).read_bytes()
            self.assertEqual((len(body),hashlib.sha256(body).hexdigest()),(entry['bytes'],entry['sha256']))
        for module,digest in MIGRATION['frozen_compilers'].items():
            self.assertEqual(hashlib.sha256((ROOT/'aws/shared'/str(module+'.py')).read_bytes()).hexdigest(),digest)

    def test_short_book_and_opportunity_read_boundaries_never_accept_legacy_accounting_votes(self):
        for function,reader in (('justhodl-short-book','rj'),('justhodl-opportunity-engine','load')):
            nodes=[n for n in ast.walk(tree(function)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                and n.func.attr=='decision_view' and ast.unparse(n.func.value)=="__import__('statement_context')"]
            self.assertEqual(len(nodes),1)
            for packet in (LEGACY,current()):
                result=eval(compile(ast.Expression(nodes[0]),function,'eval'),{reader:lambda key:packet})
                self.assertEqual(result['all_results'],[]);self.assertEqual(result['independent_investment_votes'],0)
                self.assertIsNone(result['m_score']);self.assertFalse(result['sizing_eligible'])

    def test_census_retains_all_source_labels_and_classification_evidence_without_grades(self):
        node=next(n for n in tree('justhodl-fundamental-census').body if isinstance(n,ast.FunctionDef) and n.name=='universe')
        for packet in (LEGACY,current()):
            client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps(packet).encode())}
            scope={'json':json,'S3':client,'BUCKET':'test'}
            exec(compile(ast.Module(body=[node],type_ignores=[]),'census','exec'),scope)
            rows=scope['universe']()
            self.assertEqual(len(rows),0 if packet is LEGACY else packet['reported_names'])
            if rows:
                self.assertEqual({r['t'] for r in rows},{r['symbol'] for r in packet['issuers']})
                self.assertTrue(all('classification_source' in r and not r['index_membership_verified'] for r in rows))

    def test_fundamental_graphs_explains_unqualified_medians_and_percentiles(self):
        source=tree('justhodl-fundamental-graphs');names={'build_sector_medians','foren_rows','factor_dna'}
        nodes=[n for n in source.body if isinstance(n,ast.FunctionDef) and n.name in names]
        for packet in (LEGACY,current()):
            client=Mock();client.get_object.side_effect=lambda **kwargs:{'Body':BytesIO(json.dumps(packet).encode())}
            scope={'json':json,'_s3':client,'S3_BUCKET':'test','SECMED_KEY':'data/test-medians.json',
                'SECMED_MAP':[],'time':time,'_FOREN':{'rows':None,'ts':0},'datetime':__import__('datetime').datetime,
                'timezone':__import__('datetime').timezone}
            exec(compile(ast.Module(body=nodes,type_ignores=[]),'fundamental-graphs','exec'),scope)
            doc=scope['build_sector_medians']();self.assertEqual(doc['sectors'],{})
            self.assertEqual(doc['quality']['status'],'unavailable');self.assertEqual(doc['accounting_context']['independent_investment_votes'],0)
            result=scope['factor_dna']('ABC');self.assertEqual(result['state'],'insufficient')
            self.assertIn('no qualified',result['why']);self.assertFalse(result['accounting_context']['sizing_eligible'])

    def test_native_validation_storage_and_read_only_http_cannot_trigger_side_effects(self):
        module=native();client=Mock();storage=module.EvidenceStorage(client,module.publish_current)
        with patch.object(module.boto3,'client',side_effect=AssertionError('No AWS expected')):
            result=module.lambda_handler({'validate_only':True})
        self.assertFalse(json.loads(result['body'])['published'])
        for key in ('data/trade-tickets.json','portfolio/account.json','data/short-book.json'):
            with self.assertRaises(ValueError):storage.get_object(Bucket=module.BUCKET,Key=key)
            with self.assertRaises(ValueError):storage.head_object(Bucket=module.BUCKET,Key=key)
            with self.assertRaises(ValueError):storage.put_object(Bucket=module.BUCKET,Key=key,Body=b'{}')
        with self.assertRaises(ValueError):storage.put_object(Bucket=module.BUCKET,Key=producer.READY,Body=b'{}')
        with self.assertRaises(ValueError):storage.put_object(Bucket=module.BUCKET,Key=producer.CURRENT,Body=b'{}')
        client.get_object.assert_not_called();client.put_object.assert_not_called()
        for packet,expected in ((LEGACY,503),(current(),307)):
            client.get_object.return_value={'Body':BytesIO(json.dumps(packet).encode())}
            with patch.object(module.boto3,'client',return_value=client),patch.object(module.producer,'run',side_effect=AssertionError('No publication from HTTP')):
                result=module.lambda_handler({'httpMethod':'GET'})
            self.assertEqual(result['statusCode'],expected)
            self.assertEqual(result['headers']['Cache-Control'],'no-store')


if __name__=='__main__':unittest.main(verbosity=2)
