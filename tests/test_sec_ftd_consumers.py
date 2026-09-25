from pathlib import Path
from io import BytesIO
from unittest.mock import Mock, patch
import ast, hashlib, importlib.util, json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import sec_ftd_context as gate
from test_sec_ftd_context import packet
MIGRATION = json.loads((ROOT / 'tests/fixtures/sec-ftd-consumer-migration.json').read_bytes())
LEGACY = {'score': 99, 'call': 'LONG', 'board': [{'ticker': 'ABC', 'score': 99, 'state': 'LOADED'}],
    'rows': [{'ticker': 'ABC', 'score': 99, 'state': 'LOADED'}], 'top_picks': [{'ticker': 'ABC', 'score': 99}],
    'by_ticker': {'ABC': {'score': 99}}}


def native():
    path = ROOT / 'aws/lambdas/justhodl-squeeze-fuel/source/lambda_function.py'
    spec = importlib.util.spec_from_file_location('sec_ftd_native_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Tests(unittest.TestCase):
    def test_every_complete_predecessor_remains_byte_identical(self):
        for entry in MIGRATION['archives'].values():
            body = (ROOT / entry['file']).read_bytes()
            self.assertEqual(len(body), entry['bytes'])
            self.assertEqual(hashlib.sha256(body).hexdigest(), entry['sha256'])

    def test_accepted_compilers_are_frozen(self):
        hashes = {'sec_ftd_measurements': '3536dce70f4ed7df4028d2b890e32176f4dc9e270fd8bb2a6cb24f91362195a0',
            'sec_ftd_research_model': '32ab46ad21b4b7b9368bdd61df15557a8da78249bdcf0fb986af7d442baa4e54',
            'sec_ftd_research_store': '0533b9341ab5d2422736ac7ab1144de1e6851dbffb88206fc28acdf2abe56c60',
            'sec_ftd_source': '69e9ad4792d5b97dc302aaa61d0f69715fa26b3db690b143c6ff100e6a123bfe'}
        for name, expected in hashes.items():
            self.assertEqual(hashlib.sha256((ROOT / ('aws/shared/' + name + '.py')).read_bytes()).hexdigest(), expected)

    def test_each_real_consumer_boundary_removes_legacy_votes_and_keeps_valid_references(self):
        for name, entry in MIGRATION['consumers'].items():
            tree = ast.parse((ROOT / entry['path']).read_text(encoding='utf-8'))
            for value in (LEGACY, packet()):
                with self.subTest(name=name, contract=value.get('contract')):
                    if entry['method'] == 'wrapped_read':
                        found = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                            and isinstance(node.func, ast.Attribute) and node.func.attr == 'decision_view'
                            and ast.unparse(node.func.value) == "__import__('sec_ftd_context')"]
                        self.assertEqual(len(found), entry['count'])
                        result = eval(compile(ast.Expression(body=found[0]), name, 'eval'), {entry['reader']: lambda *a: value})
                    else:
                        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == entry['function'])
                        client = Mock()
                        client.get_object.side_effect = lambda **kw: {'Body': BytesIO(json.dumps(value).encode())}
                        scope = {'json': json, 's3': client, 'B': 'bucket', 'BUCKET': 'bucket'}
                        exec(compile(ast.Module(body=[node], type_ignores=[]), name, 'exec'), scope)
                        read = scope[entry['function']]
                        result = read(gate.CURRENT)
                        self.assertEqual(read('data/unrelated.json'), value)
                    self.assertEqual(result['rows'], [])
                    self.assertEqual(result['board'], [])
                    self.assertEqual(result['top_picks'], [])
                    self.assertEqual(result['independent_investment_votes'], 0)
                    self.assertTrue(all(result[k] is False for k in gate.FLAGS))
                    self.assertEqual(result['research_context']['native_reference_available'], value is not LEGACY)

    def test_native_validation_and_storage_boundary_cannot_access_accounts_or_send_actions(self):
        mod = native()
        with patch.object(mod.boto3, 'client', side_effect=AssertionError('No AWS expected')):
            output = mod.lambda_handler({'validate_only': True})
        self.assertEqual(output['statusCode'], 200)
        self.assertFalse(json.loads(output['body'])['published'])
        client = Mock()
        storage = mod.EvidenceStorage(client, mod.publish_current)
        for key in ('portfolio/account.json', 'data/trade-tickets.json', 'data/short-interest.json', 'data/other.json'):
            with self.assertRaises(ValueError):
                storage.get_object(Bucket=mod.BUCKET, Key=key)
            with self.assertRaises(ValueError):
                storage.put_object(Bucket=mod.BUCKET, Key=key, Body=b'{}')
        with self.assertRaises(ValueError):
            storage.put_object(Bucket=mod.BUCKET, Key=mod.PUBLISHED_KEY, Body=b'{}')
        client.get_object.assert_not_called()
        client.put_object.assert_not_called()

    def test_http_uses_canonical_object_to_avoid_lambda_buffer_limit_and_never_collects(self):
        mod = native()
        for value, expected in ((packet(), 307), (LEGACY, 503)):
            client = Mock()
            client.get_object.return_value = {'Body': BytesIO(json.dumps(value).encode())}
            with patch.object(mod.boto3, 'client', return_value=client), \
                 patch.object(mod.producer, 'run', side_effect=AssertionError('HTTP cannot collect')):
                output = mod.lambda_handler({'httpMethod': 'GET'})
            self.assertEqual(output['statusCode'], expected)
            self.assertEqual(output['headers']['Cache-Control'], 'no-store')
            if expected == 307:
                self.assertEqual(output['headers']['Location'], 'https://justhodl.ai/data/squeeze-fuel.json')
                self.assertEqual(output['body'], '')
            client.put_object.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
