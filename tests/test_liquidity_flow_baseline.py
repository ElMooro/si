from pathlib import Path
import ast, hashlib, importlib.util, io, unittest
ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / 'aws/ops/staged/ops_6115_liquidity_flow_original_baseline.py'
spec = importlib.util.spec_from_file_location('liquidity_baseline', PATH)
baseline = importlib.util.module_from_spec(spec); spec.loader.exec_module(baseline)


class Client:
    def __init__(self): self.files = {}; self.requests = []
    def put_object(self, **request):
        self.requests.append(request)
        if request.get('IfNoneMatch') == '*' and request['Key'] in self.files:
            error = RuntimeError('exists'); error.response = {'Error': {'Code': 'PreconditionFailed'}}; raise error
        self.files[request['Key']] = request['Body']
    def get_object(self, **request): return {'Body': io.BytesIO(self.files[request['Key']])}


class Tests(unittest.TestCase):
    def test_large_whole_predecessor_roundtrip_is_content_addressed_and_private(self):
        client = Client(); raw = b'complete predecessor\x00' * 5000
        ref = baseline.retain(client, raw)
        self.assertEqual(ref['bytes'], len(raw)); self.assertEqual(ref['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertTrue(ref['key'].startswith(baseline.PRIVATE)); self.assertEqual(client.files[ref['key']], raw)
        self.assertEqual(client.requests[0]['IfNoneMatch'], '*')
        self.assertEqual(baseline.retain(client, raw), ref)

    def test_retained_key_cannot_accept_different_bytes(self):
        client = Client(); raw = b'predecessor'; ref = baseline.retain(client, raw)
        client.files[ref['key']] = b'changed'
        with self.assertRaises(AssertionError): baseline.retain(client, raw)
        for value in (b'', None, 'partial text'):
            with self.assertRaises(ValueError): baseline.retain(client, value)

    def test_second_claim_fails_instead_of_repeating_ambiguous_baseline(self):
        client = Client(); baseline.journal(client, {'status': 'claimed'}, True)
        with self.assertRaises(RuntimeError): baseline.journal(client, {'status': 'claimed'}, True)
        baseline.journal(client, {'status': 'failed'})
        self.assertEqual(client.files[baseline.STATUS], b'{"status":"failed"}')

    def test_operation_has_no_provider_acquisition_invocation_or_environment_secret_read(self):
        tree = ast.parse(PATH.read_text(encoding='utf-8'))
        methods = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
        self.assertFalse({'invoke', 'get_parameter', 'get_secret_value', 'get_function_configuration', 'update_function_code', 'publish', 'send_message'} & methods)
        self.assertIn('restore', methods)
        puts = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'put_object']
        self.assertEqual(len(puts), 2)  # Only private retain and private journal helpers.


if __name__ == '__main__': unittest.main(verbosity=2)
