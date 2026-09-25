from pathlib import Path
import ast, hashlib, io, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/ops/staged'))
import ops_6116_liquidity_flow_retained_arithmetic as acceptance


class Client:
    def __init__(self, raw): self.raw = raw; self.requests = []
    def get_object(self, **request): self.requests.append(request); return {'Body': io.BytesIO(self.raw)}


class Tests(unittest.TestCase):
    def test_exact_retained_reference_and_bytes_required_before_parsing(self):
        raw = b'whole original'; digest = hashlib.sha256(raw).hexdigest()
        ref = {'key': acceptance.baseline.PRIVATE+digest+'.bin', 'sha256': digest, 'bytes': len(raw)}
        client = Client(raw); self.assertEqual(acceptance.read(client, ref), raw)
        with self.assertRaises(ValueError): acceptance.read(Client(raw+b'!'), ref)
        for bad in ({**ref, 'key': 'data/current.json'}, {**ref, 'bytes': 0}, {**ref, 'sha256': 'x'}):
            client = Client(raw)
            with self.assertRaises(ValueError): acceptance.read(client, bad)
            self.assertEqual(client.requests, [])

    def test_no_native_invocation_secret_lookup_or_provider_acquisition(self):
        tree = ast.parse(Path(acceptance.__file__).read_text(encoding='utf-8'))
        calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
        self.assertFalse(calls & {'invoke', 'urlopen', 'get_parameter', 'get_secret_value', 'update_function_code', 'publish', 'send_message'})
        puts = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'put_object']
        self.assertEqual(len(puts), 1)
        self.assertEqual(next(k.value.id for k in puts[0].keywords if k.arg == 'Key'), 'STATUS')


if __name__ == '__main__': unittest.main(verbosity=2)
