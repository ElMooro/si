"""Offline behavioral tests for runner metadata-only page API identity proof."""
import base64
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('page_api_identity', ROOT / 'aws/ops/pending/ops_5240_page_api_identity.py')
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


class MetadataClient:
    def __init__(self, bindings, mismatch=False, denied=False):
        self.bindings = {r[1]: r for r in bindings}
        self.calls = []
        self.mismatch = mismatch
        self.denied = denied

    def get_function_url_config(self, FunctionName):
        self.calls.append(('get_function_url_config', FunctionName))
        if self.denied and FunctionName == 'fmp-fundamentals-agent':
            raise PermissionError('PRIVATE provider detail MUST NOT BE LOGGED')
        hostname = self.bindings[FunctionName][2]
        if self.mismatch and FunctionName == 'fmp-fundamentals-agent':
            hostname = 'other.lambda-url.us-east-1.on.aws'
        return {'FunctionUrl': 'https://' + hostname + '/', 'FunctionArn': self.arn(FunctionName), 'SecretExtra': 'PRIVATE'}

    def get_function_configuration(self, FunctionName):
        self.calls.append(('get_function_configuration', FunctionName))
        return {'FunctionArn': self.arn(FunctionName), 'CodeSha256': base64.b64encode(b'x' * 32).decode(),
                'State': 'Active', 'Environment': {'Variables': {'SECRET': 'PRIVATE'}}, 'Description': 'PRIVATE'}

    @staticmethod
    def arn(name):
        return 'arn:aws:lambda:us-east-1:857687956942:function:' + name

    def __getattr__(self, name):
        raise AssertionError('Only the two metadata APIs are permitted: ' + name)


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.bindings = mod.reviewed_bindings(ROOT)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for page, _function, hostname in self.bindings:
            (self.root / page).write_text('https://' + hostname + '/')

    def tearDown(self):
        self.temp.cleanup()

    def test_exact_release_bindings_and_allowlisted_metadata_only(self):
        self.assertEqual({row[1] for row in self.bindings}, {'fmp-fundamentals-agent', 'fedliquidityapi'})
        client = MetadataClient(self.bindings)
        rows = mod.inspect_identities(client, self.bindings, self.root)
        self.assertEqual([row['status'] for row in rows], ['VERIFIED', 'VERIFIED'])
        self.assertEqual(len(client.calls), 4)
        self.assertNotIn('PRIVATE', json.dumps(rows))
        self.assertTrue(all(row['configuration_function_arn'] and row['code_sha256'] for row in rows))

    def test_mismatch_records_other_case_without_short_circuit(self):
        client = MetadataClient(self.bindings, mismatch=True)
        rows = mod.inspect_identities(client, self.bindings, self.root)
        self.assertEqual([row['status'] for row in rows], ['MISMATCH', 'VERIFIED'])
        self.assertEqual(rows[0]['actual_hostname'], 'other.lambda-url.us-east-1.on.aws')
        self.assertEqual(len(client.calls), 4)

    def test_denial_still_reads_configuration_and_other_case_without_error_body(self):
        client = MetadataClient(self.bindings, denied=True)
        rows = mod.inspect_identities(client, self.bindings, self.root)
        self.assertEqual([row['status'] for row in rows], ['UNPROVEN', 'VERIFIED'])
        self.assertEqual(rows[0]['errors'], [{'call': 'get_function_url_config', 'error_type': 'PermissionError'}])
        self.assertTrue(rows[0]['code_sha256'])
        self.assertNotIn('PRIVATE', json.dumps(rows))
        self.assertEqual(len(client.calls), 4)

    def test_page_binding_drift_and_invalid_config_cannot_verify(self):
        (self.root / self.bindings[0][0]).write_text('changed endpoint')
        rows = mod.inspect_identities(MetadataClient(self.bindings), self.bindings, self.root)
        self.assertEqual(rows[0]['status'], 'UNPROVEN')
        self.assertIsNone(mod.safe_hash('PRIVATE'))
        self.assertIsNone(mod.safe_arn('PRIVATE'))


if __name__ == '__main__':
    unittest.main()
