"""Acceptance uses invented public metadata and fake SDKs, never AWS."""
import base64
from datetime import timedelta
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
import zipfile

from catalog_fixture import ROOT, STAMP, full_store, run

SPEC = importlib.util.spec_from_file_location('pages_acceptance', ROOT / 'aws/ops/staged/ops_6438_provider_catalog_pages_acceptance.py')
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
sys.path.insert(0, str(ROOT / 'aws/ops/checks'))
from release_package_evidence import shared_imports


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.outputs = run(1000, full_store(1001))['writes']

    def fixture(self, old=False):
        source = ROOT / 'aws/lambdas/justhodl-provider-catalog/source'
        paths = list(source.glob('*.py'))
        package = io.BytesIO()
        with zipfile.ZipFile(package, 'w') as archive:
            for p in paths + shared_imports(ROOT, paths):
                raw = p.read_bytes()
                if old and p.name == 'lambda_function.py':
                    needle=b'kw = {"Bucket": BUCKET, "Prefix": pref,\n                      "MaxKeys": 1000}'
                    raw = raw.replace(needle,needle.replace(b'1000',b'400'))
                archive.writestr(p.name, raw)
        raw = package.getvalue()
        config = json.loads((source.parent / 'config.json').read_bytes())
        cfg = {'FunctionName': probe.FUNCTION, 'FunctionArn': 'invented-native-arn',
               'State': 'Active', 'LastUpdateStatus': 'Successful',
               'CodeSha256': base64.b64encode(hashlib.sha256(raw).digest()).decode(),
               'Runtime': config['runtime'], 'Handler': config['handler'], 'MemorySize': config['memory'],
               'Timeout': config['timeout'], 'EphemeralStorage': {'Size': config['ephemeral_storage']},
               'Architectures': ['x86_64'], 'Role': config['role'], 'Environment': {'Variables': config['env']},
               'TracingConfig': {'Mode': 'Active'}, 'DeadLetterConfig': {'TargetArn': 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default'}}
        receipt = {'function': probe.FUNCTION, 'verified': True, 'code_sha256': cfg['CodeSha256'],
                   'commit': 'a'*40, 'deployed_at': (STAMP-timedelta(hours=1)).isoformat()}
        def get(**kw):
            self.assertEqual(kw['Bucket'], probe.BUCKET)
            body = json.dumps(receipt).encode() if kw['Key'].startswith('data/ops/releases/') else self.outputs[kw['Key']]['body']
            return {'Body': io.BytesIO(body)}
        def head(**kw):
            self.assertEqual(kw['Bucket'], probe.BUCKET)
            return {'ContentLength': len(self.outputs[kw['Key']]['body']), 'LastModified': STAMP+timedelta(minutes=1)}
        lam = SimpleNamespace(get_function=lambda **kw: {'Configuration': cfg, 'Code': {'Location': 'https://invented.invalid/private-signed-url'}})
        s3 = SimpleNamespace(get_object=get, head_object=head)
        events = SimpleNamespace(describe_rule=lambda **kw: {'Name': probe.RULE, 'State': 'ENABLED', 'ScheduleExpression': 'rate(1 hour)'},
                                 list_targets_by_rule=lambda **kw: {'Targets': [{'Id': 'invented-target', 'Arn': 'invented-native-arn', 'Input': 'invented-payload-never-output'}]})
        return lam, s3, events, probe.Reader(), lambda *a, **kw: io.BytesIO(raw), cfg

    def test_candidate_exact_source_controls_and_natural_publication(self):
        lam,s3,events,reader,opener,_ = self.fixture()
        result = probe.inspect(lam,s3,events,reader,opener)
        self.assertEqual(reader.calls,9)
        self.assertTrue(result['natural_publication_after_candidate_release'])
        self.assertTrue(all(result['release_controls'].values()))
        self.assertNotIn('invented-payload', json.dumps(result))
        self.assertNotIn('private-signed-url', json.dumps(result))
        self.assertNotIn('Environment', result)

    def test_predecessor_is_baseline_and_never_candidate_acceptance(self):
        lam,s3,events,reader,opener,_ = self.fixture(old=True)
        result = probe.inspect(lam,s3,events,reader,opener)
        self.assertEqual(result['handler_sha256'],probe.OLD_HASH)
        self.assertEqual(result['source_phase'],'predecessor')
        self.assertFalse(result['natural_publication_after_candidate_release'])

    def test_unknown_release_controls_stop_before_schedule_or_body_reads(self):
        lam,s3,events,reader,opener,cfg = self.fixture()
        cfg['TracingConfig']['Mode']='PassThrough'
        with self.assertRaisesRegex(probe.Stop,'release_control_mismatch'):
            probe.inspect(lam,s3,events,reader,opener)
        self.assertEqual(reader.calls,1)

    def test_extra_target_page_stops_without_following_token(self):
        lam,s3,events,reader,opener,_ = self.fixture()
        events.list_targets_by_rule=lambda **kw: {'Targets': [], 'NextToken': 'invented-extra-page'}
        with self.assertRaisesRegex(probe.Stop,'target_row_bound_reached'):
            probe.inspect(lam,s3,events,reader,opener)
        self.assertEqual(reader.calls,3)

    def test_denial_stops_immediately_and_withholds_error_details(self):
        class Denied(Exception):
            response={'Error':{'Code':'AccessDenied'}}
        def deny(**kw):raise Denied('invented secret in SDK error')
        reader=probe.Reader()
        with self.assertRaisesRegex(probe.Stop,'^access_denied_stop$'):
            reader.read(deny)
        self.assertEqual(reader.calls,1)
        reader.calls=probe.MAX_CALLS
        with self.assertRaisesRegex(probe.Stop,'^api_bound_reached$'):
            reader.read(deny)
        self.assertEqual(reader.calls,probe.MAX_CALLS)

    def test_over_limit_stream_is_closed(self):
        stream=io.BytesIO(b'invented')
        with self.assertRaisesRegex(probe.Stop,'byte_bound_reached'):
            probe.bounded(stream,3)
        self.assertTrue(stream.closed)


if __name__ == '__main__':unittest.main()
