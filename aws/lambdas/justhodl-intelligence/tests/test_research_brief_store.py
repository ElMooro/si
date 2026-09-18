import ast
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests'), str(Path(__file__).resolve().parents[1]/'source')]
import report_observations
import research_brief_store as store
from research_brief_model import build, digest, encoded
from test_research_brief_model import source_packet, NOW


class Error(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class S3:
    def __init__(self): self.objects = {}; self.puts = []; self.races = 0
    def get_object(self, Bucket, Key):
        if Key not in self.objects: raise Error('NoSuchKey')
        body = self.objects[Key]
        return {'Body': io.BytesIO(body), 'ETag': hashlib.sha256(body).hexdigest()}
    def put_object(self, Bucket, Key, Body, **args):
        if Key == store.CURRENT and self.races:
            self.races -= 1; raise Error('PreconditionFailed')
        current = self.objects.get(Key)
        if args.get('IfNoneMatch') and current is not None: raise Error('PreconditionFailed')
        if args.get('IfMatch') and (current is None or hashlib.sha256(current).hexdigest() != args['IfMatch']): raise Error('PreconditionFailed')
        self.objects[Key] = Body; self.puts.append(Key)


def client():
    s3 = S3(); packet = source_packet()
    packet.pop('replay')
    compiler = Path(report_observations.__file__).read_bytes(); sha = hashlib.sha256(compiler).hexdigest()
    manifest = {'compiler': {'key': 'data/report-research/compilers/'+sha+'.py', 'sha256': sha}, 'output_sha256': digest(packet)}
    key = 'data/report-research/runs/'+digest(manifest)+'.json'
    packet['replay'] = {'manifest_key': key, 'compiler_sha256': sha, 'output_sha256': digest(packet)}
    s3.objects.update({store.SOURCE: encoded(packet), key: encoded(manifest), manifest['compiler']['key']: compiler})
    return s3


class StoreTests(unittest.TestCase):
    def setUp(self):
        clock = patch.object(store, 'datetime', SimpleNamespace(now=lambda *_: datetime.fromisoformat(NOW)))
        clock.start(); self.addCleanup(clock.stop)

    def test_real_handler_has_only_the_new_publication_route(self):
        source = ROOT/'aws/lambdas/justhodl-intelligence/source/lambda_function.py'
        tree = ast.parse(source.read_text(encoding='utf-8'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'lambda_handler')
        node.decorator_list = []
        calls = []
        env = {'json': json, 's3': object(), 'BUCKET': 'test', 'publish_research_brief': lambda *args: calls.append(args) or {'published': True}}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), env)
        result = env['lambda_handler']({'action': 'legacy', 'suppress_alerts': False}, None)
        self.assertEqual(result['statusCode'], 200); self.assertEqual(len(calls), 1)
        env['publish_research_brief'] = lambda *args: (_ for _ in ()).throw(ValueError('sensitive URL'))
        self.assertNotIn('sensitive URL', env['lambda_handler']({}, None)['body'])

    def test_publication_replays_and_preserves_legacy_before_cas(self):
        s3 = client(); previous = b'{"version":"3.0","scores":{"khalid_index":46}}'
        s3.objects[store.CURRENT] = previous; s3.races = 1
        result = store.run(s3, 'test')
        self.assertTrue(result['published'])
        packet = json.loads(s3.objects[store.CURRENT]); manifest = json.loads(s3.objects[packet['replay']['manifest_key']])
        retained = json.loads(s3.objects[manifest['input']['key']])
        self.assertEqual(digest(build(retained, packet['generated_at'])), manifest['output_sha256'])
        self.assertEqual(s3.objects[store.PREFIX+'legacy-unvalidated/'+hashlib.sha256(previous).hexdigest()+'.json'], previous)
        self.assertEqual(s3.puts[-1], store.CURRENT)

    def test_corrupted_source_and_compiler_never_write_current(self):
        for target in ('packet', 'compiler', 'manifest'):
            s3 = client(); packet = json.loads(s3.objects[store.SOURCE])
            if target == 'packet':
                packet['measurements']['ICSA']['current_decimal'] = '1'; s3.objects[store.SOURCE] = encoded(packet)
            elif target == 'compiler':
                s3.objects['data/report-research/compilers/'+packet['replay']['compiler_sha256']+'.py'] = b'corrupt'
            else:
                key = packet['replay']['manifest_key']; m = json.loads(s3.objects[key]); m['output_sha256'] = 'b'*64; s3.objects[key] = encoded(m)
            with self.assertRaises(ValueError): store.run(s3, 'test')
            self.assertNotIn(store.CURRENT, s3.puts)

    def test_newer_current_is_not_overwritten_and_repeated_races_are_bounded(self):
        s3 = client(); s3.objects[store.CURRENT] = encoded({'generated_at': '2099-01-01T00:00:00Z'})
        self.assertFalse(store.run(s3, 'test')['published'])
        self.assertNotIn(store.CURRENT, s3.puts)
        s3 = client(); s3.races = 10
        with self.assertRaises(RuntimeError): store.run(s3, 'test')
        self.assertEqual(s3.races, 6)

    def test_immutable_receipt_cannot_mask_different_bytes(self):
        s3 = S3(); s3.objects['data/run.json'] = b'old'
        with self.assertRaises(ValueError): store.immutable(s3, 'test', 'data/run.json', b'new')


if __name__ == '__main__': unittest.main()
