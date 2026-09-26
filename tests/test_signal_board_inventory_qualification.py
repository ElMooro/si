"""Retained compiler closure, immutable inputs and one-attempt qualification."""
from pathlib import Path
import io
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('tests', 'aws/ops/staged', 'aws/ops/checks', 'aws/shared')]
import ops_6142_signal_board_inventory_qualification as qualification
import test_signal_board_candidate as fixtures
import signal_board_candidate as candidate
import verify_signal_board_inventory as independent


class Storage:
    def __init__(self): self.objects = {}; self.puts = []
    def put_object(self, **kw):
        if kw.get('IfNoneMatch') == '*' and kw['Key'] in self.objects:
            raise ValueError('Claim already exists')
        self.objects[kw['Key']] = kw['Body']; self.puts.append(kw)
    def get_object(self, **kw): return {'Body': io.BytesIO(self.objects[kw['Key']])}
    def retain(self, raw):
        digest = candidate.sha(raw)
        ref = {'key': qualification.baseline.PRIVATE+digest+'.bin', 'sha256': digest, 'bytes': len(raw)}
        self.objects[ref['key']] = raw
        return ref


class Tests(unittest.TestCase):
    def inputs(self):
        store = Storage(); bodies, captures, _ = fixtures.fixture()
        for entry in captures.values():
            if 'original' in entry:
                entry['original'] = store.retain(bodies[entry['original']['key']])
        compilers = {name: store.retain(path.read_bytes()) for name, path in qualification.compiler_paths().items()}
        return store, {'registry': fixtures.ROWS, 'captures': captures, 'generated_at': fixtures.NOW}, compilers

    def test_full_fresh_process_replay_without_network_or_native_execution(self):
        store, inputs, compilers = self.inputs()
        out, proof = qualification.isolated(store, inputs, compilers)
        read = lambda ref: qualification.checked(store, ref)
        expected = candidate.build(inputs['registry'], inputs['captures'], read, inputs['generated_at'])
        self.assertEqual(out, candidate.encoded(expected))
        self.assertEqual(proof, candidate.encoded(independent.verify(expected, inputs['registry'], inputs['captures'], read)))

    def test_tampered_and_unreviewed_compilers_rejected_before_process(self):
        store, inputs, compilers = self.inputs()
        with patch.object(qualification.subprocess, 'run') as run:
            compilers['signal_board_candidate.py'] = store.retain(b'raise RuntimeError("replacement")')
            with self.assertRaises(ValueError): qualification.isolated(store, inputs, compilers)
            run.assert_not_called()
            del compilers['signal_board_candidate.py']
            with self.assertRaises(ValueError): qualification.isolated(store, inputs, compilers)

    def test_tampered_source_fails_before_process(self):
        store, inputs, compilers = self.inputs()
        ref = inputs['captures']['data/canary-warroom.json']['original']; store.objects[ref['key']] += b' '
        with patch.object(qualification.subprocess, 'run') as run:
            with self.assertRaises(ValueError): qualification.isolated(store, inputs, compilers)
            run.assert_not_called()

    def test_only_accepted_private_research_objects_are_read(self):
        store = Storage(); ref = store.retain(b'complete')
        self.assertEqual(qualification.checked(store, ref), b'complete')
        for key in ('data/pm-decision.json', 'data/sizing.json', qualification.baseline.PRIVATE+'../private.bin'):
            with self.assertRaises(ValueError): qualification.checked(store, {**ref, 'key': key})
        self.assertEqual(qualification.checked(store, store.retain(b'')), b'')

    def test_one_durable_claim_and_verified_journal_readback(self):
        store = Storage(); qualification.journal(store, {'status': 'claimed'}, True)
        self.assertEqual(store.puts[0]['IfNoneMatch'], '*')
        with self.assertRaises(ValueError): qualification.journal(store, {'status': 'claimed'}, True)
        qualification.journal(store, {'status': 'complete'})
        self.assertNotIn('IfNoneMatch', store.puts[-1])

    def test_native_reads_and_private_outputs_only(self):
        text = Path(qualification.__file__).read_text(encoding='utf-8')
        for forbidden in ('.invoke(', '.update_function_code(', '.put_rule(', '.update_schedule(', '.get_parameter(', '.get_secret_value(', 'public_response('):
            self.assertNotIn(forbidden, text)
        for required in ('sys.exit(1)', 'journal(s3, progress, True)', "if not privacy['all_denied']", 'scheduled_qualified_packages', 'private_account_reads'):
            self.assertIn(required, text)


if __name__ == '__main__': unittest.main(verbosity=2)
