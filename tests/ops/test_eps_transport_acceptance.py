from pathlib import Path
from copy import deepcopy
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6283_eps_transport_acceptance as op
import test_eps_target_acceptance as existing


class Tests(unittest.TestCase):
    def test_actual_writer_replays_and_binds_the_bundled_import(self):
        raw,prior,memory=existing.Tests().publication();before=list(memory.writes);out=op.publication(raw,prior)
        self.assertTrue(out['current_compiler_publication_verified']);self.assertEqual(out['compiler_files'],3)
        self.assertFalse(out['investment_authority']);self.assertEqual(memory.writes,before)

    def test_missing_changed_import_or_transport_never_qualifies(self):
        raw,prior,_=existing.Tests().publication();p=json.loads(raw)
        for edit in [lambda p:p.pop('source_imports'),lambda p:p['source_imports']['managed_secret.py'].update(sha256='wrong'),
            lambda p:p['transport'].update(stop_after_authorization_error=False),lambda p:p['transport'].update(follow_redirects=True),
            lambda p:p['transport'].update(max_response_bytes=9999999),lambda p:p['source_files']['lambda_function.py'].update(sha256='wrong')]:
            q=deepcopy(p);edit(q)
            with self.assertRaises(ValueError):op.publication(json.dumps(q).encode(),prior)

    def test_old_native_publication_stays_pending_without_scope_expansion(self):
        out=op.publication(json.dumps({'measurement_contract':op.original.compiler().CONTRACT,'version':'1.1.0'}).encode())
        self.assertFalse(out['current_compiler_publication_verified']);self.assertIn('pending_original_fanout',out['status'])
        source=Path(op.__file__).read_text(encoding='utf-8')
        for forbidden in ('invoke(', 'put_object(', 'update_function', 'get_secret_value', 'list_objects'):self.assertNotIn(forbidden,source)
        self.assertEqual(op.KEY,'data/eps-revision-velocity.json')


if __name__=='__main__':unittest.main(verbosity=2)
