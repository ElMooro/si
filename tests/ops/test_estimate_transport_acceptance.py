from pathlib import Path
from copy import deepcopy
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6281_estimate_transport_acceptance as op
from test_estimate_observations_acceptance import Tests as Existing


class Tests(unittest.TestCase):
    def test_actual_writer_binds_four_source_files_and_complete_public_replay(self):
        raw,prior,m=Existing().publication();writes=list(m.writes);out=op.publication(raw,prior)
        self.assertTrue(out['current_compiler_publication_verified']);self.assertEqual(out['compiler_files'],4)
        self.assertFalse(out['calendar_originals_verified']);self.assertFalse(out['investment_authority']);self.assertEqual(writes,m.writes)
    def test_forged_source_closure_or_transport_cannot_pass_new_publication(self):
        raw,prior,_=Existing().publication();p=json.loads(raw)
        for edit in [lambda p:p['source_files'].pop('benzinga.py'),lambda p:p['source_files']['managed_secret.py'].update(sha256='bad'),
            lambda p:p['estimate_transport'].update(follow_redirects=True),lambda p:p['estimate_transport'].update(credential_location='query'),
            lambda p:p['estimate_transport'].update(automatic_retries=True)]:
            q=deepcopy(p);edit(q)
            with self.assertRaises(ValueError):op.publication(json.dumps(q).encode(),prior)
    def test_older_publication_explicitly_pending_and_no_scope_expansion(self):
        out=op.publication(json.dumps({'measurement_contract':op.original.compiler().CONTRACT,'version':'3.2.0'}).encode())
        self.assertFalse(out['current_compiler_publication_verified']);self.assertIn('pending_original_schedule',out['status'])
        source=Path(op.__file__).read_text(encoding='utf-8')
        for forbidden in ('invoke(', 'put_object(', 'update_function', 'get_secret_value', 'list_objects'):self.assertNotIn(forbidden,source)
        self.assertEqual(op.KEY,'data/estimate-revisions.json')


if __name__=='__main__':unittest.main(verbosity=2)
