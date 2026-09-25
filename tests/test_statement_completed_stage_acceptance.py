from pathlib import Path
import copy,importlib.util,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged')]
spec=importlib.util.spec_from_file_location('stage_acceptance',ROOT/'aws/ops/staged/ops_6110_accounting_completed_stage_acceptance.py')
acceptance=importlib.util.module_from_spec(spec);spec.loader.exec_module(acceptance)
from test_statement_producer import fixture


def evidence():
    f,s3,reference=fixture();proof=f.verify(f.compile())
    previous={'status':'failed','error_type':'HTTPError','request_id':acceptance.prior.REQUEST,
        'diagnostic':acceptance.prior.DIAGNOSTIC,'replay':acceptance.prior.REFERENCE,
        'stages':{'source_replay_seconds':148.951,'independent_check_seconds':80.1}}
    # The failed journal really has no qualification field. 6107's code puts
    # it only into an intermediate journal and then overwrites that journal.
    ready={'replay':acceptance.prior.REFERENCE,'qualification':proof}
    return previous,ready,Path(acceptance.prior.__file__).read_bytes()


class Tests(unittest.TestCase):
    def test_final_failure_journal_without_inline_proof_recovers_exact_verified_ready(self):
        previous,ready,script=evidence();before=copy.deepcopy(previous)
        self.assertNotIn('qualification',previous)
        self.assertEqual(acceptance.completed_proof(previous,ready,script),ready['qualification'])
        self.assertEqual(previous,before)
        self.assertEqual(ready['qualification']['metric_comparisons'],34)

    def test_missing_incomplete_or_wrong_failure_stages_cannot_be_adopted(self):
        changes=[lambda p:p['stages'].pop('independent_check_seconds'),
            lambda p:p.update(error_type='AssertionError'),lambda p:p.update(status='arithmetic_verified'),
            lambda p:p.update(request_id='another'),lambda p:p.update(replay={}),lambda p:p.update(diagnostic={})]
        changes += [lambda p,v=v:p['stages'].update(independent_check_seconds=v) for v in (0,-1,True,'80',None,float('nan'),float('inf'))]
        for change in changes:
            previous,ready,script=evidence();change(previous)
            with self.assertRaises(ValueError):acceptance.completed_proof(previous,ready,script)

    def test_different_code_snapshot_or_missing_proof_is_rejected(self):
        previous,ready,script=evidence()
        with self.assertRaises(ValueError):acceptance.completed_proof(previous,ready,script+b'\n')
        for update in ({'replay':{}},{'qualification':None},{'qualification':{}},{'qualification':[]}):
            with self.assertRaises(ValueError):acceptance.completed_proof(previous,{**ready,**update},script)

    def test_current_delivery_is_identified_get_without_invocation_or_private_target(self):
        request=acceptance.public_request('data/forensic-screen.json')
        self.assertEqual(request.get_method(),'GET');self.assertIsNone(request.data)
        self.assertEqual(request.get_header('User-agent'),'JustHodl-research-acceptance/1.0')
        self.assertFalse(request.has_header('Authorization'))
        for key in ('data/account.json','https://example.com','data/forensic-screen.json?kickoff=1'):
            with self.assertRaises(ValueError):acceptance.public_request(key)


if __name__=='__main__':unittest.main(verbosity=2)
