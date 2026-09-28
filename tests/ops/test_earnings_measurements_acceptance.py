from pathlib import Path
import importlib.util
import json
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6241_earnings_measurements_acceptance as op
spec=importlib.util.spec_from_file_location('earnings_fixture',ROOT/'aws/lambdas/justhodl-earnings-quality/tests/run_tests.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)


class Tests(unittest.TestCase):
    def test_previous_version_remains_pending(self):
        self.assertEqual(op.publication(b'{"version":"1.0.0","as_of":"2026-09-27T14:18:41Z"}')['status'],'pending_original_schedule_publication')

    def test_complete_native_arithmetic_and_tamper_rejection(self):
        _,writes=fixture.Tests().handler();p=writes[0]
        self.assertEqual(op.publication(json.dumps(p).encode())['observations'],48)
        p['issuer_rows'][0]['amounts']['operating_cash_flow']=999
        with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())

    def test_missing_occurrence_rank_and_eligibility_promotion_fail(self):
        for key in ('population','rank','eligibility'):
            _,writes=fixture.Tests().handler();p=writes[0]
            if key=='population':p['issuer_rows'].pop()
            elif key=='rank':p['all_ranked']=[{'ticker':'TEST','quality_score':1}]
            else:p['sizing_eligible']=True
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())

    def test_typed_counts_indices_and_explicit_call_are_required(self):
        changes=[lambda p:p.update(n_received=float(p['n_received'])),lambda p:p.update(signals_logged=False),
                 lambda p:p.update(notifications_sent=0.0),lambda p:p.update(n_aligned=float(p['n_aligned'])),
                 lambda p:p['issuer_rows'][0].update(request_index=False),lambda p:p.pop('call')]
        for change in changes:
            _,writes=fixture.Tests().handler();p=writes[0];change(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())


if __name__=='__main__':unittest.main()
