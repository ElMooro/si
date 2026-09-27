from pathlib import Path
from unittest.mock import patch
import importlib.util
import json
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6237_capex_measurements_acceptance as op
spec=importlib.util.spec_from_file_location('capex_fixture',ROOT/'aws/lambdas/justhodl-capex-pulse/tests/test_measurements.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)


class Tests(unittest.TestCase):
    def test_old_version_pending(self):
        self.assertEqual(op.publication(b'{"version":"1.1.1","rows":[]}')['status'],'pending_original_schedule_publication')

    def test_actual_handler_reproduction_and_tamper_rejection(self):
        writes,*_=fixture.Tests().handler();packet=writes[op.KEY]
        with patch.object(op,'HYPERSCALERS',['TEST']):
            self.assertEqual(op.publication(json.dumps(packet).encode())['observations'],8)
            packet['rows'][0]['reported_window_amount']=999
            with self.assertRaises(ValueError):op.publication(json.dumps(packet).encode())

    def test_cohort_tampering_and_eligibility_promotion_fail(self):
        for target in ('cohort','eligibility'):
            writes,*_=fixture.Tests().handler();packet=writes[op.KEY]
            if target=='cohort':packet['market']['yoy_pct']=999
            else:packet['calls_eligible']=True
            with patch.object(op,'HYPERSCALERS',['TEST']),self.assertRaises(ValueError):op.publication(json.dumps(packet).encode())


if __name__=='__main__':unittest.main()
