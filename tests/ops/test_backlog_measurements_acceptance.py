from pathlib import Path
import importlib.util
import json
import copy
import sys
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6235_backlog_measurements_acceptance as op
spec=importlib.util.spec_from_file_location('backlog_native_fixture',ROOT/'aws/lambdas/justhodl-backlog/tests/run_tests.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)


class Tests(unittest.TestCase):
    def test_legacy_packet_remains_pending(self):
        self.assertEqual(op.publication(b'{"version":"1.0","by_ticker":{}}')['status'],'pending_original_schedule_publication')

    def test_complete_native_projection_and_tamper_rejection(self):
        ns,writes,_=fixture.Tests().handler();ns['lambda_handler']();p=writes[op.KEY]
        result=op.publication(json.dumps(p).encode());self.assertEqual(result['rebuilt_rows'],1)
        self.assertEqual(result['retained_legacy_rows'],1);self.assertFalse(result['provider_originals_replayed'])
        p['by_ticker']['TEST']['rpo_qoq']=999
        with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())

    def test_observation_mutation_and_legacy_reactivation_are_rejected(self):
        ns,writes,_=fixture.Tests().handler();ns['lambda_handler']();p=writes[op.KEY]
        p['by_ticker']['TEST']['measurements']['rpo']['observations'][0]['source']['val']=900
        with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())
        ns,writes,_=fixture.Tests().handler();ns['lambda_handler']();p=writes[op.KEY]
        p['by_ticker']['OLD']['rpo']=42
        with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())


    def test_numeric_permission_and_counter_substitutions_cannot_pass_replay(self):
        ns,writes,_=fixture.Tests().handler();ns['lambda_handler']();original=writes[op.KEY]
        for edit in (lambda p:p['by_ticker']['TEST'].update(calls_eligible=0),
                     lambda p:p.update(ledger_size=float(p['ledger_size'])),
                     lambda p:p['by_ticker']['TEST']['measurements']['rpo'].update(sizing_eligible=0)):
            p=copy.deepcopy(original);edit(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())


if __name__=='__main__':unittest.main()
