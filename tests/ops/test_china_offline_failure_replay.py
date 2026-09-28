from pathlib import Path
from unittest.mock import patch
from copy import deepcopy
from datetime import datetime, timedelta
import importlib.util, sys, unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6295_china_offline_failure_replay as op


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=ROOT/'aws/lambdas/justhodl-china-liquidity/tests/run_tests.py'
        spec=importlib.util.spec_from_file_location('china_offline_fixture',path)
        cls.fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.fixture)
        cls.fixture.Tests.setUpClass()

    def retained_fixture(self):
        native=self.fixture.Tests();memory=self.fixture.Memory()
        before={key:op.store.get(memory,op.BUCKET,key) for key in op.store.KEYS}
        memory,packet,_=native.execute(memory)
        plan=op.store.strict(op.store.retained(memory,op.BUCKET,packet['publication_context']['manifest']))
        attempts=deepcopy(plan['http_attempts']);sources={}
        for i,row in enumerate(attempts):
            row['requested_at']=(datetime.fromisoformat(self.fixture.AT)+timedelta(seconds=i)).isoformat()
            if row.get('original'):
                sources[row['original']['sha256']]=op.store.retained(memory,op.BUCKET,row['original'])
        return native,before,attempts,sources

    def test_actual_calculator_uses_whole_retained_responses_without_network_or_public_writes(self):
        native,before,attempts,sources=self.retained_fixture();original=deepcopy(before)
        with patch('urllib.request.urlopen',side_effect=AssertionError('Network forbidden')), patch('urllib.request.OpenerDirector.open',side_effect=AssertionError('Network forbidden')):
            result=op.calculate_offline(native.module,before,attempts,sources)
        self.assertEqual(result['status'],'offline_calculation_completed',result)
        self.assertEqual(result['attempts_replayed'],len(attempts));self.assertEqual(before,original)
        self.assertEqual(result['provider_requests'],0);self.assertEqual(result['public_writes'],0)
        self.assertEqual(set(result['staged_keys']),set(op.store.KEYS))

    def test_request_drift_is_diagnosed_without_issuing_another_request(self):
        native,before,attempts,sources=self.retained_fixture()
        attempts[0]['request']['parameters']['series_id']='DIFFERENT'
        with patch('urllib.request.OpenerDirector.open',side_effect=AssertionError('Network forbidden')):
            result=op.calculate_offline(native.module,before,attempts,sources)
        self.assertEqual(result['status'],'failed');self.assertEqual(result['attempts_replayed'],0)
        self.assertEqual(result['reason'],'Retained acquisition request order differs')
        self.assertEqual(result['staged_keys'],[])
        self.assertNotIn('offline-placeholder',str(result))

    def test_unexpected_exception_text_is_withheld(self):
        native,before,attempts,sources=self.retained_fixture()
        with patch.object(op.store,'calculate',side_effect=RuntimeError('private-body-do-not-print')):
            result=op.calculate_offline(native.module,before,attempts,sources)
        self.assertEqual(result['error_type'],'RuntimeError')
        self.assertNotIn('private-body-do-not-print',str(result))
        self.assertTrue(result['source_frames'])


if __name__=='__main__':unittest.main(verbosity=2)
