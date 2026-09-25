from pathlib import Path
import importlib.util,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged')]
spec=importlib.util.spec_from_file_location('delivery_acceptance',ROOT/'aws/ops/staged/ops_6108_accounting_completed_replay_acceptance.py')
acceptance=importlib.util.module_from_spec(spec);spec.loader.exec_module(acceptance)


class Tests(unittest.TestCase):
    def test_identified_readonly_public_request_has_no_default_python_agent_or_credentials(self):
        request=acceptance.public_request('data/forensic-screen.json')
        self.assertEqual(request.full_url,'https://justhodl.ai/data/forensic-screen.json')
        self.assertEqual(request.get_method(),'GET');self.assertIsNone(request.data)
        self.assertEqual(request.get_header('User-agent'),'JustHodl-research-acceptance/1.0')
        self.assertEqual(request.get_header('Cache-control'),'no-cache');self.assertFalse(request.has_header('Authorization'))
        for key in ('data/account.json','https://example.com','data/forensic-screen.json?kickoff=1'):
            with self.assertRaises(ValueError):acceptance.public_request(key)


if __name__=='__main__':unittest.main(verbosity=2)
