from pathlib import Path
from unittest.mock import patch
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_5991_option_flow_candidate_qualification as audit
from test_option_flow_research import fixture


class Tests(unittest.TestCase):
    def fixture(self):
        with patch.object(audit.model,'CONTINUITY',('SPY',)):
            blobs,inputs=fixture();out=audit.model.build(inputs,blobs.__getitem__,blobs.__setitem__)
        summary=audit.model.checked(out['chains']['SPY']['chain'],blobs.__getitem__)
        return blobs,inputs['chains']['SPY'],summary
    def test_original_fields_and_sums(self):
        blobs,chain,summary=self.fixture();counts=audit.verify_records(chain,summary,blobs.__getitem__)
        self.assertEqual(counts['retained_rows'],2);self.assertEqual(counts['dated_bar_groups'],1)
        self.assertGreater(counts['true_zero_values'],0)
    def test_altered_aggregate_detected(self):
        blobs,chain,summary=self.fixture();summary['reported_open_interest']['calls']['value']='1'
        with self.assertRaises(AssertionError):audit.verify_records(chain,summary,blobs.__getitem__)
    def test_altered_population_detected(self):
        blobs,chain,summary=self.fixture();summary['daily_bar_update_groups'][0]['calls']['population_rows']+=1
        with self.assertRaises(AssertionError):audit.verify_records(chain,summary,blobs.__getitem__)
    def test_altered_count_detected(self):
        blobs,chain,summary=self.fixture();summary['coverage']['returned_rows']+=1
        with self.assertRaises(AssertionError):audit.verify_records(chain,summary,blobs.__getitem__)
    def test_altered_missingness_detected(self):
        blobs,chain,summary=self.fixture();summary['coverage']['field_quality']['open_interest']['reported_zero']+=1
        with self.assertRaises(AssertionError):audit.verify_records(chain,summary,blobs.__getitem__)


if __name__=='__main__':unittest.main()
