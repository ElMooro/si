from copy import deepcopy
from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'tests'), str(ROOT/'aws/ops/staged')]
from test_massive_research import fixture, model, store
import ops_6003_massive_composite_candidate as audit


class Tests(unittest.TestCase):
    def setUp(self):
        self.memory, self.inputs, _ = fixture(); self.read = store.reader(self.memory, 'bucket')
        self.output = model.build(self.inputs, self.read)
    def test_all_parent_rows_and_family_graph_reconcile(self):
        result = audit.independent(self.output, self.read)
        self.assertEqual(result['parent_instrument_rows'], 5)
        self.assertEqual(result['population_expiry_strike_groups'], 1)
    def test_duplicate_row_reference_is_not_coverage(self):
        self.output['instruments']['SPY'].append(deepcopy(self.output['instruments']['SPY'][0]))
        with self.assertRaises(AssertionError): audit.independent(self.output, self.read)
    def test_missing_instrument_reference_fails(self):
        self.output['instruments']['SPY'].pop()
        with self.assertRaises(AssertionError): audit.independent(self.output, self.read)
    def test_false_independence_fails(self):
        self.output['dependency_graph']['statistical_independence_established'] = True
        with self.assertRaises(AssertionError): audit.independent(self.output, self.read)
    def test_wrong_parent_edge_fails(self):
        self.output['dependency_graph']['edges'][0]['source'] = 'wrong'
        with self.assertRaises(AssertionError): audit.independent(self.output, self.read)
    def test_wrong_coverage_count_fails(self):
        self.output['sources']['options']['instrument_count'] = 999
        with self.assertRaises(AssertionError): audit.independent(self.output, self.read)


if __name__ == '__main__': unittest.main()
