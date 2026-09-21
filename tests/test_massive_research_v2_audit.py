from pathlib import Path
from copy import deepcopy
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/ops/staged')]
from test_massive_research_v2 import fixture,model,store
import ops_6018_cross_asset_composition_candidate as audit

class Tests(unittest.TestCase):
    def setUp(self):
        memory,inputs,_=fixture();self.read=store.reader(memory,'bucket');self.output=model.build(inputs,self.read)
    def test_all_seven_parents_reconcile(self):
        result=audit.independent(self.output,self.read)
        self.assertEqual(result['fx_pairs'],19);self.assertEqual(result['dated_futures_contracts'],14)
        self.assertEqual(result['parent_instrument_rows'],38)
    def test_wrong_contract_pointer_fails(self):
        key=next(k for k in self.output['instruments'] if k.startswith('FUTURE:'))
        self.output['instruments'][key][0]['pointer']='/products/ES/contracts/0'
        with self.assertRaises(AssertionError):audit.independent(self.output,self.read)
    def test_relabelled_pair_clock_fails(self):
        self.output['instruments']['FX:MASSIVE:EUR_USD'][0]['source_capture_completed_at']=self.output['generated_at']
        with self.assertRaises(AssertionError):audit.independent(self.output,self.read)
    def test_unearned_metal_unit_fails(self):
        self.output['instrument_identities']['FX:MASSIVE:XAU_USD']['metal_base_quantity_unit_verified']=True
        with self.assertRaises(AssertionError):audit.independent(self.output,self.read)
    def test_removed_currency_dependency_fails(self):
        next(g for g in self.output['dependency_graph']['shared_exposures'] if g['key']=='currency:USD')['instruments'].pop()
        with self.assertRaises(AssertionError):audit.independent(self.output,self.read)
    def test_extra_graph_node_fails(self):
        graph=self.output['dependency_graph'];graph['nodes']['unused']=deepcopy(next(n for n in graph['nodes'].values() if n['kind']=='fx'))
        with self.assertRaises(AssertionError):audit.independent(self.output,self.read)
    def test_invented_futures_deadline_fails(self):
        self.output['sources']['futures']['source_review_due_at']='2026-09-22T21:00:00Z'
        with self.assertRaises(AssertionError):audit.independent(self.output,self.read)

if __name__=='__main__':unittest.main(verbosity=2)
