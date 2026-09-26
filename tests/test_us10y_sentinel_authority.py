"""Exercise the allocator's actual Sentinel read, including legacy self-claims."""
from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
import ast,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
import us10y_sentinel_authority as gate
from release_package_evidence import shared_imports


class Tests(unittest.TestCase):
    def packets(self):
        return (None,[],{}, {'tier':'CRITICAL','generated_at':'2026-09-26T12:20:00Z','fred_date':'2026-09-24'},
            {'tier':'RED','sizing_eligible':True,'forecast_qualified':True,'decision_qualification':{'status':'qualified'}},
            {'tier':'CRITICAL','contract':'invented-certified-model','replay':{'verified':True}})

    def test_reported_bands_and_self_claims_never_authorize_a_veto(self):
        for packet in self.packets():
            previous=deepcopy(packet);result=gate.context(packet)
            self.assertEqual(result['status'],'ABSTAIN');self.assertEqual(result['independent_investment_votes'],0)
            self.assertTrue(all(result[k] is False for k in gate.FLAGS));self.assertEqual(packet,previous)
        self.assertEqual(gate.context(self.packets()[3])['reported_observation_date'],'2026-09-24')

    def test_actual_allocator_cannot_change_its_ranking_from_an_unqualified_band(self):
        source=ROOT/'aws/lambdas/justhodl-master-allocator/source/lambda_function.py'
        nodes=[n for n in ast.parse(source.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='_best_asset_now']
        self.assertEqual(len(nodes),1)
        for packet in self.packets():
            scope={'_BA_UNIV':[('SPY','Stocks'),('GLD','Gold')],'_BA_DEFENSIVE':{'CASH','GLD'},
                '_ba_closes':lambda symbol:symbol,
                '_ba_mom':lambda symbol:{'score':{'SPY':10,'GLD':5,'BIL':1}[symbol],'r12_1':.1,'r6':.1,'r3':.1},
                '_ba_s3json':lambda key:packet if key==gate.CURRENT else {},'time':SimpleNamespace(sleep=lambda _:None)}
            exec(compile(ast.Module(body=nodes,type_ignores=[]),'allocator','exec'),scope)
            result=scope['_best_asset_now']()
            self.assertEqual(result['winner']['asset'],'SPY');self.assertEqual(len(result['ranked']),3)
            self.assertEqual(result['risk_override'],{'active':False,'reasons':[]})
            self.assertFalse(result['sentinel_context']['sizing_eligible'])

    def test_actual_package_includes_authority_boundary(self):
        source=ROOT/'aws/lambdas/justhodl-master-allocator/source'
        self.assertIn('us10y_sentinel_authority.py',[p.name for p in shared_imports(ROOT,list(source.glob('*.py')))])


if __name__=='__main__':unittest.main(verbosity=2)
