"""Actual production read boundaries; no consumer or provider invocation."""
from datetime import datetime,timedelta
from pathlib import Path
import ast,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import money_volume_research as guard
import money_volume_model as model
from test_money_volume_research import build,STAMP

class PriceVolumeBoundaries(unittest.TestCase):
    def test_actual_sector_consumers_cannot_relabel_proxies_as_money(self):
        canary={'sectors':[{'sector':'Technology','net_flow_usd':1e12}],
            'institutional_sector_tilt':[{'sector':'Technology','net_fund_actions':1000}]}
        for fn,var in [('justhodl-sector-flow-state','mf'),('justhodl-sector-capital-fusion','mfs')]:
            tree=ast.parse((ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8'))
            node=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==var for t in n.targets))
            ns={'rj':lambda _:canary};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-money-volume-boundary','exec'),ns)
            self.assertEqual(ns[var]['sectors'],[]);self.assertEqual(ns[var]['institutional_sector_tilt'],[]);self.assertFalse(ns[var]['calls_eligible'])
    def test_only_current_unmodified_native_context_is_admitted_without_authority(self):
        p=build();p['replay']={'manifest_key':'data/money-volume-research/runs/'+'a'*64+'.json','output_sha256':model.sha(model.encoded(p))};at=datetime.fromisoformat(STAMP)
        self.assertTrue(guard.context(p,at)['available']);self.assertFalse(guard.context(p,at+timedelta(hours=27))['available'])
        self.assertFalse(guard.context({**p,'calls_eligible':True},at)['available'])
        p['stocks'][0]['price_volume_pressure_usd_proxy']+=1;self.assertFalse(guard.context(p,at)['available'])

if __name__=='__main__':unittest.main(verbosity=2)
