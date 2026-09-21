"""Actual production read boundaries; no consumer or provider invocation."""
from datetime import datetime,timedelta
from pathlib import Path
import ast,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import money_volume_research as guard
import money_volume_model as model
from test_money_volume_research import build,STAMP

class PriceVolumeBoundaries(unittest.TestCase):
    def test_actual_sector_consumers_cannot_relabel_proxies_as_money(self):
        canary={'sectors':[{'sector':'Technology','net_flow_usd':1e12}],
            'institutional_sector_tilt':[{'sector':'Technology','net_fund_actions':1000}]}
        import sector_fusion_store as fusion
        from test_sector_fusion_research import fixtures,STAMP
        rotation,_,_=fixtures()
        inputs={'contract':'sector-fusion-inputs.v1','kind':'flow','sources':dict.fromkeys(fusion.SOURCES,{}),'generated_at':STAMP}
        def source(ref,key,read,metadata=False):return rotation if key==fusion.ROOTS[0] else canary
        with patch.object(fusion,'source',side_effect=source),patch.object(fusion.prices,'replay',return_value={k:v for k,v in rotation.items() if k!='replay'}):
            with self.assertRaisesRegex(ValueError,'Native sector price or stock-volume root'):fusion.compile_output(inputs,lambda _:None)
        # Capital is only a projection; it cannot accept a legacy canary either.
        inputs.update(kind='capital',sources={fusion.model.CURRENT:{},fusion.model.CAPITAL_CURRENT:{}})
        with patch.object(fusion,'source',return_value=canary),self.assertRaisesRegex(ValueError,'Canonical matrix'):fusion.compile_output(inputs,lambda _:None)
    def test_only_current_unmodified_native_context_is_admitted_without_authority(self):
        p=build();p['replay']={'manifest_key':'data/money-volume-research/runs/'+'a'*64+'.json','output_sha256':model.sha(model.encoded(p))};at=datetime.fromisoformat(STAMP)
        self.assertTrue(guard.context(p,at)['available']);self.assertFalse(guard.context(p,at+timedelta(hours=27))['available'])
        self.assertFalse(guard.context({**p,'calls_eligible':True},at)['available'])
        p['stocks'][0]['price_volume_pressure_usd_proxy']+=1;self.assertFalse(guard.context(p,at)['available'])

if __name__=='__main__':unittest.main(verbosity=2)
