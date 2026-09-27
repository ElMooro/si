from pathlib import Path
import importlib.util
import json
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6239_inventory_measurements_acceptance as op
spec=importlib.util.spec_from_file_location('inventory_fixture',ROOT/'aws/lambdas/justhodl-inventory-drawdown/tests/run_tests.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)


def packet():
    m=op.compiler();sources={'data/bottleneck-boom.json':{'ranks':[{'ticker':'TEST'}]}}
    plan=m.universe(sources)
    return {'measurement_contract':m.CONTRACT,'generated_at':'2026-09-27T00:00:00Z','checked_as_of':'2026-09-27',
            'call':None,'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,
            'source_contexts':sources,'universe_plan':plan,'signals_logged':0,'boom_setups':[],
            'sector_drawdown':[m.sector(sid,'Total',None,fixture.received(fixture.months()),'2026-09-27') for sid in ('ISRATIO','MNFCTRIRSA','RETAILIRSA','WHLSLRIRSA','AISRSA','MRTSIR441USS','MRTSIR444USS','MRTSIR452USS','MRTSIR448USS')],
            'stock_drawdown_board':[m.stock('TEST',fixture.received(fixture.quarters()),plan['contexts']['TEST'],'2026-09-27')]}


class Tests(unittest.TestCase):
    def test_legacy_publication_remains_pending(self):
        self.assertEqual(op.publication(b'{"version":"1.0.0","sector_drawdown":[],"stock_drawdown_board":[]}')['status'],'pending_original_schedule_publication')

    def test_complete_observations_reproduce_and_tamper_fails(self):
        p=packet();self.assertEqual(op.publication(json.dumps(p).encode())['observations'],650)
        p['stock_drawdown_board'][0]['dio_chg_pct']=999
        with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())

    def test_missing_population_and_false_authority_fail(self):
        for key in ('population','eligibility','universe','monthly'):
            p=packet()
            if key=='population':p['stock_drawdown_board']=[]
            elif key=='eligibility':p['sizing_eligible']=True
            elif key=='universe':p['universe_plan']['requested']=[]
            else:p['sector_drawdown'][0]['latest_ratio']=999
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())


if __name__=='__main__':unittest.main()
