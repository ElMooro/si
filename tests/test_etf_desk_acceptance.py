"""Independent desk acceptance arithmetic rejects altered original evidence."""
from pathlib import Path
from unittest import mock
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import ops_5986_etf_desk_native_acceptance as audit
import etf_desk_store as store
from test_etf_desk_store import fixture


class Acceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with mock.patch.object(store.catalog,'DESK',('SPY','VOO','BND')),mock.patch.dict(store.flow_catalog.ETF_UNIVERSE,
                {'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True):
            cls.db,cls.inputs=fixture();read=store.reader(cls.db,'fixture')
            cls.output=store.compile_output(cls.inputs,read,lambda k,b:store.immutable(cls.db,'fixture',k,b,read=read))
    def test_all_three_source_families_and_matched_totals(self):
        result=audit.independent_desk(self.output,self.inputs,store.reader(self.db,'fixture'))
        self.assertEqual(result['profiles']['profiles'],6)
        self.assertEqual(result['holdings']['counts']['snapshots'],6)
        self.assertEqual(result['flows']['counts']['group_windows'],3)
        self.assertGreater(result['flows']['counts']['original_row_positions'],0)
    def test_altered_flow_value_is_rejected_without_normalizer(self):
        changed=copy.deepcopy(self.output);window=changed['funds']['BND']['flows']['aligned_windows']['5']
        self.assertEqual(window['status'],'matched_reporting_window');window['flow_usd_decimal']='999999'
        with self.assertRaises(AssertionError):audit.independent_desk(changed,self.inputs,store.reader(self.db,'fixture'))
    def test_original_holdings_position_change_is_rejected(self):
        db=copy.copy(self.db);db.objects=dict(self.db.objects)
        ref=self.inputs['extra_holdings']['BND']['current']['pages'][0]['original']
        body=json.loads(db.objects[ref['key']]);body['results'][0]['shares_held']=999999
        db.objects[ref['key']]=store.model.encoded(body)
        with self.assertRaises((AssertionError,ValueError)):
            audit.independent_holdings({'funds':{'BND':self.output['funds']['BND']['holdings']}},store.reader(db,'fixture'))


if __name__=='__main__':unittest.main()
