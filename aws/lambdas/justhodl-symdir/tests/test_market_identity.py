import unittest
from unittest.mock import Mock,patch
import test_warehouse_routing


class MarketIdentityTests(unittest.TestCase):
    def test_mapping_preserves_original_packet_and_discloses_equivalence(self):
        fixture=test_warehouse_routing.WarehouseTests();m=fixture.module();self.addCleanup(fixture.doCleanups)
        m._tv_bank_doc=Mock(return_value=None)
        m._get_json=Mock(return_value={'map':{}})
        packet={'id':'fred:DGS10','n':1,'rows':[['2000-01-01',5]],'unit':'percent'}
        m._fetch_series=Mock(return_value=packet)
        got=m.r_tvsym('TVC:US10Y','TVC:US10Y',None)
        self.assertEqual(packet['id'],'fred:DGS10')
        self.assertEqual(got['id'],'TVC:US10Y')
        self.assertEqual(got['via'],'fred:DGS10')
        self.assertEqual(got['routing_evidence']['resolved_id'],'fred:DGS10')
        self.assertFalse(got['routing_evidence']['equivalence_verified'])
        m._tv_pull.assert_not_called()

    def test_legacy_bank_never_claims_upstream_provenance(self):
        fixture=test_warehouse_routing.WarehouseTests();m=fixture.module();self.addCleanup(fixture.doCleanups)
        got=m._bars_result('NASDAQ:AAPL','tv',{'bars':[[946684800,1,2,.5,1.5,None]]},'AAPL','warehouse')
        self.assertEqual(got['n'],1)
        q=got['market_history_quality'];self.assertEqual(q['status'],'legacy_bank_unqualified')
        for key in ('provider_identity_verified','full_history_verified','raw_upstream_replay_verified','point_in_time_verified'):
            self.assertFalse(q[key])
