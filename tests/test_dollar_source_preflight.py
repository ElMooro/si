from pathlib import Path
from unittest.mock import Mock
import io,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6020_dollar_source_preflight as op
from test_futures_research_store import Memory

class Tests(unittest.TestCase):
    def test_complete_bytes_including_unknown_predecessor_fields_survive(self):
        s=Memory();raw=b'{ "future_field":[null,0], "regime":"OLD" }\n';s.data[op.PACKETS[0]]=raw
        ref,doc=op.capture(s,op.PACKETS[0]);self.assertEqual(s.data[ref['original']['key']],raw)
        self.assertEqual(doc['future_field'],[None,0]);self.assertEqual(ref['original']['bytes'],len(raw))
    def test_private_unreviewed_or_foreign_packet_refused_before_read(self):
        s=Mock()
        for key in ('data/trade-tickets.json','portfolio/current.json','https://example.com/data/report-measurements.json'):
            with self.assertRaises(ValueError):op.capture(s,key)
        s.get_object.assert_not_called()
    def test_canonical_read_allowlist_excludes_private_state_and_foreign_paths(self):
        self.assertTrue(op.canonical_key('data/evidence/fred/'+'a'*64+'/'+'b'*64+'.bin.gz'))
        self.assertTrue(op.canonical_key('data/report-research/runs/'+'c'*64+'.json'))
        for key in ('data/accounts.json','data/report-research/runs/../current.json','data/evidence/polygon/'+'a'*64+'.gz'):
            self.assertFalse(op.canonical_key(key))
    def test_generation_does_not_become_observation_or_replay_proof(self):
        result=op.describe({'generated_at':'2026-09-21T00:00:00Z','regime':'PUMP'})
        self.assertIsNone(result['as_of']);self.assertFalse(result['original_provider_replay_performed_by_inventory'])
        self.assertIsNone(result['reported_authority']['forecast_qualified'])
    def test_absent_series_is_missing_not_zero(self):
        self.assertEqual(op.inventory({'measurements':{}},{'DEXUSAL':None}),{'DEXUSAL':{'status':'absent'}})
    def test_corrupted_retention_readback_is_rejected(self):
        s=Mock();s.get_object.return_value={'Body':io.BytesIO(b'changed')}
        with self.assertRaises(ValueError):op.retained(s,b'whole original')

if __name__=='__main__':unittest.main(verbosity=2)
