from pathlib import Path
from unittest.mock import Mock
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6046_short_volume_baseline as op
from test_option_flow_store import S3

class Tests(unittest.TestCase):
    def test_history_reports_duplicates_order_and_invalid_dates_without_qualification(self):
        rows=[{'date':'2026-09-24','ts':3,'svr':0},{'date':'2026-09-23','ts':2},
              {'date':'2026-09-24','ts':4},{'date':'broken','ts':5}]
        out=op.history_inventory({'tickers':{'AAPL':rows}})
        self.assertEqual(out['rows'],4);self.assertEqual(out['symbols'],1)
        self.assertEqual(out['date_counts'],{'2026-09-23':1,'2026-09-24':2})
        self.assertEqual(out['field_counts'],{'date':4,'ts':4,'svr':1})
        self.assertEqual(out['detected_errors'],{'duplicate_symbol_dates':1,'invalid_dates':1,'series_not_date_ordered':1})
        self.assertFalse(out['provider_originals_verified']);self.assertFalse(out['security_identity_continuity_verified'])
    def test_public_packet_is_inventoried_without_promoting_legacy_scores(self):
        out=op.packet_inventory({'tickers':{'AAPL':{'squeeze_score':90}},'names':[{'ticker':'AAPL'}],'squeeze_candidates':[{'symbol':'AAPL'}],'version':'1.1.0'})
        self.assertEqual(out['tickers_count'],1);self.assertFalse(out['research_qualified']);self.assertNotIn('squeeze_score',out)
        self.assertEqual(out['names_count'],1)
    def test_whole_predecessor_identity_and_protected_write(self):
        db=S3({});raw=b'{"version":"legacy","missing":null,"zero":0}'
        ref=op.protect(db,raw);self.assertEqual(op.checked(db,ref),raw)
        wrong=dict(ref,bytes=ref['bytes']-1)
        with self.assertRaises(ValueError):op.checked(db,wrong)
    def test_unreviewed_reads_are_rejected_before_storage_access(self):
        db=Mock()
        for key in ('data/trade-tickets.json','data/portfolio.json','audit-private/other.bin'):
            with self.assertRaises(ValueError):op.read(db,key)
        db.get_object.assert_not_called()
    def test_incomplete_shapes_are_rejected_and_zero_history_is_explicit(self):
        with self.assertRaises(ValueError):op.history_inventory({'tickers':[]})
        self.assertEqual(op.history_inventory({'tickers':{}})['rows'],0)
        with self.assertRaises(ValueError):op.packet_inventory([])

if __name__=='__main__':unittest.main(verbosity=2)
