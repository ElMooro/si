from pathlib import Path
from unittest.mock import Mock
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged')]
import ops_6005_fx_original_source_preflight as op
from test_option_flow_store import S3


class Tests(unittest.TestCase):
    def test_original_retention_and_readback_use_whole_bytes(self):
        raw=b'{"close":1.123456789012345678901,"missing":null,"zero":0}'
        s3=S3();ref=op.retain(s3,raw)
        self.assertEqual(op.checked(s3,ref),raw)
        self.assertEqual(s3.data[ref['key']],raw);self.assertEqual(s3.writes[0]['IfNoneMatch'],'*')
        self.assertEqual(s3.writes[0]['CacheControl'],'no-store')
    def test_corrupt_immutable_original_is_not_overwritten(self):
        raw=b'{"value":1}';s3=S3();ref=op.retain(s3,raw);s3.data[ref['key']]=b'{"value":2}'
        with self.assertRaises(AssertionError):op.retain(s3,raw)
        self.assertEqual(s3.data[ref['key']],b'{"value":2}')
    def test_foreign_or_malformed_original_reference_never_reads(self):
        s3=Mock()
        for ref in ({'key':'data/trade-tickets.json','sha256':'a'*64,'bytes':1},
                    {'key':op.PRIVATE+'../private.bin','sha256':'../private','bytes':1},
                    {'key':op.PRIVATE+'a'*64+'.bin','sha256':'a'*64,'bytes':True}):
            with self.assertRaises(AssertionError):op.checked(s3,ref)
        s3.get_object.assert_not_called()
    def test_snapshot_refuses_account_or_alert_state_reads(self):
        s3=Mock()
        for key in ('data/trade-tickets.json','data/_alerts/prepump-router-state.json','data/predictions-snapshots/latest.json'):
            with self.assertRaises(AssertionError):op.snapshot(s3,key)
        s3.get_object.assert_not_called()
    def test_duplicate_status_claim_is_not_overwritten(self):
        s3=S3();doc={'status':'claimed'}
        op.write_status(s3,op.STATUS,doc,IfNoneMatch='*')
        with self.assertRaises(Exception):op.write_status(s3,op.STATUS,{'status':'repeated'},IfNoneMatch='*')
        self.assertEqual(json.loads(s3.data[op.STATUS]),doc)
    def test_summaries_reconcile_all_identities_without_return_inference(self):
        s3=S3();sources={}
        for pair,ticker in op.capture.PAIRS.items():
            raw=json.dumps({'ticker':ticker,'status':'OK','resultsCount':2,'results':[{'t':1789689600000,'c':0},{'t':1789776000000,'c':None}]}).encode()
            sources[pair]={'stop':'complete_returned_pagination','pagination_complete':True,'pages':[{'http_status':200,'original':op.retain(s3,raw)}]}
        rows=op.summaries(s3,sources);self.assertEqual(len(rows),19)
        for row in rows.values():
            self.assertEqual(row['close_states'],{'zero':1,'null':1});self.assertTrue(row['no_return_or_regime_computed'])
            self.assertEqual(row['returned_rows'],2)
        sources.pop('EUR_USD')
        with self.assertRaises(AssertionError):op.summaries(s3,sources)


if __name__=='__main__':unittest.main(verbosity=2)
