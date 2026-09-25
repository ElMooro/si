from pathlib import Path
from datetime import date
from io import BytesIO
from unittest.mock import Mock
import sys,time,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6034_offexchange_source_preflight as op
from test_option_flow_store import S3
class Tests(unittest.TestCase):
    def test_reviewed_request_inventory_has_explicit_dates_and_no_coverage_claim(self):
        rows=op.plan(date(2026,9,25),'2026-09-24');self.assertEqual(len(rows),8)
        self.assertEqual(rows['daily:CNMS']['url'],'https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260924.txt')
        for row in rows.values():
            if row['body']:self.assertEqual(row['body']['offset'],0);self.assertEqual(row['body']['limit'],2000)
        for day in ('2026-09-25','2026-08-01','invalid'):
            with self.assertRaises(ValueError):op.plan(date(2026,9,25),day)
    def test_first_page_can_never_claim_complete_market_coverage(self):
        rows=[{'summaryTypeCode':'ATS_W_SMBL','weekStartDate':'2026-08-31','tierIdentifier':'T1','issueSymbolIdentifier':'AAPL','totalWeeklyShareQuantity':0}]*2
        result=op.describe(op.encoded(rows),'probe',{'record-total':'20000'})
        self.assertFalse(result['coverage_complete']);self.assertEqual(result['candidate_key_duplicates'],1);self.assertEqual(result['reported_total'],'20000')
        self.assertEqual(result['boundary_rows'][0]['totalWeeklyShareQuantity'],0)
    def test_complete_cnms_file_retains_date_fields_rows_and_trailer(self):
        raw=b'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20260924|AAPL|1|0|3|B,Q,N\n1\n'
        result=op.describe(raw,'daily_file',{});self.assertEqual(result['rows'],1);self.assertEqual(result['dates'],['20260924']);self.assertEqual(result['trailer'],'1')
    def test_whole_response_and_only_reviewed_headers_are_retained(self):
        db=S3({});response=BytesIO(b'[]');response.status=200;response.headers={'Record-Total':'12000','Set-Cookie':'not-retained'}
        opener=Mock();opener.open.return_value=response;spec=op.plan(date(2026,9,25),'2026-09-24')['probe:ATS_W_SMBL']
        result=op.fetch(db,spec,opener,time.monotonic()+10);self.assertEqual(op.original(db,result['original']),b'[]');self.assertEqual(result['headers'],{'record-total':'12000'})
        self.assertFalse(result['inventory']['coverage_complete']);self.assertEqual(opener.open.call_args.args[0].get_method(),'POST')
    def test_provider_error_body_is_preserved_and_not_promoted(self):
        db=S3({});opener=Mock();opener.open.side_effect=urllib.error.HTTPError('https://api.finra.org',503,'Unavailable',{},BytesIO(b'{"error":"unavailable"}'))
        spec=op.plan(date(2026,9,25),'2026-09-24')['probe:ATS_W_SMBL'];out=op.fetch(db,spec,opener,time.monotonic()+10)
        self.assertEqual(out['status'],'provider_error_retained');self.assertEqual(out['http_status'],503);self.assertFalse(out['inventory']['coverage_complete']);self.assertEqual(len(db.writes),1)
    def test_transport_failure_has_no_fabricated_empty_original_or_retry(self):
        db=S3({});opener=Mock();opener.open.side_effect=TimeoutError();spec=op.plan(date(2026,9,25),'2026-09-24')['daily:CNMS'];out=op.fetch(db,spec,opener,time.monotonic()+10)
        self.assertIsNone(out['original']);self.assertEqual(out['status'],'transport_unavailable');self.assertEqual(opener.open.call_count,1);self.assertEqual(db.writes,[])
    def test_unreviewed_destinations_and_private_account_reads_are_denied(self):
        db=Mock();opener=Mock()
        with self.assertRaises(ValueError):op.get(db,'data/trade-tickets.json')
        with self.assertRaises(ValueError):op.fetch(db,{'url':'https://evil.test/source','body':None,'kind':'metadata'},opener,time.monotonic()+10)
        db.get_object.assert_not_called();opener.open.assert_not_called()
    def test_ambiguous_json_and_truncated_byte_reads_are_rejected(self):
        for raw in (b'{"x":1,"x":2}',b'{"value":NaN}'):
            with self.assertRaises(ValueError):op.strict(raw)
        with self.assertRaises(ValueError):op.bounded(BytesIO(b'12345'),4)
if __name__=='__main__':unittest.main(verbosity=2)
