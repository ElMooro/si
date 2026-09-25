from pathlib import Path
from io import BytesIO
from unittest.mock import Mock
import sys,time,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6048_short_volume_original_history as op
from test_option_flow_store import S3
class Tests(unittest.TestCase):
    def test_fractional_daily_volumes_and_zero_are_independently_reconciled(self):
        raw=b'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20260924|AAPL|1.25|0.25|2.5|B,Q,N\n20260924|ZERO|0|0|0|N\n2\n'
        out=op.audit_daily(raw,'2026-09-24');self.assertEqual(out['rows'],2);self.assertEqual(out['zero_volume_rows'],1)
        self.assertEqual(out['exact_share_sums_as_rationals'],{'short_including_exempt':'5/4','short_exempt_subset':'1/4','reported_total':'5/2'})
        self.assertFalse(out['short_interest_available']);self.assertFalse(out['direction_inferred'])
    def test_empty_provider_response_is_retained_without_becoming_market_zero(self):
        db=S3({});response=BytesIO(b'');response.status=204;response.headers={};transport=Mock(return_value=response)
        result=op.fetch(db,'empty',op.index.URL,time.monotonic()+10,transport)
        self.assertEqual(result['original']['bytes'],0);self.assertEqual(op.original(db,result['original']),b'')
        with self.assertRaises(ValueError):op.successful(db,result)
        with self.assertRaises(Exception):op.fetch(db,'empty',op.index.URL,time.monotonic()+10,transport)
        self.assertEqual(transport.call_count,1)
    def test_unreviewed_url_and_credentials_are_rejected_before_any_write(self):
        db=Mock();transport=Mock()
        for url in ('https://evil.test/file',op.index.URL+'?apiKey=secret',op.index.URL+'?custom_month%5Bmonth%5D=09&custom_year%5Byear%5D=0&custom_year%5Byear%5D=1'):
            with self.assertRaises(ValueError):op.fetch(db,'bad',url,time.monotonic()+10,transport)
        db.put_object.assert_not_called();transport.assert_not_called()
    def test_transport_failure_is_journalled_and_never_retried(self):
        db=S3({});transport=Mock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):op.fetch(db,'timeout',op.index.URL,time.monotonic()+10,transport)
        state=op.base.retained.strict(op.base.read(db,op.key('timeout')))
        self.assertEqual(state['status'],'failed');self.assertEqual(transport.call_count,1)
    def test_truncated_response_is_rejected(self):
        with self.assertRaises(ValueError):op.response_bytes(BytesIO(b'12345'),4)
        with self.assertRaises(ValueError):op.audit_daily(b'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n0\n','2026-09-24')
if __name__=='__main__':unittest.main(verbosity=2)
