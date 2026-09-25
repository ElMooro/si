from pathlib import Path
from copy import deepcopy
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import offexchange_measurements as model

class Tests(unittest.TestCase):
    def daily(self,line='20260924|AAPL|1.250001|0.250001|2.500002|B,Q,N',trailer='1'):
        return ('|'.join(model.CNMS_FIELDS)+'\r\n'+line+'\r\n'+trailer+'\r\n').encode()
    def row(self,code='ATS_W_SMBL'):
        return {'summaryTypeCode':code,'weekStartDate':'2026-08-31','summaryStartDate':'2026-08-31','tierIdentifier':'T1',
            'issueSymbolIdentifier':'AAPL','MPID':None,'firmCRDNumber':None,'totalWeeklyShareQuantity':'10.123456',
            'totalWeeklyTradeCount':2,'initialPublishedDate':'2026-09-14','lastUpdateDate':'2026-09-21','lastReportedDate':'2026-09-04'}
    def parsed(self,row):return model.weekly(json.dumps([row]).encode(),row['summaryTypeCode'],row['weekStartDate'],row['tierIdentifier'])[0]
    def test_fractional_shares_are_exact_and_exempt_is_not_added_twice(self):
        row=model.cnms(self.daily(),'2026-09-24')[0]
        self.assertEqual(row['short_volume_shares'],'1.250001');self.assertEqual(row['short_volume_pct'],'50.000000000000')
        self.assertEqual(row['source_line'],2);self.assertEqual(row['source_fields']['ShortExemptVolume'],'0.250001')
    def test_truncation_wrong_date_duplicate_and_bad_scope_are_rejected(self):
        for raw in (self.daily(trailer='2'),self.daily().replace(b'20260924',b'20260923'),self.daily().replace(b'B,Q,N',b'B,F'),self.daily(line='20260924|AAPL|2|3|4|N'),self.daily(line='20260924|AAPL|5|0|4|N')):
            with self.assertRaises(ValueError):model.cnms(raw,'2026-09-24')
        line='20260924|AAPL|1|0|2|N'
        with self.assertRaises(ValueError):model.cnms(self.daily(line=line+'\n'+line,trailer='2'),'2026-09-24')
    def test_zero_is_preserved_with_no_division_or_direction(self):
        row=model.cnms(self.daily(line='20260924|AAPL|0|0|0|N'),'2026-09-24')[0]
        self.assertIsNone(row['short_volume_pct']);self.assertEqual(row['missing_reason'],'zero_reported_volume')
    def test_invalid_exact_numbers_are_rejected(self):
        for v in (True,None,0.1,'NaN','-1','1e4',' 1','1.1234567890123'):
            with self.assertRaises(ValueError):model.scalar(v)
    def test_json_decimals_preserve_precision_and_duplicate_keys_fail(self):
        self.assertEqual(str(model.strict(b'{"n":0.123456789012}')['n']),'0.123456789012')
        for raw in (b'{"n":1,"n":2}',b'{"n":NaN}'):
            with self.assertRaises(ValueError):model.strict(raw)
    def test_weekly_grain_dates_and_mean_are_explicit(self):
        row=self.parsed(self.row());self.assertEqual(row['average_shares_per_reported_trade'],'5.061728000000')
        self.assertEqual(row['source_row'],0);self.assertEqual(row['initialPublishedDate'],'2026-09-14')
        self.assertNotIn('venue_fingerprint',row)
    def test_wrong_partition_firm_summary_and_fractional_trade_count_fail(self):
        for key,value in [('summaryStartDate','2026-08-24'),('MPID','ABCD'),('firmCRDNumber',42),('totalWeeklyTradeCount','2.5'),('initialPublishedDate','2026-01-01')]:
            row=self.row();row[key]=value
            with self.assertRaises(ValueError):model.weekly(json.dumps([row]).encode(),'ATS_W_SMBL','2026-08-31','T1')
    def test_missing_leg_is_null_and_never_joined_across_weeks(self):
        a=self.parsed(self.row());b=deepcopy(a);b.update(leg='non_ats',week_start='2026-08-24')
        out=model.join_weekly([a,b]);self.assertEqual(len(out),2)
        for row in out:self.assertIsNone(row['reported_offexchange_shares']);self.assertIsNone(row['market_share_pct']);self.assertIsNone(row['signal'])
    def test_same_period_legs_reconcile_without_becoming_total_market_share(self):
        a=self.parsed(self.row());b=self.parsed(self.row('OTC_W_SMBL'));row=model.join_weekly([a,b])[0]
        self.assertEqual(row['reported_offexchange_shares'],'20.246912');self.assertEqual(row['ats_pct_of_reported_offexchange'],'50.000000000000');self.assertIsNone(row['market_share_pct'])
        with self.assertRaises(ValueError):model.join_weekly([a,a])
    def test_short_page_advances_by_actual_count_and_does_not_mean_complete(self):
        headers={'record-total':'10','record-offset':'0','record-limit':'5','total-records-on-page':'2'}
        row=model.page(b'[{},{}]',headers,0,5);self.assertEqual(row['next_offset'],2);self.assertFalse(row['reported_end_reached']);self.assertFalse(row['snapshot_atomic'])
    def test_missing_headers_offset_mismatch_and_count_disagreement_fail(self):
        base={'record-total':'10','record-offset':'0','record-limit':'5','total-records-on-page':'2'}
        for headers in ({},dict(base,**{'record-offset':'2'}),dict(base,**{'total-records-on-page':'1'})):
            with self.assertRaises(ValueError):model.page(b'[{},{}]',headers,0,5)
        with self.assertRaises(ValueError):model.page(b'[]',dict(base,**{'total-records-on-page':'0'}),0,5)
    def test_documented_optional_count_header_is_not_required_when_live_api_omits_it(self):
        row=model.page(b'[{},{}]',{'record-total':'2','record-offset':'0','record-limit':'2000'},0,2000)
        self.assertTrue(row['reported_end_reached']);self.assertFalse(row['page_count_header_present']);self.assertEqual(row['rows'],2)
if __name__=='__main__':unittest.main(verbosity=2)
