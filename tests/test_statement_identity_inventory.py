from pathlib import Path
import json,sys,unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/p) for p in ('aws/shared','aws/ops/checks')]
import statement_identity_inventory as inventory
from test_statement_research_model import Fixture


class Tests(unittest.TestCase):
    def test_complete_index_preserves_multiple_classes_and_ambiguous_tickers(self):
        rows={str(i):{'cik_str':i+1,'ticker':'TEST'+str(i),'title':'Reported entity '+str(i)} for i in range(1000)}
        rows['1'].update(ticker='TEST0');rows['2'].update(cik_str=1)
        result=inventory.ticker_index(json.dumps(rows).encode())
        self.assertEqual(len(result['rows']),1000);self.assertEqual(result['by_ticker']['TEST0'],['0000000001','0000000002'])
        self.assertEqual(result['by_ticker']['TEST2'],['0000000001']);self.assertFalse(result['historical_security_continuity_verified'])
        with self.assertRaises(ValueError):inventory.ticker_index(b'{}')
        rows['0']['cik_str']=True
        with self.assertRaises(ValueError):inventory.ticker_index(json.dumps(rows).encode())

    def test_wrong_cik_is_not_relabelled_and_bad_clock_metadata_remains_inspectable(self):
        f=Fixture();cap=next(iter(f.capsules.values()));rows=json.loads(f.files[cap['original']['key']])
        rows[0].update(cik='0000000999',acceptedDate='2020-01-01 12:00:00')
        result=inventory.compare(cap,rows,{'by_ticker':{'ABC':['0000000123']}})[0]
        self.assertEqual(result['reported_identity']['cik'],'0000000999')
        self.assertEqual(result['current_identity_status'],'provider_cik_differs_from_current_sec_index')
        self.assertEqual(result['statement_identity_problem'],'filing_precedes_period_end')
        self.assertEqual(result['clock_issues'],['acceptedDate_precedes_period_end'])
        self.assertEqual(result['reported_identity']['acceptedDate'],'2020-01-01 12:00:00')
        self.assertEqual(result['source_row'],0)

    def test_absent_or_ambiguous_index_does_not_manufacture_a_match(self):
        f=Fixture();cap=next(iter(f.capsules.values()));rows=json.loads(f.files[cap['original']['key']])
        for mapping,status in (({},'not_in_current_sec_ticker_index'),({'ABC':['0000000123','0000000009']},'ambiguous_current_sec_ticker_index'),({'ABC':['0000000123']},'current_ticker_cik_pair_corroborated')):
            result=inventory.compare(cap,rows,{'by_ticker':mapping})[0];self.assertEqual(result['current_identity_status'],status)
            self.assertFalse(result['historical_security_continuity_verified'])


if __name__=='__main__':unittest.main(verbosity=2)
