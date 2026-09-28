"""Read-only preliminary tests: extracted original functions, synthetic providers."""
from pathlib import Path
from collections import defaultdict
from datetime import datetime,timezone,timedelta
import ast,time,unittest,json,hashlib
ROOT=Path(__file__).resolve().parents[2]
import sys
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6257_options_flow_original_baseline as op
SOURCE=ROOT/'tests/fixtures/pre-options-flow-observations.py.txt'
def extracted(name,ns):
    fn=next(n for n in ast.parse(SOURCE.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated original options function>','exec'),ns);return ns[name]
def contracts():return [{'ticker':'O:OTHER_C','contract_type':'call','strike_price':100,'underlying_ticker':'OTHER','expiration_date':'2030-01-01'},{'ticker':'O:OTHER_P','contract_type':'put','strike_price':100,'underlying_ticker':'OTHER','expiration_date':'2030-01-01'}]
def bars(call=True):
    return [{'t':int((datetime(2027,1,1,tzinfo=timezone.utc)+timedelta(days=i)).timestamp()*1000),'v':1 if i<5 else 100,'unknown':'retained?'} for i in range(10)] if call else [{'t':r['t'],'v':10} for r in bars()[:5]]
def evaluate(chain=None,history=None):
    ns={'time':time,'DAYS_BACK':20,'defaultdict':defaultdict,'get_spot_price':lambda *a:100,'get_contracts':lambda *a:contracts() if chain is None else chain,'get_contract_volume_history':lambda t,*a:bars(t.endswith('_C'))}
    return extracted('evaluate_ticker',ns)('TEST',history or {})
class Tests(unittest.TestCase):
    def test_whole_original_is_pinned_without_native_import(self):
        self.assertFalse(op.source_check('justhodl-options-flow-scanner',SOURCE.read_bytes())['imported_or_executed'])
        with self.assertRaises(ValueError):op.source_check('justhodl-options-flow-scanner',SOURCE.read_bytes()+b'\n')
        self.assertNotIn('lambda_function',sys.modules)

    def test_consumer_sources_are_pinned_but_their_outputs_are_out_of_scope(self):
        self.assertEqual(op.KEYS,('data/options-flow-scanner.json',))
        for name in ('best-ideas','flow-confluence','options-confluence'):
            raw=(ROOT/'tests/fixtures'/('pre-options-flow-consumer-'+name+'.py.txt')).read_bytes()
            self.assertFalse(op.source_check('justhodl-'+name,raw)['imported_or_executed'])
            self.assertIsNone(op.SOURCES['justhodl-'+name][1])
        class Denied:
            def get_object(self,**kw):raise AssertionError('No consumer read permitted')
        with self.assertRaises(ValueError):op.capture(Denied(),'data/best-ideas.json')
    def test_reference_pagination_and_documented_expiry_bounds_are_ignored(self):
        calls=[];ns={'time':time,'POLY_KEY':'synthetic','_http_get_json':lambda url,**kw:(calls.append(url) or {'results':contracts(),'next_url':'https://api.polygon.io/v3/reference/options/contracts?cursor=next'})}
        out=extracted('get_contracts',ns)('TEST',100,14,90);self.assertEqual(out,contracts());self.assertEqual(len(calls),1)
        self.assertNotIn('expiration_date.lte',calls[0]);self.assertIn('expiration_date.gte='+time.strftime('%Y-%m-%d'),calls[0])
    def test_future_wrong_underlying_and_missing_put_sessions_become_bullish(self):
        out=evaluate();self.assertEqual(out['tier'],'TIER_A_BULLISH_FLOW');self.assertEqual(out['metrics']['avg_cpr_recent_5d'],100)
        self.assertEqual(out['metrics']['total_put_vol_5d'],0);self.assertEqual(out['score'],70)
    def test_daily_short_sale_percent_changes_are_called_short_covering(self):
        hist={'TEST':[{'date':'2027-01-01','short_pct':80 if i<5 else 40,'total_vol':100} for i in range(10)]}
        out=evaluate(history=hist);self.assertIn('SHORTS_COVERING',out['flags']);self.assertEqual(out['score'],85)
    def test_missing_put_and_duplicate_contracts_are_not_unknown_population(self):
        out=evaluate(chain=[contracts()[0]]*2);self.assertEqual(out['metrics']['n_call_contracts'],2)
        self.assertEqual(out['metrics']['total_call_vol_5d'],1000);self.assertEqual(out['metrics']['total_put_vol_5d'],0)
    def test_three_rows_become_overlapping_five_day_windows(self):
        ns={'time':time,'DAYS_BACK':20,'defaultdict':defaultdict,'get_spot_price':lambda *a:100,'get_contracts':lambda *a:[contracts()[0]],'get_contract_volume_history':lambda *a:bars()[:3]}
        out=extracted('evaluate_ticker',ns)('TEST',{});self.assertEqual(out['metrics']['total_call_vol_5d'],3)
        self.assertEqual(out['metrics']['cpr_change_pct'],0);self.assertEqual(out['metrics']['avg_cpr_recent_5d'],out['metrics']['avg_cpr_older'])
    def test_quote_issuer_currency_boolean_price_and_failed_response_are_not_distinguished(self):
        ns={'managed_secret':lambda *a:'synthetic','_http_get_json':lambda *a,**kw:[{'symbol':'OTHER','currency':'JPY','price':True}]}
        self.assertEqual(extracted('get_spot_price',ns)('TEST'),1)
        ns={'time':time,'POLY_KEY':'synthetic','_http_get_json':lambda *a,**kw:{'status':'NOT_AUTHORIZED','error':'denied'}}
        self.assertEqual(extracted('get_contracts',ns)('TEST',100),[])
    def test_finra_wrong_date_impossible_counts_and_duplicate_symbols_survive(self):
        raw='Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20270927|TEST|10|1|100|Q\n20270928|TEST|200|2|100|N\n'
        out=extracted('fetch_finra_short_volume',{'_http_get_text':lambda *a,**kw:raw})('20260925')
        self.assertEqual(out['TEST']['short_pct'],200);self.assertNotIn('date',out['TEST'])
if __name__=='__main__':unittest.main(verbosity=2)
