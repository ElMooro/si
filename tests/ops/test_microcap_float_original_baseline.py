"""Reproduce original float/short-sale mistakes using extracted functions only."""
from pathlib import Path
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6255_microcap_float_original_baseline as op
FIXTURE=ROOT/'tests/fixtures/pre-microcap-float-observations.py.txt'


def prices():return [{'date':'2027-01-01','close':10,'volume':3_000_000,'symbol':'OTHER','currency':'JPY'} for _ in range(30)]
def shorts():return [{'date':'2027-01-01','short_pct':75,'short_vol':6_000_000,'total_vol':8_000_000} for _ in range(5)]
def scope(body='',history=None,quote=None):
    ns={'json':json,'FMP_KEY':'synthetic','fetch_url':lambda *a,**kw:body,'fetch_history':lambda *a,**kw:history if history is not None else prices(),'fetch_quote':lambda *a,**kw:quote}
    return ns
def extracted(name,ns):
    tree=ast.parse(FIXTURE.read_bytes());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated original short-sale functions>','exec'),ns);return ns[name]
def evaluate(finra=None,stock=None,history=None,quote=None):
    return extracted('evaluate_ticker',scope(history=history,quote=quote))(stock or {'symbol':'TEST','market_cap':500_000_000,'price':10}, {'TEST':shorts() if finra is None else finra})


class Tests(unittest.TestCase):
    def test_original_source_pinned_without_native_import(self):
        result=op.source_check('justhodl-microcap-float-squeeze',FIXTURE.read_bytes());self.assertFalse(result['imported_or_executed'])
        with self.assertRaises(ValueError):op.source_check('justhodl-microcap-float-squeeze',FIXTURE.read_bytes()+b'\n')
        self.assertNotIn('lambda_function',sys.modules)
    def test_invented_float_and_duplicate_future_sources_receive_top_tier(self):
        out=evaluate();self.assertEqual(out['metrics']['shares_outstanding'],50_000_000);self.assertEqual(out['metrics']['float_shares'],40_000_000)
        self.assertEqual(out['metrics']['float_turnover_30d_pct'],7.5);self.assertEqual(out['tier'],'TIER_S_PARABOLIC_SETUP')
        self.assertEqual({p['date'] for p in prices()},{'2027-01-01'})
    def test_daily_short_sale_flow_manufactures_days_to_cover_without_positions(self):
        out=evaluate();self.assertEqual(out['metrics']['days_to_cover_proxy'],10)
        values=shorts()
        for row in values:row.update(short_vol=60_000_000,total_vol=80_000_000)
        out=evaluate(values);self.assertEqual(out['metrics']['days_to_cover_proxy'],100);self.assertIn('DAYS_TO_COVER_10+',out['flags'])
    def test_missing_older_volume_and_return_windows_become_zero(self):
        out=evaluate()['metrics'];self.assertEqual(out['short_pct_change'],0);self.assertEqual(out['ret_30d'],0);self.assertEqual(out['ret_60d'],0)
    def test_wrong_quote_issuer_currency_and_absent_revenue_do_not_prevent_tier(self):
        out=evaluate(stock={'symbol':'TEST'},quote={'symbol':'OTHER','currency':'JPY','price':10,'marketCap':500_000_000})
        self.assertEqual(out['symbol'],'TEST');self.assertEqual(out['tier'],'TIER_S_PARABOLIC_SETUP')
    def test_price_projection_discards_records_and_coerces_missing_volume_boolean_price(self):
        values=[{'date':'2027-01-01','close':True,'volume':None,'unknown':'lost'} for _ in range(95)]
        out=extracted('fetch_history',scope(json.dumps(values)))('TEST')
        self.assertEqual(len(out),90);self.assertEqual(out[0]['close'],1);self.assertEqual(out[0]['volume'],0);self.assertNotIn('unknown',out[0])
    def test_finra_parser_ignores_date_and_impossible_counts_and_overwrites_duplicates(self):
        text='Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20270927|TEST|10|1|100|Q\n20270928|TEST|200|2|100|N\n'
        out=extracted('fetch_finra_short_volume',scope(text))('20260925')
        self.assertEqual(len(out),1);self.assertEqual(out['TEST']['short_pct'],200);self.assertNotIn('date',out['TEST']);self.assertNotIn('short_exempt_volume',out['TEST'])


if __name__=='__main__':unittest.main(verbosity=2)
