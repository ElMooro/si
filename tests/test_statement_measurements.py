from pathlib import Path
from copy import deepcopy
from decimal import Decimal
from fractions import Fraction
import sys, unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'aws/shared'))
import statement_measurements as model


def bundle():
    common={'symbol':'ABC','cik':'0000000123','reportedCurrency':'EUR','date':'2025-12-31','fiscalYear':'2025',
        'period':'FY','filingDate':'2026-02-15','acceptedDate':'2026-02-15 12:00:00'}
    values={model.I:{'revenue':100,'grossProfit':40,'costOfRevenue':60,'operatingIncome':12,'netIncome':10,
        'sellingGeneralAndAdministrativeExpenses':0,'generalAndAdministrativeExpenses':99},
        model.B:{'totalAssets':200,'totalLiabilities':150,'totalEquity':50,'totalDebt':30,'cashAndCashEquivalents':10,
            'totalCurrentAssets':80,'totalCurrentLiabilities':40,'netReceivables':20,'goodwill':0},
        model.C:{'operatingCashFlow':15,'netCashProvidedByOperatingActivities':15,'freeCashFlow':11}}
    return {key:{'values':{**common,**value},'source_id':str(i+1)*64,'source_row':0} for i,(key,value) in enumerate(values.items())}


class Tests(unittest.TestCase):
    def test_all_metrics_retain_exact_field_origins_and_never_add_forecast_or_grade(self):
        value=model.compute(bundle());metrics=value['metrics']
        expected={'gross_margin_pct':'40.000000000000','net_margin_pct':'10.000000000000','cash_conversion_multiple':'1.500000000000',
            'net_debt_derived':'20.000000000000','earnings_cash_gap_to_assets_pct':'-2.500000000000',
            'balance_identity_residual':'0.000000000000','gross_profit_residual':'0.000000000000',
            'sga_to_revenue_pct':'0.000000000000','goodwill_to_assets_pct':'0.000000000000'}
        for key,number in expected.items():self.assertEqual(metrics[key]['value'],number,key)
        self.assertEqual(metrics['net_debt_derived']['unit'],'EUR')
        self.assertEqual(metrics['sga_to_revenue_pct']['inputs'][0]['field'],'sellingGeneralAndAdministrativeExpenses')
        for metric in metrics.values():
            for field in metric['inputs']:
                self.assertEqual(field['source_row'],0);self.assertEqual(len(field['source_id']),64)
            self.assertFalse(metric['supports_investment_action'])
        self.assertIsNone(value['m_score']);self.assertIsNone(value['grade']);self.assertEqual(value['independent_investment_votes'],0)

    def test_missing_debt_or_cash_cannot_become_zero_or_strength_points(self):
        for field in ('totalDebt','cashAndCashEquivalents'):
            b=bundle();del b[model.B]['values'][field]
            result=model.compute(b)['metrics']['net_debt_derived']
            self.assertIsNone(result['value']);self.assertEqual(result['status'],'required_provider_field_missing')
        b=bundle();del b[model.I]['values']['sellingGeneralAndAdministrativeExpenses']
        self.assertIsNone(model.compute(b)['metrics']['sga_to_revenue_pct']['value'])

    def test_missing_statement_only_withholds_metrics_that_require_it(self):
        b=bundle();del b[model.C]
        value=model.compute(b)
        self.assertFalse(value['complete_three_statement_bundle'])
        self.assertEqual(value['metrics']['gross_margin_pct']['value'],'40.000000000000')
        self.assertEqual(value['metrics']['net_debt_derived']['value'],'20.000000000000')
        self.assertEqual(value['metrics']['cash_conversion_multiple']['status'],'required_statement_record_missing')
        self.assertIsNone(value['metrics']['cash_conversion_multiple']['value'])

    def test_every_identity_dimension_including_currency_and_filing_vintage_must_match(self):
        changes={'symbol':'XYZ','cik':'0000000321','reportedCurrency':'USD','date':'2025-12-30','fiscalYear':'2024',
            'period':'Q4','filingDate':'2026-02-14','acceptedDate':'2026-02-15 13:00:00'}
        for key,change in changes.items():
            b=bundle();b[model.C]['values'][key]=change;value=model.compute(b)
            self.assertEqual(value['alignment_status'],'statement_identity_or_filing_vintage_mismatch',key)
            self.assertTrue(all(metric['value'] is None for metric in value['metrics'].values()))

    def test_nonpositive_denominators_do_not_create_returns_or_performance(self):
        for v in (0,-1):
            b=bundle();b[model.I]['values']['netIncome']=v
            metric=model.compute(b)['metrics']['cash_conversion_multiple']
            self.assertIsNone(metric['value']);self.assertEqual(metric['status'],'nonpositive_denominator')
        b=bundle();b[model.C]['values']['operatingCashFlow']=0
        self.assertEqual(model.compute(b)['metrics']['operating_cash_margin_pct']['value'],'0.000000000000')

    def test_exact_fraction_agreement_rounding_and_large_dynamic_range(self):
        b=bundle();b[model.I]['values']['revenue']=12345678901234567;b[model.I]['values']['grossProfit']=1000000000000001
        metric=model.compute(b)['metrics']['gross_margin_pct'];frac=Fraction(1000000000000001*100,12345678901234567)
        # Independent integer half-even rounding; no production Decimal routine.
        whole,rem=divmod(frac.numerator*10**12,frac.denominator)
        if 2*rem>frac.denominator or (2*rem==frac.denominator and whole%2):whole+=1
        expected=str(whole//10**12)+'.'+str(whole%10**12).zfill(12)
        self.assertEqual(metric['value'],expected)
        for numerator,expected in ((1,'0.000000000000'),(3,'0.000000000002')):
            b=bundle();b[model.B]['values'].update(totalCurrentAssets=numerator,totalCurrentLiabilities=2*10**12)
            self.assertEqual(model.compute(b)['metrics']['current_ratio']['value'],expected)
        b=bundle();b[model.B]['values'].update(totalCurrentAssets=Decimal('1e100'),totalCurrentLiabilities=Decimal('1e-100'))
        self.assertEqual(Decimal(model.compute(b)['metrics']['current_ratio']['value']),Decimal('1e200'))

    def test_malformed_dates_sources_and_nonfinite_numbers_never_qualify(self):
        for key,change in (('date','2025-02-30'),('filingDate','2020-01-01'),('acceptedDate','no date'),('acceptedDate','2026-02-15'),('reportedCurrency',''),('cik','0000000000')):
            b=bundle();b[model.I]['values'][key]=change
            self.assertTrue(all(m['value'] is None for m in model.compute(b)['metrics'].values()))
        for change in (True,Decimal('NaN'),Decimal('Infinity'),'5',5.0):
            b=bundle();b[model.I]['values']['revenue']=change
            self.assertIsNone(model.compute(b)['metrics']['gross_margin_pct']['value'])
        b=bundle();b[model.I]['source_row']=-1
        self.assertEqual(model.compute(b)['alignment_status'],'exact_source_row_required')
        self.assertTrue(all(m['value'] is None for m in model.compute({model.I:[]})['metrics'].values()))
        self.assertTrue(all(m['value'] is None for m in model.compute({model.I:{'values':[]}})['metrics'].values()))
        with self.assertRaises(ValueError):model.compute(None)


if __name__=='__main__':unittest.main(verbosity=2)
