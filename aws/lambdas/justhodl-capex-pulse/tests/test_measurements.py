from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
from copy import deepcopy
import ast
import json
import sys
import unittest

ROOT=Path(__file__).resolve().parents[4];SOURCE=ROOT/'aws/lambdas/justhodl-capex-pulse/source'
sys.path.insert(0,str(SOURCE))
from capex_measurements import CONTRACT,number,decode,dossier,aggregate,intentions


def cash(symbol='TEST',currency='USD',count=8):
    dates=[('2026-06-30','2026-04-01','Q2'),('2026-03-31','2026-01-01','Q1'),
           ('2025-12-31','2025-10-01','Q4'),('2025-09-30','2025-07-01','Q3'),
           ('2025-06-30','2025-04-01','Q2'),('2025-03-31','2025-01-01','Q1'),
           ('2024-12-31','2024-10-01','Q4'),('2024-09-30','2024-07-01','Q3')]
    return [{'symbol':symbol,'cik':'0000000001','date':end,'startDate':start,'period':period,
             'reportedCurrency':currency,'capitalExpenditure':-200 if i<4 else -100}
            for i,(end,start,period) in enumerate(dates[:count])]


def calculate(rows=None,symbol='TEST'):return dossier(symbol,rows if rows is not None else cash(),{'sec':'Test','mc_b':100},'2026-09-27')


def native(**extra):
    tree=ast.parse((SOURCE/'lambda_function.py').read_bytes())
    wanted={'lambda_handler'}
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in wanted]
    ns={'CONTRACT':CONTRACT,'decode':decode,'number':number,'dossier':dossier,'aggregate':aggregate,
        'datetime':datetime,'timezone':timezone,'json':json,'time':SimpleNamespace(sleep=lambda n:None),
        'N_TOP':160,'HYPERSCALERS':['TEST'],'BUCKET':'fixture-only','OUT':'data/capex-pulse.json',
        'HIST':'data/history/capex-pulse.json',**extra}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated actual Capex>','exec'),ns);return ns


class Tests(unittest.TestCase):
    def test_explicit_periods_currency_and_zero_are_preserved(self):
        rows=cash();rows[0]['capitalExpenditure']=0;p=calculate(rows)
        self.assertEqual(p['reported_window_amount'],600);self.assertEqual(p['yoy_pct'],50)
        self.assertEqual(p['provider_response'],rows);self.assertIsNone(p['intensity_pct'])
        self.assertFalse(p['forecast_qualified']);self.assertFalse(p['annual_comparability_verified'])

    def test_missing_period_currency_and_amount_do_not_become_ttm_or_usd(self):
        for key,value in [('startDate',None),('date','2026-02-30'),('reportedCurrency',None),('capitalExpenditure',False),('capitalExpenditure',None),('capitalExpenditure','0'),('period','FY')]:
            rows=cash();rows[0][key]=value;p=calculate(rows)
            self.assertIsNone(p['capex_ttm_b']);self.assertIsNone(p['yoy_pct'])
            self.assertEqual(p['provider_response'],rows)

    def test_array_order_cannot_change_results_and_duplicate_periods_abstain(self):
        rows=cash();rows.reverse();self.assertEqual(calculate(rows)['yoy_pct'],100)
        rows.append(deepcopy(rows[-1]));p=calculate(rows)
        self.assertIsNone(p['reported_window_amount']);self.assertEqual(len(p['observations']),9)

    def test_wrong_issuer_currency_future_or_noncontiguous_period_abstains(self):
        for field,value in [('cik','2'),('symbol','OTHER'),('reportedCurrency','JPY'),('date','2027-06-30'),('period','Q4')]:
            rows=cash();rows[1][field]=value;self.assertIsNone(calculate(rows)['reported_window_amount'])
        rows=cash();rows[4]['cik']='2';self.assertIsNone(calculate(rows)['yoy_pct'])

    def test_foreign_amounts_remain_local_without_spot_fx_conversion(self):
        p=calculate(cash(currency='JPY'));self.assertEqual(p['reported_window_amount'],800)
        self.assertEqual(p['reported_currency'],'JPY');self.assertEqual(p['yoy_pct'],100)
        self.assertIsNone(p['current_window_usd']);self.assertIsNone(p['capex_ttm_b'])
        result=aggregate([p]);self.assertIsNone(result['capex_ttm_b']);self.assertEqual(result['unmeasured_or_foreign_n'],1)

    def test_partial_prior_never_inflates_matched_cohort(self):
        one=calculate();two=calculate(cash(symbol='SECOND',count=5),'SECOND')
        two['current_window']['cik']='2'
        p=aggregate([one,two]);self.assertEqual(p['comparison_current_usd'],800)
        self.assertEqual(p['comparison_prior_usd'],400);self.assertEqual(p['comparison_n'],1)
        self.assertEqual(p['comparison_missing_n'],1);self.assertEqual(p['yoy_pct'],100)

    def test_distinct_fiscal_windows_and_duplicate_issuer_are_never_one_total(self):
        a=calculate();b=deepcopy(a);b['ticker']='SECOND';b['current_window']['cik']='2'
        b['current_window'].update(start_date='2025-06-01',end_date='2026-05-31')
        p=aggregate([a,b]);self.assertIsNone(p['capex_ttm_b']);self.assertEqual(len(p['cohorts']),2)
        b['current_window']['cik']='1';p=aggregate([a,b]);self.assertIsNone(p['capex_ttm_b'])
        self.assertEqual(p['ambiguous_issuer_n'],2)

    def test_empty_population_and_overflow_cannot_publish_measured_zero(self):
        self.assertIsNone(aggregate([])['capex_ttm_b'])
        rows=cash()
        for row in rows:row['capitalExpenditure']=-1e308
        p=calculate(rows);self.assertIsNone(p['reported_window_amount']);json.dumps(p,allow_nan=False)

    def test_strict_source_decode(self):
        for raw in (b'{"x":0,"x":1}',b'{"x":NaN}',b'{"x":1e-999}',b'{"x":1e999}',b'{"x":"\xff"}'):
            with self.assertRaises(ValueError):decode(raw)

    def test_monthly_survey_exact_calendar_pairs_not_lexical_year_or_skipped_missing(self):
        source={'observations':[{'date':'2026-09-01','value':'20'},{'date':'2026-08-01','value':'10'},
                                {'date':'2026-07-01','value':'0'},{'date':'2025-09-01','value':'5'}]}
        p=intentions(source,'2026-09-27');self.assertEqual(p['avg_3m'],10);self.assertEqual(p['delta_12m'],15)
        self.assertIsNone(p['read']);self.assertEqual(p['delta_unit'],'index_points');self.assertEqual(p['original_response'],source)
        source['observations'][0]['value']='.';p=intentions(source,'2026-09-27')
        self.assertIsNone(p['latest']);self.assertIsNone(p['avg_3m']);self.assertIsNone(p['delta_12m'])

    def handler(self,rows=None,unavailable=False,history=None):
        writes={};calls=[];fx=[];prior=history if history is not None else {str(i):{'whole':i} for i in range(405)}
        def read(key,default=None):
            if key=='data/stock-xray.json':return {'cards':{'TEST':{'mc_b':100,'sec':'Test'}}}
            if key=='data/history/capex-pulse.json':return prior.copy()
            raise AssertionError(key)
        def source(symbol):calls.append(symbol);return None if unavailable else (rows if rows is not None else cash())
        def spot(currency):fx.append(currency);return 0.1,'Unverified fixture'
        ns=native(_j=read,_fmp_cf=source,_usd_per=spot,_fred_intentions=lambda:None,
                  s3=SimpleNamespace(put_object=lambda **k:writes.update({k['Key']:json.loads(k['Body'])})))
        ns['lambda_handler']();return writes,prior,calls,fx

    def test_actual_handler_retains_all_history_and_source_rows_without_size_outlier_dropping(self):
        rows=cash()
        for row in rows:row['capitalExpenditure']=-1e12
        writes,prior,calls,fx=self.handler(rows)
        p=writes['data/capex-pulse.json'];self.assertEqual(p['n'],1);self.assertEqual(p['rows'][0]['provider_response'],rows)
        self.assertEqual(p['excluded_outliers'],[]);self.assertEqual(calls,['TEST']);self.assertEqual(fx,[])
        for key,value in prior.items():self.assertEqual(writes['data/history/capex-pulse.json'][key],value)

    def test_short_statement_remains_visible_and_foreign_fx_is_context_only(self):
        writes,_,calls,fx=self.handler(cash(currency='JPY',count=1));self.assertEqual(calls,['TEST']);self.assertEqual(fx,[])
        self.assertEqual(len(writes['data/capex-pulse.json']['rows'][0]['observations']),1)
        writes,_,_,fx=self.handler(cash(currency='JPY'));row=writes['data/capex-pulse.json']['rows'][0]
        self.assertEqual(fx,['JPY']);self.assertIsNone(row['current_window_usd'])
        self.assertFalse(row['legacy_spot_fx_context']['historical_flow_conversion_eligible'])

    def test_unavailable_acquisition_preserves_publication_and_history(self):
        writes,_,_,_=self.handler(unavailable=True);self.assertEqual(writes,{})

    def test_same_day_history_revision_is_preserved_without_nesting_identical_refreshes(self):
        today=datetime.now(timezone.utc).date().isoformat();original={today:{'legacy':42}}
        writes,_,_,_=self.handler(history=original);first=writes['data/history/capex-pulse.json']
        self.assertEqual(first[today]['previous_same_day_original'],original[today])
        writes,_,_,_=self.handler(history=first);self.assertEqual(writes['data/history/capex-pulse.json'],first)

    def test_actual_prior_reader_distinguishes_missing_from_denied_or_malformed(self):
        tree=ast.parse((SOURCE/'lambda_function.py').read_bytes())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_j')
        class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
        def raised(error):raise error
        for error,missing in ((Missing(),True),(PermissionError(),False),(ValueError('malformed'),False)):
            ns={'BUCKET':'fixture-only','decode':decode,'s3':SimpleNamespace(get_object=lambda **k:raised(error))}
            exec(compile(ast.Module(body=[node],type_ignores=[]),'<actual prior reader>','exec'),ns)
            if missing:self.assertEqual(ns['_j']('data/history/capex-pulse.json',{}),{})
            else:
                with self.assertRaises(type(error)):ns['_j']('data/history/capex-pulse.json',{})


if __name__=='__main__':unittest.main(verbosity=2)
