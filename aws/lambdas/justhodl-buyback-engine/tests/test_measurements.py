from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
import ast
import datetime
import json
import sys
import unittest

ROOT=Path(__file__).resolve().parents[4];SOURCE=ROOT/'aws/lambdas/justhodl-buyback-engine/source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
from buyback_measurements import CONTRACT,dossier,number,decode


def sources():
    profile=[{'symbol':'TEST','cik':'0000000001','sector':'Industrials','industry':'Machines','companyName':'Test company'}]
    cash=[]
    for end,start,period in [('2026-06-30','2026-04-01','Q2'),('2026-03-31','2026-01-01','Q1'),('2025-12-31','2025-10-01','Q4'),('2025-09-30','2025-07-01','Q3')]:
        cash.append({'symbol':'TEST','cik':'0000000001','date':end,'startDate':start,'period':period,'reportedCurrency':'USD',
                     'commonStockRepurchased':-100,'commonStockIssuance':0,'netCommonStockIssuance':0,'netStockIssuance':-500,
                     'commonDividendsPaid':0,'netDividendsPaid':-900,'stockBasedCompensation':5,'netDebtIssuance':100,
                     'operatingCashFlow':0,'netCashProvidedByOperatingActivities':900,'capitalExpenditure':-20})
    metrics=[{'symbol':'TEST','date':'2026-06-30','reportedCurrency':'USD','marketCap':10000}]
    shares=[{'symbol':'TEST','date':'2026-06-30','numberOfShares':80},{'symbol':'TEST','date':'2025-06-30','numberOfShares':100}]
    return [profile,cash,metrics,shares]


def calculate(data=None):return dossier('TEST',*(data if data is not None else sources()),'2026-09-27')


def native(**extra):
    tree=ast.parse((SOURCE/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and not n.name.startswith('_legacy_')]
    ns={'CONTRACT':CONTRACT,'dossier':dossier,'number':number,'decode':decode,'datetime':datetime,'json':json,'EXCLUDE_TICKERS':set(),
        'S3_BUCKET':'fixture-only','OUT_KEY':'data/buyback-engine.json','time':SimpleNamespace(sleep=lambda seconds:None)}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated actual buyback>','exec'),ns);ns.update(extra);return ns


class MeasurementTests(unittest.TestCase):
    def test_duplicate_underflow_and_nonfinite_json_fail_without_a_fake_value(self):
        for raw in (b'{"val":0,"val":1}',b'{"val":1e-999}',b'{"val":1e999}',b'{"val":NaN}'):
            with self.assertRaises(ValueError):decode(raw)
        self.assertEqual(decode(b'{"val":0}'),{'val':0})

    def test_explicit_zero_cannot_be_replaced_by_broader_stock_or_dividend_alias(self):
        p=calculate();self.assertEqual(p['net_buyback_ttm'],0);self.assertEqual(p['dividend_yield'],0)
        self.assertEqual(p['gross_repurchases_ttm'],400)
        self.assertEqual(p['fcf_yield_annualized'],-0.8)
        self.assertEqual(p['provider_responses']['cash_flow'],sources()[1])

    def test_two_rows_and_absent_durations_do_not_become_a_year(self):
        data=sources();data[1]=data[1][:2];self.assertIsNone(calculate(data)['gross_repurchases_ttm'])
        data=sources()
        for r in data[1]:r.pop('startDate')
        p=calculate(data);self.assertIsNone(p['gross_repurchases_ttm'])
        self.assertEqual(len(p['measurements']['cashflow_observations']),4)
        self.assertFalse(p['measurements']['cashflow_window']['reported_calendar_duration_aligned'])

    def test_mixed_currency_and_issuer_quarters_cannot_be_summed(self):
        for key,value in [('reportedCurrency','JPY'),('cik','0000000002')]:
            data=sources();data[1][1][key]=value
            p=calculate(data);self.assertIsNone(p['gross_repurchases_ttm']);self.assertIsNone(p['net_buyback_yield'])
            self.assertEqual(p['provider_responses']['cash_flow'][1][key],value)

    def test_missing_boolean_and_malformed_inputs_are_not_zeros(self):
        for value in (None,False,'0',float('nan')):
            data=sources();data[1][0]['commonStockRepurchased']=value
            self.assertIsNone(calculate(data)['gross_repurchases_ttm'])
        data=sources();data[1][0].pop('operatingCashFlow');data[1][0].pop('netCashProvidedByOperatingActivities')
        self.assertIsNone(calculate(data)['fcf_yield_annualized'])
        data=sources();data[1][0]['operatingCashFlow']=False
        self.assertIsNone(calculate(data)['fcf_yield_annualized'])

    def test_cashflow_order_is_irrelevant_and_conflicting_fifth_observation_is_retained(self):
        data=sources();data[1].reverse();self.assertEqual(calculate(data)['gross_repurchases_ttm'],400)
        data=sources();extra=deepcopy(data[1][-1]);extra['commonStockRepurchased']=-900;data[1].append(extra)
        p=calculate(data);self.assertIsNone(p['gross_repurchases_ttm']);self.assertEqual(len(p['provider_responses']['cash_flow']),5)

    def test_invalid_future_and_noncontiguous_periods_abstain(self):
        for key,value in [('date','2026-02-30'),('date','2026-12-31'),('startDate','2026-01-03'),('period','FY')]:
            data=sources();data[1][0][key]=value;self.assertIsNone(calculate(data)['gross_repurchases_ttm'])
        data=sources();data[1][2]['period']='Q2';self.assertIsNone(calculate(data)['gross_repurchases_ttm'])

    def test_market_cap_must_have_explicit_matching_date_and_currency(self):
        for field,value in [('reportedCurrency',None),('reportedCurrency','JPY'),('date','2026-03-31'),('marketCap',False),('marketCap',0)]:
            data=sources();data[2][0][field]=value;p=calculate(data)
            self.assertEqual(p['gross_repurchases_ttm'],400);self.assertIsNone(p['gross_buyback_yield'])

    def test_nine_month_share_change_is_not_yoy_and_split_adjustment_is_not_asserted(self):
        data=sources();data[3][1]['date']='2025-09-30';self.assertIsNone(calculate(data)['share_count_reduction_yoy'])
        p=calculate();self.assertEqual(p['share_count_reduction_yoy'],20)
        self.assertFalse(p['measurements']['reported_shares']['split_and_corporate_action_adjusted'])
        self.assertIsNone(p['net_issuer']);self.assertIsNone(p['active_execution']);self.assertIsNone(p['debt_funded'])

    def test_combined_yield_overflow_and_malformed_profile_labels_do_not_break_publication(self):
        data=sources();data[0][0].update(sector=123,industry=True)
        data[2][0]['marketCap']=100
        for row in data[1]:
            row['netCommonStockIssuance']=-2.5e307
            row['commonDividendsPaid']=-2.5e307
        p=calculate(data)
        self.assertEqual(p['net_buyback_yield'],1e308)
        self.assertEqual(p['dividend_yield'],1e308)
        self.assertIsNone(p['shareholder_yield'])
        self.assertEqual(p['provider_responses']['profile'],data[0])
        json.dumps(p,allow_nan=False)

    def test_latest_invalid_or_duplicate_share_record_cannot_fall_back_to_older_value(self):
        for bad in (False,None,-1):
            data=sources();data[3][0]['numberOfShares']=bad;self.assertIsNone(calculate(data)['share_count_reduction_yoy'])
        data=sources();data[3].append(deepcopy(data[3][0]));self.assertIsNone(calculate(data)['share_count_reduction_yoy'])

    def test_actual_acquisition_keeps_one_row_without_extra_provider_requests(self):
        calls=[];data=sources();data[1]=data[1][:1]
        def fmp(path):calls.append(path);return data[0] if path.startswith('profile?') else data[1]
        p=native(fmp=fmp)['analyze_ticker']('TEST')
        self.assertEqual(len(calls),2);self.assertEqual(len(p['provider_responses']['cash_flow']),1)
        self.assertIsNone(p['gross_repurchases_ttm'])

    def test_source_calculations_and_self_reported_flags_cannot_authorize_forecast_scores(self):
        ns=native();p=calculate();p.update(calls_eligible=True,forecast_qualified=True)
        self.assertEqual(ns['classify_and_score'](p,99,True),(None,'RESEARCH_ONLY',False,False))

    def handler(self,unavailable=False):
        writes={};data=sources()
        scanner={'top_opportunities':[{'ticker':'TEST','authorization_usd':99999,'market_cap':10000,'company':'Test company'}]}
        def read(key,default=None):
            return {'data/buyback-scanner.json':scanner,'data/attention-confluence.json':{'tickers':{'TEST':{}}},
                    'data/earnings-blackout.json':{},'data/share-flows.json':{}}[key]
        def fmp(path):
            if path.startswith('earnings-calendar?'):return []
            if unavailable:return None
            for prefix,index in [('profile?',0),('cash-flow-statement?',1),('key-metrics?',2),('enterprise-values?',3)]:
                if path.startswith(prefix):return deepcopy(data[index])
            raise AssertionError(path)
        ns=native(_read=read,fmp=fmp,s3=SimpleNamespace(put_object=lambda **k:writes.update({k['Key']:json.loads(k['Body'])})))
        return ns,writes,scanner

    def test_actual_handler_preserves_records_and_abstains_without_null_comparison_crashes(self):
        ns,writes,scanner=self.handler();ns['lambda_handler']();p=writes['data/buyback-engine.json']
        self.assertEqual(p['n_scored'],0);self.assertEqual(p['n_research_rows'],1)
        self.assertIsNone(p['tickers']['TEST']['buyback_score']);self.assertIsNone(p['tickers']['TEST']['auth_pct_mcap'])
        self.assertEqual(p['unverified_scanner_records'],scanner['top_opportunities'])
        self.assertEqual(p['high_conviction_pumps'],[]);self.assertFalse(p['calls_eligible'])
        self.assertEqual(p['tickers']['TEST']['provider_responses']['cash_flow'],sources()[1])

    def test_total_acquisition_failure_preserves_previous_output(self):
        ns,writes,_=self.handler(unavailable=True);result=ns['lambda_handler']()
        self.assertTrue(result['kept_prior']);self.assertEqual(writes,{})


if __name__=='__main__':unittest.main()
