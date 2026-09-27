"""Actual accounting helper/native entrypoint, with no network or AWS side effects."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace
import ast
import io
import json
import sys
import time
import unittest
import urllib.parse

ROOT=Path(__file__).resolve().parents[4];SOURCE=ROOT/'aws/lambdas/justhodl-earnings-quality/source'
sys.path.insert(0,str(SOURCE))
from earnings_measurements import CONTRACT,decode,number,dossier


def acquisitions(symbol='TEST',currency='USD'):
    periods=[('2026-06-30','2026-04-01','Q2',2026),('2026-03-31','2026-01-01','Q1',2026),
             ('2025-12-31','2025-10-01','Q4',2025),('2025-09-30','2025-07-01','Q3',2025),
             ('2025-06-30','2025-04-01','Q2',2025),('2025-03-31','2025-01-01','Q1',2025),
             ('2024-12-31','2024-10-01','Q4',2024),('2024-09-30','2024-07-01','Q3',2024)]
    out={key:{'status':'received','response':[]} for key in ('income','cash_flow','balance_sheet')}
    for endpoint in out:
        for end,start,period,year in periods:
            filing=(date.fromisoformat(end)+timedelta(days=20)).isoformat()
            row={'symbol':symbol,'cik':'00001','date':end,'startDate':start,'period':period,'fiscalYear':year,
                 'filingDate':filing,'acceptedDate':filing+'T12:00:00','reportedCurrency':currency}
            row.update({'income':{'netIncome':100,'revenue':1000,'grossProfit':500},
                        'cash_flow':{'operatingCashFlow':0,'netCashProvidedByOperatingActivities':900,'capitalExpenditure':-5},
                        'balance_sheet':{'totalAssets':1000,'netReceivables':100}}[endpoint])
            out[endpoint]['response'].append(row)
    out['quote']={'status':'received','response':[{'symbol':symbol,'name':'Test','marketCap':3e9}]}
    return out


def calculate(a=None):return dossier('TEST',acquisitions() if a is None else a,'2026-09-27')


def extracted(path,wanted,ns):
    tree=ast.parse(path.read_bytes());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in wanted]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated actual Earnings Quality>','exec'),ns);return ns


def native(**extra):
    ns={'CONTRACT':CONTRACT,'decode':decode,'number':number,'dossier':dossier,'datetime':datetime,'timezone':timezone,
        'ThreadPoolExecutor':ThreadPoolExecutor,'as_completed':as_completed,'json':json,'time':time,
        'urllib':urllib,'FMP_KEY':'fixture','VERSION':'1.1.0','S3_BUCKET':'fixture-only','S3_KEY':'data/earnings-quality.json',
        'FALLBACK_UNIVERSE':['TEST'],**extra}
    return extracted(SOURCE/'lambda_function.py',{'_accounting_universe','_accounting_acquire','_accounting_fetch','lambda_handler'},ns)


class Tests(unittest.TestCase):
    def test_zero_ocf_is_preserved_and_conflicting_alias_is_reconciled(self):
        p=calculate();self.assertEqual(p['amounts']['operating_cash_flow'],0)
        self.assertEqual(p['amounts']['free_cash_flow_derived'],-20);self.assertEqual(p['measurements']['cash_conversion_ratio'],0)
        self.assertEqual(p['statement_observations']['cash_flow'][0]['operating_cash_alias_residual'],-900)
        self.assertEqual(p['measurements']['earnings_cash_gap_pct_end_assets'],40)
        self.assertEqual(p['measurements']['cash_flow_accruals_pct_average_assets'],40)
        self.assertIsNone(p['quality_score']);self.assertIsNone(p['sloan_accruals_pct_assets'])

    def test_original_reproduces_zero_alias_and_fake_partial_ttm_defects(self):
        a=acquisitions();f={k:v['response'] for k,v in a.items() if k!='quote'}
        ns=extracted(ROOT/'tests/fixtures/pre-earnings-quality-statement-observations.py.txt',{'analyze_ticker'},
                     {'fmp_quote':lambda s:{'market_cap':3e9},'fmp_financials':lambda s:f})
        self.assertEqual(ns['analyze_ticker']('TEST')['ttm_ocf_usd'],3600)
        for rows in f.values():del rows[2:]
        self.assertEqual(ns['analyze_ticker']('TEST')['ttm_ni_usd'],200)
        for k in f:a[k]['response']=f[k]
        self.assertEqual(calculate(a)['amounts'],{})

    def test_missing_boolean_and_nonfinite_are_not_zero_income(self):
        for invalid in (None,True,'100',float('nan'),float('inf')):
            a=acquisitions();a['income']['response'][0]['netIncome']=invalid;p=calculate(a)
            self.assertIsNone(p['amounts']['net_income']);self.assertIsNone(p['measurements']['cash_conversion_ratio'])
        a=acquisitions();a['income']['response'][0]['netIncome']=0
        self.assertEqual(calculate(a)['amounts']['net_income'],300)

    def test_missing_cash_flow_does_not_fall_through_to_alias(self):
        a=acquisitions();del a['cash_flow']['response'][0]['operatingCashFlow'];p=calculate(a)
        self.assertIsNone(p['amounts']['operating_cash_flow']);self.assertIsNone(p['amounts']['free_cash_flow_derived'])

    def test_reordering_is_stable_and_duplicate_periods_abstain(self):
        a=acquisitions()
        for k in ('income','cash_flow','balance_sheet'):a[k]['response'].reverse()
        self.assertEqual(calculate(a)['amounts'],calculate()['amounts'])
        a['income']['response'].append(deepcopy(a['income']['response'][-1]));self.assertEqual(calculate(a)['amounts'],{})

    def test_explicit_durations_currency_issuer_and_filing_alignment_required(self):
        for key,value in [('startDate',None),('reportedCurrency','JPY'),('cik','2'),('symbol','OTHER'),('period','FY'),('filingDate','2026-07-21'),('acceptedDate','2026-07-20T13:00:00'),('fiscalYear',2024)]:
            a=acquisitions();a['cash_flow']['response'][0][key]=value
            self.assertEqual(calculate(a)['amounts'],{},(key,value))

    def test_missing_month_quarter_future_and_invalid_dates_abstain(self):
        for value in ('2026-02-30','2027-06-30',None):
            a=acquisitions();a['income']['response'][0]['date']=value;self.assertEqual(calculate(a)['amounts'],{})
        a=acquisitions();a['income']['response'].pop(1);self.assertEqual(calculate(a)['amounts'],{})

    def test_foreign_amounts_never_become_usd_aliases(self):
        p=calculate(acquisitions(currency='JPY'));self.assertEqual(p['reported_currency'],'JPY')
        self.assertEqual(p['amounts']['net_income'],400);self.assertIsNone(p['ttm_ni_usd'])

    def test_current_and_average_assets_have_distinct_explicit_formulas(self):
        a=acquisitions();a['balance_sheet']['response'][4]['totalAssets']=3000;p=calculate(a)
        self.assertEqual(p['measurements']['earnings_cash_gap_pct_end_assets'],40)
        self.assertEqual(p['measurements']['cash_flow_accruals_pct_average_assets'],20)
        a['balance_sheet']['response'][0]['cik']='2';p=calculate(a)
        self.assertIsNone(p['measurements']['earnings_cash_gap_pct_end_assets']);self.assertIsNone(p['balance_source_rows']['ending'])

    def test_negative_net_income_is_not_cash_conversion_quality(self):
        a=acquisitions()
        for r in a['income']['response']:r['netIncome']=-100
        p=calculate(a);self.assertIsNone(p['measurements']['cash_conversion_ratio']);self.assertEqual(p['amounts']['net_income'],-400)

    def test_year_comparison_cannot_use_mixed_units_or_partial_prior(self):
        a=acquisitions()
        self.assertEqual(calculate(a)['measurements']['dsri_reported'],1)
        self.assertEqual(calculate(a)['measurements']['gmi_reported'],1)
        a['income']['response'].pop();p=calculate(a);self.assertFalse(p['windows']['current']['annual_prior_aligned'])
        self.assertNotIn('dsri_reported',p['measurements'])

    def test_whole_source_rows_and_overflow_safe(self):
        a=acquisitions();a['income']['response'][0]['netIncome']=1e308;a['income']['response'][1]['netIncome']=1e308;p=calculate(a)
        self.assertIsNone(p['amounts']['net_income']);json.dumps(p,allow_nan=False)
        self.assertEqual(p['acquisitions'],a);self.assertEqual(sum(len(v) for v in p['statement_observations'].values()),24)

    def handler(self,unavailable=False):
        writes=[];ns=native(s3=SimpleNamespace(put_object=lambda **kw:writes.append(kw)))
        ns['_accounting_universe']=lambda:{'requested':['TEST','TEST'],'universe_names':['TEST','TEST'],'not_attempted':[]}
        ns['_accounting_acquire']=lambda s:({} if unavailable else acquisitions(s))
        result=ns['lambda_handler']();return result,[json.loads(w['Body']) for w in writes]

    def test_native_full_occurrences_no_rank_ticket_notification_or_learning(self):
        result,writes=self.handler();self.assertEqual(result['statusCode'],200);p=writes[0]
        self.assertEqual(len(p['issuer_rows']),2);self.assertEqual([r['request_index'] for r in p['issuer_rows']],[0,1])
        self.assertEqual(p['n_aligned'],2)
        for k in ('all_ranked','top_20_high_quality','top_10_low_quality_avoid','trade_tickets'):self.assertEqual(p[k],[])
        self.assertFalse(p['calls_eligible']);self.assertEqual(p['notifications_sent'],0);self.assertEqual(p['signals_logged'],0)

    def test_complete_acquisition_failure_preserves_previous_packet(self):
        result,writes=self.handler(unavailable=True);self.assertEqual(result['statusCode'],503);self.assertEqual(writes,[])

    def test_acquisition_scope_and_empty_rows_retained_without_extra_queries(self):
        calls=[];ns=native()
        def fetch(url):
            calls.append(url);return {'status':'received','response':[{'marketCap':3e9}]} if '/quote?' in url else {'status':'received','response':[]}
        ns['_accounting_fetch']=fetch;out=ns['_accounting_acquire']('TEST')
        self.assertEqual(len(calls),4);self.assertEqual(out['income']['response'],[])
        ns['_accounting_fetch']=lambda url:{'status':'received','response':[{'marketCap':False}]}
        self.assertEqual(ns['_accounting_acquire']('TEST')['income']['status'],'not_requested_quote_gate')

    def test_bad_universe_aborts_missing_uses_declared_fallback_and_cap_is_preserved(self):
        for raw in (b'[]',b'{bad',b'{"picks":{}}',b'{"picks":[1],"picks":[]}'):
            ns=native(s3=SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(raw)}))
            with self.assertRaises(ValueError):ns['_accounting_universe']()
        raw=json.dumps({'picks':[{'ticker':'A'+str(i)} for i in range(205)]}).encode()
        p=native(s3=SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(raw)}))['_accounting_universe']()
        self.assertEqual(len(p['requested']),120);self.assertEqual(len(p['not_attempted']),80);self.assertEqual(len(p['original']['picks']),205)

    def test_rate_limit_no_retry_and_no_error_url_disclosure(self):
        calls=[]
        class RateLimited(Exception):code=429
        def denied(*a,**kw):calls.append(1);raise RateLimited('url?apikey=DO_NOT_PUBLISH')
        ns=native(urllib=SimpleNamespace(request=SimpleNamespace(Request=lambda *a,**kw:None,urlopen=denied)))
        result=ns['_accounting_fetch']('synthetic');self.assertEqual(len(calls),1);self.assertNotIn('DO_NOT_PUBLISH',json.dumps(result))

    def test_invalid_provider_json_does_not_expand_request_count(self):
        calls=[]
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self):return b'{"a":1,"a":2}'
        def response(*args,**kw):calls.append(1);return Response()
        ns=native(urllib=SimpleNamespace(request=SimpleNamespace(Request=lambda *a,**kw:None,urlopen=response)))
        self.assertEqual(ns['_accounting_fetch']('synthetic')['status'],'invalid_json');self.assertEqual(len(calls),1)

    def test_strict_original_json(self):
        for raw in (b'{"a":1,"a":2}',b'NaN',b'1e999',b'1e-999',b'"\xff"'):
            with self.assertRaises((ValueError,UnicodeError)):decode(raw)


if __name__=='__main__':unittest.main(verbosity=2)
