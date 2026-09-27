"""Actual pure compiler and isolated native orchestration; no AWS/provider calls."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace
import ast
import builtins
import copy
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / 'aws/lambdas/justhodl-backlog/source'
sys.path.insert(0, str(SOURCE))
from backlog_measurements import CONTRACT, compile_concept, row_from_concepts, decode

TAG = 'RevenueRemainingPerformanceObligation'
EPS = 'EarningsPerShareDiluted'


def observation(end, val, **extra):
    return {'end': end, 'val': val, 'filed': '2026-09-15', 'accn': '0000000001-26-000001', 'form': '10-Q', **extra}


def payload(rows, tag=TAG, **units):
    return {'cik': 1, 'tag': tag, 'taxonomy': 'us-gaap', 'units': {('USD/shares' if tag == EPS else 'USD'): rows, **units}}


def compile(rows, tag=TAG, **units):
    return compile_concept(payload(rows, tag, **units), '0000000001', tag, '2026-09-27')


def native(**extra):
    tree = ast.parse((SOURCE/'lambda_function.py').read_bytes())
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and not n.name.startswith('_legacy_')]
    namespace = {'CONTRACT': CONTRACT, 'compile_concept': compile_concept, 'row_from_concepts': row_from_concepts, 'decode': decode,
                 'datetime': datetime, 'timezone': timezone, 'timedelta': timedelta, 'json': json,
                 'ThreadPoolExecutor': ThreadPoolExecutor, 'as_completed': as_completed,
                 'time': SimpleNamespace(time=lambda: 0), 'print': lambda *args: None,
                 'RPO_TAGS': [TAG], 'DEF_TAGS': ['ContractWithCustomerLiability'], 'EPS_TAGS': [EPS],
                 'SECTOR_GROUP': {}, 'SEED': ['TEST'], 'BUCKET': 'fixture-only', 'OUT_KEY': 'data/backlog.json'}
    exec(builtins.compile(ast.Module(body=funcs, type_ignores=[]), '<isolated actual backlog>', 'exec'), namespace)
    namespace.update(extra)
    return namespace


class Tests(unittest.TestCase):
    def test_duplicate_overflow_and_underflow_source_values_are_rejected(self):
        for raw in (b'{"val":1,"val":2}',b'{"val":NaN}',b'{"val":1e999}',b'{"val":1e-999}'):
            with self.assertRaises(ValueError):decode(raw)
        self.assertEqual(decode(b'{"val":0,"other":null}'),{'val':0,'other':None})

    def test_annual_observation_is_not_array_position_quarterly_growth(self):
        c=compile([observation('2024-12-31',100), observation('2025-12-31',120)])
        self.assertEqual(c['yoy'],20);self.assertIsNone(c['qoq'])
        self.assertEqual(c['comparisons']['yoy']['prior']['end'],'2024-12-31')

    def test_exact_calendar_pairs_survive_unsorted_and_duplicate_observations(self):
        rows=[observation('2026-06-30',120),observation('2025-06-30',80),observation('2026-03-31',100)]
        c=compile(rows+rows[:1]);self.assertEqual(c['qoq'],20);self.assertEqual(c['yoy'],50)
        self.assertEqual(len(c['observations']),4)
        self.assertEqual(compile(list(reversed(rows)))['qoq'],20)

    def test_currency_units_and_boolean_blanks_never_become_dollars(self):
        c=compile([observation('2026-06-30',False),observation('2026-03-31',None)], EUR=[observation('2026-06-30',900)])
        self.assertIsNone(c['latest']);self.assertEqual(len(c['observations']),3)
        self.assertEqual(c['observations'][-1]['source_unit'],'EUR')
        zero=compile([observation('2026-06-30',0),observation('2026-03-31',100)])
        self.assertEqual(zero['qoq'],-100)

    def test_latest_filed_amendment_wins_independent_of_array_order(self):
        newer=observation('2026-06-30',120,filed='2026-09-15',accn='0000000001-26-000002',form='10-Q/A')
        older=observation('2026-06-30',100,filed='2026-08-15')
        c=compile([newer,older]);self.assertEqual(c['latest']['value'],120)
        self.assertEqual(c['latest']['accessions'],['0000000001-26-000002'])
        self.assertEqual(len(c['observations']),2)

    def test_conflicting_latest_vintages_abstain_instead_of_arbitrary_last(self):
        c=compile([observation('2026-06-30',100),observation('2026-06-30',120,accn='0000000001-26-000002')])
        self.assertIsNone(c['latest']);self.assertEqual(c['status'],'ambiguous_latest_period_or_value')

    def test_eps_annual_is_not_quarterly_and_mixed_ytd_periods_are_ambiguous(self):
        c=compile([observation('2024-12-31',5,start='2024-01-01'),observation('2025-12-31',6,start='2025-01-01')],EPS)
        self.assertIsNone(c['qoq']);self.assertEqual(c['yoy'],20)
        c=compile([observation('2026-06-30',2,start='2026-04-01'),observation('2026-06-30',4,start='2026-01-01')],EPS)
        self.assertIsNone(c['latest'])

    def test_eps_actual_quarters_compare_but_noncalendar_durations_do_not(self):
        c=compile([observation('2026-03-31',1,start='2026-01-01'),observation('2026-06-30',2,start='2026-04-01')],EPS)
        self.assertEqual(c['qoq'],100)
        c=compile([observation('2026-03-28',1,start='2025-12-28'),observation('2026-06-27',2,start='2026-03-29')],EPS)
        self.assertIsNone(c['qoq']);self.assertEqual(c['latest']['period_kind'],'unreviewed_duration')

    def test_issuer_dates_and_accessions_require_real_source_evidence(self):
        for bad in ('2026-02-30','2026-12-31'):
            c=compile([observation(bad,100)]);self.assertIsNone(c['latest'])
        for fields in ({'filed':'2026-12-31'},{'accn':'unknown'},{'filed':'2020-01-01'},{'form':'8-K'}):
            self.assertIsNone(compile([observation('2026-06-30',100,**fields)])['latest'])
        p=payload([]);p['cik']=2
        with self.assertRaises(ValueError):compile_concept(p,'1',TAG,'2026-09-27')

    def test_no_circular_forecast_or_cross_source_ratios_and_all_inputs_retained(self):
        c=compile([observation('2025-12-31',100),observation('2026-03-31',110)])
        concepts={'rpo':c,'deferred':c,'eps':c};r=row_from_concepts('TEST','0000000001',{},concepts)
        self.assertEqual(r['rpo_unit'],'USD');self.assertEqual(r['rpo_qoq'],10)
        for k in ('ev_to_rpo','rev_yoy','rpo_minus_rev_growth','demand_accelerating','deferred_accelerating'):self.assertIsNone(r[k])
        self.assertFalse(r['calls_eligible']);self.assertEqual(r['measurements'],concepts)

    def test_acquisition_unavailable_is_never_confirmed_negative_cache(self):
        ns=native(http_json=lambda url: None)
        with self.assertRaises(ValueError):ns['concept_series']('1',TAG)
        ns=native(http_json=lambda url:{'_source_status':404})
        self.assertEqual(ns['concept_series']('1',TAG)['source_status'],'not_found')

    def test_foreign_unit_does_not_expand_legacy_fallback_requests(self):
        calls=[]
        def http(url):
            calls.append(url)
            p=payload([]);p['units']={'EUR':[observation('2026-03-31',100),observation('2026-06-30',120)]}
            return p
        ns=native(http_json=http)
        tag,c=ns['first_series']('1',[TAG,'ContractWithCustomerLiability'])
        self.assertEqual(len(calls),1);self.assertEqual(tag,TAG);self.assertIsNone(c['latest'])
        self.assertEqual(len(c['observations']),2)

    def test_unreviewed_carry_is_idempotent_and_never_nests_originals_daily(self):
        ns=native();row={'ticker':'OLD','rpo':99,'rpo_qoq':500}
        once=ns['qualify_carried'](row)
        self.assertEqual(ns['qualify_carried'](once),once)
        self.assertEqual(once['legacy_unverified_original'],row)

    def test_actual_issuer_analysis_retains_complete_enrichment_without_unsafe_ratios(self):
        requests=[];enrich=[]
        def http(url):
            requests.append(url);tag=url.rsplit('/',1)[-1][:-5]
            if tag!=TAG:return {'_source_status':404}
            return payload([observation('2025-12-31',100),observation('2026-03-31',110)])
        def fmp(symbol,endpoint):
            enrich.append(endpoint);return [{'symbol':symbol,'unknown_provider_field':123,'revenue':900}]
        ns=native(http_json=http,fetch_fmp=fmp)
        r=ns['analyze']('TEST',{'TEST':'0000000001'},{'TEST':{'sector':'Industrials'}})
        self.assertEqual(len(requests),3);self.assertEqual(len(enrich),2)
        self.assertEqual(r['rpo_qoq'],10);self.assertEqual(r['rpo_unit'],'USD')
        self.assertEqual(r['provider_enrichment']['income_statement'][0]['unknown_provider_field'],123)
        self.assertIsNone(r['rev_yoy']);self.assertIsNone(r['ev_to_rpo']);self.assertFalse(r['calls_eligible'])

    def handler(self, *, unavailable=False, prior_error=False):
        writes={};old={'OLD':{'ticker':'OLD','rpo':999,'rpo_qoq':500,'refreshed_at':'2020-01-01'}}
        def read(key,default=None):
            if key=='data/backlog-coverage-cache.json':return {'has_backlog':[],'no_backlog':['HISTORICAL_NEGATIVE']}
            if key=='data/universe.json':return {'stocks':[]}
            if key=='data/backlog.json':
                if prior_error:raise PermissionError('Synthetic denied original')
                return {'by_ticker':copy.deepcopy(old)}
            raise AssertionError(key)
        c=compile([observation('2025-12-31',100),observation('2026-03-31',110)])
        row=row_from_concepts('TEST','0000000001',{}, {'rpo':c,'deferred':c,'eps':c})
        ns=native(load_cik_map=lambda:{'TEST':'0000000001'},read_json=read,
                  analyze=lambda *a:None if unavailable else copy.deepcopy(row),
                  s3=SimpleNamespace(put_object=lambda **k:writes.update({k['Key']:json.loads(k['Body'])})))
        return ns,writes,old

    def test_native_preserves_whole_prior_rows_but_quarantines_legacy_values(self):
        ns,writes,old=self.handler();ns['lambda_handler']()
        p=writes['data/backlog.json'];self.assertEqual(len(p['by_ticker']),2)
        carried=p['by_ticker']['OLD'];self.assertEqual(carried['legacy_unverified_original'],old['OLD'])
        self.assertIsNone(carried['rpo']);self.assertIsNone(carried['rpo_qoq'])
        self.assertEqual(p['quality']['status'],'partial');self.assertFalse(p['calls_eligible'])
        self.assertEqual(writes['data/backlog-coverage-cache.json']['no_backlog'],['HISTORICAL_NEGATIVE'])

    def test_native_empty_acquisition_and_denied_prior_leave_outputs_untouched(self):
        ns,writes,_=self.handler(unavailable=True);self.assertEqual(ns['lambda_handler']()['statusCode'],503);self.assertEqual(writes,{})
        ns,writes,_=self.handler(prior_error=True)
        with self.assertRaises(PermissionError):ns['lambda_handler']()
        self.assertEqual(writes,{})


if __name__ == '__main__': unittest.main()
