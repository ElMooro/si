"""Actual pure compiler and isolated native orchestration; no AWS/provider calls."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace
from io import BytesIO
from threading import Barrier,Event,Lock,Thread
import hashlib
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
import backlog_store as store

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
                 'load_public_head':store.load_head,'publish_public_head':store.publish,
                 'datetime': datetime, 'timezone': timezone, 'timedelta': timedelta, 'json': json,
                 'ThreadPoolExecutor': ThreadPoolExecutor, 'as_completed': as_completed,
                 'time': SimpleNamespace(time=lambda: 0), 'print': lambda *args: None,
                 'RPO_TAGS': [TAG], 'DEF_TAGS': ['ContractWithCustomerLiability'], 'EPS_TAGS': [EPS],
                 'SECTOR_GROUP': {}, 'SEED': ['TEST'], 'BUCKET': 'fixture-only', 'OUT_KEY': 'data/backlog.json'}
    exec(builtins.compile(ast.Module(body=funcs, type_ignores=[]), '<isolated actual backlog>', 'exec'), namespace)
    namespace.update(extra)
    return namespace


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self,packet,writes=None):
        self.raw={store.HEAD:json.dumps(packet).encode()};self.writes=writes if writes is not None else {};self.reads=[];self.puts=[];self.denied=False;self.corrupt=False;self.race=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise StorageError('AccessDenied')
        if key not in self.raw:raise StorageError('NoSuchKey')
        raw=self.raw[key]
        if self.corrupt and key.startswith(store.PREFIX):raw=b'corrupt'
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.puts.append(kw)
        if kw.get('IfNoneMatch')=='*' and key in self.raw:raise StorageError('PreconditionFailed')
        if 'IfMatch' in kw and (key not in self.raw or kw['IfMatch']!=hashlib.sha256(self.raw[key]).hexdigest()):raise StorageError('PreconditionFailed')
        if self.race and key==store.HEAD:raise StorageError('PreconditionFailed')
        self.raw[key]=kw['Body'];self.writes[key]=json.loads(kw['Body'])


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
                  s3=Memory({'by_ticker':copy.deepcopy(old)},writes))
        ns['s3'].denied=prior_error
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
        with self.assertRaises(StorageError) as error:ns['lambda_handler']()
        self.assertEqual(error.exception.response['Error']['Code'],'AccessDenied')
        self.assertEqual(writes,{})


    def test_whole_prior_current_archives_and_code_identity(self):
        ns,writes,old=self.handler();db=ns['s3'];prior=db.raw[store.HEAD];ns['lambda_handler']();raw=db.raw[store.HEAD];p=store.decode(raw)
        self.assertEqual(p['publication_contract'],store.CONTRACT);self.assertEqual(p['source_files'],store.source_identity())
        self.assertEqual(db.raw[p['previous_publication']['key']],prior);self.assertEqual(db.raw[store.reference(raw)['key']],raw)
        self.assertEqual(set(p['by_ticker']),{'OLD','TEST'})
        self.assertEqual(next(x for x in db.puts if x['Key']==store.HEAD)['IfMatch'],hashlib.sha256(prior).hexdigest())
    def test_archive_corruption_and_conditional_races_preserve_exact_head(self):
        for flag in ('corrupt','race'):
            ns,_,_=self.handler();db=ns['s3'];prior=db.raw[store.HEAD];setattr(db,flag,True)
            with self.assertRaises((ValueError,StorageError)):ns['lambda_handler']()
            self.assertEqual(db.raw[store.HEAD],prior)
            self.assertNotIn('data/backlog-coverage-cache.json',[k['Key'] for k in db.puts])
    def test_missing_and_malformed_head_are_distinct(self):
        db=Memory({'by_ticker':{}});db.raw={}
        self.assertEqual(store.load_head(db,'fixture'),(None,None,None))
        store.publish(db,'fixture',{'by_ticker':{}},None,None)
        self.assertEqual(next(x for x in db.puts if x['Key']==store.HEAD)['IfNoneMatch'],'*')
        for raw in (b'[]',b'{"by_ticker":[]}',b'{"by_ticker":{},"by_ticker":{}}'):
            db.raw[store.HEAD]=raw
            with self.assertRaises(ValueError):store.load_head(db,'fixture')
        with self.assertRaises(ValueError):store.whole({'Body':BytesIO(b'{}'),'ContentLength':3})
        db=Memory({'by_ticker':{}});original=db.get_object;db.get_object=lambda **kw:{**original(**kw),'ETag':None}
        with self.assertRaises(ValueError):store.load_head(db,'fixture')
    def test_history_chain_and_create_race_keep_exact_bytes(self):
        ns,_,_=self.handler();db=ns['s3'];ns['lambda_handler']();first=db.raw[store.HEAD]
        ns['lambda_handler']();second=store.decode(db.raw[store.HEAD])
        self.assertEqual(second['previous_publication'],store.reference(first))
        self.assertEqual(db.raw[second['previous_publication']['key']],first)
        current=db.raw[store.HEAD]
        with self.assertRaises(StorageError):store.publish(db,'fixture',{'by_ticker':{}},None,None)
        self.assertEqual(db.raw[store.HEAD],current)
    def test_overlapping_actual_writers_cannot_erase_a_completed_issuer(self):
        ns,_,_=self.handler();db=ns['s3'];prior=db.raw[store.HEAD];barrier=Barrier(2);first_written=Event();lock=Lock();failures=[]
        original_get=db.get_object;original_put=db.put_object
        def get(**kw):
            with lock:result=original_get(**kw)
            if kw['Key']==store.HEAD:barrier.wait(timeout=5)
            return result
        def put(**kw):
            is_head=kw['Key']==store.HEAD;p=json.loads(kw['Body'])
            if is_head and 'SECOND' in p['by_ticker']:self.assertTrue(first_written.wait(timeout=5))
            with lock:result=original_put(**kw)
            if is_head:first_written.set()
            return result
        db.get_object=get;db.put_object=put
        def run(name):
            try:
                local,_,_=self.handler();analyze=local['analyze'];local['SEED']=[name];local['load_cik_map']=lambda:{name:'0000000001'};local['s3']=db
                def row(*args):
                    result=analyze(*args);result['ticker']=name;return result
                local['analyze']=row;local['lambda_handler']()
            except Exception as exc:failures.append(exc)
        threads=[Thread(target=run,args=(name,)) for name in ('FIRST','SECOND')]
        for thread in threads:thread.start()
        for thread in threads:thread.join(timeout=10);self.assertFalse(thread.is_alive())
        self.assertEqual(len(failures),1);self.assertIsInstance(failures[0],StorageError)
        self.assertEqual(set(store.decode(db.raw[store.HEAD])['by_ticker']),{'FIRST','OLD'})
        self.assertEqual(db.raw[store.reference(prior)['key']],prior)
        self.assertTrue(all(k==store.HEAD or k.startswith(store.PREFIX) for k in db.reads))


if __name__ == '__main__': unittest.main()
