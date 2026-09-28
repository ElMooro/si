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
from unittest.mock import patch
import gzip,zlib,urllib.error

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / 'aws/lambdas/justhodl-backlog/source'
sys.path.insert(0, str(SOURCE))
from backlog_measurements import CONTRACT, compile_concept, row_from_concepts, decode
import backlog_store as store
import backlog_sources as sources

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
                 'Capture':sources.Capture,'UA':'fixture','FMP_KEY':'fixture-secret',
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
        self.raw[key]=kw['Body'];self.writes[key]=kw['Body'] if key.startswith(sources.PREFIX) else json.loads(kw['Body'])


def response(raw, code=200, encoding='', length=None):
    class Response(BytesIO):
        pass
    r=Response(raw);r.code=code;r.headers={'Content-Encoding':encoding,'Content-Length':str(len(raw)) if length is None else length}
    return r


def captured_handler():
    ns,_,_=Tests().handler();db=ns['s3'];calls=[];originals=[]
    def opener(req, timeout):
        url=req.full_url;calls.append(url)
        if url.endswith('/company_tickers.json'):
            raw=b'{"0":{"ticker":"TEST","cik_str":1,"title":"Synthetic issuer"}}';code=200;encoding=''
        elif '/companyconcept/' in url:
            tag=url.rsplit('/',1)[-1][:-5]
            raw=json.dumps(payload([observation('2025-12-31',100),observation('2026-03-31',110)]),ensure_ascii=False).encode() if tag==TAG else b'<html>missing concept</html>'
            code=200 if tag==TAG else 404;encoding='gzip';raw=gzip.compress(raw,mtime=0)
        else:
            raw=b'[ {"symbol":"TEST", "unknown_field":"whole original preserved", "revenue":900} ]';code=200;encoding='deflate';raw=zlib.compress(raw)
        originals.append(raw)
        return response(raw,code,encoding)
    # Restore actual acquisition/analysis functions, retaining only isolated storage.
    actual=native();ns['load_cik_map']=actual['load_cik_map'];ns['analyze']=actual['analyze']
    ns['Capture']=lambda db,bucket,ua,secret:sources.Capture(db,bucket,ua,secret,opener=opener)
    return ns,db,calls,originals


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
        row=row_from_concepts('TEST','0000000001',{'sector':None,'cap_bucket':None,'group':None}, {'rpo':c,'deferred':c,'eps':c})
        ns=native(load_cik_map=lambda **kw:{'TEST':'0000000001'},read_json=read,
                  analyze=lambda *a,**kw:None if unavailable else copy.deepcopy(row),
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
                local,_,_=self.handler();analyze=local['analyze'];local['SEED']=[name];local['load_cik_map']=lambda **kw:{name:'0000000001'};local['s3']=db
                def row(*args,**kwargs):
                    result=analyze(*args,**kwargs);result['ticker']=name;return result
                local['analyze']=row;local['lambda_handler']()
            except Exception as exc:failures.append(exc)
        threads=[Thread(target=run,args=(name,)) for name in ('FIRST','SECOND')]
        for thread in threads:thread.start()
        for thread in threads:thread.join(timeout=10);self.assertFalse(thread.is_alive())
        self.assertEqual(len(failures),1);self.assertIsInstance(failures[0],StorageError)
        self.assertEqual(set(store.decode(db.raw[store.HEAD])['by_ticker']),{'FIRST','OLD'})
        self.assertEqual(db.raw[store.reference(prior)['key']],prior)
        self.assertTrue(all(k==store.HEAD or k.startswith(store.PREFIX) for k in db.reads))


    def test_actual_native_originals_replay_same_six_requests_and_whole_bytes(self):
        ns,db,calls,originals=captured_handler();ns['lambda_handler']();p=json.loads(db.raw[store.HEAD])
        self.assertEqual(len(calls),6);self.assertEqual(len(p['provider_sources']['attempts']),6)
        for raw in originals:self.assertEqual(db.raw[sources.reference(raw)['key']],raw)
        result=sources.replay(p,lambda ref:sources.read_original(db,'fixture',ref))
        self.assertEqual(result['current_issuers'],1);self.assertEqual(result['current_source_observations'],2)
        self.assertEqual(result['retained_responses'],6);self.assertFalse(result['selection_context_replayed'])
        self.assertNotIn('fixture-secret',json.dumps(p));self.assertFalse(p['calls_eligible'])
        self.assertTrue(any(x['content_encoding']=='gzip' for x in p['provider_sources']['attempts']))
        self.assertEqual(p['by_ticker']['TEST']['rpo_qoq'],10)

    def test_original_replay_rejects_altered_measurement_binding_population_and_bytes(self):
        ns,db,_,_=captured_handler();ns['lambda_handler']();p=json.loads(db.raw[store.HEAD])
        for edit in (lambda p:p['by_ticker']['TEST']['measurements']['rpo'].update(qoq=999),
                     lambda p:p['by_ticker']['TEST']['measurements']['rpo']['source_attempt'].update(request_index=0),
                     lambda p:p['provider_sources']['attempts'][0].update(request_index=True),
                     lambda p:p.update(slice_this_run=2),
                     lambda p:p['by_ticker']['TEST']['provider_enrichment'].update(income_statement=[])):
            bad=copy.deepcopy(p);edit(bad)
            with self.assertRaises(ValueError):sources.replay(bad,lambda ref:sources.read_original(db,'fixture',ref))
        ref=p['provider_sources']['attempts'][0]['original_ref'];db.raw[ref['key']]=b'{}'
        with self.assertRaises(ValueError):sources.replay(p,lambda ref:sources.read_original(db,'fixture',ref))

    def test_transport_error_never_stores_credentialed_exception_or_becomes_absence(self):
        db=Memory({'by_ticker':{}})
        def fail(*args,**kw):raise urllib.error.URLError('https://provider.invalid/?apikey=fixture-secret')
        c=sources.Capture(db,'fixture','fixture','fixture-secret',opener=fail)
        value,attempt=c.acquire('https://data.sec.gov/api/xbrl/companyconcept/CIK1/us-gaap/'+TAG+'.json')
        self.assertIsNone(value);self.assertEqual(attempt['status'],'transport_error');self.assertNotIn('original_ref',attempt)
        self.assertNotIn('fixture-secret',json.dumps(c.finish()));self.assertEqual(db.puts,[])

    def test_partial_oversized_and_invalid_responses_are_not_usable_measurements(self):
        url='https://www.sec.gov/files/company_tickers.json'
        for raw,encoding,length,status in ((b'{}','','3','incomplete_response'),(b'bad','','3','invalid_response'),
                                          (b'bad','gzip','3','invalid_response')):
            db=Memory({'by_ticker':{}});c=sources.Capture(db,'fixture','fixture','',opener=lambda *a,**k:response(raw,200,encoding,length))
            value,attempt=c.acquire(url);self.assertIsNone(value);self.assertEqual(attempt['status'],status)
        with patch.object(sources,'WIRE_BOUND',8):
            c=sources.Capture(db,'fixture','fixture','',opener=lambda *a,**k:response(b'0123456789'))
            value,attempt=c.acquire(url);self.assertIsNone(value);self.assertEqual(attempt['status'],'response_bound_exceeded');self.assertNotIn('original_ref',attempt)

    def test_secret_echo_and_uncheckable_fmp_body_stop_before_original_publication(self):
        url='https://financialmodelingprep.com/stable/key-metrics-ttm?symbol=TEST&apikey=fixture-secret'
        for raw in (b'{"error":"fixture-secret"}',b'{"error":"fixture%2Dsecret"}',
                    b'{"error":"fixture-\\u0073ecret"}',b'not JSON fixture-\\u0073ecret'):
            db=Memory({'by_ticker':{}});c=sources.Capture(db,'fixture','fixture','fixture-secret',opener=lambda *a,**k:response(raw))
            with self.assertRaises(PermissionError):c.acquire(url)
            with self.assertRaises(ValueError):c.finish()
            self.assertEqual(db.puts,[])

    def test_source_archive_failure_keeps_head_and_cache_unchanged(self):
        ns,db,_,_=captured_handler();prior=db.raw[store.HEAD];put=db.put_object
        def fail(**kw):
            if kw['Key'].startswith(sources.PREFIX):raise StorageError('AccessDenied')
            return put(**kw)
        db.put_object=fail
        with self.assertRaises(StorageError):ns['lambda_handler']()
        self.assertEqual(db.raw[store.HEAD],prior);self.assertNotIn('data/backlog-coverage-cache.json',db.raw)

    def test_source_reference_scope_and_compression_bombs_fail_before_reads(self):
        db=Memory({'by_ticker':{}})
        for ref in ({'key':'data/private.json','bytes':2,'sha256':'0'*64},
                    {'key':sources.PREFIX+'0'*64+'.bin','bytes':True,'sha256':'0'*64}):
            with self.assertRaises(ValueError):sources.read_original(db,'fixture',ref)
        self.assertEqual(db.reads,[])
        with patch.object(sources,'DECODED_BOUND',16):
            for encoding,raw in (('gzip',gzip.compress(b'a'*40)),('deflate',zlib.compress(b'a'*40))):
                with self.assertRaises(ValueError):sources.expanded(raw,encoding)

    def test_request_endpoint_validation_cannot_expand_provider_scope(self):
        for url in ('http://www.sec.gov/files/company_tickers.json','https://attacker.invalid/a',
                    'https://financialmodelingprep.com/stable/key-metrics-ttm?symbol=TEST&symbol=OTHER',
                    'https://financialmodelingprep.com/stable/income-statement?symbol=TEST&period=annual&limit=6'):
            with self.assertRaises(ValueError):sources.endpoint(url)
        self.assertNotIn('apikey',sources.endpoint('https://financialmodelingprep.com/stable/key-metrics-ttm?symbol=TEST&apikey=hidden'))

    def test_concurrent_capture_keeps_every_attempt_and_identical_existing_original(self):
        db=Memory({'by_ticker':{}});lock=Lock();barrier=Barrier(4);put=db.put_object;get=db.get_object
        def write(**kw):
            with lock:return put(**kw)
        def read(**kw):
            with lock:return get(**kw)
        db.put_object=write;db.get_object=read
        def open_response(*args,**kwargs):barrier.wait(timeout=5);return response(b'{}')
        c=sources.Capture(db,'fixture','fixture','',opener=open_response)
        with ThreadPoolExecutor(max_workers=4) as ex:
            results=list(ex.map(lambda _:c.acquire('https://www.sec.gov/files/company_tickers.json'),range(4)))
        self.assertEqual([a['request_index'] for a in c.finish()['attempts']],list(range(4)))
        self.assertTrue(all(value=={} for value,attempt in results));self.assertEqual(db.raw[sources.reference(b'{}')['key']],b'{}')

    def test_failed_worker_source_persistence_prevents_partial_success_publication(self):
        ns,db,_,_=captured_handler();prior=db.raw[store.HEAD];put=db.put_object;seen=0
        def write(**kw):
            nonlocal seen
            if kw['Key'].startswith(sources.PREFIX):
                seen+=1
                if seen==2:raise StorageError('AccessDenied')
            return put(**kw)
        db.put_object=write
        with self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(db.raw[store.HEAD],prior);self.assertNotIn('data/backlog-coverage-cache.json',db.raw)


    def test_replay_requires_exact_json_types_and_original_utf8(self):
        ns,db,_,_=captured_handler();ns['lambda_handler']();p=json.loads(db.raw[store.HEAD])
        p['by_ticker']['TEST']['measurements']['rpo']['observations'][0]['source']['val']=100.0
        with self.assertRaises(ValueError):sources.replay(p,lambda ref:sources.read_original(db,'fixture',ref))
        for raw in ('{"test":1}'.encode('utf-16'),'{} '.encode('utf-32')):
            value,status=sources.result(raw,'',200,'https://www.sec.gov/files/company_tickers.json')
            self.assertIsNone(value);self.assertEqual(status,'invalid_response')
        self.assertFalse(sources.same(False,0));self.assertFalse(sources.same(1,1.0))


if __name__ == '__main__': unittest.main()
