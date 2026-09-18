"""Future windows, original bytes, traversal coverage and publication tests."""
import ast
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import types
import urllib.parse
import urllib.request
HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(HERE.parents[2]/'shared'),str(HERE.parents[2]/'shared/tests')]
from test_prospective_journal import Store, recorded, NOW
from prospective_journal import PREFIX,canonical,digest,read_record,persist_once,stamp,projection,register,ensure_protocol
from instrument_identity import resolve_instrument
from forward_price_measurement import CONTRACT,evaluate,marks
from outcome_price_evidence import PriceEvidenceVerifier,EASTERN
from evidence_store import capture


class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}


class Frozen(datetime):
    @classmethod
    def now(cls,tz=None):return NOW if tz else NOW.replace(tzinfo=None)


def loaded(store):
    original=store.get_object
    def get(**kw):
        if kw['Key'] not in store.rows:raise Missing()
        return original(**kw)
    store.get_object=get
    def listing(**kw):
        keys=sorted(k for k in store.rows if k.startswith(kw['Prefix']) and k>kw.get('StartAfter',''))
        return {'Contents':[{'Key':k} for k in keys[:kw['MaxKeys']]],'IsTruncated':len(keys)>kw['MaxKeys']}
    store.list_objects_v2=listing
    def publish(client,bucket,key,document):store.put_object(Bucket=bucket,Key=key,Body=canonical(document))
    env=dict(gzip=gzip,hashlib=hashlib,io=io,json=json,os=os,Path=Path,re=re,time=time,
             urllib=urllib,datetime=Frozen,timezone=timezone,timedelta=timedelta,
             PREFIX=PREFIX,canonical=canonical,digest=digest,read_record=read_record,persist_once=persist_once,stamp=stamp,
             CONTRACT=CONTRACT,evaluate=evaluate,marks=marks,PriceEvidenceVerifier=PriceEvidenceVerifier,EASTERN=EASTERN,
             s3=store,BUCKET='fixture',STATE=PREFIX+'evaluator-state.json',SUMMARY='data/prospective-outcomes.json',
             capture=capture,publish_current=publish,managed_secret=lambda *a:'fixture')
    tree=ast.parse((HERE.parent/'source/lambda_function.py').read_text())
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef)],type_ignores=[]),'evaluator','exec'),env)
    env['compiler_identity']=lambda:{'fixture':'c'*64}
    return env


def test_new_forecasts_remain_pending_without_quote_requests_or_fake_results():
    store,_,refs=recorded();env=loaded(store)
    env['source_packet']=lambda *a:(_ for _ in ()).throw(AssertionError('future quote requested'))
    result=env['run']();summary=json.loads(store.rows[env['SUMMARY']]['Body'])
    assert result['status_counts']=={'PENDING_FORWARD_WINDOW':2}
    assert summary['coverage']['scan_complete_in_this_run'] is True
    assert summary['forecasts_checked']==1 and summary['net_return_pct'] is None
    assert summary['archive_checks']['marks_verified']==0 and not summary['sizing_eligible']
    assert not any('/measurements/' in k for k in store.rows)


def test_corrupt_forecast_is_reported_and_cannot_be_measured():
    store,_,refs=recorded();store.rows[refs[0]['key']]['LastModified']=NOW+timedelta(days=2)
    env=loaded(store);env['run']();summary=json.loads(store.rows[env['SUMMARY']]['Body'])
    assert summary['status_counts']=={'EVIDENCE_OR_REPLAY_REJECTED':1} and summary['evidence_errors']==1


def test_invalid_cursor_cannot_read_other_prefixes():
    store,_,_=recorded();env=loaded(store)
    store.put_object(Key=env['STATE'],Body=canonical({'next_after':'portfolio/snapshot.json'}))
    try:env['run']();raise AssertionError('invalid cursor accepted')
    except ValueError:pass


def test_private_evidence_path_is_rejected_before_a_read():
    store=Store();env=loaded(store)
    try:env['retained_packet']({'evidence':{'key':'portfolio/risk.json'}});raise AssertionError('private evidence accepted')
    except ValueError:pass


def test_actual_handler_retains_and_replays_measured_fixture_without_rewriting_it():
    from test_forward_price_measurement import fixture
    store,record,packet,_=fixture();env=loaded(store)
    class Later(datetime):
        @classmethod
        def now(cls,tz=None):return store.now
    env['datetime']=Later
    data={sym:packet(sym,[100,101,102,103,104,110 if sym=='AAA' else 105]) for sym in ('AAA','SPY')}
    env['source_packet']=lambda sym,*a:data[sym]
    first=env['run']();assert first['status_counts']['MEASURED_PRICE_ONLY']==1
    key=PREFIX+'measurements/'+record['forecast_id']+'/s5.json';saved=store.rows[key]['Body']
    second=env['run']();assert second['status_counts']['MEASURED_PRICE_ONLY']==1 and store.rows[key]['Body']==saved
    changed=json.loads(saved);changed['output']['asset_price_return_pct']=999
    store.rows[key]['Body']=canonical(changed)
    assert env['run']()['status_counts']=={'EVIDENCE_OR_REPLAY_REJECTED':1}


def test_request_budget_advances_only_fully_checked_forecasts_without_starvation():
    store=Store();protocol=ensure_protocol(store,'fixture')
    for i in range(40):
        sym='A'+chr(65+i//26)+chr(65+i%26)
        pick={'identity':resolve_instrument(sym),'direction':'UP','prediction_origin':'explicit_direction'}
        src=projection('data/example.json',{'generated_at':NOW.isoformat()},[pick],format(i,'064x'),NOW.isoformat())
        register(store,'fixture',src,protocol,'c'*64,NOW)
    env=loaded(store)
    class Later(datetime):
        @classmethod
        def now(cls,tz=None):return NOW+timedelta(days=10)
    env['datetime']=Later;env['source_packet']=lambda *a:None
    first=env['run']();assert first['forecasts_checked']==29
    cursor=json.loads(store.rows[env['STATE']]['Body'])['next_after'];assert cursor
    second=env['run']();assert second['forecasts_checked']==11
    assert json.loads(store.rows[env['STATE']]['Body'])['next_after'] is None


def test_market_probe_archives_prices_without_creating_a_forecast_or_measurement():
    from test_forward_price_measurement import fixture
    store,_,packet,_=fixture();env=loaded(store)
    class Later(datetime):
        @classmethod
        def now(cls,tz=None):return store.now
    env['datetime']=Later;env['source_packet']=lambda *a:packet('SPY',[100]*6)
    before=set(store.rows);result=env['lambda_handler']({'validation_only':True},None)
    assert result['observation_count']==6 and result['archive_checks']['marks_verified']==6
    assert result['forecast_writes']==0 and result['legacy_ledger_writes']==0
    assert all(k.startswith('data/evidence/') for k in set(store.rows)-before)


if __name__=='__main__':
    tests=[(n,f) for n,f in sorted(globals().items()) if n.startswith('test_') and callable(f)]
    for n,f in tests:f();print('ok',n)
    print('Evaluator tests passed:',len(tests))
    subprocess.run([sys.executable,str(HERE.parents[2]/'shared/tests/test_forward_price_measurement.py')],check=True)
