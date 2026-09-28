"""Harvest semantic identity and direction, without calling price providers."""
import ast
import io
import json
import math
import hashlib
import re
import sys
import time
import types
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[2]/'shared'))
from instrument_identity import resolve_instrument
from research_identity import resolve_pick_identity, identity_policy, research_output_source, source_selection_policy
from private_artifact import public_source_allowed
from prospective_journal import projection
import research_source_reader
TREE=ast.parse((HERE.parent/'source/lambda_function.py').read_text(encoding='utf-8'))


def load():
    env=dict(json=json,math=math,re=re,time=time,uuid=uuid,datetime=datetime,timezone=timezone,timedelta=timedelta,
             Decimal=Decimal,ThreadPoolExecutor=ThreadPoolExecutor,as_completed=as_completed,resolve_instrument=resolve_instrument,
             urllib=types.SimpleNamespace(parse=urllib.parse,request=types.SimpleNamespace(Request=urllib.request.Request)),
             FMP='fixture',POLYGON='fixture',_trust=lambda *a:1,Path=Path,hashlib=hashlib,
             __file__=str(HERE.parent/'source/lambda_function.py'),public_source_allowed=public_source_allowed,projection=projection,research_source_reader=research_source_reader,
             resolve_pick_identity=resolve_pick_identity,identity_policy=identity_policy,
             research_output_source=research_output_source,source_selection_policy=source_selection_policy)
    names={'VERSION','S3_BUCKET','SIGNALS_TABLE','SEEN_KEY','SUMMARY_KEY','TOP_PER_ENGINE','DEDUP_DAYS','WINDOWS','MAX_SIGNALS','TICKER_RE','LIST_KEYS','SYM_KEYS','SCORE_KEYS','SKIP_SUBSTR'}
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) or isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in names for t in n.targets)]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'harvester','exec'),env)
    return env


def test_crypto_source_and_untyped_tokens_cannot_borrow_equity_identity():
    env=load()
    for symbol in ('BTC','ETH','SOL','XRP','LTC','BCH','DOGE','ADA','AVAX','LINK'):
        item={'symbol':symbol,'direction':'UP'}
        assert env['extract_picks']({'picks':[item]})[0]['identity'] is None
        token=env['extract_picks']({'picks':[item]},'data/crypto-emergence.json')[0]
        assert token['identity']['asset_class']=='crypto'
        typed=env['extract_picks']({'picks':[{**item,'asset_class':'equity'}]},'data/equity-research.json')[0]
        assert typed['identity']['instrument_id']=='equity:US:'+symbol
        conflict=env['extract_picks']({'picks':[{**item,'asset_class':'equity'}]},'data/crypto-emergence.json')[0]
        assert conflict['identity'] is None
    env['urllib'].request.urlopen=lambda *a,**k:(_ for _ in ()).throw(AssertionError('Ambiguous ticker must not fetch a price'))
    assert env['get_price']('LTC') is None
    rows=env['extract_picks']({'picks':[{'symbol':'LTC','direction':'UP'}]},'data/crypto-emergence.json')
    source=projection('data/crypto-emergence.json',{'generated_at':datetime.now(timezone.utc).isoformat()},rows,'a'*64,datetime.now(timezone.utc).isoformat())
    assert source['observations']==[] and source['unsupported_identity_count']==1
    assert resolve_pick_identity('DOT') is None
    assert resolve_pick_identity('DOT',source_key='data/crypto-emergence.json') is None


def test_release_preserves_existing_schedule_bindings():
    sys.path.insert(0,str(HERE.parents[3]/'scripts'))
    from normalize_lambda_config import normalize_config
    config=json.loads((HERE.parent/'config.json').read_bytes())
    normalized=normalize_config(config)
    assert 'schedule' not in normalized
    assert normalized['release_schedule_note']['binding_action']=='PRESERVE_EXISTING'
    assert config['preserved_schedule_reference']['cron']==config['schedule']=='cron(15 23 * * ? *)'


def test_rank_membership_is_not_an_up_forecast_and_explicit_short_is_preserved():
    env=load();rows=env['extract_picks']({'top_picks':[{'ticker':'AAA','score':50},{'ticker':'BBB','side':'SHORT'},{'ticker':'BTC'}]})
    assert rows[0]['direction']=='NEUTRAL' and rows[0]['prediction_origin']=='rank_observation'
    assert rows[1]['direction']=='DOWN' and rows[1]['prediction_origin']=='explicit_direction'
    assert rows[2]['identity'] is None


def test_provider_routes_cannot_turn_bitcoin_into_the_btc_fund():
    env=load();calls=[]
    def open_quote(req,timeout):
        calls.append(req.full_url)
        symbol=urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query)['symbol'][0]
        return io.BytesIO(json.dumps([{'symbol':symbol,'price':80000 if symbol=='BTCUSD' else 28}]).encode())
    env['urllib'].request.urlopen=open_quote
    assert env['get_price']('BTC') is None and not calls
    assert env['get_price']('BTC',resolve_instrument('BTC','crypto'))==80000
    assert 'symbol=BTCUSD' in calls[-1]
    assert env['get_price']('BTC',resolve_instrument('BTC','etf'))==28


def test_wrong_provider_symbol_and_nonfinite_price_are_unavailable():
    env=load()
    env['urllib'].request.urlopen=lambda *a,**kw:io.BytesIO(json.dumps([{'symbol':'WRONG','price':28}]).encode())
    assert env['get_price']('AAA') is None
    env['urllib'].request.urlopen=lambda *a,**kw:io.BytesIO(json.dumps([{'symbol':'AAA','price':float('inf')}]).encode())
    assert env['get_price']('AAA') is None


def test_handler_records_observation_origin_and_skips_ambiguous_identity():
    env=load();written=[];objects={}
    class Writer:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def put_item(self,Item):written.append(Item)
    env['ddb']=types.SimpleNamespace(Table=lambda _:types.SimpleNamespace(batch_writer=Writer))
    env['s3']=types.SimpleNamespace(put_object=lambda **kw:objects.update({kw['Key']:kw['Body']}))
    env['_read']=lambda key: {'top_picks':[{'ticker':'AAA'},{'ticker':'BTC'},{'ticker':'BBB','side':'SHORT'}]} if key=='data/example.json' else {}
    env['list_outputs']=lambda:['data/example.json'];env['get_price']=lambda *a:100
    doc=env['_read']('data/example.json');doc['generated_at']=datetime.now(timezone.utc).isoformat()
    env['read_research_source']=lambda key:(doc,'a'*64,datetime.now(timezone.utc).isoformat())
    env['ensure_protocol']=lambda *a:{};env['register']=lambda *a:[]
    env['publish_journal']=lambda *a:{'capture':{},'records_in_capture':0,'new_records':0,'coverage':{}}
    env['lambda_handler']({},None)
    assert len(written)==2
    assert written[0]['predicted_direction']=='NEUTRAL' and written[1]['predicted_direction']=='DOWN'
    assert all(not r['sizing_eligible'] and r['baseline_status']=='QUOTE_CONTEXT_ONLY_UNVERIFIED_EXECUTION' for r in written)
    summary=json.loads(objects[env['SUMMARY_KEY']]);assert summary['n_skipped_ambiguous_identity']==1


def test_capture_only_path_registers_before_quotes_without_legacy_writes():
    env=load();events=[]
    env['s3']=object();env['list_outputs']=lambda:['data/example.json'];env['_read']=lambda key:{}
    env['ensure_protocol']=lambda *a:events.append('protocol') or {}
    env['read_research_source']=lambda key:({'generated_at':datetime.now(timezone.utc).isoformat(),'top_picks':[{'ticker':'AAA','side':'SHORT'}]},'a'*64,datetime.now(timezone.utc).isoformat())
    env['register']=lambda *a:events.append('registered') or []
    env['publish_journal']=lambda *a:events.append('manifest') or {'capture':{},'records_in_capture':1,'new_records':1}
    env['get_price']=lambda *a:(_ for _ in ()).throw(AssertionError('quote call in capture-only path'))
    result=env['lambda_handler']({'capture_only':True},None)
    assert result['legacy_ledger_writes']==0 and events==['protocol','registered','manifest']


def test_capture_scan_is_not_truncated_by_legacy_signal_limit():
    env=load();scanned=[];env['MAX_SIGNALS']=1
    env['s3']=object();env['list_outputs']=lambda:['data/example.json','data/second.json'];env['_read']=lambda key:{}
    env['ensure_protocol']=lambda *a:{}
    def source(key):
        scanned.append(key)
        return {'generated_at':datetime.now(timezone.utc).isoformat(),'top_picks':[{'ticker':'AAA','side':'SHORT'}]},'a'*64,datetime.now(timezone.utc).isoformat()
    env['read_research_source']=source;env['register']=lambda *a:[]
    env['publish_journal']=lambda *a:{'capture':{},'records_in_capture':2,'new_records':2}
    env['lambda_handler']({'capture_only':True},None)
    assert len(scanned)==2


def test_unsupported_projection_reports_gap_without_losing_other_sources():
    env=load();problems=[];registered=[]
    env['s3']=object();env['list_outputs']=lambda:['data/unsupported@name.json','data/example.json'];env['_read']=lambda key:{}
    env['ensure_protocol']=lambda *a:{}
    env['read_research_source']=lambda key:({'generated_at':datetime.now(timezone.utc).isoformat(),'top_picks':[{'ticker':'AAA','side':'SHORT'}]},'a'*64,datetime.now(timezone.utc).isoformat())
    env['register']=lambda *a:registered.append(a[2]['source_key']) or []
    def published(*args):
        problems.extend(args[3]);return {'capture':{},'records_in_capture':1,'new_records':1}
    env['publish_journal']=published
    env['lambda_handler']({'capture_only':True},None)
    assert registered==['data/example.json']
    assert problems==[{'source_key':'data/unsupported@name.json','reason':'UNSUPPORTED_SOURCE_PROJECTION'}]


def test_whole_predecessor_self_ingests_its_summary_but_repair_excludes_before_read():
    env=load();legacy=load()
    fixture=HERE.parents[3]/'tests/fixtures/pre-research-self-ingestion-harvester.py.txt'
    tree=ast.parse(fixture.read_bytes())
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef)],type_ignores=[]),'whole_predecessor_functions','exec'),legacy)
    keys=['data/prospective-research.json','data/prospective-outcomes.json','data/example.json','data/prospective-research-alternative.json']
    def paginator(name):
        assert name=='list_objects_v2'
        return types.SimpleNamespace(paginate=lambda **kw:iter([{'Contents':[{'Key':k} for k in keys]}]))
    for module in (env,legacy):module['s3']=types.SimpleNamespace(get_paginator=paginator)
    assert legacy['list_outputs']()==keys
    assert env['list_outputs']()==keys[2:]
    summary={'record_previews':[{'symbol':'AAA','direction':'UP'}]}
    assert legacy['extract_picks'](summary,'data/prospective-research.json')[0]['prediction_origin']=='explicit_direction'
    assert env['extract_picks'](summary,'data/prospective-research.json')==[]
    class NoRead:
        def get_object(self,**kw):raise AssertionError('Research summary must be excluded before acquisition')
    env['s3']=NoRead()
    for key in keys[:2]:
        try:env['read_research_source'](key)
        except ValueError as exc:assert str(exc)=='RESEARCH_OUTPUT_ECHO'
        else:raise AssertionError('Own derived output was acquired')


def test_source_selection_policy_is_retained_in_both_whole_capture_and_summary():
    env=load();writes={}
    env['s3']=object();env['JOURNAL_PREFIX']='data/research-forecasts/'
    from prospective_journal import digest
    env['digest']=digest
    def once(client,bucket,key,doc):
        writes[key]=doc;return {'key':key,'sha256':digest(doc)}
    env['persist_once']=once
    env['publish_current']=lambda client,bucket,key,doc:writes.update({key:doc})
    result=env['publish_journal']([],[],{},[],0,0,datetime.now(timezone.utc))
    assert result['source_selection_policy']==source_selection_policy()
    assert writes[result['capture']['key']]['source_selection_policy']==source_selection_policy()
    assert result['sizing_eligible'] is False


if __name__=='__main__':
    tests=[(n,f) for n,f in sorted(globals().items()) if n.startswith('test_') and callable(f)]
    for n,f in tests:f();print('ok',n)
    print('Harvester integrity tests passed:',len(tests))
    import subprocess
    subprocess.run([sys.executable,str(HERE.parents[2]/'shared/tests/test_prospective_journal.py')],check=True)
    subprocess.run([sys.executable,str(HERE.parents[3]/'tests/test_research_source_reader.py')],check=True)
