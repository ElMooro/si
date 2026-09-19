"""Immutable public originals, deterministic replay and conditional publication."""
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import canonical_macro_sources
import global_liquidity_calendar
import global_liquidity_research as model
import report_observations
import research_brief_model

CURRENT='data/global-liquidity.json'
HISTORY='data/global-liquidity-history.json'
PREFIX='data/global-liquidity-research/'
MAX_BYTES=64*1024*1024
COMPILERS=(model,global_liquidity_calendar,canonical_macro_sources,report_observations,research_brief_model)


def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))


def bounded(stream):
    raw=stream.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('public research object exceeds size bound')
    return raw


def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:
            raise ValueError('public research path required')
        body=client.get_object(Bucket=bucket,Key=key)['Body']
        try:raw=bounded(body)
        finally:body.close()
        if key.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:raw=bounded(stream)
        return raw
    return read


def immutable(client,bucket,key,raw,kind='application/json'):
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
    if raw_reader(client,bucket)(key)!=raw:raise ValueError('immutable artifact differs')


def previous_context(client,bucket,read):
    references={}
    for key in (CURRENT,HISTORY):
        try:raw=read(key)
        except Exception as exc:
            if code(exc) not in ('404','NoSuchKey'):raise
            continue
        try:old=json.loads(raw)
        except (ValueError,UnicodeDecodeError):old=None
        if isinstance(old,dict) and old.get('contract') in (model.CONTRACT,'global-liquidity-history-research.v1'):
            references.update(old.get('legacy_context') or {});continue
        sha=hashlib.sha256(raw).hexdigest();dest=PREFIX+'legacy-unvalidated/'+sha+('.json' if old is not None else '.bin')
        immutable(client,bucket,dest,raw,'application/json' if old is not None else 'application/octet-stream')
        references[key]={'key':dest,'sha256':sha,'bytes':len(raw),'status':'UNQUALIFIED_LEGACY'}
    return references


def publish(client,bucket,key,output):
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body']);etag=obj['ETag']
            try:previous=json.loads(raw)
            except (ValueError,UnicodeDecodeError):previous={}
            if not isinstance(previous,dict):previous={}
        except Exception as exc:
            if code(exc) not in ('404','NoSuchKey'):raise
            previous={};etag=None
        for field in ('generated_at','source_generated_at'):
            if previous.get(field) and output.get(field) and model.clock(previous[field])>model.clock(output[field]):return False
        try:
            client.put_object(Bucket=bucket,Key=key,Body=model.encoded(output),ContentType='application/json',CacheControl='no-store',
                              **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            return True
        except Exception as exc:
            if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
    raise RuntimeError('conditional publication retry limit exceeded')


def run(client,bucket):
    read=raw_reader(client,bucket);source=json.loads(read('data/report-measurements.json'))
    originals=canonical_macro_sources.originals(source,read,model.SERIES)
    legacy=previous_context(client,bucket,read);stamp=datetime.now(timezone.utc).isoformat()
    output=model.build(source,originals,stamp,legacy)
    inputs={'source':source,'legacy_context':legacy};raw=model.encoded(inputs);key=PREFIX+'inputs/'+model.digest(inputs)+'.json'
    immutable(client,bucket,key,raw)
    compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();dest=PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,dest,body,'text/plain');compilers[module.__name__]={'key':dest,'sha256':sha}
    output_raw=model.encoded(output);output_key=PREFIX+'outputs/'+model.digest(output)+'.json'
    immutable(client,bucket,output_key,output_raw)
    manifest={'contract':'global-liquidity-replay.v1','generated_at':stamp,
        'input':{'key':key,'sha256':model.digest(inputs),'bytes':len(raw)},'compilers':compilers,'output_sha256':model.digest(output),
        'output':{'key':output_key,'sha256':model.digest(output),'bytes':len(output_raw)}}
    run_key=PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,run_key,model.encoded(manifest))
    retained=json.loads(read(key))
    if model.digest(model.build(retained['source'],originals,stamp,retained['legacy_context']))!=manifest['output_sha256']:
        raise ValueError('retained input replay differs')
    output['replay']={'manifest_key':run_key,'output_sha256':manifest['output_sha256'],'compilers':compilers}
    published=publish(client,bucket,CURRENT,output);history_published=False
    if published:
        history={'contract':'global-liquidity-history-research.v1','generated_at':stamp,'source_generated_at':source['generated_at'],
            'calendar_research':output['calendar_research'],'source_replay':output['replay'],'legacy_context':legacy,
            'snapshots':[],'calls_eligible':False,'sizing_eligible':False,
            'scope':'Dated current-vintage reconstruction. Previous unqualified snapshots remain immutable in legacy_context.'}
        history_published=publish(client,bucket,HISTORY,history)
    return {'published':published,'history_published':history_published,'generated_at':stamp,'quality':output['quality'],
            'replay':output['replay'],'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
