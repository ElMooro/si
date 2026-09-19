"""Retained input, reviewed compiler and conditional LCE publication."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re

import report_observations
import research_brief_model
import lce_research_model
import lce_research_catalog
import ciss_source_model
import ciss_readthrough
import macro_donor_inputs
import donor_contract
from evidence_store import read_verified
from lce_research_model import build,digest,encoded,clock

CURRENT='data/liquidity-credit-engine.json'
PREFIX='data/lce-research/'
COMPILERS=(lce_research_model,lce_research_catalog,report_observations,research_brief_model,
           ciss_source_model,ciss_readthrough,macro_donor_inputs,donor_contract)
MAX_BYTES=32*1024*1024


def code(exc):return getattr(exc,'response',{}).get('Error',{}).get('Code')


def read(client,bucket,key):
    obj=client.get_object(Bucket=bucket,Key=key);raw=obj['Body'].read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('research object exceeds bound')
    return json.loads(raw),raw,obj['ETag']


def optional(client,bucket,key):
    try:return read(client,bucket,key)[0]
    except Exception as exc:
        if code(exc) not in ('404','NoSuchKey'):raise
        return None


def immutable(client,bucket,key,raw,kind='application/json'):
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
    if client.get_object(Bucket=bucket,Key=key)['Body'].read(len(raw)+1)!=raw:
        raise ValueError('immutable research artifact differs')


def originals(client,bucket,source):
    ref=source.get('replay') or {};key=ref.get('manifest_key','')
    if not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',key):raise ValueError('source replay missing')
    manifest=read(client,bucket,key)[0]
    if key!='data/report-research/runs/'+digest(manifest)+'.json':raise ValueError('source manifest differs')
    reviewed=Path(report_observations.__file__).read_bytes();sha=hashlib.sha256(reviewed).hexdigest()
    if manifest.get('compiler')!={'key':'data/report-research/compilers/'+sha+'.py','sha256':sha}:
        raise ValueError('source compiler differs from reviewed code')
    if client.get_object(Bucket=bucket,Key=manifest['compiler']['key'])['Body'].read(len(reviewed)+1)!=reviewed:
        raise ValueError('retained source compiler differs')
    if ref.get('compiler_sha256')!=sha or ref.get('output_sha256')!=manifest['output_sha256']:
        raise ValueError('source reference differs')
    if digest({k:v for k,v in source.items() if k!='replay'})!=manifest['output_sha256']:
        raise ValueError('source content differs')
    result={}
    for sid in lce_research_catalog.SERIES:
        if sid not in source.get('measurements',{}):continue
        entry=manifest['inputs'][sid]
        result[sid]={'evidence':entry['evidence'],'acquired_at':entry['acquired_at'],
            **{part:json.loads(read_verified(client,bucket,receipt)) for part,receipt in entry['evidence'].items()}}
    return result


def run(client,bucket):
    source=read(client,bucket,'data/report-measurements.json')[0]
    observed=originals(client,bucket,source)
    ciss=optional(client,bucket,'data/ciss-stress.json');repo=optional(client,bucket,'data/repo-market.json')
    stamp=datetime.now(timezone.utc).isoformat()
    output=build(source,ciss,repo,observed,stamp)
    inputs={'macro':source,'ciss':ciss,'repo':repo}
    raw=encoded(inputs);key=PREFIX+'inputs/'+digest(inputs)+'.json'
    immutable(client,bucket,key,raw)
    compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();path=PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,path,body,'text/plain');compilers[module.__name__]={'key':path,'sha256':sha}
    manifest={'contract':'liquidity-credit-replay.v1','generated_at':stamp,
        'input':{'key':key,'sha256':digest(inputs),'bytes':len(raw)},'compilers':compilers,
        'upstream_replay':source['replay'],'output_sha256':digest(output),
        'scope':output['scope']}
    run_key=PREFIX+'runs/'+digest(manifest)+'.json'
    immutable(client,bucket,run_key,encoded(manifest))
    retained=read(client,bucket,key)[0]
    if digest(build(retained['macro'],retained['ciss'],retained['repo'],observed,stamp))!=manifest['output_sha256']:
        raise ValueError('retained LCE replay differs')
    output['replay']={'manifest_key':run_key,'output_sha256':manifest['output_sha256'],'compilers':compilers}
    for _ in range(4):
        try:previous,old,etag=read(client,bucket,CURRENT)
        except Exception as exc:
            if code(exc) not in ('404','NoSuchKey'):raise
            previous=old=etag=None
        if previous:
            if clock(previous['generated_at'])>clock(stamp):return {'published':False,'reason':'newer run current'}
            if previous.get('contract')==output['contract']:
                if clock(previous['source_generated_at'])>clock(source['generated_at']):
                    return {'published':False,'reason':'newer macro source current'}
                newq=output['ciss_systemic'];oldq=previous.get('ciss_systemic') or {}
                if oldq.get('warehouse_generated_at') and newq.get('warehouse_generated_at') and clock(oldq['warehouse_generated_at'])>clock(newq['warehouse_generated_at']):
                    return {'published':False,'reason':'newer CISS source current'}
            else:immutable(client,bucket,PREFIX+'legacy-unvalidated/'+hashlib.sha256(old).hexdigest()+'.json',old)
        try:
            client.put_object(Bucket=bucket,Key=CURRENT,Body=encoded(output),ContentType='application/json',CacheControl='no-cache',
                **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            return {'published':True,'generated_at':stamp,'quality':output['quality'],'replay':output['replay']}
        except Exception as exc:
            if code(exc) not in ('412','PreconditionFailed','409','ConditionalRequestConflict'):raise
    raise RuntimeError('LCE publication race retry limit exceeded')
