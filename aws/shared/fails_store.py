"""Original acquisition, immutable replay and conditional FR2004 publication."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.request
import evidence_store
import fails_native as native
import fails_research as model

MAX_BYTES=24*1024*1024
PRIVATE='audit-private/20260909-originals/fails-research/'
COMPILERS=(model,native)


def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')


def bounded(body,limit=MAX_BYTES):
    try:raw=body.read(limit+1)
    finally:body.close()
    if len(raw)>limit:raise ValueError('fails artifact exceeds byte bound')
    return raw


def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('public evidence path required')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def immutable(client,bucket,key,raw,kind='application/json'):
    if not (key.startswith(model.PREFIX) or key.startswith(PRIVATE)):raise ValueError('retention prefix differs')
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
                          CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('retained bytes differ')


def preserve(client,bucket,read):
    key=model.PREFIX+'migration.json'
    try:marker=native.strict_json(read(key))
    except Exception as exc:
        if not missing(exc):raise
        raw=read(model.CURRENT);old=native.strict_json(raw)
        if old.get('contract')==model.CONTRACT:raise ValueError('legacy retention marker missing')
        sha=hashlib.sha256(raw).hexdigest()
        immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream')
        marker={'contract':'fails-legacy-preservation.v1','sha256':sha,'bytes':len(raw),'source':model.CURRENT,
                'protected_backup':True,'generated_at':old.get('generated_at')}
        immutable(client,bucket,key,native.encoded(marker))
    if marker.get('contract')!='fails-legacy-preservation.v1' or not re.fullmatch('[a-f0-9]{64}',str(marker.get('sha256'))):
        raise ValueError('legacy retention identity differs')
    return marker


def acquire(client,bucket,name):
    url=native.URLS[name]
    req=urllib.request.Request(url,headers={'User-Agent':'JustHodl source research','Accept':'application/pdf' if name in native.DEFINITIONS else '*/*'})
    for attempt in range(2):
        try:
            with urllib.request.build_opener().open(req,timeout=20) as response:raw=response.read(8*1024*1024+1)
            break
        except urllib.error.HTTPError as exc:
            if attempt or exc.code not in (429,500,502,503,504):raise
            time.sleep(2)
    if not raw or len(raw)>8*1024*1024:raise ValueError('original response exceeds capture bound')
    stamp=now()
    receipt=evidence_store.capture(client,bucket,'fr2004',url.split('?')[0],raw,received_at=native.clock(stamp))
    return {'url':url,'acquired_at':stamp,'evidence':receipt}


def previous(read):
    packet=native.strict_json(read(model.CURRENT))
    if packet.get('contract')!=model.CONTRACT:return None
    ref=packet['replay'];key=ref['manifest_key']
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('previous manifest path differs')
    manifest=native.strict_json(read(key))
    if key!=model.PREFIX+'runs/'+native.digest(manifest)+'.json' or ref['output_sha256']!=manifest['output_sha256']:
        raise ValueError('previous manifest differs')
    result=manifest['output'];raw=read(result['key'])
    if result['key']!=model.PREFIX+'outputs/'+result['sha256']+'.json' or result['sha256']!=manifest['output_sha256'] or len(raw)!=result['bytes'] or hashlib.sha256(raw).hexdigest()!=result['sha256']:
        raise ValueError('previous output differs')
    if native.strict_json(raw)!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('previous pointer differs')
    return result


def publish(client,bucket,packet,expected_previous):
    stamp=native.clock(packet['generated_at']);source_clock=native.clock(packet['source_generated_at'])
    for _ in range(5):
        obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=native.strict_json(bounded(obj['Body']))
        if old.get('contract')==model.CONTRACT:
            oldstamp=native.clock(old['generated_at'])
            if oldstamp>stamp or native.clock(old['source_generated_at'])>source_clock:return False
            if oldstamp==stamp:
                if old!=packet:raise ValueError('same-clock conflicting output')
                return True
            # Retain the full correction chain. A concurrent publisher requires a fresh run.
            if expected_previous is None or old['replay']['output_sha256']!=expected_previous['sha256']:return False
            if old['as_of'] and packet['as_of']<old['as_of']:raise ValueError('observation regression')
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=native.encoded(packet),ContentType='application/json',
                              CacheControl='no-store',IfMatch=obj['ETag'])
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('fails publication conflict bound')


def replay(manifest,read):
    if manifest.get('contract')!='fr2004-fails-replay.v1' or set(manifest.get('compilers',{}))!={m.__name__ for m in COMPILERS}:
        raise ValueError('fails replay contract differs')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:
            raise ValueError('reviewed compiler differs; use matching checkout')
    def load(name):
        ref=manifest[name];raw=read(ref['key'])
        if ref['key']!=model.PREFIX+name+'s/'+ref['sha256']+'.json' or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
            raise ValueError('retained '+name+' differs')
        return native.strict_json(raw)
    inputs=load('input')
    if inputs['generated_at']!=manifest['generated_at']:raise ValueError('replay clock differs')
    output=model.compile_research(inputs,read)
    if output!=load('output') or native.digest(output)!=manifest['output_sha256']:raise ValueError('original replay differs')
    return output


def run(client,bucket):
    read=raw_reader(client,bucket);legacy=preserve(client,bucket,read);prior=previous(read)
    names=list(native.URLS)
    with ThreadPoolExecutor(max_workers=4) as pool:
        captures=dict(zip(names,pool.map(lambda name:acquire(client,bucket,name),names)))
    stamp=now();inputs={'contract':'fr2004-original-inputs.v1','generated_at':stamp,'sources':captures,
                       'legacy_context':legacy,'previous_output':prior}
    output=model.compile_research(inputs,read);refs={}
    for name,value in (('input',inputs),('output',output)):
        raw=native.encoded(value);sha=native.digest(value);key=model.PREFIX+name+'s/'+sha+'.json'
        immutable(client,bucket,key,raw);refs[name]={'key':key,'sha256':sha,'bytes':len(raw)}
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,raw,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    manifest={'contract':'fr2004-fails-replay.v1','generated_at':stamp,'compilers':compilers,**refs,'output_sha256':refs['output']['sha256']}
    key=model.PREFIX+'runs/'+native.digest(manifest)+'.json';immutable(client,bucket,key,native.encoded(manifest))
    replay(manifest,read)
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']}
    published=publish(client,bucket,{**output,'replay':ref},prior)
    return {'published':published,'generated_at':stamp,'replay':ref,'quality':output['quality'],
            'original_series':len(output['series_coverage']),'retained_observations':sum(r['observations'] for r in output['series_coverage'].values()),
            'signals_emitted':0,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
