"""Retain public projections and exact outputs; never execute downloaded compiler code."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import alpha_research as model

MAX_BYTES = 16*1024*1024
PRIVATE_BACKUP = 'audit-private/20260909-originals/alpha-research/'
COMPILERS = (model,)


def now(): return datetime.now(timezone.utc).isoformat()
def code(exc): return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc): return code(exc) in ('NoSuchKey','404')
def conflict(exc): return code(exc) in ('PreconditionFailed','ConditionalRequestConflict','412','409')


def bounded(body):
    try: raw=body.read(MAX_BYTES+1)
    finally: body.close()
    if len(raw)>MAX_BYTES: raise ValueError('Alpha artifact size bound')
    return raw


def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:
            raise ValueError('registered public artifact path required')
        return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    return read


def immutable(client,bucket,key,raw,kind='application/json',private=False):
    if private and not key.startswith(PRIVATE_BACKUP): raise ValueError('protected legacy prefix required')
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
                          CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw: raise ValueError('retained Alpha bytes differ')


def preserve_legacy(client,bucket,kind):
    """Use the pre-existing anonymously denied backup prefix, not public research archives."""
    marker=model.PREFIX+'migration-'+kind+'.json'; read=raw_reader(client,bucket)
    try:
        old=json.loads(read(marker))
        if old.get('contract')!='alpha-legacy-preservation.v1': raise ValueError('legacy preservation marker differs')
        return
    except Exception as exc:
        if not missing(exc): raise
    keys=[model.CURRENT,'data/alpha-compass-history.json'] if kind=='compass' else [model.BRIEF,'data/alpha-brief.md']
    retained=[]
    for key in keys:
        try: raw=read(key)
        except Exception as exc:
            if missing(exc): continue
            raise
        sha=hashlib.sha256(raw).hexdigest(); target=PRIVATE_BACKUP+sha+'.bin'
        immutable(client,bucket,target,raw,'application/octet-stream',True)
        retained.append({'source':key,'sha256':sha,'bytes':len(raw),'protected_backup':True})
    immutable(client,bucket,marker,model.encoded({'contract':'alpha-legacy-preservation.v1','objects':retained}))


def collect(read):
    sources={}
    for name,key in model.SOURCES.items():
        status='captured_projection'; sha=None
        try:
            raw=read(key); sha=hashlib.sha256(raw).hexdigest(); document=json.loads(raw)
            if not isinstance(document,dict) or not document: status='source_unavailable';document={}
        except Exception: status='source_read_failed';document={}
        try: projection=model.project(name,document)
        except (ValueError,TypeError,KeyError,OverflowError):
            status='source_contract_rejected';projection=model.project(name,{})
        sources[name]={'source':key,'acquired_at':now(),'status':status,'projection':projection,
            'projection_sha256':model.digest(projection),'source_packet_sha256':sha,
            'basis':'typed_public_input_projection; complete source packet and provider originals not publicly archived'}
    return {'generated_at':now(),'sources':sources}


def compiler_refs(client,bucket):
    refs={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,raw,'text/x-python');refs[module.__name__]={'key':key,'sha256':sha}
    return refs


def retain(client,bucket,inputs,output,kind):
    refs={}
    for name,value in (('input',inputs),('output',output)):
        raw=model.encoded(value);sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+name+'s/'+sha+'.json'
        immutable(client,bucket,key,raw);refs[name]={'key':key,'sha256':sha,'bytes':len(raw)}
    manifest={'contract':'alpha-replay.v1','kind':kind,'generated_at':output['generated_at'],
              'compilers':compiler_refs(client,bucket),**refs,'output_sha256':refs['output']['sha256'],
              'scope':'typed_public_input_replay; not_original_provider_evidence'}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json'
    immutable(client,bucket,key,model.encoded(manifest))
    ref={'manifest_key':key,'output_sha256':refs['output']['sha256']}
    if replay(manifest,raw_reader(client,bucket))!=output: raise ValueError('Alpha compiler replay differs')
    return ref


def replay(manifest,read):
    if manifest.get('contract')!='alpha-replay.v1' or manifest.get('kind') not in ('compass','brief'): raise ValueError('Alpha replay contract required')
    if set(manifest['compilers'])!={m.__name__ for m in COMPILERS}: raise ValueError('compiler set differs')
    for module in COMPILERS:
        local=Path(module.__file__).read_bytes();sha=hashlib.sha256(local).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=local:
            raise ValueError('reviewed compiler differs; use matching release checkout')
    def load(name):
        ref=manifest[name];raw=read(ref['key'])
        if ref['key']!=model.PREFIX+name+'s/'+ref['sha256']+'.json' or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
            raise ValueError('retained Alpha '+name+' differs')
        return json.loads(raw)
    inputs=load('input')
    if manifest['kind']=='compass': output=model.build(inputs)
    else:
        ref=inputs['source_replay'];child=load_manifest(ref,read)
        if child['kind']!='compass': raise ValueError('brief must bind a Compass snapshot')
        compass=replay(child,read)
        output=model.build_brief(compass,inputs['generated_at'],ref)
    retained=load('output')
    if output!=retained or model.digest(output)!=manifest['output_sha256'] or output['generated_at']!=manifest['generated_at']:
        raise ValueError('reproduced Alpha output differs')
    return output


def load_manifest(ref,read):
    key=ref.get('manifest_key')
    if not isinstance(key,str) or not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):
        raise ValueError('Alpha run identity required')
    raw=read(key);manifest=json.loads(raw)
    if key!=model.PREFIX+'runs/'+hashlib.sha256(raw).hexdigest()+'.json' or manifest['output_sha256']!=ref.get('output_sha256'):
        raise ValueError('Alpha manifest identity differs')
    return manifest


def publish(client,bucket,key,packet):
    stamp=model.clock(packet['generated_at'])
    for _ in range(5):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);previous=json.loads(bounded(obj['Body']))
            if previous.get('contract') in (model.CONTRACT,'alpha-brief-research.v1'):
                old=model.clock(previous['generated_at'])
                if old>stamp: return False
                if old==stamp and previous!=packet: raise ValueError('conflicting Alpha snapshots at same clock')
            condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=key,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('Alpha current publication conflict; immutable run retained')


def run_compass(client,bucket):
    preserve_legacy(client,bucket,'compass')
    inputs=collect(raw_reader(client,bucket));output=model.build(inputs);ref=retain(client,bucket,inputs,output,'compass')
    published=publish(client,bucket,model.CURRENT,{**output,'replay':ref})
    return {'published':published,'replay':ref,'ideas':len(output['research_ideas']),'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0}


def run_brief(client,bucket):
    preserve_legacy(client,bucket,'brief');read=raw_reader(client,bucket)
    packet=json.loads(read(model.CURRENT));ref=packet['replay'];manifest=load_manifest(ref,read)
    if manifest['kind']!='compass': raise ValueError('current Compass contract required')
    compass=replay(manifest,read)
    if {k:v for k,v in packet.items() if k!='replay'}!=compass: raise ValueError('current Alpha pointer differs')
    at=now();inputs={'generated_at':at,'source_replay':ref};output=model.build_brief(compass,at,ref)
    brief_ref=retain(client,bucket,inputs,output,'brief');published=publish(client,bucket,model.BRIEF,{**output,'replay':brief_ref})
    # Existing markdown URL is a static entry point into the authoritative atomic JSON.
    # It cannot accidentally serve a different run's advice during concurrent writes.
    if published:
        text='# Alpha research brief\n\nRead the current reproducible brief at https://justhodl.ai/alpha-compass.html#brief .\n\n**WAIT** — research abstention; no portfolio change.\n'
        client.put_object(Bucket=bucket,Key='data/alpha-brief.md',Body=text.encode(),ContentType='text/markdown; charset=utf-8',CacheControl='no-store')
    return {'published':published,'replay':brief_ref,'source_replay':ref,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0}
