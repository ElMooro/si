"""Retain exact public research inputs and compiler; publish with a clocked CAS.

Only the new research namespace is writable. The predecessor packet and all
private learning/account outputs remain outside this store's read boundary.
"""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,importlib,re,sys,time
import genealogy_public_archive as archive
import genealogy_research_model as model

PREFIX='data/signal-genealogy-research/'
CURRENT=PREFIX+'current.json'
MAX=64*1024*1024
COMPILERS=('genealogy_research_store','genealogy_research_model','genealogy_public_archive',
           'genealogy_registration_model','genealogy_capture_timing','prospective_journal',
           'research_identity','instrument_identity','private_artifact','managed_secret')


def sha(raw):return hashlib.sha256(raw).hexdigest()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def conflict(exc):return code(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')
def allowed(key):
    return type(key) is str and (key==CURRENT or re.fullmatch(re.escape(PREFIX)+r'(inputs|outputs|runs|compilers)/[a-f0-9]{64}\.(json|py)',key) is not None)


def bounded(obj):
    try:raw=obj['Body'].read(MAX+1)
    finally:obj['Body'].close()
    if len(raw)>MAX or type(obj.get('ContentLength')) is not int or len(raw)!=obj['ContentLength']:
        raise ValueError('research_object_length')
    return raw


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('unreviewed_research_read')
        return bounded(client.get_object(Bucket=bucket,Key=key))
    return read


def checked(ref,kind,read):
    if type(ref) is not dict or set(ref)!={'key','sha256','bytes'} or not archive.hex64(ref['sha256']):
        raise ValueError('typed_artifact_reference')
    if type(ref['bytes']) is not int or not 0<ref['bytes']<=MAX:raise ValueError('bounded_artifact_reference')
    suffix='.py' if kind=='compilers' else '.json'
    if ref['key']!=PREFIX+kind+'/'+ref['sha256']+suffix:raise ValueError('artifact_reference_path')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('artifact_reference_bytes')
    return raw


def retain_bytes(client,bucket,kind,raw):
    if kind not in ('inputs','outputs','runs','compilers') or type(raw) is not bytes or not 0<len(raw)<=MAX:
        raise ValueError('bounded_research_artifact')
    suffix='.py' if kind=='compilers' else '.json'
    ref={'key':PREFIX+kind+'/'+sha(raw)+suffix,'sha256':sha(raw),'bytes':len(raw)}
    try:
        client.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',
                          ContentType='text/x-python' if kind=='compilers' else 'application/json',
                          CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if checked(ref,kind,reader(client,bucket))!=raw:raise ValueError('immutable_research_readback')
    return ref


def compiler_bytes():
    return {name:Path(importlib.import_module(name).__file__).read_bytes() for name in COMPILERS}


def retain(client,bucket,inputs,output):
    if archive.canonical(model.compile_frozen(inputs))!=archive.canonical(output):raise ValueError('frozen_replay_differs')
    manifest={'contract':'genealogy-native-replay.v1','generated_at':output['generated_at'],
              'input':retain_bytes(client,bucket,'inputs',archive.canonical(inputs)),
              'output':retain_bytes(client,bucket,'outputs',archive.canonical(output)),
              'compilers':{name:retain_bytes(client,bucket,'compilers',raw) for name,raw in compiler_bytes().items()},
              'scope':'Complete retained public journal reconciliation and frozen compiler replay; original upstream engine-model replay remains unverified.'}
    ref=retain_bytes(client,bucket,'runs',archive.canonical(manifest))
    # Re-read the stored inputs/output and every compiler; never execute stored code.
    if archive.canonical(replay(ref,reader(client,bucket)))!=archive.canonical(output):raise ValueError('retained_replay_differs')
    return ref


def replay(ref,read):
    manifest=archive.strict_json(checked(ref,'runs',read))
    if type(manifest) is not dict or set(manifest)!={'contract','generated_at','input','output','compilers','scope'} or manifest['contract']!='genealogy-native-replay.v1':
        raise ValueError('replay_manifest_schema')
    current=compiler_bytes()
    if type(manifest['compilers']) is not dict or set(manifest['compilers'])!=set(current):raise ValueError('complete_compiler_closure_required')
    for name,raw in current.items():
        if checked(manifest['compilers'][name],'compilers',read)!=raw:raise ValueError('reviewed_compiler_closure_differs')
    inputs=archive.strict_json(checked(manifest['input'],'inputs',read))
    output=model.compile_frozen(inputs)
    if archive.canonical(output)!=checked(manifest['output'],'outputs',read) or output['generated_at']!=manifest['generated_at']:
        raise ValueError('retained_research_output_differs')
    return output


def publish(client,bucket,packet):
    if type(packet) is not dict or packet.get('contract')!='genealogy-research-head.v1':raise ValueError('public_head_contract')
    # A caller cannot attach a good replay receipt to modified summary fields.
    output=replay(packet['replay'],reader(client,bucket))
    if archive.canonical({k:v for k,v in packet.items() if k!='replay'})!=archive.canonical(model.summary(output)):
        raise ValueError('public_head_replay_differs')
    at=archive.clock(packet['generated_at']);raw=archive.canonical(packet)
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=CURRENT);previous=archive.strict_json(bounded(obj))
            if previous.get('contract')!='genealogy-research-head.v1':raise ValueError('existing_head_contract')
            old=archive.clock(previous['generated_at'])
            if old>at:return False
            if old==at:
                if archive.canonical(previous)!=raw:raise ValueError('conflicting_same_clock_publication')
                return True
            condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if code(exc) not in ('NoSuchKey','404'):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=CURRENT,Body=raw,ContentType='application/json',CacheControl='no-store',**condition)
            observed=reader(client,bucket)(CURRENT)
            if observed!=raw:
                new=archive.strict_json(observed)
                if new.get('contract')!='genealogy-research-head.v1' or archive.clock(new['generated_at'])<=at:
                    raise ValueError('public_head_readback_differs')
                return False
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('public_head_conflict_limit')


def inventory(client,prefix,cutoff):
    if prefix not in (archive.PREFIX+'captures/',archive.PREFIX+'records/'):raise ValueError('unreviewed_inventory_prefix')
    at=archive.clock(cutoff);rows=[];seen=set();pages=0;later=0
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=archive.BUCKET,Prefix=prefix):
        pages+=1
        for item in page.get('Contents',[]):
            key=item['Key'];stored=item['LastModified'];size=item['Size']
            if type(key) is not str or not re.fullmatch(re.escape(prefix)+r'[a-f0-9]{64}\.json',key) or key in seen:
                raise ValueError('invalid_inventory_identity')
            seen.add(key)
            if not isinstance(stored,datetime) or stored.tzinfo is None or type(size) is not int or not 0<size<=archive.MAX_OBJECT_BYTES:
                raise ValueError('invalid_inventory_metadata')
            if stored>at:later+=1;continue
            rows.append({'key':key,'bytes':size,'last_modified':stored.astimezone(timezone.utc).isoformat()})
    rows.sort(key=lambda row:row['key'])
    return {'prefix':prefix,'cutoff':cutoff,'listing_complete':True,'listing_pages':pages,'objects':rows,
            'objects_at_cutoff':len(rows),'objects_after_cutoff':later,'total_bytes':sum(r['bytes'] for r in rows),
            'inventory_sha256':sha(archive.canonical(rows))}
