"""Retain and replay complete public originals before publishing Genealogy.

The portable checkpoint is an immutable original archive for this run. Its
disposable CURRENT cache pointer is not publication proof. No stored SQL or
compiler is executed, and no live learning/consumer output is read.
"""
import hashlib
import importlib
from pathlib import Path
import re
import tempfile

import genealogy_cache_checkpoint as checkpoint
import genealogy_public_archive as archive
import genealogy_revision_cache as revision
import genealogy_streamed_pipeline as pipeline

PREFIX = 'data/signal-genealogy-research/'
CURRENT = PREFIX+'current.json'
CONTRACT = 'genealogy-streamed-replay.v1'
MAX = 64*1024*1024
COMPILERS = ('genealogy_native_publication','genealogy_native_runtime','genealogy_streamed_pipeline',
    'genealogy_cache_checkpoint','genealogy_revision_cache','genealogy_spooled_timing',
    'genealogy_source_fold','genealogy_research_model','genealogy_public_archive',
    'genealogy_registration_model','genealogy_capture_timing','prospective_journal',
    'research_identity','instrument_identity','private_artifact','managed_secret')
FILES = {'inputs':'inputs.json','output':'output.json','membership':'membership.json','head':'head.json'}


def sha(raw):return hashlib.sha256(raw).hexdigest()
def require(ok, message):archive.require(ok, message)
def canonical(value):return archive.canonical(value)


def allowed(key):
    return type(key) is str and (key == CURRENT or re.fullmatch(
        re.escape(PREFIX)+r'(artifacts|runs|compilers)/[a-f0-9]{64}\.(json|py)',key))


def reference(raw, kind):
    require(kind in ('artifacts','runs','compilers') and type(raw) is bytes and 0<len(raw)<=MAX,
            'complete_publication_bytes_required')
    digest=sha(raw)
    return {'key':PREFIX+kind+'/'+digest+('.py' if kind=='compilers' else '.json'),
            'bytes':len(raw),'sha256':digest}


def typed_ref(ref, kind):
    require(kind in ('artifacts','runs','compilers') and type(ref) is dict and
            set(ref)=={'key','bytes','sha256'} and archive.hex64(ref['sha256']) and
            type(ref['bytes']) is int and 0<ref['bytes']<=MAX and
            ref['key']==PREFIX+kind+'/'+ref['sha256']+('.py' if kind=='compilers' else '.json'),
            'typed_publication_reference')


def read_ref(client, ref, kind, destination=None):
    """Hash the whole response; optionally spool it without a full byte list."""
    typed_ref(ref,kind)
    obj=client.get_object(Bucket=archive.BUCKET,Key=ref['key']);stream=obj['Body']
    out=None;pieces=[];digest=hashlib.sha256();size=0
    try:
        require(type(obj.get('ContentLength')) is int and obj['ContentLength']==ref['bytes'],
                'publication_object_length')
        if destination is not None:out=Path(destination).open('xb')
        while True:
            raw=stream.read(min(65536,ref['bytes']+1-size))
            if not raw:break
            size+=len(raw);require(size<=ref['bytes'],'publication_object_length');digest.update(raw)
            if out is not None:out.write(raw)
            else:pieces.append(raw)
        require(size==ref['bytes'] and digest.hexdigest()==ref['sha256'],'publication_object_hash')
    finally:
        stream.close()
        if out is not None:out.close()
    return b''.join(pieces) if destination is None else None


def retain_bytes(client, raw, kind):
    ref=reference(raw,kind)
    try:
        client.put_object(Bucket=archive.BUCKET,Key=ref['key'],Body=raw,IfNoneMatch='*',
            ContentType='text/x-python' if kind=='compilers' else 'application/json',
            CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if not checkpoint.conflict(exc):raise
    require(read_ref(client,ref,kind)==raw,'publication_retained_bytes_differ')
    return ref


def retain_file(client, path, proof):
    ref={'key':PREFIX+'artifacts/'+proof['sha256']+'.json',
         'bytes':proof['bytes'],'sha256':proof['sha256']}
    typed_ref(ref,'artifacts')
    with Path(path).open('rb') as stream:
        try:
            client.put_object(Bucket=archive.BUCKET,Key=ref['key'],Body=stream,IfNoneMatch='*',
                ContentType='application/json',CacheControl='public, max-age=31536000, immutable')
        except Exception as exc:
            if not checkpoint.conflict(exc):raise
    # No in-memory copy of a large retained artifact is necessary for readback.
    with tempfile.TemporaryDirectory(prefix='genealogy-readback-') as directory:
        read_ref(client,ref,'artifacts',Path(directory)/'verified.json')
    return ref


def compiler_bytes():
    return {name:Path(importlib.import_module(name).__file__).read_bytes() for name in COMPILERS}


def retain(client, directory, proof, inventories, original_ref):
    """Retain a candidate run. It cannot authorize a publication without replay."""
    archive.validate_inventory(inventories)
    pipeline.verify_files(directory,proof)
    cutoff=next(iter(inventories.values()))['cutoff']
    require(proof['cutoff']==cutoff,'publication_cutoff_differs')
    originals=checkpoint.validate(client,archive.BUCKET,original_ref)
    rows=originals['revisions']
    # A partial disposable cache cannot masquerade as complete retained inputs.
    require(originals['cutoff']==cutoff and originals['entries']==len(rows),
            'complete_original_archive_required')
    reconcile_inventory(inventories,rows)
    files={name:retain_file(client,Path(directory)/filename,proof['files'][name])
           for name,filename in FILES.items()}
    doc={'contract':CONTRACT,'cutoff':cutoff,'inventories':inventories,'originals':original_ref,
         'revision_sha256':revision.revision_digest(rows),'files':files,
         'compilers':{name:retain_bytes(client,raw,'compilers') for name,raw in compiler_bytes().items()}}
    return retain_bytes(client,canonical(doc),'runs')


def reconcile_inventory(inventories, rows):
    archive.validate_inventory(inventories);revision_rows=checkpoint.revision_rows(rows)
    expected={revision.PROTOCOL_KEY}
    for prefix,inventory in inventories.items():
        expected.update(row['key'] for row in inventory['objects'])
        received=[{k:row[k] for k in ('key','bytes','last_modified')}
                  for row in rows if row['key'].startswith(prefix)]
        require(received==inventory['objects'],'retained_original_inventory_differs')
    require(set(revision_rows)==expected,'retained_original_population_differs')


class NoLiveOriginals:
    def get_object(self, **kwargs):
        raise ValueError('retained_replay_cannot_fetch_live_originals')


def replay(client, ref, directory):
    """Replay every original with the current reviewed compiler, in fresh scratch."""
    directory=Path(directory)
    require(not directory.exists(),'fresh_replay_directory_required');directory.mkdir()
    raw=read_ref(client,ref,'runs');doc=archive.strict_json(raw)
    require(type(doc) is dict and set(doc)=={'contract','cutoff','inventories','originals',
            'revision_sha256','files','compilers'} and doc['contract']==CONTRACT and canonical(doc)==raw,
            'publication_manifest_schema')
    current=compiler_bytes()
    require(type(doc['compilers']) is dict and set(doc['compilers'])==set(current),
            'complete_publication_compiler_closure')
    for name,source in current.items():
        require(read_ref(client,doc['compilers'][name],'compilers')==source,
                'reviewed_publication_compiler_differs')
    # Read metadata first, but grant it no authority: restore below validates
    # the complete checkpoint, every original and all manifest totals before
    # any computation. Avoid fetching/decompressing every shard twice here.
    originals=archive.strict_json(checkpoint.checked(client,archive.BUCKET,doc['originals'],'snapshots'))
    require(type(originals) is dict and originals.get('contract')==checkpoint.CONTRACT,
            'original_checkpoint_manifest_required')
    rows=originals['revisions'];reconcile_inventory(doc['inventories'],rows)
    require(originals['cutoff']==doc['cutoff']==next(iter(doc['inventories'].values()))['cutoff'] and
            originals['entries']==len(rows) and doc['revision_sha256']==revision.revision_digest(rows),
            'complete_original_archive_required')
    require(type(doc['files']) is dict and set(doc['files'])==set(FILES),'complete_publication_files')
    cache,restored=checkpoint.restore(client,archive.BUCKET,rows,directory/'originals.sqlite',doc['originals'])
    try:
        require(restored['restored']==len(rows) and restored['cache']['entries']==len(rows),
                'complete_original_restore_required')
        result=pipeline.collect(cache.bind(NoLiveOriginals(),rows),doc['inventories'],directory/'recomputed')
        require(cache.stats()['misses']==0,'retained_replay_original_missing')
    finally:cache.close()
    for name,filename in FILES.items():
        expected=result['files'][name];ref_file=doc['files'][name]
        typed_ref(ref_file,'artifacts')
        require((ref_file['bytes'],ref_file['sha256'])==(expected['bytes'],expected['sha256']),
                'complete_retained_calculation_differs')
        read_ref(client,ref_file,'artifacts',directory/filename)
    head=archive.strict_json((directory/'head.json').read_bytes())
    require(head['generated_at']==doc['cutoff'],'public_head_cutoff_differs')
    return {**head,'replay':ref}


def publish(client, ref, directory, before_publish=None):
    """Construct the head only from complete original replay, then clocked CAS."""
    packet=replay(client,ref,directory);raw=canonical(packet);at=archive.clock(packet['generated_at'])
    for _ in range(4):
        if before_publish is not None:before_publish()
        try:
            previous,tag=read_current(client)
            old=archive.strict_json(previous)
            require(old.get('contract')=='genealogy-research-head.v1','existing_research_head_contract')
            old_at=archive.clock(old['generated_at'])
            if old_at>at:return {'published':False,'reason':'newer_head_preserved','packet':packet}
            if old_at==at:
                require(previous==raw,'conflicting_same_clock_research_publication')
                return {'published':True,'reason':'identical_head','packet':packet}
            condition={'IfMatch':revision.strong_etag(tag)}
        except Exception as exc:
            if checkpoint.error(exc) not in ('NoSuchKey','404'):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=archive.BUCKET,Key=CURRENT,Body=raw,
                ContentType='application/json',CacheControl='no-store',**condition)
            observed,_=read_current(client)
            if observed==raw:return {'published':True,'reason':'published','packet':packet}
            newer=archive.strict_json(observed)
            require(newer.get('contract')=='genealogy-research-head.v1' and
                    archive.clock(newer['generated_at'])>at,'research_publication_readback_differs')
            return {'published':False,'reason':'newer_head_preserved','packet':packet}
        except Exception as exc:
            if not checkpoint.conflict(exc):raise
    raise RuntimeError('research_publication_conflict_limit')


def read_current(client):
    obj=client.get_object(Bucket=archive.BUCKET,Key=CURRENT);stream=obj['Body']
    try:
        raw=stream.read(MAX+1)
        require(type(obj.get('ContentLength')) is int and 0<len(raw)<=MAX and
                len(raw)==obj['ContentLength'],'complete_research_head_required')
        return raw,obj.get('ETag')
    finally:stream.close()
