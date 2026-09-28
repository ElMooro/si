"""Complete public-original orchestration with isolated source/store boundaries.

This module has no AWS client construction, provider request, invocation or
private data reader. The eventual native handler supplies its ordinary client.
All scratch is disposable; a published original archive supplies warm bytes,
never cached validation verdicts. Exceeded budgets fail without partial heads.
"""
from pathlib import Path
import tempfile
import time

import genealogy_public_archive as archive
import genealogy_revision_cache as revision
import genealogy_cache_checkpoint as checkpoint
import genealogy_streamed_pipeline as pipeline
import genealogy_native_publication as publication


def discover(client, cutoff):
    """Complete typed original listing at one explicit storage cutoff.

    Pagination and later arrivals do not enter the calculation identity. Every
    listed key/revision is validated; a missing terminal page is not complete.
    Subsequent reconciliation detects replacement/deletion/backdated addition.
    This is not an atomic storage snapshot or a historical model-availability
    assertion.
    """
    at=archive.clock(cutoff)
    archive.require(cutoff==at.isoformat(),'canonical_native_cutoff_required')
    inventories={};revisions=[]
    for prefix in revision.PREFIXES:
        seen=set();terminal=False;found=[]
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=archive.BUCKET,Prefix=prefix):
            archive.require(type(page) is dict and not terminal and type(page.get('IsTruncated')) is bool,
                            'native_listing_incomplete')
            archive.require(type(page.get('Contents',[])) is list,'native_listing_contents')
            for item in page.get('Contents',[]):
                row=revision.metadata(item['Key'],item['Size'],item['LastModified'],item.get('ETag'))
                archive.require(row['key'].startswith(prefix) and row['key'] not in seen,'native_listing_identity')
                seen.add(row['key'])
                if archive.clock(row['last_modified'])<=at:found.append(row)
                archive.require(len(found)+len(revisions)+1<=checkpoint.MAX_ENTRIES,'complete_original_population_exceeds_budget')
            terminal=page['IsTruncated'] is False
        archive.require(terminal,'native_listing_incomplete')
        found.sort(key=lambda r:r['key']);revisions.extend(found)
        rows=[{k:r[k] for k in ('key','bytes','last_modified')} for r in found]
        inventories[prefix]={'prefix':prefix,'cutoff':cutoff,'listing_complete':True,
            'objects':rows,'objects_at_cutoff':len(rows),'total_bytes':sum(r['bytes'] for r in rows),
            'inventory_sha256':revision.revision_digest(rows)}
    obj=client.head_object(Bucket=archive.BUCKET,Key=revision.PROTOCOL_KEY)
    protocol=revision.metadata(revision.PROTOCOL_KEY,obj['ContentLength'],obj['LastModified'],obj.get('ETag'))
    archive.require(archive.clock(protocol['last_modified'])<=at,'protocol_after_cutoff')
    revisions.append(protocol)
    archive.validate_inventory(inventories)
    return inventories,sorted(revisions,key=lambda r:r['key'])


def previous_originals(storage):
    """Use only typed, hashed original data from the preceding research run.

    Compiler bytes are not trusted or executed here. Restore and the current
    pipeline validate the originals again; changed current revisions are fetched
    anew. A damaged checkpoint is an explicit failure, not a silent fallback.
    """
    try:raw,_=publication.read_current(storage)
    except Exception as exc:
        if checkpoint.error(exc) in ('NoSuchKey','404'):return None
        raise
    head=archive.strict_json(raw)
    archive.require(type(head) is dict and head.get('contract')=='genealogy-research-head.v1',
                    'prior_research_head_required')
    manifest_raw=publication.read_ref(storage,head['replay'],'runs')
    doc=archive.strict_json(manifest_raw)
    archive.require(type(doc) is dict and set(doc)=={'contract','cutoff','inventories','originals',
        'revision_sha256','files','compilers'} and doc['contract']==publication.CONTRACT and
        archive.canonical(doc)==manifest_raw and doc['cutoff']==head['generated_at'],
        'prior_original_manifest_required')
    return doc['originals']


def run(source, storage, cutoff, directory, guard=None):
    """Prepare, retain, replay, reconcile current revisions, then publish.

    Source reads and artifact writes can be supplied separately for isolated
    qualification. The source adapter never writes; the artifact adapter never
    reads a private ledger or an upstream engine result. No result head advances
    on a partial calculation, changed population or failed original replay.
    """
    directory=Path(directory)
    archive.require(not directory.exists(),'fresh_native_directory_required');directory.mkdir()
    check=guard or (lambda phase:None)
    started=time.monotonic();check('inventory')
    inventories,rows=discover(source,cutoff)

    def reconcile():
        check('publication')
        archive.require(revision.revisions(source,inventories)==rows,'native_original_revisions_changed')

    with tempfile.TemporaryDirectory(prefix='collect-',dir=directory) as scratch:
        root=Path(scratch);check('restore')
        original_ref=previous_originals(storage)
        cache,restored=checkpoint.restore(storage,archive.BUCKET,rows,root/'originals.sqlite',original_ref)
        try:
            bound=cache.bind(source,rows);check('calculate')
            proof=pipeline.collect(bound,inventories,root/'calculated')
            reconcile();check('retain')
            original_ref=checkpoint.snapshot(storage,archive.BUCKET,bound,cutoff)
            ref=publication.retain(storage,root/'calculated',proof,inventories,original_ref)
            cache_stats=cache.stats()
        finally:cache.close()
    # Drop the entire first disposable calculation before allocating its replay.
    retained=time.monotonic();check('replay')
    result=publication.publish(storage,ref,directory/'replayed',before_publish=reconcile)
    return {**result,'retained_manifest':ref,'original_checkpoint':original_ref,
        'original_objects':len(rows),'original_bytes':sum(r['bytes'] for r in rows),
        'revision_sha256':revision.revision_digest(rows),'restored':restored,'cache':cache_stats,
        'whole_computation':proof,'timings_seconds':{'prepare_and_retain':round(retained-started,3),
            'replay_and_publish':round(time.monotonic()-retained,3)},
        'capacity_note':'Complete current population only. Future archive growth, actual durable transport and native Lambda capacity require separate acceptance.'}
