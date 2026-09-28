"""Complete accepted calculation without an in-memory capture-context history.

Public originals still pass every archive validator. The temporary context and
timing spools are disposable; no native publication or durable cache is implied.
Record reconciliation remains in memory, with explicit artifact size limits.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import threading

import genealogy_public_archive as archive
import genealogy_capture_timing as timing
import genealogy_registration_model as registration
import genealogy_research_model as baseline
from genealogy_spooled_timing import compile_spooled

MAX_ARTIFACT_BYTES = 64*1024*1024
DEFINITION = ('Complete retained public journal at the declared storage cutoff. Registration order and '
              'selected-membership intervals are observations of collection, not model ancestry, '
              'predictive leadership, independent evidence or investable performance.')


def file_chunks(path):
    with path.open('rb') as stream:
        while True:
            raw = stream.read(65536)
            if not raw:
                return
            yield raw


def write_fragments(path, fragments, limit):
    digest = hashlib.sha256(); size = 0
    with path.open('xb') as out:
        for raw in fragments:
            archive.require(type(raw) is bytes, 'typed_json_fragment_required')
            size += len(raw)
            archive.require(size <= limit, 'complete_artifact_exceeds_budget')
            out.write(raw); digest.update(raw)
    return {'file': path.name, 'bytes': size, 'sha256': digest.hexdigest()}


def object_fragments(values, streams=None):
    streams = streams or {}
    yield b'{'
    for position, key in enumerate(sorted(set(values)|set(streams))):
        if position:
            yield b','
        yield archive.canonical(key)+b':'
        if key in streams:
            yield from streams[key]()
        else:
            yield archive.canonical(values[key])
    yield b'}'


def collect(client, inventories, directory, workers=8, artifact_limit=MAX_ARTIFACT_BYTES):
    """Return file proofs only after the complete population has reconciled.

    The caller must reconcile current object revisions again before accepting
    or publishing this result. READY.json is a local computation receipt only.
    An interrupted directory is never reused or mistaken for a complete run.
    """
    archive.validate_inventory(inventories)
    archive.require(type(artifact_limit) is int and 0 < artifact_limit <= MAX_ARTIFACT_BYTES,
                    'typed_artifact_budget_required')
    directory = Path(directory)
    archive.require(not directory.exists(), 'fresh_pipeline_directory_required')
    directory.mkdir()
    lock = threading.Lock()
    cutoff = next(iter(inventories.values()))['cutoff']
    context_path = directory/'contexts.sqlite'
    with closing(sqlite3.connect(context_path, check_same_thread=False)) as db:
        db.execute('PRAGMA cache_size=-4096'); db.execute('PRAGMA temp_store=FILE')
        db.execute('CREATE TABLE contexts (key TEXT PRIMARY KEY, body TEXT NOT NULL)')

        def observe(document, evidence):
            context = timing.capture_context(document, evidence)
            with lock, db:
                db.execute('INSERT INTO contexts VALUES (?,?)',
                           (context['capture_key'], archive.canonical(context).decode()))

        audit = archive.audit(client, inventories, workers=workers, capture_observer=observe)
        registered = registration.compile_archive(audit)
        archive.require(audit['cutoff'] == cutoff, 'audit_cutoff_differs')
        archive.require(audit['inventory_hashes'] == {p:i['inventory_sha256'] for p,i in inventories.items()},
                        'audit_inventory_differs')
        listed = {r['key']:r for r in inventories[archive.PREFIX+'captures/']['objects']}
        captured = {r['key']:r for r in audit['captures']}
        evidence = {r['key']:r for r in audit['evidence']}
        count = db.execute('SELECT COUNT(*) FROM contexts').fetchone()[0]
        archive.require(count == len(captured) == len(audit['captures']) == len(listed), 'capture_population_differs')
        archive.require(set(captured) == set(listed), 'capture_population_differs')
        archive.require(len(evidence) == len(audit['evidence']), 'duplicate_read_evidence')

        def contexts():
            for key, body in db.execute('SELECT key,body FROM contexts ORDER BY key'):
                context = archive.strict_json(body.encode())
                summary, proof, meta = captured[key], evidence[key], listed[key]
                archive.require(context['capture_key'] == key and context['capture_sha256'] == summary['sha256'] == proof['sha256'],
                                'capture_projection_hash_differs')
                archive.require(proof['bytes'] == meta['bytes'] and archive.clock(proof['last_modified']) == archive.clock(meta['last_modified']),
                                'capture_storage_evidence_differs')
                archive.require(context['generated_at'] == summary['generated_at'] and context['started_at'] == summary['started_at'],
                                'capture_projection_clock_differs')
                archive.require(context['candidate_scan_complete'] is summary['coverage']['candidate_scan_complete'],
                                'capture_projection_coverage_differs')
                archive.require(len(context['sources']) == summary['sources_with_observations'], 'capture_source_population_differs')
                yield context

        def context_array():
            yield b'['
            for n, context in enumerate(contexts()):
                if n:
                    yield b','
                yield archive.canonical(context)
            yield b']'

        membership_path = directory/'membership.json'
        timing_proof = compile_spooled(contexts(), cutoff, directory/'timing.sqlite', membership_path)
        archive.require(timing_proof['output_bytes'] <= artifact_limit, 'complete_artifact_exceeds_budget')
        # Full registration arrays are retained; the capture history is never
        # rebuilt as a Python list merely to serialize the input artifact.
        inputs = write_fragments(directory/'inputs.json', object_fragments(
            {'contract':'genealogy-research-inputs.v1', 'inventories':inventories, 'audit':audit},
            {'contexts':context_array}), artifact_limit)
        membership = archive.strict_json(membership_path.read_bytes())
        identity = lambda row:(row['instrument_id'], row['direction'], row['source_key'])
        archive.require({identity(r) for r in registered['first_registrations']} ==
                        {identity(r) for r in membership['first_observations']}, 'registration_membership_population_differs')
        archive.require(registered['possible_comparisons'] == membership['possible_comparisons'], 'comparison_population_differs')
        output = {'contract':baseline.CONTRACT, 'engine':'signal-genealogy', 'version':'2.0.0', 'generated_at':cutoff,
            'input_sha256':inputs['sha256'], 'archive':audit, 'registration':registered, 'membership':membership,
            'authority':{'forecast_qualified':False, 'calls_eligible':False, 'sizing_eligible':False, 'execution_eligible':False},
            'independent_evidence_count':None, 'original_engine_replay_verified':False, 'definition':DEFINITION}
        head = baseline.summary(output)
        del output['membership']
        result = write_fragments(directory/'output.json', object_fragments(output,
            {'membership':lambda:file_chunks(membership_path)}), artifact_limit)
        head_proof = write_fragments(directory/'head.json', [archive.canonical(head)], artifact_limit)
        files = {'inputs':inputs, 'output':result, 'head':head_proof,
                 'membership':{'file':membership_path.name,'bytes':timing_proof['output_bytes'],'sha256':timing_proof['complete_output_sha256']}}
        receipt = {'contract':'genealogy-streamed-computation.v1', 'cutoff':cutoff, 'files':files,
            'timing':timing_proof, 'coverage':head['coverage'], 'comparison_status_counts':head['comparison_status_counts'],
            'context_database_bytes':context_path.stat().st_size, 'artifact_byte_limit':artifact_limit,
            'native_deployed':False, 'calls_eligible':False, 'sizing_eligible':False}
        verify_files(directory, receipt)
        # Final local completeness marker. No AWS object is written here.
        write_fragments(directory/'READY.json', [archive.canonical(receipt)], artifact_limit)
        return receipt


def verify_files(directory, receipt):
    expected = {'inputs':'inputs.json','output':'output.json','head':'head.json','membership':'membership.json'}
    archive.require(receipt.get('contract') == 'genealogy-streamed-computation.v1' and
                    set(receipt.get('files', {})) == set(expected), 'complete_computation_receipt_required')
    limit = receipt.get('artifact_byte_limit')
    archive.require(type(limit) is int and 0 < limit <= MAX_ARTIFACT_BYTES, 'typed_artifact_budget_required')
    directory = Path(directory).resolve()
    for name, filename in expected.items():
        ref = receipt['files'][name]
        archive.require(set(ref) == {'file','bytes','sha256'} and ref['file'] == filename, 'computation_file_reference')
        archive.require(type(ref['bytes']) is int and 0 < ref['bytes'] <= limit and archive.hex64(ref['sha256']),
                        'computation_file_reference')
        path = directory/filename
        archive.require(not path.is_symlink() and path.resolve().parent == directory, 'computation_file_boundary')
        size = 0; digest = hashlib.sha256()
        for raw in file_chunks(path):
            size += len(raw)
            archive.require(size <= ref['bytes'], 'computation_file_length')
            digest.update(raw)
        archive.require(size == ref['bytes'] and digest.hexdigest() == ref['sha256'], 'computation_file_changed')
    return True
