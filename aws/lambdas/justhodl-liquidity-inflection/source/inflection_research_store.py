"""Retained original inputs and race-safe publication, without AI or account access."""
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import inflection_research_model as model
import inflection_research_catalog as catalog
import inflection_sources
import liquidity_calendar
import report_observations
import research_brief_model
import fred_vintage_model

CURRENT = 'data/liquidity-inflection.json'
BRIEF = 'data/liquidity-inflection-decisive-call.json'
PREFIX = 'data/inflection-research/'
COMPILERS = (model, catalog, inflection_sources, liquidity_calendar, report_observations,
             research_brief_model, fred_vintage_model)
MAX_BYTES = 64*1024*1024


def code(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))


def bounded(stream):
    raw = stream.read(MAX_BYTES+1)
    if len(raw) > MAX_BYTES: raise ValueError('research object exceeds bound')
    return raw


def raw_reader(client, bucket):
    def read(key):
        if not isinstance(key, str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key:
            raise ValueError('public research path required')
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if key.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream: raw = bounded(stream)
        return raw
    return read


def optional(read, key):
    try: return json.loads(read(key))
    except Exception as exc:
        if code(exc) not in ('404', 'NoSuchKey'): raise
        return None


def immutable(client, bucket, key, raw, kind='application/json'):
    try: client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw:
        raise ValueError('immutable artifact differs')


def preserve_legacy(client, bucket, read, key):
    previous = optional(read, key)
    if previous is None: return None
    if previous.get('contract') in (model.CONTRACT, 'liquidity-inflection-brief.v1'):
        return previous.get('legacy_context')
    raw = read(key); sha = hashlib.sha256(raw).hexdigest(); dest = PREFIX+'legacy-unvalidated/'+sha+'.json'
    immutable(client, bucket, dest, raw)
    return {'key': dest, 'sha256': sha, 'bytes': len(raw), 'status': 'UNQUALIFIED_LEGACY'}


def auxiliary_inputs(client, bucket, read):
    documents = {}; errors = {}
    for name in catalog.AUXILIARIES:
        key = 'data/'+name+'.json'
        try: raw = read(key)
        except Exception as exc:
            if code(exc) not in ('404', 'NoSuchKey'): raise
            documents[name] = None; errors[name] = {'status': 'unavailable', 'reason': 'source_missing'}
            continue
        try: documents[name] = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            sha = hashlib.sha256(raw).hexdigest(); dest = PREFIX+'unparsed-context/'+sha+'.bin'
            immutable(client, bucket, dest, raw, 'application/octet-stream')
            documents[name] = None
            errors[name] = {'status': 'unparsed_context', 'reason': 'invalid_json', 'key': dest, 'sha256': sha, 'bytes': len(raw)}
    return documents, errors


def publish(client, bucket, key, output):
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=key); previous = json.loads(bounded(obj['Body'])); etag = obj['ETag']
        except Exception as exc:
            if code(exc) not in ('404', 'NoSuchKey'): raise
            previous = {}; etag = None
        if previous.get('generated_at') and model.clock(previous['generated_at']) > model.clock(output['generated_at']):
            return False
        for field in ('source_generated_at', 'archive_collection_generated_at'):
            if previous.get(field) and output.get(field) and model.clock(previous[field]) > model.clock(output[field]):
                return False
        try:
            client.put_object(Bucket=bucket, Key=key, Body=model.encoded(output), ContentType='application/json',
                CacheControl='no-store', **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
            return True
        except Exception as exc:
            if code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'): raise
    raise RuntimeError('publication race retry limit exceeded')


def run(client, bucket):
    read = raw_reader(client, bucket)
    source = json.loads(read('data/report-measurements.json'))
    originals = inflection_sources.macro_originals(source, read)
    collection = json.loads(read('data/vintage/_index.json'))
    archives = inflection_sources.archive_originals(collection, read)
    auxiliary, auxiliary_errors = auxiliary_inputs(client, bucket, read)
    legacy = preserve_legacy(client, bucket, read, CURRENT)
    legacy_brief = preserve_legacy(client, bucket, read, BRIEF)
    stamp = datetime.now(timezone.utc).isoformat()
    inputs = {'macro': source, 'archives': archives, 'archive_collection': collection, 'auxiliary': auxiliary, 'auxiliary_errors': auxiliary_errors}
    output = model.build(inputs, originals, stamp)
    output.update(legacy_context=legacy, archive_collection_generated_at=collection['generated_at'])
    raw = model.encoded(inputs); key = PREFIX+'inputs/'+model.digest(inputs)+'.json'
    immutable(client, bucket, key, raw)
    compilers = {}
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); sha = hashlib.sha256(body).hexdigest(); dest = PREFIX+'compilers/'+sha+'.py'
        immutable(client, bucket, dest, body, 'text/plain'); compilers[module.__name__] = {'key': dest, 'sha256': sha}
    manifest = {'contract': 'liquidity-inflection-replay.v1', 'generated_at': stamp,
        'input': {'key': key, 'sha256': model.digest(inputs), 'bytes': len(raw)}, 'compilers': compilers,
        'legacy_context': legacy, 'archive_collection_generated_at': collection['generated_at'],
        'output_sha256': model.digest(output)}
    run_key = PREFIX+'runs/'+model.digest(manifest)+'.json'
    immutable(client, bucket, run_key, model.encoded(manifest))
    retained = json.loads(read(key)); reproduced = model.build(retained, originals, stamp)
    reproduced.update(legacy_context=legacy, archive_collection_generated_at=collection['generated_at'])
    if model.digest(reproduced) != manifest['output_sha256']: raise ValueError('retained-input replay differs')
    output['replay'] = {'manifest_key': run_key, 'output_sha256': manifest['output_sha256'], 'compilers': compilers}
    published = publish(client, bucket, CURRENT, output)
    brief_published = False
    if published:
        brief = {'contract': 'liquidity-inflection-brief.v1', 'generated_at': stamp, 'source_generated_at': source['generated_at'],
            'headline': 'WAIT — descriptive liquidity research', 'summary': model.REASON,
            'methodology': output['methodology'], 'decision': output['decision'], 'model': 'deterministic-original-source',
            'call': None, 'calls_eligible': False, 'sizing_eligible': False, 'paid_ai_calls': 0,
            'source_replay': output['replay'], 'legacy_context': legacy_brief}
        brief_published = publish(client, bucket, BRIEF, brief)
    return {'published': published, 'brief_published': brief_published, 'generated_at': stamp,
            'quality': output['quality'], 'replay': output['replay'], 'paid_ai_calls': 0,
            'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0}
