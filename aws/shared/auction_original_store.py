"""Retain complete source pages, freeze reviewed compilers and replay before use.

Writes only immutable source/measurement artifacts. No current packet reads,
native invocations, provider requests, schedule changes or decision authority.
"""
from datetime import datetime, timezone
import gzip
import io
from pathlib import Path
import re

import auction_originals as model
import evidence_store
import treasury_instruments

MAX_ARTIFACT = 32 * 1024 * 1024
PROVIDER = 'treasury-auctions'
REPLAY_CONTRACT = 'auction-original-replay.v1'


def compiler_bytes():
    return {'auction_originals': Path(model.__file__).read_bytes(),
            'auction_original_store': Path(__file__).read_bytes(),
            'treasury_instruments': Path(treasury_instruments.__file__).read_bytes(),
            'evidence_store': Path(evidence_store.__file__).read_bytes()}


def storage_read(client, bucket, key):
    raw = client.get_object(Bucket=bucket, Key=key)['Body'].read(MAX_ARTIFACT+1)
    if len(raw) > MAX_ARTIFACT:
        raise ValueError('Stored source artifact exceeds byte bound')
    return raw


def immutable(client, bucket, key, raw, content_type='application/json'):
    if not 0 < len(raw) <= MAX_ARTIFACT:
        raise ValueError('Immutable artifact size invalid')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=content_type, IfNoneMatch='*')
    except Exception as exc:
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
        if code not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
            raise
    if storage_read(client, bucket, key) != raw:
        raise ValueError('Immutable source artifact readback differs')


def reference(kind, raw):
    return {'key': model.PREFIX+kind+'/'+model.sha(raw)+'.json', 'sha256': model.sha(raw), 'bytes': len(raw)}


def load_reference(ref, kind, read):
    if not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256', ''))):
        raise ValueError('Artifact hash missing')
    if ref.get('key') != model.PREFIX+kind+'/'+ref['sha256']+'.json' or type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= MAX_ARTIFACT:
        raise ValueError('Artifact identity or size differs')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or model.sha(raw) != ref['sha256']:
        raise ValueError('Retained artifact bytes differ')
    return model.strict_json(raw)


def original(page, expected_url, generated_at, read):
    receipt = page.get('evidence') or {}
    if page.get('request_url') != expected_url or receipt.get('source_url') != evidence_store.public_source_url(expected_url):
        raise ValueError('Original request identity differs')
    if receipt.get('contract') != 'source-evidence.v1' or receipt.get('captured') is not True or receipt.get('provider') != PROVIDER:
        raise ValueError('Original evidence receipt differs')
    sha = receipt.get('sha256')
    if not isinstance(sha, str) or not re.fullmatch('[a-f0-9]{64}', sha):
        raise ValueError('Original content identity missing')
    request_sha = model.sha(receipt['source_url'].encode())
    if receipt.get('key') != f'data/evidence/{PROVIDER}/{request_sha}/{sha}.bin.gz':
        raise ValueError('Original evidence namespace differs')
    if type(receipt.get('bytes')) is not int or not 0 < receipt['bytes'] <= model.MAX_PAGE_BYTES:
        raise ValueError('Original evidence size differs')
    acquired = model.clock(page['acquired_at'])
    if model.clock(receipt['first_received_at']) > acquired or acquired > model.clock(generated_at):
        raise ValueError('Original receipt/acquisition clock differs')
    zipped = read(receipt['key'])
    if len(zipped) > model.MAX_PAGE_BYTES+65536:
        raise ValueError('Compressed original exceeds bound')
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(zipped)) as stream:
            raw = stream.read(model.MAX_PAGE_BYTES+1)
    except (OSError, EOFError) as exc:
        raise ValueError('Original compressed bytes are corrupt') from exc
    if len(raw) != receipt['bytes'] or model.sha(raw) != sha:
        raise ValueError('Original response bytes differ')
    return {**page, 'raw': raw}


def replay(ref, read):
    """Only retained original bytes and local, exact, reviewed source are used."""
    manifest = load_reference(ref, 'runs', read)
    if manifest.get('contract') != REPLAY_CONTRACT:
        raise ValueError('Auction replay contract differs')
    sources = compiler_bytes()
    if set(manifest.get('compilers', {})) != set(sources):
        raise ValueError('Whole compiler closure required')
    for name, raw in sources.items():
        expected = {'key': model.PREFIX+'compilers/'+model.sha(raw)+'.py', 'sha256': model.sha(raw), 'bytes': len(raw)}
        if manifest['compilers'][name] != expected or read(expected['key']) != raw:
            raise ValueError('Reviewed compiler differs; use matching checkout')
    window = manifest['request_window']
    pages = manifest['pages']
    if not isinstance(pages, list) or not 1 <= len(pages) <= model.MAX_PAGES:
        raise ValueError('Complete page references required')
    hydrated = [original(page, model.url(window['start'], window['end'], window['page_size'], i), manifest['generated_at'], read)
                for i, page in enumerate(pages, 1)]
    output = model.build(hydrated, window['start'], window['end'], window['page_size'], manifest['generated_at'])
    retained = load_reference(manifest['output'], 'measurements', read)
    if model.encoded(output) != model.encoded(retained):
        raise ValueError('Complete original-source measurement replay differs')
    return output


def retain(client, bucket, pages, start, end, size=200, generated_at=None):
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    output = model.build(pages, start, end, size, generated_at)
    descriptors = []
    for page in pages:
        receipt = evidence_store.capture(client, bucket, PROVIDER, page['request_url'], page['raw'], model.clock(page['acquired_at']))
        descriptors.append({k:v for k,v in page.items() if k != 'raw'} | {'evidence': receipt})
    compilers = {}
    for name, raw in compiler_bytes().items():
        key = model.PREFIX+'compilers/'+model.sha(raw)+'.py'
        immutable(client, bucket, key, raw, 'text/plain')
        compilers[name] = {'key': key, 'sha256': model.sha(raw), 'bytes': len(raw)}
    raw_output = model.encoded(output)
    output_ref = reference('measurements', raw_output)
    output_key = model.PREFIX+'measurements/'+model.sha(raw_output)+'.json'
    immutable(client, bucket, output_key, raw_output)
    manifest = {'contract': REPLAY_CONTRACT, 'generated_at': generated_at, 'request_window': output['request_window'],
                'pages': descriptors, 'compilers': compilers, 'output': output_ref}
    raw_manifest = model.encoded(manifest)
    manifest_ref = reference('runs', raw_manifest)
    manifest_key = model.PREFIX+'runs/'+model.sha(raw_manifest)+'.json'
    immutable(client, bucket, manifest_key, raw_manifest)
    replayed = replay(manifest_ref, lambda key: storage_read(client, bucket, key))
    if model.encoded(replayed) != raw_output:
        raise ValueError('Before-publication source replay differs')
    return {'contract': REPLAY_CONTRACT, 'manifest': manifest_ref, 'measurements': output_ref,
            'coverage': output['coverage'], 'generated_at': generated_at,
            'original_bytes_replayed': True, 'direct_measurements_replayed': True,
            'scope': 'Direct FiscalData auction observations only; legacy scores/FRED/forecasts/analogs are not validated.',
            'historical_point_in_time_verified': False, 'forecast_eligible': False, 'calls_eligible': False, 'sizing_eligible': False}
