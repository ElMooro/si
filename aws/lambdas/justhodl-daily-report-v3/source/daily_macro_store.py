"""Publish the canonical macro report without racing its enrichment writer."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import report_observations
import research_brief_model
import daily_macro_model
import daily_market_model
from daily_market_store import verify_sources
from daily_macro_model import build, digest, encoded, clock, market_context

CURRENT = 'data/report.json'
SOURCE = 'data/report-measurements.json'
PREFIX = 'data/daily-research/'
MAX_BYTES = 32 * 1024 * 1024
AUGMENTATION_FIELDS = ('defi_tvl', 'eth_gas', 'funding_rates', 'leverage_sentiment',
                       'enriched_at', 'enrichment_fields', 'enrichment_provenance')

def error_code(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code')


def read(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        raise ValueError('research object exceeds bound')
    return json.loads(body), body, obj['ETag']


def immutable(client, bucket, key, body, content_type='application/json'):
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch='*')
    except Exception as exc:
        if error_code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'):
            raise
    original = client.get_object(Bucket=bucket, Key=key)['Body'].read(len(body) + 1)
    if original != body:
        raise ValueError('immutable archive readback differs')


def validate_source(client, bucket, packet):
    replay = packet.get('replay') or {}
    key = replay.get('manifest_key', '')
    if not re.fullmatch(r'data/report-research/runs/[0-9a-f]{64}\.json', key):
        raise ValueError('canonical source replay missing')
    manifest, _, _ = read(client, bucket, key)
    if key != 'data/report-research/runs/' + digest(manifest) + '.json':
        raise ValueError('source run identity differs')
    reviewed = Path(report_observations.__file__).read_bytes()
    sha = hashlib.sha256(reviewed).hexdigest()
    if manifest.get('compiler') != {'key': 'data/report-research/compilers/' + sha + '.py', 'sha256': sha}:
        raise ValueError('source compiler differs from reviewed runtime')
    if replay.get('compiler_sha256') != sha:
        raise ValueError('source compiler reference differs')
    archived = client.get_object(Bucket=bucket, Key=manifest['compiler']['key'])['Body'].read(len(reviewed) + 1)
    if archived != reviewed:
        raise ValueError('retained source compiler differs')
    payload = {k: v for k, v in packet.items() if k != 'replay'}
    if digest(payload) != manifest.get('output_sha256') or replay.get('output_sha256') != manifest['output_sha256']:
        raise ValueError('source packet differs from retained source run')
    return manifest


def run(client, bucket, collect):
    packet, _, _ = read(client, bucket, SOURCE)
    validate_source(client, bucket, packet)
    auxiliary = collect()
    if 'market_sources' in auxiliary:
        import gzip
        def original(key):
            return gzip.decompress(client.get_object(Bucket=bucket, Key=key)['Body'].read(MAX_BYTES))
        verify_sources(auxiliary['market_sources'], original)
    stamp = datetime.now(timezone.utc).isoformat()
    output = build(packet, auxiliary, stamp)
    inputs = {'macro': packet, 'auxiliary': auxiliary}
    input_body = encoded(inputs)
    input_key = PREFIX + 'inputs/' + digest(inputs) + '.json'
    immutable(client, bucket, input_key, input_body)
    compilers = {}
    for module in (daily_macro_model, research_brief_model, daily_market_model):
        body = Path(module.__file__).read_bytes()
        sha = hashlib.sha256(body).hexdigest()
        key = PREFIX + 'compilers/' + sha + '.py'
        immutable(client, bucket, key, body, 'text/plain')
        compilers[module.__name__] = {'key': key, 'sha256': sha}
    manifest = {'contract': 'daily-research-replay.v1', 'generated_at': stamp,
                'input': {'key': input_key, 'sha256': digest(inputs), 'bytes': len(input_body)},
                'upstream_replay': packet['replay'], 'compilers': compilers,
                'output_sha256': digest(output), 'base_fields': sorted(output), 'scope': output['scope']}
    manifest_key = PREFIX + 'runs/' + digest(manifest) + '.json'
    immutable(client, bucket, manifest_key, encoded(manifest))
    retained, _, _ = read(client, bucket, input_key)
    if digest(build(retained['macro'], retained['auxiliary'], stamp)) != manifest['output_sha256']:
        raise ValueError('report replay differs before publication')
    output['replay'] = {'manifest_key': manifest_key, 'output_sha256': manifest['output_sha256'],
                        'base_fields': manifest['base_fields'], 'compilers': compilers,
                        'excluded_augmentation_fields': list(AUGMENTATION_FIELDS)}
    # An augmentation writer owns only its declared fields. Re-read them on
    # each retry; neither writer can replace the other's newer base/augmentation.
    for _ in range(4):
        try:
            previous, previous_bytes, etag = read(client, bucket, CURRENT)
        except Exception as exc:
            if error_code(exc) not in ('404', 'NoSuchKey'):
                raise
            previous, previous_bytes, etag = None, None, None
        if previous:
            if clock(previous['generated_at']) > clock(stamp):
                return {'published': False, 'reason': 'newer report is already current'}
            if previous.get('contract') == output['contract']:
                if clock(previous['source_generated_at']) > clock(packet['generated_at']):
                    return {'published': False, 'reason': 'newer macro source is already current'}
            else:
                legacy_key = PREFIX + 'legacy-unvalidated/' + hashlib.sha256(previous_bytes).hexdigest() + '.json'
                immutable(client, bucket, legacy_key, previous_bytes)
        candidate = {**output, **{key: previous[key] for key in AUGMENTATION_FIELDS if previous and key in previous}}
        candidate['market_intelligence'] = market_context(candidate)
        if previous and any(key in previous for key in AUGMENTATION_FIELDS):
            candidate['augmentation_quality'] = {'status': 'legacy_unverified',
                                                 'generated_at': previous.get('enriched_at'),
                                                 'original_source_verified': False}
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(candidate), ContentType='application/json',
                              CacheControl='no-cache', **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
            return {'published': True, 'generated_at': stamp, 'quality': output['quality'],
                    'decision': output['decision'], 'replay': output['replay']}
        except Exception as exc:
            if error_code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'):
                raise
    raise RuntimeError('report publication race retry bound exceeded')
