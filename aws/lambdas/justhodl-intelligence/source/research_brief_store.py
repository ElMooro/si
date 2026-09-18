"""Retain and conditionally publish the deterministic macro research brief."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import report_observations
import research_brief_model
from research_brief_model import build, digest, encoded

CURRENT = 'intelligence-report.json'
SOURCE = 'data/report-measurements.json'
PREFIX = 'data/research-intelligence/'
MAX_BYTES = 32 * 1024 * 1024


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


def run(client, bucket):
    packet, _, _ = read(client, bucket, SOURCE)
    validate_source(client, bucket, packet)
    stamp = datetime.now(timezone.utc).isoformat()
    output = build(packet, stamp)
    input_body = encoded(packet)
    input_key = PREFIX + 'inputs/' + digest(packet) + '.json'
    immutable(client, bucket, input_key, input_body)
    compiler = Path(research_brief_model.__file__).read_bytes()
    compiler_sha = hashlib.sha256(compiler).hexdigest()
    compiler_key = PREFIX + 'compilers/' + compiler_sha + '.py'
    immutable(client, bucket, compiler_key, compiler, 'text/plain')
    manifest = {'contract': 'research-intelligence-replay.v1', 'generated_at': stamp,
                'input': {'key': input_key, 'sha256': digest(packet), 'bytes': len(input_body)},
                'upstream_replay': packet['replay'], 'compiler': {'key': compiler_key, 'sha256': compiler_sha},
                'output_sha256': digest(output), 'scope': output['scope']}
    manifest_key = PREFIX + 'runs/' + digest(manifest) + '.json'
    immutable(client, bucket, manifest_key, encoded(manifest))
    retained, _, _ = read(client, bucket, input_key)
    if digest(build(retained, stamp)) != manifest['output_sha256']:
        raise ValueError('brief replay differs before publication')
    output['replay'] = {'manifest_key': manifest_key, 'output_sha256': manifest['output_sha256'],
                        'compiler_sha256': compiler_sha}
    for _ in range(4):
        try:
            previous, previous_bytes, etag = read(client, bucket, CURRENT)
        except Exception as exc:
            if error_code(exc) not in ('404', 'NoSuchKey'):
                raise
            previous, previous_bytes, etag = None, None, None
        if previous:
            if previous.get('generated_at', '') > stamp:
                return {'published': False, 'reason': 'newer brief is already current'}
            if previous.get('contract') == output['contract']:
                if previous.get('source_generated_at', '') > packet['generated_at']:
                    return {'published': False, 'reason': 'newer source is already current'}
            else:
                # Preserve the old public output unchanged; it is explicitly
                # legacy audit material, not an evidence-backed current brief.
                legacy_key = PREFIX + 'legacy-unvalidated/' + hashlib.sha256(previous_bytes).hexdigest() + '.json'
                immutable(client, bucket, legacy_key, previous_bytes)
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(output), ContentType='application/json',
                              CacheControl='no-cache', **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
            return {'published': True, 'generated_at': stamp, 'quality': output['quality'],
                    'decision': output['decision'], 'replay': output['replay']}
        except Exception as exc:
            if error_code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'):
                raise
    raise RuntimeError('brief publication race retry bound exceeded')
