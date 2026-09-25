"""Complete, separately bounded Risk Gate inputs; no concatenation size trap.

Each source is retained whole. The small index binds its path, exact bytes and
hash. Replay checks the entire inventory before reading any referenced object.
Older combined-input manifests remain readable under their original bound.
"""
import hashlib
import json
import re
from risk_gate_research_model import encoded

PREFIX = 'data/risk-gate-research/inputs/'
CONTRACT = 'risk-gate-input-index.v1'
OBJECT_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 128 * 1024 * 1024
INDEX_LIMIT = 128 * 1024
FLEET_LIMIT = 64


def reference(raw):
    sha = hashlib.sha256(raw).hexdigest()
    return {'key': PREFIX + sha + '.json', 'sha256': sha, 'bytes': len(raw)}


def check_reference(ref, limit):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
            or not isinstance(ref['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', ref['sha256'])
            or ref['key'] != PREFIX + ref['sha256'] + '.json'
            or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= limit):
        raise ValueError('Bounded content-addressed Risk Gate input required')


def checked(ref, read, limit):
    check_reference(ref, limit)
    raw = read(ref['key'])
    if (not isinstance(raw, bytes) or len(raw) != ref['bytes']
            or hashlib.sha256(raw).hexdigest() != ref['sha256']):
        raise ValueError('Retained Risk Gate input differs')
    return json.loads(raw)


def inventory(index):
    if (not isinstance(index, dict) or set(index) != {'contract', 'macro', 'ciss', 'fleet'}
            or index['contract'] != CONTRACT or not isinstance(index['fleet'], dict)
            or len(index['fleet']) > FLEET_LIMIT
            or any(not isinstance(key, str) or not re.fullmatch(r'data/[a-z0-9-]+\.json', key)
                   for key in index['fleet'])):
        raise ValueError('Complete Risk Gate input index required')
    refs = [index['macro'], index['ciss'], *index['fleet'].values()]
    for ref in refs:
        check_reference(ref, OBJECT_LIMIT)
    if sum(ref['bytes'] for ref in refs) > TOTAL_LIMIT:
        raise ValueError('Risk Gate input population exceeds bound')
    return refs


def prepare(macro, ciss, fleet):
    objects = {}

    def part(value):
        raw = encoded(value)
        ref = reference(raw)
        check_reference(ref, OBJECT_LIMIT)
        objects[ref['key']] = raw
        return ref

    index = {'contract': CONTRACT, 'macro': part(macro), 'ciss': part(ciss),
             'fleet': {key: part(value) for key, value in fleet.items()}}
    inventory(index)
    raw = encoded(index)
    ref = reference(raw)
    check_reference(ref, INDEX_LIMIT)
    objects[ref['key']] = raw
    return ref, objects


def restore(manifest, read):
    if manifest.get('contract') == 'risk-gate-replay.v1':
        inputs = checked(manifest['input'], read, OBJECT_LIMIT)
        if not isinstance(inputs, dict) or set(inputs) != {'macro', 'ciss', 'fleet'}:
            raise ValueError('Complete legacy Risk Gate input required')
        return inputs
    if manifest.get('contract') != 'risk-gate-replay.v2':
        raise ValueError('Unsupported Risk Gate replay')
    index = checked(manifest['input'], read, INDEX_LIMIT)
    inventory(index)
    return {'macro': checked(index['macro'], read, OBJECT_LIMIT),
            'ciss': checked(index['ciss'], read, OBJECT_LIMIT),
            'fleet': {key: checked(ref, read, OBJECT_LIMIT) for key, ref in index['fleet'].items()}}
