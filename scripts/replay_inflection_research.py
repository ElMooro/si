"""Reproduce liquidity research from originals with reviewed local code only."""
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/lambdas/justhodl-liquidity-inflection/source')]
from inflection_research_store import COMPILERS, PREFIX
from inflection_research_model import build, digest
from inflection_sources import macro_originals, archive_originals
from replay_fred_vintage import read_public


def replay(manifest, read=read_public):
    if manifest.get('contract') != 'liquidity-inflection-replay.v1': raise ValueError('unsupported liquidity replay')
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); sha = hashlib.sha256(body).hexdigest(); ref = manifest['compilers'][module.__name__]
        if ref != {'key': PREFIX+'compilers/'+sha+'.py', 'sha256': sha} or read(ref['key']) != body:
            raise ValueError('reviewed/retained compiler differs; use matching release checkout')
    descriptor = manifest['input']; raw = read(descriptor['key'])
    if descriptor['key'] != PREFIX+'inputs/'+descriptor['sha256']+'.json' or len(raw) != descriptor['bytes'] or hashlib.sha256(raw).hexdigest() != descriptor['sha256']:
        raise ValueError('retained research input differs')
    inputs = json.loads(raw); originals = macro_originals(inputs['macro'], read)
    if archive_originals(inputs['archive_collection'], read) != inputs['archives']:
        raise ValueError('retained archive differs from original replay')
    out = build(inputs, originals, manifest['generated_at'])
    out.update(legacy_context=manifest['legacy_context'], archive_collection_generated_at=manifest['archive_collection_generated_at'])
    if digest(out) != manifest['output_sha256']: raise ValueError('liquidity output differs')
    return out


if __name__ == '__main__':
    packet = json.loads(read_public('data/liquidity-inflection.json'))
    key = packet['replay']['manifest_key']; manifest = json.loads(read_public(key))
    if key != PREFIX+'runs/'+digest(manifest)+'.json': raise ValueError('run identity differs')
    out = replay(manifest)
    if out != {k: v for k, v in packet.items() if k != 'replay'}: raise ValueError('current packet differs')
    print('REPRODUCED', len(out['series']), 'native identities;', out['calendar_research']['weekly_observations'],
          'weekly archive samples;', digest(out), 'Calls/sizing remain unqualified')
