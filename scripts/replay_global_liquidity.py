"""Reproduce the published three-bank research using reviewed local code only."""
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-global-liquidity/source')]
from global_liquidity_store import COMPILERS,PREFIX
from global_liquidity_research import build,digest,SERIES
from canonical_macro_sources import originals
from replay_fred_vintage import read_public


def replay(manifest,read=read_public):
    if manifest.get('contract')!='global-liquidity-replay.v1':raise ValueError('unsupported replay contract')
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=body:
            raise ValueError('reviewed/retained compiler differs; use matching release checkout')
    descriptor=manifest['input'];raw=read(descriptor['key'])
    if descriptor['key']!=PREFIX+'inputs/'+descriptor['sha256']+'.json' or len(raw)!=descriptor['bytes'] or hashlib.sha256(raw).hexdigest()!=descriptor['sha256']:
        raise ValueError('retained research input differs')
    inputs=json.loads(raw);observed=originals(inputs['source'],read,SERIES)
    out=build(inputs['source'],observed,manifest['generated_at'],inputs['legacy_context'])
    if digest(out)!=manifest['output_sha256']:raise ValueError('research output differs')
    descriptor=manifest['output'];raw=read(descriptor['key'])
    if descriptor['key']!=PREFIX+'outputs/'+manifest['output_sha256']+'.json' or descriptor['sha256']!=manifest['output_sha256'] or len(raw)!=descriptor['bytes'] or hashlib.sha256(raw).hexdigest()!=descriptor['sha256'] or json.loads(raw)!=out:
        raise ValueError('immutable output differs')
    return out


if __name__=='__main__':
    packet=json.loads(read_public('data/global-liquidity.json'))
    key=packet['replay']['manifest_key'];manifest=json.loads(read_public(key))
    if key!=PREFIX+'runs/'+digest(manifest)+'.json':raise ValueError('run identity differs')
    out=replay(manifest)
    if out!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('current packet differs')
    print('REPRODUCED',len(out['series']),'native identities;',out['calendar_research']['expected_weekly_slots'],
          'weekly calendar slots;',digest(out),'Calls/sizing remain unqualified')
