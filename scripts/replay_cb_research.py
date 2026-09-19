"""Rebuild CB research from retained original responses and reviewed local compilers."""
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-cb-injection/source')]
from cb_store import COMPILERS,PREFIX,original_inputs
from cb_research import build,digest
from replay_fred_vintage import read_public


def replay(manifest,read=read_public):
    if manifest.get('contract')!='cb-replay.v1':raise ValueError('CB replay contract required')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:
            raise ValueError('reviewed compiler differs; use the matching release checkout')
    ref=manifest['input'];raw=read(ref['key'])
    if ref['key']!=PREFIX+'inputs/'+ref['sha256']+'.json' or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
        raise ValueError('retained input differs')
    inputs=json.loads(raw);fred,ecb=original_inputs(inputs,read)
    output=build(inputs['source'],fred,ecb,inputs['fails_context'],manifest['generated_at'],inputs['legacy_context'],inputs['acquisition_errors'])
    sha=digest(output);ref=manifest['output'];raw=read(ref['key'])
    if (sha!=manifest['output_sha256'] or ref['key']!=PREFIX+'outputs/'+sha+'.json' or ref['sha256']!=sha
            or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=sha or json.loads(raw)!=output):
        raise ValueError('original-source reproduction differs')
    return output


if __name__=='__main__':
    packet=json.loads(read_public('data/cb-injection.json'));key=packet['replay']['manifest_key']
    manifest=json.loads(read_public(key))
    if key!=PREFIX+'runs/'+digest(manifest)+'.json':raise ValueError('run identity differs')
    output=replay(manifest)
    if output!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('current output differs')
    print('REPRODUCED',len(output['measurements']),'native identities;',len(output['ecb_components']),
          'original ECB component series;',digest(output),'No Calls/sizing authority')
