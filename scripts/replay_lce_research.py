"""Reproduce LCE from retained FRED/ECB originals using reviewed local code."""
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-liquidity-credit-engine/source')]
from lce_research_store import COMPILERS
from lce_research_model import build,digest,SERIES
from replay_report_research import replay as macro_replay,read_public
from replay_ciss_research import replay as ciss_replay


def replay(manifest,read=read_public):
    if manifest.get('contract')!='liquidity-credit-replay.v1':raise ValueError('unsupported LCE replay')
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();ref=manifest['compilers'][module.__name__]
        if hashlib.sha256(body).hexdigest()!=ref['sha256'] or read(ref['key'])!=body:
            raise ValueError('reviewed/retained compiler differs; use matching release checkout')
    raw=read(manifest['input']['key'])
    if len(raw)!=manifest['input']['bytes'] or hashlib.sha256(raw).hexdigest()!=manifest['input']['sha256']:
        raise ValueError('retained LCE input differs')
    inputs=json.loads(raw);source=inputs['macro'];ref=source['replay']
    if ref!=manifest['upstream_replay']:raise ValueError('upstream reference differs')
    upstream=json.loads(read(ref['manifest_key']))
    if ref['manifest_key']!='data/report-research/runs/'+digest(upstream)+'.json':raise ValueError('macro run identity differs')
    reconstructed=macro_replay(upstream,read=read)
    if reconstructed!={k:v for k,v in source.items() if k!='replay'}:raise ValueError('macro differs from original replay')
    ciss=inputs.get('ciss')
    if ciss and ciss.get('replay'):
        cissref=ciss['replay'];cm=json.loads(read(cissref['manifest_key']))
        if cissref['manifest_key']!='data/ciss-research/runs/'+digest(cm)+'.json':raise ValueError('ECB run identity differs')
        if ciss_replay(cm,read=read)!={k:v for k,v in ciss.items() if k!='replay'}:raise ValueError('ECB differs from original replay')
    originals={}
    for sid in SERIES:
        if sid not in source['measurements']:continue
        descriptor=upstream['inputs'][sid]
        item={'evidence':descriptor['evidence'],'acquired_at':descriptor['acquired_at']}
        for part,receipt in descriptor['evidence'].items():
            raw=read(receipt['key'])
            if len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=receipt['sha256']:
                raise ValueError('selected LCE original source differs')
            item[part]=json.loads(raw)
        originals[sid]=item
    out=build(source,inputs['ciss'],inputs['repo'],originals,manifest['generated_at'])
    if digest(out)!=manifest['output_sha256']:raise ValueError('LCE output differs')
    return out


if __name__=='__main__':
    packet=json.loads(read_public('data/liquidity-credit-engine.json'))
    key=packet['replay']['manifest_key'];manifest=json.loads(read_public(key))
    if key!='data/lce-research/runs/'+digest(manifest)+'.json':raise ValueError('LCE run identity differs')
    out=replay(manifest)
    if out!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('current packet differs')
    print('REPRODUCED',len(out['series']),'LCE series',digest(out),'FRED and ECB original responses; repo remains unverified context')
