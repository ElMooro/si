"""Reproduce Risk Gate from retained FRED/ECB originals using reviewed local code."""
import hashlib
import gzip
import io
import json
from pathlib import Path
import sys
import urllib.request
import re

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-risk-gate/source')]
from risk_gate_research_store import COMPILERS
from risk_gate_research_inputs import restore
import risk_gate_research_inputs
from risk_gate_research_model import build,digest,encoded,SERIES,CONTRACT
from replay_report_research import replay as macro_replay
from replay_ciss_research import replay as ciss_replay


def read_public(key):
    if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:
        raise ValueError('Safe public artifact path required')
    request=urllib.request.Request('https://justhodl.ai/'+key+'?exact=1&nogen=1',
        headers={'User-Agent':'justhodl-verify-release/1.0'})
    limit=32*1024*1024
    with urllib.request.urlopen(request,timeout=45) as response:raw=response.read(limit+1)
    if len(raw)>limit:raise ValueError('Public artifact exceeds bound')
    if key.endswith('.gz'):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:raw=stream.read(limit+1)
        if len(raw)>limit:raise ValueError('Original response exceeds bound')
    return raw


def strict(raw):
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate research JSON key')
            value[key]=item
        return value
    def invalid(value):raise ValueError('Nonfinite research JSON number')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
    encoded(value)
    return value


def same_json(left,right):return encoded(left)==encoded(right)


def replay(manifest,read=read_public):
    if manifest.get('contract') not in ('risk-gate-replay.v1','risk-gate-replay.v2'):raise ValueError('unsupported Risk Gate replay')
    original_reader=read
    def read(key):
        raw=original_reader(key)
        if key.endswith('.json'):strict(raw)
        return raw
    modules=(*COMPILERS,risk_gate_research_inputs) if manifest['contract']=='risk-gate-replay.v2' else COMPILERS
    if set(manifest['compilers'])!={m.__name__ for m in modules}:raise ValueError('Complete reviewed compiler closure required')
    for module in modules:
        body=Path(module.__file__).read_bytes();ref=manifest['compilers'][module.__name__]
        if hashlib.sha256(body).hexdigest()!=ref['sha256'] or read(ref['key'])!=body:
            raise ValueError('reviewed/retained compiler differs; use matching release checkout')
    inputs=restore(manifest,read);source=inputs['macro'];ref=source['replay']
    if ref!=manifest['upstream_replay']:raise ValueError('upstream reference differs')
    upstream=strict(read(ref['manifest_key']))
    if ref['manifest_key']!='data/report-research/runs/'+digest(upstream)+'.json':raise ValueError('macro run identity differs')
    reconstructed=macro_replay(upstream,read=read)
    if not same_json(reconstructed,{k:v for k,v in source.items() if k!='replay'}):raise ValueError('macro differs from original replay')
    ciss=inputs.get('ciss')
    if ciss and ciss.get('replay'):
        cissref=ciss['replay'];cm=strict(read(cissref['manifest_key']))
        if cissref['manifest_key']!='data/ciss-research/runs/'+digest(cm)+'.json':raise ValueError('ECB run identity differs')
        if not same_json(ciss_replay(cm,read=read),{k:v for k,v in ciss.items() if k!='replay'}):raise ValueError('ECB differs from original replay')
    originals={}
    for sid in SERIES:
        if sid not in source['measurements']:continue
        descriptor=upstream['inputs'][sid]
        item={'evidence':descriptor['evidence'],'acquired_at':descriptor['acquired_at']}
        for part,receipt in descriptor['evidence'].items():
            raw=read(receipt['key'])
            if len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=receipt['sha256']:
                raise ValueError('selected Risk Gate original source differs')
            item[part]=strict(raw)
        originals[sid]=item
    out=build(source,inputs['ciss'],inputs['fleet'],originals,manifest['generated_at'])
    if digest(out)!=manifest['output_sha256']:raise ValueError('Risk Gate output differs')
    return out


def verify(packet,read=read_public):
    if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT:raise ValueError('Native Risk Gate publication required')
    ref=packet.get('replay') or {};key=ref.get('manifest_key','')
    if not isinstance(key,str) or not re.fullmatch(r'data/risk-gate-research/runs/[a-f0-9]{64}\.json',key):
        raise ValueError('Exact Risk Gate manifest path required')
    raw=read(key);manifest=strict(raw)
    if key!='data/risk-gate-research/runs/'+hashlib.sha256(raw).hexdigest()+'.json':raise ValueError('Risk Gate run identity differs')
    if not same_json(ref,{'manifest_key':key,'output_sha256':manifest['output_sha256'],'compilers':manifest['compilers']}):
        raise ValueError('Risk Gate public replay reference differs')
    out=replay(manifest,read)
    if not same_json(out,{k:v for k,v in packet.items() if k!='replay'}):raise ValueError('Current packet differs')
    return {'contract':out['contract'],'generated_at':out['generated_at'],'replayed':True,
        'requested_series':len(SERIES),'reconstructed_series':len(out['series']),'output_sha256':digest(out),
        'calls_eligible':out['calls_eligible'],'sizing_eligible':out['sizing_eligible'],
        'scope':'Retained FRED/ECB original-source reconstruction; fleet remains unqualified context.'}


if __name__=='__main__':
    print(json.dumps(verify(strict(read_public('data/risk-gate.json'))),sort_keys=True))
