#!/usr/bin/env python3
"""Reproduce a public Treasury run from original archived responses.

Usage: python scripts/replay_tenor_research.py data/tenor-research/runs/<sha>.json
Uses local reviewed compiler sources only; never executes downloaded source code.
Checksums establish consistency, not independent authenticity or predictive edge.
"""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import urllib.request
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
from tenor_research_model import compile_research

MAX_BYTES=8*1024*1024

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

def fetch(key):
    if not re.fullmatch(r'data/(?:tenor-research/runs/[a-f0-9]{64}\.json|evidence/[a-z0-9_-]+/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)',key):
        raise ValueError('unsupported public evidence key')
    req=urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(req,timeout=30) as response:raw=response.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('response exceeds declared byte bound')
    return raw

def original(ref,reader):
    if ref.get('contract')!='source-evidence.v1' or ref.get('captured') is not True:raise ValueError('original evidence receipt required')
    if not isinstance(ref.get('bytes'),int) or not 0<ref['bytes']<=MAX_BYTES:raise ValueError('invalid original byte bound')
    expected=f"data/evidence/{ref['provider']}/{hashlib.sha256(ref['source_url'].encode()).hexdigest()}/{ref['sha256']}.bin.gz"
    if ref['key']!=expected:raise ValueError('original source request/content identity mismatch')
    with gzip.GzipFile(fileobj=io.BytesIO(reader(ref['key']))) as stream:raw=stream.read(MAX_BYTES+1)
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('original response differs')
    return json.loads(raw)

def replay(key,reader=fetch):
    if not re.fullmatch(r'data/tenor-research/runs/[a-f0-9]{64}\.json',key):raise ValueError('expected immutable run key')
    raw=reader(key)
    if hashlib.sha256(raw).hexdigest()!=Path(key).stem:raise ValueError('run hash differs')
    run=json.loads(raw)
    if run.get('contract')!='treasury-tenor-run.v1':raise ValueError('unsupported run')
    if set(run.get('compilers',{}))!={'tenor_research_model','treasury_instruments'}:raise ValueError('incomplete compiler identity')
    for name,ref in run['compilers'].items():
        if hashlib.sha256((ROOT/'aws/shared'/(name+'.py')).read_bytes()).hexdigest()!=ref['sha256']:
            raise ValueError('local compiler differs; check out the reviewed release that produced this run')
    pages=[original(ref,reader) for ref in run['fiscal_pages']]
    fred=original(run['fred'],reader) if run['fred'] else None
    output=compile_research(pages,fred,run['as_of'],run['source_complete'])
    output['quality']['source_errors']=run['source_errors']
    sha=hashlib.sha256(canonical(output)).hexdigest()
    if sha!=run['output_sha256'] or canonical(output)!=canonical(run['output']):raise ValueError('reproduced measurements differ')
    return {'status':'REPRODUCED','output_sha256':sha,'source_pages':len(pages),
            'quality':output['quality'],'sizing_eligible':False,
            'scope':'Current retrieved source vintage; no historical release-timing or forecast validation'}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('run_key')
    try:print(json.dumps(replay(parser.parse_args().run_key),indent=2))
    except Exception as exc:print(str(exc),file=sys.stderr);sys.exit(1)
