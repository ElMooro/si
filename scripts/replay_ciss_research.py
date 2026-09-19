"""Replay original ECB CSVs using the reviewed local compiler, never downloaded code."""
from concurrent.futures import ThreadPoolExecutor
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
import ciss_source_model as model
from evidence_store import public_source_url

MAX=64*1024*1024
BASE='https://data-api.ecb.europa.eu/service/data/'


def read_public(key):
    if not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('unsafe source path')
    suffix='?exact=1' if key in ('data/ciss-stress.json','data/ciss-ai.json') else ''
    req=urllib.request.Request('https://justhodl.ai/'+key+suffix,headers={'User-Agent':'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(req,timeout=45) as response:raw=response.read(MAX+1)
    if len(raw)>MAX:raise ValueError('public object exceeds bound')
    if key.endswith('.gz'):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:raw=stream.read(MAX+1)
        if len(raw)>MAX:raise ValueError('original source exceeds bound')
    return raw


def compiler(ref,read):
    body=Path(model.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest()
    if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=body:
        raise ValueError('reviewed local compiler differs; use the matching release checkout')


def hydrate(items,read):
    def one(pair):
        key,item=pair;e=item['evidence']
        expected=BASE+key.replace('.','/',1)+'?format=csvdata' if '.' in key else BASE+key+'?format=csvdata&lastNObservations=1'
        if item['request_url']!=expected or e['source_url']!=public_source_url(expected):raise ValueError('ECB request differs')
        request_sha=hashlib.sha256(e['source_url'].encode()).hexdigest()
        if e.get('contract')!='source-evidence.v1' or e.get('provider')!='ecb' or e.get('captured') is not True or e['key']!='data/evidence/ecb/'+request_sha+'/'+e['sha256']+'.bin.gz':
            raise ValueError('ECB evidence identity differs')
        raw=read(e['key'])
        if len(raw)!=e['bytes'] or hashlib.sha256(raw).hexdigest()!=e['sha256']:raise ValueError('original ECB response differs')
        return key,{**item,'raw':raw}
    with ThreadPoolExecutor(max_workers=4) as pool:return dict(pool.map(one,items.items()))


def replay(manifest,read=read_public):
    if manifest['contract']!='ciss-research-replay.v1':raise ValueError('unsupported replay')
    compiler(manifest['compiler'],read)
    out=model.build(hydrate(manifest['discoveries'],read),hydrate(manifest['histories'],read),manifest['generated_at'],manifest['errors'])
    out['collection_started_at']=manifest['collection_started_at']
    if model.digest(out)!=manifest['output_sha256']:raise ValueError('CISS reconstruction differs')
    return out


def replay_commentary(manifest,read=read_public):
    if manifest['contract']!='ciss-commentary-replay.v1':raise ValueError('unsupported commentary replay')
    compiler(manifest['compiler'],read)
    raw=read(manifest['input']['key'])
    if hashlib.sha256(raw).hexdigest()!=manifest['input']['sha256']:raise ValueError('commentary input differs')
    source=json.loads(raw);ref=source['replay']
    if manifest['source_replay']!=ref:raise ValueError('commentary upstream reference differs')
    upstream=json.loads(read(ref['manifest_key']))
    if ref['manifest_key']!=model.PREFIX+'runs/'+model.digest(upstream)+'.json' or ref['output_sha256']!=upstream['output_sha256']:raise ValueError('upstream identity differs')
    rebuilt=replay(upstream,read)
    if rebuilt!={k:v for k,v in source.items() if k!='replay'}:raise ValueError('upstream original-source reconstruction differs')
    out=model.commentary(source,manifest['generated_at'])
    if model.digest(out)!=manifest['output_sha256']:raise ValueError('commentary reconstruction differs')
    return out


if __name__=='__main__':
    key='data/ciss-ai.json' if '--commentary' in sys.argv else 'data/ciss-stress.json'
    packet=json.loads(read_public(key));ref=packet['replay'];manifest=json.loads(read_public(ref['manifest_key']))
    prefix=model.PREFIX+('commentary/' if '--commentary' in sys.argv else '')+'runs/'
    if ref['manifest_key']!=prefix+model.digest(manifest)+'.json':raise ValueError('manifest identity differs')
    out=(replay_commentary if '--commentary' in sys.argv else replay)(manifest)
    if out!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('current packet differs')
    print('REPRODUCED',key,model.digest(out),'from original ECB CSVs and the reviewed compiler')
