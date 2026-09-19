"""Replay ALFRED archive from original bytes and matching reviewed local compiler."""
import argparse
import hashlib
import gzip,io,re,urllib.request
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import fred_vintage_model as model


def read_public(key):
    if not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('safe public path required')
    req=urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(req,timeout=45) as response:raw=response.read(64*1024*1024+1)
    if len(raw)>64*1024*1024:raise ValueError('public archive bound exceeded')
    if key.endswith('.gz'):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:raw=stream.read(64*1024*1024+1)
        if len(raw)>64*1024*1024:raise ValueError('decompressed archive bound exceeded')
    return raw



def replay(manifest,read=read_public):
    if manifest.get('contract') not in ('fred-vintage-replay.v1','fred-vintage-catalog-replay.v1'):raise ValueError('unsupported vintage replay')
    local=Path(model.__file__).read_bytes();ref=manifest['compiler']
    if hashlib.sha256(local).hexdigest()!=ref['sha256'] or read(ref['key'])!=local:
        raise ValueError('reviewed/retained compiler differs; use matching release checkout')
    def original(descriptor):
        raw=read(descriptor['evidence']['key'])
        return {**descriptor,'raw':raw}
    if manifest['contract']=='fred-vintage-catalog-replay.v1':
        for entry in manifest['segments']:
            doc=json.loads(read(entry['key']));model.validate_segment(entry,doc,manifest['series'])
            key=doc['replay']['manifest_key'];child=json.loads(read(key))
            if key!=model.PREFIX+'runs/'+model.digest(child)+'.json':raise ValueError('segment run identity differs')
            if replay(child,read)!={k:v for k,v in doc.items() if k!='replay'}:raise ValueError('segment original replay differs')
        output=model.compile_catalog(manifest['series'],original(manifest['definition']),manifest['segments'],
            manifest['generated_at'],manifest['collection_id'],manifest['archive_end'],manifest['collection_started_at'])
    else:
        output=model.compile_series(manifest['series'],original(manifest['definition']),
            [original(d) for d in manifest['pages']],manifest['generated_at'],manifest['collection_id'],
            manifest['archive_end'],manifest['collection_started_at'],manifest.get('archive_start'))
    if model.digest(output)!=manifest['output_sha256']:raise ValueError('archive replay differs')
    return output


def load_collection(collection=None,read=read_public):
    if collection is not None and not re.fullmatch(r'[a-f0-9]{64}',collection):
        raise ValueError('collection must be a lowercase SHA-256')
    key=model.PREFIX+'collections/'+collection+'.json' if collection else 'data/vintage/_index.json'
    raw=read(key)
    if collection and hashlib.sha256(raw).hexdigest()!=collection:raise ValueError('immutable collection differs')
    index=json.loads(raw)
    if index.get('contract')!='fred-vintage-index.v1':raise ValueError('original archive index required')
    return index


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('series',choices=model.SERIES)
    parser.add_argument('--collection',help='Immutable collection SHA-256; omit for the current index')
    args=parser.parse_args();index=load_collection(args.collection);entry=index['detail'][args.series]
    if entry['status']!='source_replayed':raise ValueError('series is unavailable in this collection')
    raw=read_public(entry['key']);packet=json.loads(raw)
    if model.digest(packet)!=entry['sha256']:raise ValueError('immutable series output differs')
    if packet['series']!=args.series or packet['collection_id']!=index['collection_id']:raise ValueError('series or collection differs')
    key=packet['replay']['manifest_key'];manifest=json.loads(read_public(key))
    if key!=model.PREFIX+'runs/'+model.digest(manifest)+'.json':raise ValueError('run identity differs')
    output=replay(manifest)
    if output!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('series packet differs')
    print('REPRODUCED',args.series,output['n_vintages'],'original archive periods',model.digest(output))
