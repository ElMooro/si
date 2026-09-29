"""Reconstruct retained TIC originals with local reviewed compilers; no acquisition or writes."""
import hashlib,json
from pathlib import Path
import tic_original as native
import tic_research as model
import evidence_store,report_observations
COMPILERS=(native,model,evidence_store,report_observations)
MAX_BYTES=32*1024*1024

def replay(manifest,read):
    if manifest.get('contract')!='tic-original-replay.v1' or set(manifest.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('tic replay contract differs')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:raise ValueError('reviewed compiler differs; use matching checkout')
    def load(name):
        ref=manifest[name];body=read(ref['key'])
        if ref['key']!=model.PREFIX+name+'s/'+ref['sha256']+'.json' or len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=ref['sha256']:raise ValueError('retained '+name+' differs')
        return native.strict_json(body)
    output,histories=model.build(load('input'),read,manifest['generated_at'])
    # Ordinary JSON retains numeric output types; source parsing alone uses exact strings.
    reference=json.loads(read(manifest['output']['key']))
    if output!=reference or model.digest(output)!=manifest['output_sha256']:raise ValueError('tic original replay differs')
    load('output')
    for key,body in histories.items():
        if read(key)!=body:raise ValueError('tic history shard differs')
    return output
