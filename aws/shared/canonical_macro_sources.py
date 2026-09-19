"""Verify selected canonical measurements against retained original responses."""
import hashlib
import json
from pathlib import Path
import re
import report_observations
from research_brief_model import digest


def originals(source, read, series):
    ref=source.get('replay') or {}; key=ref.get('manifest_key','')
    if not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',key):
        raise ValueError('canonical source replay missing')
    manifest=json.loads(read(key))
    if key!='data/report-research/runs/'+digest(manifest)+'.json':raise ValueError('macro manifest differs')
    body=Path(report_observations.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest()
    compiler=manifest['compiler']
    if compiler!={'key':'data/report-research/compilers/'+sha+'.py','sha256':sha} or read(compiler['key'])!=body:
        raise ValueError('original compiler differs from reviewed code')
    if ref.get('compiler_sha256')!=sha or ref.get('output_sha256')!=manifest['output_sha256']:
        raise ValueError('canonical source reference differs')
    if digest({k:v for k,v in source.items() if k!='replay'})!=manifest['output_sha256']:
        raise ValueError('canonical source content differs')
    output={}
    for sid in series:
        if sid not in source.get('measurements',{}):continue
        entry=manifest['inputs'][sid]
        item={'evidence':entry['evidence'],'acquired_at':entry['acquired_at']}
        for part,receipt in entry['evidence'].items():
            raw=read(receipt['key'])
            if len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=receipt['sha256']:
                raise ValueError('native original response differs')
            item[part]=json.loads(raw)
        output[sid]=item
    return output
