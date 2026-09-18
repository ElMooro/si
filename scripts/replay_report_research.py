"""Reproduce the public FRED measurement report from its original response bytes."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
from report_observations import build, digest


def read_public(key):
    if not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key:
        raise ValueError('safe data path required')
    req = urllib.request.Request('https://justhodl.ai/'+key, headers={'User-Agent': 'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(req, timeout=45) as response:
        body = response.read(32*1024*1024+1)
    if len(body) > 32*1024*1024: raise ValueError('public object exceeds bound')
    if key.endswith('.gz'):
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream: body = stream.read(4*1024*1024+1)
        if len(body) > 4*1024*1024: raise ValueError('source exceeds bound')
    return body


def replay(manifest, read=read_public):
    if manifest.get('contract') != 'report-research-replay.v1': raise ValueError('wrong replay contract')
    source = (ROOT/'aws/shared/report_observations.py').read_bytes()
    if hashlib.sha256(source).hexdigest() != manifest['compiler']['sha256']:
        raise ValueError('local reviewed compiler differs; check out matching release, never execute downloaded code')
    inputs = {}
    for sid, descriptor in manifest['inputs'].items():
        entry = {'evidence': descriptor['evidence'], 'acquired_at': descriptor['acquired_at']}
        for part, receipt in descriptor['evidence'].items():
            raw = read(receipt['key'])
            if len(raw) != receipt['bytes'] or hashlib.sha256(raw).hexdigest() != receipt['sha256']:
                raise ValueError('original source differs')
            entry[part] = json.loads(raw)
        inputs[sid] = entry
    out = build(manifest['catalog'], inputs, manifest['generated_at'], manifest['errors'])
    if digest(out) != manifest['output_sha256']: raise ValueError('reproduced output differs')
    return out


if __name__ == '__main__':
    packet = json.loads(read_public('data/report-measurements.json'))
    manifest = json.loads(read_public(packet['replay']['manifest_key']))
    out = replay(manifest)
    print('REPRODUCED', len(out['measurements']), 'series from original definitions and observations', digest(out))
