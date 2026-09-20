"""Independently replay native FRED vault observations from their retained source bytes."""
import argparse
import gzip
import io
import json
from pathlib import Path
import re
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/lambdas/justhodl-tradingview/source'))
import fred_level_model as model


def read_public(key):
    if not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key: raise ValueError('safe public key required')
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
             headers={'User-Agent': 'justhodl-verify-release/1.0'}), timeout=45) as response:
        raw = response.read(32*1024*1024+1)
    if len(raw) > 32*1024*1024: raise ValueError('artifact exceeds bound')
    if key.endswith('.gz'):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream: raw = stream.read(32*1024*1024+1)
        if len(raw) > 32*1024*1024: raise ValueError('decompressed artifact exceeds bound')
    return raw


def replay(reference, read=read_public):
    def verified(ref):
        raw = read(ref['key'])
        if model.digest(raw) != ref['sha256'] or len(raw) != ref['bytes']: raise ValueError('artifact identity differs')
        return raw
    manifest = json.loads(verified(reference))
    if manifest.get('contract') != 'fred-native-level-replay.v1': raise ValueError('unsupported replay')
    compiler = verified(manifest['compiler'])
    if compiler != Path(model.__file__).read_bytes(): raise ValueError('use matching reviewed compiler checkout')
    originals = {}
    for kind in ('observations', 'definition'):
        ref = manifest['evidence'][kind]
        if ref.get('provider') != 'fred' or ref.get('captured') is not True: raise ValueError('original FRED receipt required')
        originals[kind] = verified(ref)
    out = model.compile_level(manifest['series_id'], originals['observations'], originals['definition'], manifest['collected_at'])
    if model.digest(model.encoded(out)) != manifest['output_sha256']: raise ValueError('original-source calculation differs')
    return {**out, 'evidence': manifest['evidence'], 'replay': reference}


def verify_vault(packet, read=read_public):
    replayed, aliases = {}, 0
    for row in packet['symbols']:
        if row.get('contract_version') != model.CONTRACT: continue
        sid = row['series_id']; ref = row['replay']
        identity = (sid, ref['sha256'])
        if identity not in replayed: replayed[identity] = replay(ref, read)
        expected = replayed[identity]
        # Failed later refreshes can change status/quality, but never renew source clocks.
        allowed_changed = {'status', 'quality'} if row.get('quality', {}).get('status') == 'refresh_unavailable' else set()
        for key, value in expected.items():
            if key not in allowed_changed and row.get(key) != value: raise ValueError('vault observation differs: '+sid+'/'+key)
        aliases += 1
    if not replayed: raise ValueError('no source-bound native levels found')
    return {'series_vintages_replayed': len(replayed), 'alias_rows_checked': aliases}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--vault', default='data/tradingview.json')
    args = parser.parse_args()
    print(json.dumps(verify_vault(json.loads(read_public(args.vault))), sort_keys=True))
