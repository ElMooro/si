"""Replay the daily report's base fields; macro originals verified separately.

The market-collector input snapshot is reproducible, not original-source price
proof. Augmentation-owned fields are intentionally outside this base receipt.
"""
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import daily_macro_model
import research_brief_model
from daily_macro_model import build,digest
from replay_report_research import read_public,replay as replay_macro


def replay(manifest,read=read_public):
    if manifest.get('contract')!='daily-research-replay.v1': raise ValueError('unsupported report replay')
    for module in (daily_macro_model,research_brief_model):
        body=Path(module.__file__).read_bytes()
        ref=manifest['compilers'][module.__name__]
        if hashlib.sha256(body).hexdigest()!=ref['sha256'] or read(ref['key'])!=body:
            raise ValueError('retained/local reviewed compiler differs')
    raw=read(manifest['input']['key'])
    if len(raw)!=manifest['input']['bytes'] or hashlib.sha256(raw).hexdigest()!=manifest['input']['sha256']:
        raise ValueError('retained report input differs')
    inputs=json.loads(raw);source=inputs['macro']
    if source['replay']!=manifest['upstream_replay']: raise ValueError('source replay differs')
    upstream=json.loads(read(source['replay']['manifest_key']))
    if source['replay']['manifest_key']!='data/report-research/runs/'+digest(upstream)+'.json':
        raise ValueError('source run identity differs')
    reconstructed=replay_macro(upstream,read=read)
    if {k:v for k,v in source.items() if k!='replay'}!=reconstructed:
        raise ValueError('report macro input differs from original-source replay')
    output=build(source,inputs['auxiliary'],manifest['generated_at'])
    if digest(output)!=manifest['output_sha256'] or sorted(output)!=manifest['base_fields']:
        raise ValueError('report reconstruction differs')
    return output


if __name__=='__main__':
    packet=json.loads(read_public('data/report.json'))
    key=packet['replay']['manifest_key'];manifest=json.loads(read_public(key))
    if key!='data/daily-research/runs/'+digest(manifest)+'.json': raise ValueError('run identity differs')
    output=replay(manifest)
    if any(packet.get(key)!=value for key,value in output.items()): raise ValueError('current base differs')
    print('REPRODUCED base report',digest(output),'macro originals verified; market auxiliaries remain unverified')
