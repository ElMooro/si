"""Reproduce the research brief, including its upstream original FRED inputs."""
import hashlib
import json
from pathlib import Path
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
from research_brief_model import build, digest
from replay_report_research import read_public, replay as replay_source


def replay(manifest, read=read_public):
    if manifest.get('contract') != 'research-intelligence-replay.v1':
        raise ValueError('unsupported brief replay')
    compiler = (ROOT/'aws/shared/research_brief_model.py').read_bytes()
    if hashlib.sha256(compiler).hexdigest() != manifest['compiler']['sha256']:
        raise ValueError('local reviewed compiler differs; check out matching commit')
    original = read(manifest['input']['key'])
    if len(original) != manifest['input']['bytes'] or hashlib.sha256(original).hexdigest() != manifest['input']['sha256']:
        raise ValueError('retained brief input differs')
    packet = json.loads(original)
    if packet['replay'] != manifest['upstream_replay']:
        raise ValueError('upstream replay reference differs')
    upstream = json.loads(read(packet['replay']['manifest_key']))
    if packet['replay']['manifest_key'] != 'data/report-research/runs/' + digest(upstream) + '.json':
        raise ValueError('upstream run identity differs')
    rebuilt_source = replay_source(upstream, read=read)
    if {k: v for k, v in packet.items() if k != 'replay'} != rebuilt_source:
        raise ValueError('brief input differs from original-source reconstruction')
    output = build(packet, manifest['generated_at'])
    if digest(output) != manifest['output_sha256']:
        raise ValueError('reproduced brief differs')
    return output


if __name__ == '__main__':
    req = urllib.request.Request('https://justhodl.ai/intelligence-report.json', headers={'User-Agent': 'justhodl-verify-release/1.0'})
    with urllib.request.urlopen(req, timeout=45) as response:
        packet = json.loads(response.read(32*1024*1024))
    manifest = json.loads(read_public(packet['replay']['manifest_key']))
    output = replay(manifest)
    assert all(packet.get(key) == value for key, value in output.items()), 'current brief differs from replay'
    print('REPRODUCED', len(output['metrics_table']), 'catalog entries from original FRED inputs;', digest(output))
