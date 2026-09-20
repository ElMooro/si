"""Recompute a Stress Research run from retained provider bytes, never downloaded code."""
import argparse
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/lambdas/justhodl-stress-index/source'), str(ROOT/'aws/shared')]
from stress_store import replay
from replay_fred_levels import read_public


def verify(packet, read=read_public):
    output = replay(packet['replay'], read)
    if {k:v for k,v in packet.items() if k != 'replay'} != output:
        raise ValueError('published Stress Research differs from original-source replay')
    return {'contract': output['contract'], 'generated_at': output['generated_at'], 'replayed': True,
        'native_series': len(output['measurements']),
        'quality': output['quality'], 'sizing_eligible': output['sizing_eligible']}


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--packet', default='data/jsi.json'); args=parser.parse_args()
    print(json.dumps(verify(json.loads(read_public(args.packet))), sort_keys=True))
