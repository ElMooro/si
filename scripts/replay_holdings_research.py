"""Reproduce every retained native filing and comparison in a holdings snapshot."""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/lambdas/justhodl-13f-positions/source')]
import holdings_native as model
from holdings_store import replay
from replay_fred_vintage import read_public


def verify_current(read=read_public, run=None):
    packet = None
    if run:
        if not re.fullmatch('[a-f0-9]{64}', run):
            raise ValueError('Run hash required')
        key = model.PREFIX + 'runs/' + run + '.json'
    else:
        packet = json.loads(read(model.CURRENT)); key = packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(model.PREFIX) + r'runs/[a-f0-9]{64}\.json', key):
        raise ValueError('Holdings run identity differs')
    manifest = json.loads(read(key))
    if key != model.PREFIX + 'runs/' + model.digest(manifest) + '.json':
        raise ValueError('Holdings run hash differs')
    output = replay(manifest, read)
    if packet is not None and (output != {k: v for k, v in packet.items() if k != 'replay'}
                              or model.digest(output) != packet['replay']['output_sha256']):
        raise ValueError('Current holdings snapshot differs')
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--run'); args = parser.parse_args()
    output = verify_current(run=args.run)
    print('REPRODUCED', model.digest(output), '; SEC disclosure comparisons, no trade inference')
