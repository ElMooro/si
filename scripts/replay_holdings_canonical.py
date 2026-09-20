"""Replay complete original SEC evidence and all canonical holdings views."""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/lambdas/justhodl-13f-positions/source')]
import holdings_canonical as canonical
from replay_fred_vintage import read_public


def verify_current(read=read_public, run=None):
    current = None
    if run:
        if not re.fullmatch('[a-f0-9]{64}', run):
            raise ValueError('Canonical run hash required')
        key = canonical.model.PREFIX + 'canonical/runs/' + run + '.json'
        raw = read(key)
        ref = canonical.store.reference(key, raw)
    else:
        current = json.loads(read(canonical.CURRENT)); ref = current['canonical_replay']
    manifest = canonical.verified(ref, read, 'runs')
    products = canonical.replay(manifest, read)
    if current is not None and {k: v for k, v in current.items() if k != 'canonical_replay'} != products[canonical.CURRENT]:
        raise ValueError('Current canonical pointer differs from complete replay')
    return manifest, products


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--run'); args = parser.parse_args()
    manifest, products = verify_current(run=args.run)
    print('REPRODUCED', canonical.model.digest(manifest), ';', len(products), 'complete canonical views; no transaction inference')
