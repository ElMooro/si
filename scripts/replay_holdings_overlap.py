"""Replay overlap and its complete canonical/native SEC source using reviewed local code."""
import argparse
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/lambdas/justhodl-smart-money-cluster/source'), str(ROOT/'scripts')]
import holdings_overlap as model
import overlap_store as store
from replay_fred_vintage import read_public
from replay_holdings_canonical import verify_current as replay_canonical


def verify_current(read=read_public, run=None):
    pointer = None
    if run:
        if not re.fullmatch('[a-f0-9]{64}', run): raise ValueError('Immutable overlap run hash required')
        key = model.PREFIX+'runs/'+run+'.json'; raw = read(key); ref = model.reference(key, raw)
    else:
        pointer = model.decode(read(model.CURRENT)); ref = pointer['replay']
    manifest = model.verified(ref, read, model.PREFIX, 'runs')
    upstream = manifest['source']['canonical_replay']['sha256']
    _, products = replay_canonical(read, run=upstream)
    if model.digest(products['data/13f-positions.json']) != manifest['source']['canonical_product']['sha256']:
        raise ValueError('Original-source replay differs from overlap input')
    output = store.replay(manifest, read)
    if pointer is not None and {k: v for k, v in pointer.items() if k != 'replay'} != output:
        raise ValueError('Current overlap differs from complete replay')
    return manifest, output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--run'); args = parser.parse_args()
    manifest, output = verify_current(run=args.run)
    print('REPRODUCED', model.digest(manifest), ';', len(output['funds']), 'managers; original SEC disclosure overlap, no trades inferred')
