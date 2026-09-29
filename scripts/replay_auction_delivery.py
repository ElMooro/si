"""Verify a locally saved auction delivery bundle using reviewed current code.

Usage: python scripts/replay_auction_delivery.py locator.json --root saved-bundle
The root contains data/auction-desk-delivery/... files. No network or archived
compiler execution is supported. Identity covers supplied bytes, not originals.
"""
from pathlib import Path
import argparse
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'aws/shared'))
import auction_delivery as model


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('locator', type=Path)
    parser.add_argument('--root', required=True, type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    def read(key):
        path = (root / key).resolve()
        if not path.is_relative_to(root):
            raise ValueError('Bundle reference escapes the supplied root')
        with path.open('rb') as stream:
            raw = stream.read(model.MAX_PACKET+1)
        if len(raw) > model.MAX_PACKET:
            raise ValueError('Bundle object exceeds the complete-byte bound')
        return raw
    try:
        with args.locator.open('rb') as stream:
            raw = stream.read(model.MAX_VIEW+1)
        if len(raw) > model.MAX_VIEW:
            raise ValueError('Locator is too large')
        locator = model.strict(raw)
        source, _ = model.replay(locator, read)
        print(json.dumps({'status': 'identical_supplied_delivery', 'generated_at': source['generated_at'],
                          'view_bytes': locator['view']['bytes'], **model.FLAGS}))
        return 0
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print('Delivery replay rejected: '+str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
