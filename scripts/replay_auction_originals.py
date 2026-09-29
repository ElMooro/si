#!/usr/bin/env python3
"""Replay retained auction originals from an offline artifact directory.

The root must contain exact retained objects at their repo-style S3 keys.
The reference file is the auction_originals.manifest JSON object, not a current
engine packet. No network, AWS calls, provider queries or archive code execution.
"""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import auction_original_store as store
import auction_originals as model


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--reference', required=True, type=Path)
    args = parser.parse_args(argv)
    root = args.root.resolve(strict=True)
    if not root.is_dir():
        raise ValueError('Artifact directory required')

    def read(key):
        if not isinstance(key, str) or not key.startswith((model.PREFIX, 'data/evidence/treasury-auctions/')):
            raise ValueError('Only original auction artifacts may be read')
        path = (root/key).resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError('Artifact path escapes supplied directory')
        with path.open('rb') as source:
            raw = source.read(store.MAX_ARTIFACT+1)
        if len(raw) > store.MAX_ARTIFACT:
            raise ValueError('Artifact size exceeds bound')
        return raw

    if args.reference.stat().st_size > 8192:
        raise ValueError('Manifest reference exceeds bound')
    output = store.replay(model.strict_json(args.reference.read_bytes()), read)
    print(json.dumps({'status': 'exact_retained_originals_replayed', 'coverage': output['coverage'],
                      'generated_at': output['generated_at'], 'measurement_sha256': model.sha(model.encoded(output)),
                      'calls_eligible': False, 'sizing_eligible': False}))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'status': 'rejected', 'error': str(exc)}), file=sys.stderr)
        sys.exit(1)
