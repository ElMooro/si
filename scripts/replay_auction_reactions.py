#!/usr/bin/env python3
"""Replay a saved auction reaction input ledger or packet without network I/O."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from auction_reactions import replay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    args = parser.parse_args()
    packet = json.loads(args.file.read_text(encoding='utf-8'))
    container = packet.get('reactions', packet)
    inputs = container.get('comparison_inputs', container)
    result = replay(inputs)
    for key in ('stats', 'baseline', 'n_events'):
        if key in container and container[key] != result[key]:
            raise ValueError('Published '+key+' does not reproduce from retained inputs')
    print(json.dumps(result, allow_nan=False, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print('Reaction replay rejected: '+str(exc), file=sys.stderr)
        sys.exit(1)
