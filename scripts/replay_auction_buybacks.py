#!/usr/bin/env python3
"""Replay a saved buyback packet using current reviewed code, without network I/O."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'aws/shared'))
from auction_buybacks import replay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    args = parser.parse_args()
    print(json.dumps(replay(json.loads(args.file.read_text(encoding='utf-8'))), allow_nan=False, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print('Buyback replay rejected: '+str(exc), file=sys.stderr)
        sys.exit(1)
