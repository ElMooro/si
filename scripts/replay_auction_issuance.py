"""Offline replay of a saved issuance ledger or an auction research packet."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'aws/shared'))
from auction_issuance import replay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    parser.add_argument('--as-of', help='YYYY-MM-DD, default retained ledger date')
    args = parser.parse_args()
    if args.file.stat().st_size > 64 * 1024 * 1024:
        raise ValueError('Local artifact exceeds 64 MiB')
    root = json.loads(args.file.read_text(encoding='utf-8'))
    ledger = root.get('issuance_ledger') or (root.get('issuance_anomaly') or {}).get('ledger') or root.get('ledger') or root
    print(json.dumps(replay(ledger, args.as_of), indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
