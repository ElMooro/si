"""Reproduce a public Calls research run without AWS credentials or model APIs.

Use the matching source checkout if the bundle reports different compiler hashes.
This tool reads JSON and runs the local reviewed compiler; it never executes code
from an archive. Private account snapshots are outside this public replay scope.
"""
import argparse
import json
from pathlib import Path
import sys
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'aws/shared'))
from calls_research_replay import replay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', type=Path)
    group.add_argument('--url')
    args = parser.parse_args()
    if args.file: bundle = json.loads(args.file.read_text(encoding='utf-8'))
    else:
        req = urllib.request.Request(args.url, headers={'User-Agent': 'justhodl-verify-release/1.0'})
        with urllib.request.urlopen(req, timeout=30) as response: bundle = json.load(response)
    print(json.dumps(replay(bundle), indent=2))


if __name__ == '__main__': main()
