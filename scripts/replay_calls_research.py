"""Reproduce a public Calls research run without AWS credentials or model APIs.

Use the matching source checkout if the bundle reports different compiler hashes.
This tool reads JSON and runs the local reviewed compiler; it never executes code
from an archive. Private account snapshots are outside this public replay scope.
"""
import argparse
import gzip
import io
import json
from pathlib import Path
import sys
import urllib.request
from urllib.parse import urlsplit
import re

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'aws/shared'))
from calls_research_replay import replay
from calls_liquidity_binding import lineage


def original_reader(directory=None):
    base=directory.resolve() if directory is not None else None
    def read(key):
        if base is not None:
            path=(base/key).resolve()
            if not path.is_relative_to(base):raise ValueError('Original archive path escapes the mirror')
            with path.open('rb') as body:raw=lineage.store.bounded(body)
        else:
            request=urllib.request.Request('https://justhodl.ai/'+key,
                headers={'User-Agent':'JustHodl-release-verify/1.0'})
            raw=lineage.store.bounded(urllib.request.urlopen(request,timeout=45))
        if key.endswith('.gz'):raw=lineage.store.bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        return raw
    return lineage.ImmutableReader(read)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', type=Path)
    group.add_argument('--url')
    parser.add_argument('--originals-dir',type=Path,help='Optional complete local mirror of immutable artifacts, preserving data/... paths')
    args = parser.parse_args()
    if args.file: bundle = json.loads(args.file.read_text(encoding='utf-8'))
    else:
        parsed=urlsplit(args.url)
        if (parsed.scheme!='https' or parsed.netloc!='justhodl.ai' or parsed.query or parsed.fragment
            or not re.fullmatch(r'/data/calls-research-runs/[a-f0-9]{64}\.json',parsed.path)):
            raise ValueError('Exact public JustHodl frozen-run URL required')
        req = urllib.request.Request(args.url, headers={'User-Agent': 'justhodl-verify-release/1.0'})
        bundle=lineage.store.strict(lineage.store.bounded(urllib.request.urlopen(req,timeout=30)))
    print(json.dumps(replay(bundle,original_reader(args.originals_dir)), indent=2))


if __name__ == '__main__': main()
