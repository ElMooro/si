"""Recompute native Plumbing research from retained FRED/OFR originals."""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-crisis-plumbing/source'),str(ROOT/'aws/shared')]
from plumbing_research_store import replay,allowed
from replay_fred_levels import read_public


def read(key):
    if not allowed(key):raise ValueError('unapproved public Plumbing artifact')
    return read_public(key)


def verify(packet,reader=read):
    output=replay(packet['replay'],reader)
    if {k:v for k,v in packet.items() if k!='replay'}!=output:raise ValueError('published Plumbing differs from original-source replay')
    return {'contract':output['contract'],'generated_at':output['generated_at'],'replayed':True,'quality':output['quality'],
        'native_history_rows':sum((r.get('history') or {}).get('observations',0) for r in output['measurements'].values()),
        'comparisons':len(output['comparisons']),'sizing_eligible':output['sizing_eligible']}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--packet',default='data/crisis-plumbing.json');args=parser.parse_args()
    print(json.dumps(verify(json.loads(read(args.packet))),sort_keys=True))
