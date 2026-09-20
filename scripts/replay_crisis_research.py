"""Recompute Crisis research from retained original FRED/ECB bytes and reviewed compilers."""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-crisis-composite/source'),str(ROOT/'aws/shared')]
from crisis_research_store import replay,allowed
from replay_fred_levels import read_public


def read(key):
    if not allowed(key):raise ValueError('unapproved public Crisis artifact')
    return read_public(key)


def verify(packet,reader=read):
    output=replay(packet['replay'],reader)
    if {k:v for k,v in packet.items() if k!='replay'}!=output:raise ValueError('published Crisis differs from original-source replay')
    return {'contract':output['contract'],'generated_at':output['generated_at'],'replayed':True,
        'quality':output['quality'],'fred_history_rows':sum(len(r['history']) for r in output['measurements'].values()),
        'ecb_history_rows':len((output.get('ciss') or {}).get('history',[])),
        'comparisons':len(output['comparisons']),'sizing_eligible':output['sizing_eligible']}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--packet',default='data/crisis-composite.json');args=parser.parse_args()
    print(json.dumps(verify(json.loads(read(args.packet))),sort_keys=True))
