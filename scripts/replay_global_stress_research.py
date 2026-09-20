"""Recompute Global Stress from retained provider responses using the reviewed local compiler."""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-global-stress/source'),str(ROOT/'aws/shared')]
from global_research_store import replay
from replay_fred_levels import read_public

def verify(packet,read=read_public):
    output=replay(packet['replay'],read)
    if {k:v for k,v in packet.items() if k!='replay'}!=output:raise ValueError('published Global Stress differs from original-source replay')
    return {'contract':output['contract'],'generated_at':output['generated_at'],'replayed':True,
        'quality':output['quality'],'matched_pairs':len(output['correlations']['pairs']),'sizing_eligible':output['sizing_eligible']}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--packet',default='data/global-stress.json');args=parser.parse_args()
    print(json.dumps(verify(json.loads(read_public(args.packet))),sort_keys=True))
