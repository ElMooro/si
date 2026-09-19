"""Independently reproduce retained Alpha projections; no AWS credentials or archived code execution."""
import argparse
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import alpha_research as model
from alpha_research_store import load_manifest,replay
from replay_fred_vintage import read_public


def verify_current(key,read=read_public):
    packet=json.loads(read(key));manifest=load_manifest(packet['replay'],read);output=replay(manifest,read)
    if output!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('current Alpha output differs')
    return output


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--brief',action='store_true');args=parser.parse_args()
    output=verify_current(model.BRIEF if args.brief else model.CURRENT)
    print('REPRODUCED',output['contract'],output['generated_at'],model.digest(output),
          'Typed public projections; original provider lineage and sizing not qualified')
