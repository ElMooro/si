"""Reproduce TIC transaction amounts and every history from retained original bytes."""
import argparse,json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-capital-inflows/source')]
import tic_research as model
from tic_store import replay
from replay_fred_vintage import read_public


def verify_current(read=read_public,run=None):
    packet=None
    if run:
        if not re.fullmatch('[a-f0-9]{64}',run):raise ValueError('run hash required')
        key=model.PREFIX+'runs/'+run+'.json'
    else:
        packet=json.loads(read(model.CURRENT));key=packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('TIC run path differs')
    manifest=json.loads(read(key))
    if key!=model.PREFIX+'runs/'+model.digest(manifest)+'.json':raise ValueError('TIC run identity differs')
    output=replay(manifest,read)
    if packet is not None and (output!={k:v for k,v in packet.items() if k!='replay'} or model.digest(output)!=packet['replay']['output_sha256']):
        raise ValueError('current TIC output differs')
    return output


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--run');args=p.parse_args();out=verify_current(run=args.run)
    print('REPRODUCED',len(out['measurements']),'TIC series;',model.digest(out),'; research only, no allocation authority')
