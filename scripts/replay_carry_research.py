"""Reproduce carry measurements and every history shard from retained originals."""
import argparse,json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-carry-surface/source')]
import carry_research as model
from carry_store import replay
from replay_fred_vintage import read_public


def verify_current(read=read_public,run=None):
    packet=None
    if run:
        if not re.fullmatch('[a-f0-9]{64}',run):raise ValueError('run hash required')
        key=model.PREFIX+'runs/'+run+'.json'
    else:
        packet=json.loads(read(model.CURRENT));key=packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('run path differs')
    manifest=json.loads(read(key))
    if key!=model.PREFIX+'runs/'+model.digest(manifest)+'.json':raise ValueError('run identity differs')
    output=replay(manifest,read)
    if packet is not None and (output!={k:v for k,v in packet.items() if k!='replay'} or model.digest(output)!=packet['replay']['output_sha256']):
        raise ValueError('current output differs')
    return output


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--run');args=p.parse_args();output=verify_current(run=args.run)
    print('REPRODUCED',len(output['equities']),'equities;',len(output['measurements']),'original rate sources;',model.digest(output),'; no Calls/sizing authority')
