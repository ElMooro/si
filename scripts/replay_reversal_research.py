"""Independently reproduce a retained Reversal run from original FRED response bytes."""
import argparse, json, re, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-liquidity-reversal/source')]
from reversal_store import replay
from reversal_research import CURRENT,PREFIX,digest
from replay_fred_vintage import read_public


def verify_current(read=read_public, run=None):
    packet=None
    if run:
        if not re.fullmatch('[a-f0-9]{64}',run): raise ValueError('run hash required')
        key=PREFIX+'runs/'+run+'.json'
    else:
        packet=json.loads(read(CURRENT));key=packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key): raise ValueError('run path differs')
    manifest=json.loads(read(key))
    if key!=PREFIX+'runs/'+digest(manifest)+'.json': raise ValueError('run identity differs')
    output=replay(manifest,read)
    if packet is not None and (output!={k:v for k,v in packet.items() if k!='replay'} or digest(output)!=packet['replay']['output_sha256']):
        raise ValueError('current output differs')
    return output


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run');args=parser.parse_args()
    output=verify_current(run=args.run)
    print('REPRODUCED',output['quality']['original_verified_series'],'original native series;',
          output['inventory']['entries'],'retained inventory entries;',digest(output),'No Calls/sizing authority')
