"""Independently replay a retained FR2004 fails snapshot from original bytes."""
import argparse
import re
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-settlement-fails/source')]
import fails_native as native
import fails_research as model
from fails_store import replay
from replay_fred_vintage import read_public


def verify_current(read=read_public,run=None):
    packet=None
    if run:
        if not re.fullmatch('[a-f0-9]{64}',run):raise ValueError('run hash required')
        key=model.PREFIX+'runs/'+run+'.json'
    else:
        packet=native.strict_json(read(model.CURRENT));key=packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('run path differs')
    manifest=native.strict_json(read(key))
    if key!=model.PREFIX+'runs/'+native.digest(manifest)+'.json':raise ValueError('run identity differs')
    output=replay(manifest,read)
    if packet is not None and (output!={k:v for k,v in packet.items() if k!='replay'} or native.digest(output)!=packet['replay']['output_sha256']):
        raise ValueError('current output differs')
    return output


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run');args=parser.parse_args()
    output=verify_current(run=args.run)
    print('REPRODUCED',len(output['series_coverage']),'original FR2004 series;',native.digest(output),'; no Calls/sizing authority')
