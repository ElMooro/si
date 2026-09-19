"""Reproduce a retained dealer snapshot from original provider responses."""
import argparse,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-nyfed-pd/source')]
import dealer_original as native
import dealer_research as model
from dealer_research_store import replay
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
    p=argparse.ArgumentParser();p.add_argument('--run');args=p.parse_args();output=verify_current(run=args.run)
    print('REPRODUCED',len(output['native_series']),'original dealer series;',native.digest(output),'; no Calls/sizing authority')
