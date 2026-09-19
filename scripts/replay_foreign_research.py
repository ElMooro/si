"""Reproduce original foreign securities research and its optional TIC holdings view."""
import argparse,json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-foreign-flows/source'),str(ROOT/'aws/lambdas/justhodl-tic-flows/source')]
import foreign_research as model
from foreign_store import replay
import tic_view
from replay_fred_vintage import read_public

def verify_current(read=read_public,run=None):
    packet=None
    if run:
        if not re.fullmatch('[a-f0-9]{64}',run):raise ValueError('run hash required')
        key=model.PREFIX+'runs/'+run+'.json'
    else:
        packet=json.loads(read(model.CURRENT));key=packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('foreign run path differs')
    manifest=json.loads(read(key))
    if key!=model.PREFIX+'runs/'+model.digest(manifest)+'.json':raise ValueError('foreign run identity differs')
    output=replay(manifest,read)
    if packet is not None and (output!={k:v for k,v in packet.items() if k!='replay'} or model.digest(output)!=packet['replay']['output_sha256']):raise ValueError('current foreign output differs')
    return output

def verify_view(read=read_public,run=None):
    packet=None
    if run:
        if not re.fullmatch('[a-f0-9]{64}',run):raise ValueError('view run hash required')
        key=tic_view.PREFIX+'runs/'+run+'.json'
    else:
        packet=json.loads(read(tic_view.CURRENT));key=packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(tic_view.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('view run path differs')
    manifest=json.loads(read(key))
    if key!=tic_view.PREFIX+'runs/'+tic_view.digest(manifest)+'.json':raise ValueError('view run identity differs')
    output=tic_view.replay(manifest,read)
    if packet is not None and (output!={k:v for k,v in packet.items() if k!='replay'} or tic_view.digest(output)!=packet['replay']['output_sha256']):raise ValueError('current view differs')
    source=verify_current(read=read,run=output['foreign_manifest']['sha256'])
    if model.digest(source)!=output['foreign_output']['sha256']:raise ValueError('view source reproduction differs')
    return output

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run');parser.add_argument('--view',action='store_true');args=parser.parse_args()
    output=(verify_view if args.view else verify_current)(run=args.run)
    print('REPRODUCED',model.digest(output),'; original research only, no allocation authority')
