"""Reconstruct the whole native Yield Curve run, every series, and its view."""
import argparse,json,sys,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-yield-curve/source'),str(ROOT/'aws/shared')]
import yield_curve_store as store
from replay_fred_levels import read_public


def read(key):
    if not store.allowed(key):raise ValueError('Unapproved public curve artifact')
    if key==store.model.CURRENT:
        url='https://justhodl-data-proxy.raafouis.workers.dev/'+key+'?exact=1&nogen=1'
        with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'JustHodl-research-acceptance/1.0'}),timeout=45) as response:
            raw=response.read(store.MAX+1)
        if len(raw)>store.MAX:raise ValueError('Current packet exceeds bound')
        return raw
    return read_public(key)


def verify(packet,reader=read):
    if packet.get('contract')!=store.model.CONTRACT:raise ValueError('Native publication not yet verified')
    full,view=store.replay(packet['replay'],reader)
    if {k:v for k,v in packet.items() if k!='replay'}!=view:raise ValueError('Published view differs')
    return {'contract':full['contract'],'generated_at':full['generated_at'],'replayed':True,
        'original_rows':sum(len(row['history']) for row in full['series'].values()),'requested_series':len(full['series']),
        'current_series':full['quality']['current_series'],'missing_series':full['quality']['missing_series'],
        'calls_eligible':full['calls_eligible'],'sizing_eligible':full['sizing_eligible']}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--packet',default=store.model.CURRENT);args=parser.parse_args()
    print(json.dumps(verify(json.loads(read(args.packet))),sort_keys=True))
