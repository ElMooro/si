"""Reproduce a native ACM publication from its original workbook and code."""
import argparse,json,sys,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-term-premium/source'))
import term_premium_store as store


def read(key):
    if not store.allowed(key):raise ValueError('Unapproved public ACM artifact')
    url='https://justhodl-data-proxy.raafouis.workers.dev/'+key+'?exact=1&nogen=1'
    with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'JustHodl-research-acceptance/1.0'}),timeout=45) as response:
        return store.bounded(response)


def verify(packet,reader=read):
    if packet.get('contract')!=store.model.CONTRACT:raise ValueError('Native publication not yet verified')
    output=store.replay(packet,reader)
    return {'contract':output['contract'],'generated_at':output['generated_at'],'acquired_at':output['source']['acquired_at'],
        'workbook_sha256':output['source']['sha256'],'full_replay':True,**output['original_arithmetic_checks'],
        'current_series':output['quality']['current_series'],'calls_eligible':output['calls_eligible'],'sizing_eligible':output['sizing_eligible']}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--packet',default=store.model.CURRENT);args=parser.parse_args()
    print(json.dumps(verify(store.strict(read(args.packet))),sort_keys=True))
