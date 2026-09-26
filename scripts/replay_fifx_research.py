"""Replay every original FI/FX source and every historical statistic sequentially."""
import argparse, gzip, io, json, sys, urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-fifx-vol-migration/source'),str(ROOT/'aws/shared')]
import fifx_store as store

def read(key):
    if not store.allowed(key):raise ValueError('Unapproved FI/FX artifact path')
    url='https://justhodl-data-proxy.raafouis.workers.dev/'+key+'?exact=1&nogen=1'
    request=urllib.request.Request(url,headers={'User-Agent':'JustHodl-research-acceptance/1.0'})
    with urllib.request.urlopen(request,timeout=45) as response:raw=store.bounded(response)
    return store.bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw

def verify(packet,reader=read):
    if packet.get('contract')!=store.model.CONTRACT:raise ValueError('Native publication not yet verified')
    proofs=store.replay(packet,reader)
    return {'contract':packet['contract'],'generated_at':packet['generated_at'],'full_replay':True,
        'sources':len(proofs),'original_rows':sum(p['original_rows'] for p in proofs.values()),
        'history_rows':sum(p['history_rows'] for p in proofs.values()),
        'independent_scalar_checks':sum(p['independent_scalar_checks'] for p in proofs.values()),
        'current_sources':packet['quality']['current_sources'],'calls_eligible':False,'sizing_eligible':False}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--packet',default=store.model.CURRENT);args=parser.parse_args()
    print(json.dumps(verify(store.strict(read(args.packet))),sort_keys=True))
