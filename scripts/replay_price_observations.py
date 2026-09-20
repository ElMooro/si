"""Verify original provider responses and recompute public vault price observations."""
import argparse
import copy
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-tradingview/source'),str(ROOT/'aws/shared')]
import price_model as model
from price_io import reconstruct
from replay_fred_levels import read_public


def verify_vault(packet, read=read_public):
    replayed={};count=0;statuses={}
    for row in packet['symbols']:
        if row.get('contract_version')!=model.CONTRACT:continue
        identity=(row['instrument_id'],row['replay']['sha256'])
        if identity not in replayed:replayed[identity]=reconstruct(row['replay'],read)
        expected=replayed[identity]
        allowed=set()
        quality=row.get('quality',{}).get('status')
        if quality=='refresh_unavailable':allowed={'status','quality'}
        elif quality=='mapping_conflict':
            if row.get('status')!='MAPPING_REVIEW':raise ValueError('mapping conflict cannot be eligible')
            allowed={'status','quality'}
        for key,value in expected.items():
            if key not in allowed and row.get(key)!=value:raise ValueError('native price differs: '+row['symbol']+'/'+key)
        if any(row.get(k) is not False for k in ('comparison_eligible','calls_eligible','sizing_eligible','execution_eligible')):
            raise ValueError('unqualified price has decision authority')
        statuses[row['status']]=statuses.get(row['status'],0)+1;count+=1
    if not replayed:raise ValueError('no original-source prices found')
    return {'instrument_vintages_replayed':len(replayed),'alias_rows_checked':count,'statuses':statuses}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--vault',default='data/tradingview.json');args=parser.parse_args()
    print(json.dumps(verify_vault(json.loads(read_public(args.vault))),sort_keys=True))
