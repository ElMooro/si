"""Reconstruct current ETF holdings from protected originals on the AWS runner."""
from pathlib import Path
import argparse,json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import etf_holdings_store as store


def verify(packet,read):
    out=store.replay(packet['replay'],read)
    if out!={k:v for k,v in packet.items() if k!='replay'}:
        raise ValueError('Public holdings body differs from original-source replay')
    if any(out[k] is not False for k in store.model.PERMISSIONS) or out['call'] is not None or out['portfolio_action']!='WAIT':
        raise ValueError('Descriptive holdings cannot acquire investment authority')
    return {'contract':out['contract'],'generated_at':out['generated_at'],'originals_replayed':True,
        'configured_funds':len(out['funds']),'quality':out['quality'],'independent_investment_votes':0,
        'unit_qualification_withheld':out['quality']['weight_unit_certified'] is False,
        'no_allocation_authority':True}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet',choices=(store.model.CURRENT,store.model.LOOK_CURRENT),default=store.model.CURRENT)
    args=parser.parse_args()
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(args.packet)),read),sort_keys=True))
