"""Replay native valuation sources using the authorized AWS runner IAM.
Reads reviewed research and protected originals only; no invocation/publication.
"""
from pathlib import Path
import argparse,json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-valuations-agent/source'),str(ROOT/'aws/shared')]
import valuation_research_store as store

def verify(packet,read):
    output=store.replay(packet['replay'],read)
    if {k:v for k,v in packet.items() if k!='replay'}!=output:raise ValueError('Published valuation differs from original-source replay')
    return {'contract':output['contract'],'generated_at':output['generated_at'],'replayed':True,'quality':output['quality'],
        'declared_series':len(output['measurements']),'source_originals':2*sum(bool(m.get('evidence')) for m in output['measurements'].values()),
        'available_comparisons':{k:v['value'] for k,v in output['descriptive_comparisons'].items() if v['available']},
        'forecast_qualified':output['forecast_qualified']}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--packet',default=store.CURRENT);args=parser.parse_args()
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(args.packet)),read),sort_keys=True))
