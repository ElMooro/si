"""Reproduce a retained tail sample with read-only IAM source access."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-tail-risk/source'))
sys.path.insert(0,str(ROOT/'aws/shared'))
import tail_research_store as store

def verify(packet,read):
    body=store.replay(packet['replay'],read)
    if body!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Published tail sample differs from retained original replay')
    return {'contract':body['contract'],'generated_at':body['generated_at'],'replayed':True,
        'original_sources':len(body['lineage']['sources']),'eligible_identity_rows':body['quality']['eligible_identity_rows'],
        'sample_coverage':{r['ticker']:r['sample'] for r in body['indices']},'density_qualified':False,'forecast_qualified':False}

if __name__=='__main__':
    import argparse
    argparse.ArgumentParser(description=__doc__).parse_args()
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(store.CURRENT)),read),sort_keys=True))
