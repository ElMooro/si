"""Reproduce a retained retail sample with read-only IAM source access."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-retail-sentiment/source'))
import retail_research_store as store

def verify(packet,read):
    body=store.replay(packet['replay'],read)
    if body!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Published retail sample differs from retained original replay')
    return {'contract':body['contract'],'generated_at':body['generated_at'],'replayed':True,
        'original_sources':len(body['lineage']['sources']),'community_samples':body['quality']['community_samples_available'],
        'sample_symbols':{k:v['eligible_symbols'] for k,v in body['communities'].items()},'population_complete':False,'forecast_qualified':False}

if __name__=='__main__':
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(store.CURRENT)),read),sort_keys=True))
