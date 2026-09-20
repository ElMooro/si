"""Reproduce a retained insider sample with read-only IAM source access."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-insider-aggregate/source'))
import insider_research_store as store

def verify(packet,read):
    body=store.replay(packet['replay'],read)
    if body!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Published insider sample differs from retained original replay')
    return {'contract':body['contract'],'generated_at':body['generated_at'],'replayed':True,
        'original_pages':len(body['lineage']['sources']),'representations':body['coverage']['distinct_representation_count'],
        'eligible_representations':body['coverage']['eligible_representations'],'population_complete':False,'forecast_qualified':False}

if __name__=='__main__':
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(store.CURRENT)),read),sort_keys=True))
