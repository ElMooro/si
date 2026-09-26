"""Read-only Sentinel original-source reconstruction using existing runner IAM."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-us10y-sentinel/source'),str(ROOT/'aws/ops/checks')]
import sentinel_store as store
from sentinel_original_arithmetic import verify as arithmetic


def verify(packet,read):
    if packet.get('contract')!=store.model.CONTRACT:raise ValueError('Native Sentinel research publication pending')
    full,view=store.replay(packet['replay'],read)
    if not store.same(view,{k:v for k,v in packet.items() if k!='replay'}):raise ValueError('Public view differs from retained originals')
    originals={sid:store.sources.original(sid,'observations',row['sources']['observations'],full['generated_at'],read)
        for sid,row in full['original_sources'].items()}
    proof=arithmetic(full,originals)
    return {'contract':full['contract'],'generated_at':full['generated_at'],'replayed':True,**proof,
        'output_sha256':packet['replay']['output_sha256'],'calls_eligible':False,'sizing_eligible':False,
        'execution_eligible':False,'source_access':'runner_iam','public_redistribution_qualified':False}


if __name__=='__main__':
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(store.strict(read(store.model.CURRENT)),read),sort_keys=True))
