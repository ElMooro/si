"""Read-only original-source sector replay using authorized AWS runner IAM."""
from pathlib import Path
import argparse,json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import sector_tilt_store as store

def verify(packet,read):
    output=store.replay(packet['replay'],read)
    if {k:v for k,v in packet.items() if k!='replay'}!=output:raise ValueError('Published sector research differs from original-source replay')
    return {'contract':output['contract'],'generated_at':output['generated_at'],'replayed':True,'sectors':len(output.get('sectors',output.get('tilts',[]))),'portfolio_action':output['portfolio_action'],'no_allocation_authority':all(output[k] is False for k in store.model.PERMISSIONS)}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--packet',default=store.CURRENT);args=parser.parse_args()
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(args.packet)),read),sort_keys=True))
