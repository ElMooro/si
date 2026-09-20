"""Replay native eurodollar originals with the authorized AWS runner's IAM.

Reads only reviewed eurodollar artifacts and their protected source originals.
No producer invocation, account data, credential acquisition or publication.
"""
from pathlib import Path
import argparse,json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-eurodollar-stress/source'),str(ROOT/'aws/shared')]
import eurodollar_research_store as store


def verify(packet,read):
    output=store.replay(packet['replay'],read)
    if {k:v for k,v in packet.items() if k!='replay'}!=output:
        raise ValueError('Published eurodollar differs from retained original-source replay')
    return {'contract':output['contract'],'generated_at':output['generated_at'],'as_of':output['as_of'],
        'replayed':True,'quality':output['quality'],'reviewed_series':len(output['measurements']),
        'source_originals':2*len([m for m in output['measurements'].values() if m['evidence']]),'forecast_qualified':output['forecast_qualified']}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet',default=store.CURRENT);args=parser.parse_args()
    import boto3
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(args.packet)),read),sort_keys=True))
