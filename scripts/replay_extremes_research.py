"""Replay retained synthesis with read-only runner IAM; never execute archived code."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import extremes_native_store as store

def verify(packet,read):
    out=store.replay(packet['replay'],read)
    if out!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Published synthesis differs from retained replay')
    return {'contract':out['contract'],'engine':out['engine'],'generated_at':out['generated_at'],'replayed':True,
        'measurements':len(out['measurements']),'retained_input_packets':sum(bool(e.get('packet')) for e in out['input_evidence'].values()),
        'original_provider_replayed_here':False,'scope':out['replay_scope'],'independent_investment_votes':0}

if __name__=='__main__':
    import boto3
    engine=sys.argv[1] if len(sys.argv)>1 else 'capitulation';key=store.current(engine)
    read=store.reader(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(key)),read),sort_keys=True))
