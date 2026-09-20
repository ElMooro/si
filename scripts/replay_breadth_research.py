"""Replay native breadth originals using the authorized AWS runner's IAM.

Read scope is restricted to this engine's public artifacts and its protected
source archive. This is not an anonymous download of licensed provider originals.
"""
from pathlib import Path
import argparse, json, sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/lambdas/justhodl-market-internals/source'))
import breadth_research_store as store


def verify(packet, read):
    output = store.replay(packet['replay'], read)
    if {k: v for k, v in packet.items() if k != 'replay'} != output:
        raise ValueError('published breadth differs from retained original-source replay')
    return {'contract': output['contract'], 'generated_at': output['generated_at'], 'as_of': output['as_of'],
        'replayed': True, 'quality': output['quality'], 'sessions': len(output['calendar']['requested_sessions']),
        'current_population': len(output['current_constituents']), 'forecast_eligible': output['forecast_eligible']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('--packet', default=store.CURRENT); args = parser.parse_args()
    import boto3
    read = store.reader(boto3.client('s3', region_name='us-east-1'), 'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(args.packet)), read), sort_keys=True))
