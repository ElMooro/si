"""Replay provider fund-flow originals on the authorized AWS runner; no refresh."""
from pathlib import Path
import argparse, json, sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import provider_flow_store as store


def verify(packet, read):
    output = store.replay(packet['replay'], read)
    if output != {k: v for k, v in packet.items() if k != 'replay'}:
        raise ValueError('Published fund-flow body differs from original-source replay')
    if (output['call'] is not None or output['portfolio_action'] != 'WAIT'
            or output['quality']['independent_investment_votes'] != 0
            or any(output[k] is not False for k in store.model.PERMISSIONS)):
        raise ValueError('Descriptive flow measurements cannot carry investment authority')
    return {'contract': output['contract'], 'generated_at': output['generated_at'],
        'original_source_replayed': True, 'configured_funds': len(output['funds']),
        'configured_complexes': len(output['complexes']), 'quality': output['quality'],
        'independent_investment_votes': 0, 'no_allocation_authority': True}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', choices=[store.model.CURRENT, store.model.RADAR_CURRENT], default=store.model.CURRENT)
    args = parser.parse_args()
    import boto3
    read = store.reader(boto3.client('s3', region_name='us-east-1'), 'justhodl-dashboard-live')
    print(json.dumps(verify(json.loads(read(args.packet)), read), sort_keys=True))
