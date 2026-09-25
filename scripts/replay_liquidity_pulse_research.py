"""Replay the native Pulse packet from retained original FRED responses."""
import argparse, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/lambdas/justhodl-liquidity-pulse/source'), str(ROOT/'aws/shared')]
import liquidity_pulse_store as store
from replay_fred_levels import read_public


def read(key):
    if not store.allowed(key): raise ValueError('Unapproved public liquidity artifact')
    return read_public(key)


def verify(packet, reader=read):
    output = store.replay(packet['replay'], reader)
    if {key: value for key, value in packet.items() if key != 'replay'} != output:
        raise ValueError('Published liquidity differs from original-source replay')
    return {'contract': output['contract'], 'generated_at': output['generated_at'], 'replayed': True,
        'original_rows': sum(len(row['history']) for row in output['series'].values()),
        'requested_series': len(output['series']), 'fresh_series': output['n_series_ok'],
        'calls_eligible': output['calls_eligible'], 'sizing_eligible': output['sizing_eligible']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--packet', default=store.model.CURRENT); args = parser.parse_args()
    print(json.dumps(verify(json.loads(read(args.packet))), sort_keys=True))
