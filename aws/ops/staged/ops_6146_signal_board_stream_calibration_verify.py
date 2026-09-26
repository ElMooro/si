"""Read-only deployed source closure and receipt proof; never invoke consumers."""
from pathlib import Path
import json
import subprocess
import sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/shared')]
from ops_report import report
from release_package_evidence import check_packages
from market_runtime_evidence import bounded

FUNCTIONS = ('justhodl-streaming-fanout', 'justhodl-calibration-fleet')


def main():
    subprocess.run([sys.executable, str(ROOT/'tests/test_signal_board_stream_and_calibration.py')], cwd=ROOT, check=True)
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/justhodl-streaming-fanout/source/lambda_function.py'], cwd=ROOT, text=True).strip()
    if len(expected) != 40:
        raise ValueError('Full deployment commit required')
    lam, s3 = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3'))
    with report('ops_6146_signal_board_stream_calibration_verify') as r:
        rows = check_packages(lam, ROOT, FUNCTIONS)
        for row in rows:
            fn = row['function']
            receipt = json.loads(bounded(s3.get_object(Bucket='justhodl-dashboard-live', Key='data/ops/releases/'+fn+'.json')['Body']))
            row['release_commit'] = receipt.get('commit')
            row['receipt_matches'] = (receipt.get('commit') == expected and receipt.get('verified') is True and
                                      receipt.get('code_sha256') == row.get('code_sha256'))
            row['authority_module_checked'] = any(f['member'] == 'signal_board_authority.py' and f['match'] for f in row['files'])
        r.kv(expected_commit=expected, packages=rows, functions_checked=len(rows),
             code_and_receipt_verified=all(row['pass'] and row['receipt_matches'] and row['authority_module_checked'] for row in rows),
             consumer_invocations=0, producer_invocations=0, provider_requests=0, private_account_reads=0,
             schedules_changed=0, notifications_sent=0, public_writes=0,
             normal_publication_verified=False, original_provider_replay_performed=False,
             scope='Deployed code, configuration and source closure only; scheduled output and broader model qualification are separate.')
        if not all(row['pass'] and row['receipt_matches'] and row['authority_module_checked'] for row in rows):
            raise ValueError('Deployed package or receipt differs; inspect the report')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
