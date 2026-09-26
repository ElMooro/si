"""Runner entry point for one complete-source refresh phase; never invoke Lambda."""
from pathlib import Path
import argparse, json, os, sys
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared', 'aws/ops/checks')]
import boto3
from botocore.config import Config
import capital_structure_refresh as refresh


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['plan']+['part-'+str(i) for i in range(1,7)]+['qualify'])
    args = parser.parse_args()
    if os.environ.get('GITHUB_REF') != 'refs/heads/main': raise ValueError('Reviewed main checkout required')
    run = os.environ.get('GITHUB_RUN_ID', '')
    refresh.request_id(run)
    credential = None
    if args.phase.startswith('part-'):
        # The runner uses the existing managed provider secret; never print it.
        credential = boto3.client('ssm', region_name='us-east-1').get_parameter(
            Name='/justhodl/fmp/api-key', WithDecryption=True)['Parameter']['Value']
    client = boto3.client('s3', region_name='us-east-1', config=Config(max_pool_connections=16, retries={'max_attempts':2}))
    result = refresh.run(client, run, args.phase, credential)
    print(json.dumps({'request_id': refresh.request_id(run), 'phase': args.phase, 'result': result}), flush=True)


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        print(json.dumps({'status':'failed', 'error_type':type(exc).__name__,
            'action':'Inspect protected refresh control and phase journals. No automatic acquisition retry.'}), flush=True)
        sys.exit(1)
