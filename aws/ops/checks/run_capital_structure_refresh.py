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


def known_messages():
    """The fixed literal messages this pipeline raises (2026-10-08). Only these are ever printed:
    a message that is not one of them could carry provider or private data and stays redacted."""
    import re
    texts = []
    for name in ('run_capital_structure_refresh.py', 'capital_structure_refresh.py', 'share_structure_campaign.py',
                 'share_structure_sources.py', 'capital_structure_source.py', 'capital_structure_store.py'):
        path = ROOT / 'aws/ops/checks' / name
        if path.exists(): texts.append(path.read_text(encoding='utf-8'))
    return set(re.findall(r"raise ValueError\('([^'\\]{3,160})'\)", '\n'.join(texts)))


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        message = str(exc)
        print(json.dumps({'status':'failed', 'error_type':type(exc).__name__,
            'message': message if isinstance(exc, ValueError) and message in known_messages() else '[redacted: not a known literal]',
            'action':'Inspect protected refresh control and phase journals. No automatic acquisition retry.'}), flush=True)
        sys.exit(1)
