"""Verify pinned Calls releases, bundled helpers, schedule and fresh AWS outputs."""
import hashlib
import io
import json
import subprocess
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

COMMIT = '0704acf8afa05126a27a8becd38b7d4c7a8949b9'
BUCKET = 'justhodl-dashboard-live'
FUNCTIONS = ('justhodl-ai-brief', 'justhodl-calls-backtest', 'justhodl-position-sizer-v2')


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1',
                       config=Config(read_timeout=360, retries={'max_attempts': 0}))
    events = boto3.client('events', region_name='us-east-1')
    def read(key):
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    def source(path):
        return subprocess.check_output(['git', 'show', COMMIT + ':' + path])
    def invoke(fn):
        response = lam.invoke(FunctionName=fn, InvocationType='RequestResponse',
                              Payload=b'{"suppress_alerts":true}')
        body = json.loads(response['Payload'].read())
        if response.get('FunctionError') or body.get('statusCode') != 200:
            raise RuntimeError(fn + ': invoke failed; inspect private CloudWatch log')
        return json.loads(body.get('body') or '{}')
    def fresh(doc):
        at = datetime.fromisoformat(doc['generated_at'].replace('Z', '+00:00'))
        assert at >= started, 'Output was not refreshed by this verification'

    with report('ops_5697_calls_integrity_verify') as r:
        r.heading('Calls integrity release verification')
        for fn in FUNCTIONS:
            receipt = read('data/ops/releases/' + fn + '.json')
            assert receipt['commit'] == COMMIT, fn + ': exact release receipt missing'
            live = lam.get_function(FunctionName=fn)
            assert live['Configuration']['CodeSha256'] == receipt['code_sha256'], fn + ': AWS code hash differs'
            for name, meta in receipt['source'].items():
                raw = source('aws/lambdas/' + fn + '/source/' + name)
                assert len(raw) == meta['bytes'] and hashlib.sha256(raw).hexdigest() == meta['sha256'], fn + ': source digest differs'
            with urllib.request.urlopen(live['Code']['Location'], timeout=30) as response:
                archive = zipfile.ZipFile(io.BytesIO(response.read()))
            assert archive.read('calls_contract.py') == source('aws/shared/calls_contract.py'), fn + ': shared helper differs'
            assert archive.read('lambda_function.py') == source('aws/lambdas/' + fn + '/source/lambda_function.py'), fn + ': live handler differs'
            if fn == 'justhodl-calls-backtest':
                assert archive.read('calls_replay.py') == source('aws/lambdas/' + fn + '/source/calls_replay.py')
            r.kv(function=fn, commit=COMMIT, code_sha256=receipt['code_sha256'],
                 source_bytes=sum(m['bytes'] for m in receipt['source'].values()), live_helper_verified=True)
            r.ok(fn + ': exact receipt, source bytes, AWS code hash and bundled helper verified')

        rule = events.describe_rule(Name='justhodl-ai-brief-4h')
        assert rule['ScheduleExpression'] == 'cron(5 0,4,8,12,16,20 * * ? *)'
        assert rule['State'] == 'ENABLED'
        targets = events.list_targets_by_rule(Rule=rule['Name'])['Targets']
        assert any(':function:justhodl-ai-brief' in t['Arn'] for t in targets)
        r.ok('Enabled four-hour brief schedule verified at minute 05 UTC')

        started = datetime.now(timezone.utc)
        brief = invoke('justhodl-ai-brief')
        ledger = read('data/decisive-call-history.json')
        fresh(ledger)
        latest = max(ledger['snapshots'], key=lambda row: row['timestamp'])
        assert latest['schema_version'] == 'calls.v2'
        assert latest['decision_status'] in ('ABSTAIN', 'ERROR')
        assert latest['sizing_eligible'] is False and latest['decision_eligible'] is False
        assert 'n_open_positions' not in latest
        assert read('data/decisive-call-events/' + latest['snapshot_id'] + '.json') == latest
        r.kv(output='Calls ledger', generated_at=ledger['generated_at'], status=latest['decision_status'],
             reason=latest['decision_reason'], khalid_score=latest['khalid_score'], brief_chars=latest['brief_chars'])
        r.ok('Fresh immutable public event and ledger agree; notification suppression requested')

        invoke('justhodl-calls-backtest')
        replay = read('data/calls-replay.json')
        fresh(replay)
        assert replay['method'] == 'explicit_spy_allocation_replay.v2'
        assert replay['status'] == 'no_eligible_calls'
        assert replay['summary']['total_return_pct'] is None and replay['summary']['net_return_pct'] is None
        assert replay['calls'] == replay['nav_curve'] == [] and replay['sizing_eligible'] is False
        assert read('backtest/calls-results.json') == replay
        r.ok('Replay excludes unqualified history and publishes null performance at both keys')

        invoke('justhodl-position-sizer-v2')
        sizer = read('portfolio/sizer-v2.json')
        fresh(sizer)
        assert sizer['status'] == 'ABSTAIN' and sizer['sizing_eligible'] is False
        assert sizer['positions'] == sizer['setups'] == [] and sizer['risk_multiplier'] is None
        assert sizer['summary']['total_recommended_exposure_pct'] is None
        r.ok('Sizer abstains without buy/sell recommendations; independent constraints remain separate')

        health = {'generated_at': datetime.now(timezone.utc).isoformat(), 'commit': COMMIT,
                  'release_verified': True, 'brief_status': latest['decision_status'],
                  'brief_reason': latest['decision_reason'], 'replay_status': replay['status'],
                  'sizer_status': sizer['status'], 'sizing_eligible': False,
                  'brief_schedule': rule['ScheduleExpression']}
        s3.put_object(Bucket=BUCKET, Key='data/calls-integrity-status.json',
                      Body=json.dumps(health).encode(), ContentType='application/json', CacheControl='no-cache')
        r.ok('Public verification status published without account or portfolio details')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
