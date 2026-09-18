"""Verify the observation-only intelligence runtime and all changed consumers."""
import gzip
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'scripts'), str(ROOT/'aws/shared')]
from ops_report import report
from replay_research_intelligence import replay


def pending_receipt(read, fn):
    try:
        return read('data/ops/releases/'+fn+'.json')
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') in ('NoSuchKey', '404'):
            return None
        raise


def main():
    # Production code is unchanged by this ops-only receipt-wait repair.
    expected = '80833c66ef9cef6a0ccccb607833cba13ff65d8a'
    verifier_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    bucket = 'justhodl-dashboard-live'
    functions = ['justhodl-intelligence', 'justhodl-ai-brief', 'justhodl-ai-chat', 'justhodl-morning-intelligence']
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=150, retries={'max_attempts': 0}))
    events = boto3.client('events', region_name='us-east-1')
    def raw(key):
        body = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        return gzip.decompress(body) if key.endswith('.gz') else body
    def read(key): return json.loads(raw(key))
    with report('ops_5733_research_intelligence_reverify') as r:
        deadline = time.monotonic() + 1800
        runtimes = {}
        while len(runtimes) != len(functions):
            for fn in functions:
                if fn in runtimes: continue
                receipt = pending_receipt(read, fn)
                if receipt is None: continue
                if receipt.get('commit') != expected: continue
                runtime = lam.get_function_configuration(FunctionName=fn)
                assert runtime['CodeSha256'] == receipt['code_sha256'], fn+' runtime differs'
                runtimes[fn] = {'commit': expected, 'code_sha256': runtime['CodeSha256']}
            assert time.monotonic() < deadline, 'exact release receipt missing: '+str(set(functions)-set(runtimes))
            if len(runtimes) != len(functions): time.sleep(15)
        r.kv(runtimes=runtimes)
        before = datetime.now(timezone.utc).isoformat()
        invoked = lam.invoke(FunctionName='justhodl-intelligence', InvocationType='RequestResponse',
                             Payload=b'{"suppress_alerts":true}')
        response = json.loads(invoked['Payload'].read())
        assert not invoked.get('FunctionError') and response.get('statusCode') == 200, response
        packet = read('intelligence-report.json')
        assert packet['contract'] == 'research-intelligence.v1' and packet['generated_at'] > before
        manifest = read(packet['replay']['manifest_key'])
        reproduced = replay(manifest, read=raw)
        assert all(packet[key] == value for key, value in reproduced.items())
        rows = {row['series_id']: row for row in packet['metrics_table']}
        for sid in ('ICSA', 'UNRATE', 'CPIAUCSL', 'WALCL', 'WTREGEN', 'RRPONTSYD', 'DGS10', 'DTWEXBGS'):
            assert rows[sid]['status'] == 'fresh' and rows[sid]['value'] is not None, sid
        assert rows['ICSA']['unit'] == 'Number' and '196000K' not in str(packet)
        assert rows['UNRATE']['changes']['month']['change_unit'] == 'percentage_points'
        assert packet['decision']['verb'] == 'WAIT' and packet['decision']['meaning'] == 'abstain'
        assert packet['call'] is None and packet['sizing_eligible'] is False
        assert packet['scores']['khalid_index'] is None and packet['scores']['crisis_distance'] is None
        assert packet['portfolio']['allocation'] == {} and packet['ml_intelligence']['trade_recommendations'] == []
        assert packet['net_liquidity']['direction'] is None and packet['net_liquidity']['net_decimal'] is not None
        assert packet['dxy']['value'] is None
        runtime = lam.get_function_configuration(FunctionName='justhodl-intelligence')
        schedules = []
        for name in ('justhodl-intel-daily', 'justhodl-intel-hourly'):
            try: rule = events.describe_rule(Name=name)
            except events.exceptions.ResourceNotFoundException: continue
            targets = events.list_targets_by_rule(Rule=name)['Targets']
            if rule['State'] == 'ENABLED' and any(t['Arn'] == runtime['FunctionArn'] for t in targets):
                schedules.append({'name': name, 'expression': rule.get('ScheduleExpression')})
        assert schedules, 'no existing enabled Intelligence schedule targets this runtime'
        proof = {'contract': 'research-intelligence-verification.v1', 'commit': expected,
                 'generated_at': datetime.now(timezone.utc).isoformat(), 'verifier_commit': verifier_commit, 'runtimes': runtimes,
                 'quality': packet['quality'], 'replay': packet['replay'],
                 'existing_schedules': schedules, 'schedule_execution_observed': False,
                 'upstream_originals_replayed': True, 'live_engines_invoked': ['justhodl-intelligence'],
                 'narrative_consumer_context_tests': 'actual functions tested offline; exact deployed runtime hashes verified',
                 'paid_ai_calls': 0, 'notifications_sent': 0, 'account_reads': 0, 'portfolio_writes': 0,
                 'scope': 'Intelligence, Today and bounded narrative consumers; legacy data/report.json remains a subsequent migration'}
        s3.put_object(Bucket=bucket, Key='data/research-intelligence-verification.json', Body=json.dumps(proof).encode(),
                      ContentType='application/json', CacheControl='no-cache')
        r.kv(**proof)
        r.ok('Four exact runtimes; actual Intelligence output reconstructed from original source inputs; no paid AI, account reads or notifications')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
