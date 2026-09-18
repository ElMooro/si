"""Verify the Calls replay chain and install its independent recurring audit."""
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops'), str(ROOT / 'aws/shared')]
from ops_report import report
from calls_research_replay import replay, canonical
BUCKET = 'justhodl-dashboard-live'
FUNCTIONS = ('justhodl-ai-brief', 'justhodl-calls-research-audit', 'justhodl-calls-backtest', 'justhodl-position-sizer-v2')


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    events = boto3.client('events', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=920, retries={'max_attempts': 0}))
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--',
        'aws/lambdas/justhodl-ai-brief/source/lambda_function.py'], text=True).strip()
    def read_raw(key): return s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    def read(key): return json.loads(read_raw(key))
    def invoke(fn):
        result = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=b'{"suppress_alerts":true}')
        payload = json.loads(result['Payload'].read())
        assert not result.get('FunctionError'), fn+' runtime failed: '+str(payload.get('errorType'))
        assert payload.get('statusCode', 200) < 400, fn+' returned failure'
        return payload
    with report('ops_5716_calls_research_replay_verify') as r:
        deadline = time.monotonic()+1500
        configs = {}
        for fn in FUNCTIONS:
            while True:
                try: receipt = read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('NoSuchKey', '404'): raise
                    receipt = {}
                if receipt.get('commit') == expected: break
                assert time.monotonic() < deadline, fn+' exact receipt not available'
                time.sleep(15)
            configs[fn] = lam.get_function_configuration(FunctionName=fn)
            assert configs[fn]['CodeSha256'] == receipt['code_sha256']
            r.log(fn+' exact receipt and runtime hash: '+expected)
        started = datetime.now(timezone.utc).replace(microsecond=0)
        invoke('justhodl-ai-brief')
        public = read('data/ai-brief-public.json')
        assert datetime.fromisoformat(public['generated_at']) >= started
        ref = public['research_replay']
        raw = read_raw(ref['bundle_key'])
        assert hashlib.sha256(raw).hexdigest() == ref['bundle_sha256']
        bundle = json.loads(raw)
        independent = replay(bundle)
        assert independent['status'] == 'reproduced' and independent['inputs'] == 7
        assert all(canonical(public.get(key)) == canonical(value) for key, value in bundle['payload']['output'].items())
        assert public['call_verb'] == 'WAIT' and public['sizing_eligible'] is False and public['paid_api_calls'] == 0
        assert 'snapshot' not in bundle['payload'] and 'snapshot' not in public
        rows = public['evidence_inventory']['root_groups']
        assert len(next(row for row in rows if row['root_id'] == 'FR2004')['fields']) == 6
        invoke('justhodl-calls-research-audit')
        proof = read('data/calls-research-proofs/'+ref['payload_sha256']+'.json')
        assert proof['status'] == 'reproduced' and proof['run_id'] == ref['run_id']
        assert proof['private_account_data_read'] is False and proof['publication_overdue'] is False
        invoke('justhodl-calls-backtest')
        invoke('justhodl-position-sizer-v2')
        audit_fn = 'justhodl-calls-research-audit'
        name = 'justhodl-calls-research-audit-15m'
        rule = events.put_rule(Name=name, ScheduleExpression='rate(15 minutes)', State='ENABLED',
                               Description='Independently reproduce the frozen public Calls brief; no trade or messaging actions')
        try:
            lam.add_permission(FunctionName=audit_fn, StatementId='calls-research-audit-schedule',
                               Action='lambda:InvokeFunction', Principal='events.amazonaws.com', SourceArn=rule['RuleArn'])
        except ClientError as exc:
            if exc.response['Error']['Code'] != 'ResourceConflictException': raise
            policy = json.loads(lam.get_policy(FunctionName=audit_fn)['Policy'])
            statement = next(row for row in policy['Statement'] if row['Sid'] == 'calls-research-audit-schedule')
            assert statement['Principal']['Service'] == 'events.amazonaws.com'
            assert statement['Condition']['ArnLike']['AWS:SourceArn'] == rule['RuleArn']
        result = events.put_targets(Rule=name, Targets=[{'Id': 'calls-replay-audit', 'Arn': configs[audit_fn]['FunctionArn'], 'Input': '{}'}])
        assert result['FailedEntryCount'] == 0
        assert events.describe_rule(Name=name)['State'] == 'ENABLED'
        assert any(row['Arn'] == configs[audit_fn]['FunctionArn'] for row in events.list_targets_by_rule(Rule=name)['Targets'])
        r.kv(commit=expected, generated_at=public['generated_at'], snapshot_id=public['snapshot_id'],
             research_run_id=ref['run_id'], bundle_key=ref['bundle_key'], inputs=independent['inputs'],
             evidence_fields=independent['evidence_fields'], mapped_roots=len(rows),
             replay_checked_at=proof['generated_at'], call=public['call_verb'],
             eligible_votes=public['evidence_inventory']['eligible_votes'], schedule='rate(15 minutes)')
        r.ok('Exact releases, frozen-input replay, current output, history binding and recurring independent checker verified')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
