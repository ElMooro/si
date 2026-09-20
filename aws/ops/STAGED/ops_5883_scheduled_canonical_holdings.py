"""Exercise original collection + canonical publication through the existing Scheduler role."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import os
import re
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-13f-positions/source')]
from ops_report import report
import holdings_canonical as canonical
import holdings_native as model
import holdings_store as store
from replay_fred_vintage import read_public
from replay_holdings_canonical import verify_current


def runtime_report(message):
    if 'Status:' in message and 'Status: success' not in message:
        raise ValueError('Scheduled Lambda reported a runtime failure')
    patterns = {'request_id': r'REPORT RequestId:\s*([a-f0-9-]+)',
                'duration_ms': r'\bDuration:\s*([0-9.]+)\s*ms',
                'memory_size_mb': r'Memory Size:\s*(\d+)\s*MB',
                'max_memory_used_mb': r'Max Memory Used:\s*(\d+)\s*MB'}
    matches = {key: re.search(pattern, message) for key, pattern in patterns.items()}
    if not all(matches.values()): raise ValueError('Complete Lambda runtime report required')
    result = {key: match.group(1) for key, match in matches.items()}
    result.update(duration_ms=float(result['duration_ms']), memory_size_mb=int(result['memory_size_mb']),
                  max_memory_used_mb=int(result['max_memory_used_mb']))
    if result['duration_ms'] >= 600000 or result['max_memory_used_mb'] >= result['memory_size_mb']:
        raise ValueError('Combined holdings execution exhausted runtime capacity')
    return result


def main():
    fn, bucket = 'justhodl-13f-positions', 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(tcp_keepalive=True))
    scheduler = boto3.client('scheduler', region_name='us-east-1')
    logs = boto3.client('logs', region_name='us-east-1')
    read = store.reader(s3, bucket)
    with report('ops_5883_scheduled_canonical_holdings') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+fn], text=True).strip()
        receipt = json.loads(read('data/ops/releases/'+fn+'.json'))
        conf = lam.get_function_configuration(FunctionName=fn)
        assert receipt['commit'] == expected and receipt['code_sha256'] == conf['CodeSha256']
        previous = json.loads(read(canonical.CURRENT))
        assert previous['contract'] == canonical.CONTRACT
        schedule = scheduler.get_schedule(Name='justhodl-holdings-originals-research', GroupName='default')
        assert schedule['State'] == 'ENABLED' and schedule['ScheduleExpression'] == 'cron(40 0/2 * * ? *)'
        assert schedule['ScheduleExpressionTimezone'] == 'UTC' and schedule['Target']['Arn'] == conf['FunctionArn']
        assert schedule['Target']['RoleArn'] == 'arn:aws:iam::857687956942:role/justhodl-scheduler-role'
        assert json.loads(schedule['Target']['Input']) == {'action': 'holdings_research_collect', 'notify': False}
        run_id = os.environ.get('GITHUB_RUN_ID', '')
        assert run_id.isdigit(), 'Runner identity required'
        request_id = 'holdings-canonical-' + expected[:12] + '-' + run_id
        name = 'jh-holdings-canonical-proof-' + run_id
        when = datetime.now(timezone.utc) + timedelta(minutes=2)
        payload = {'action': 'holdings_research_collect', 'notify': False, 'request_id': request_id}
        # An immutable intent prevents the same run from blindly submitting again.
        intent = {'contract': 'holdings-scheduled-acceptance-intent.v1', 'workflow_run': run_id,
                  'request_id': request_id, 'schedule_name': name, 'scheduled_at': when.isoformat(),
                  'commit': expected, 'code_sha256': conf['CodeSha256'], 'input': payload}
        intent_key = model.PREFIX + 'canonical/acceptance-jobs/' + run_id + '.json'
        s3.put_object(Bucket=bucket, Key=intent_key, Body=model.encoded(intent), IfNoneMatch='*',
                      ContentType='application/json', CacheControl='no-store')
        r.kv(intent=intent, direct_collector_invokes=0, previous_canonical=previous['canonical_replay'])
        scheduler.create_schedule(Name=name, GroupName='default', State='ENABLED',
            ScheduleExpression='at('+when.strftime('%Y-%m-%dT%H:%M:%S')+')', ScheduleExpressionTimezone='UTC',
            FlexibleTimeWindow={'Mode': 'OFF'}, ActionAfterCompletion='DELETE',
            Description='One original-source plus canonical publication acceptance; existing role and schedule unchanged',
            Target={'Arn': conf['FunctionArn'], 'RoleArn': schedule['Target']['RoleArn'], 'Input': json.dumps(payload),
                    'RetryPolicy': {'MaximumRetryAttempts': 0, 'MaximumEventAgeInSeconds': 600}})
        actual = scheduler.get_schedule(Name=name, GroupName='default')
        assert json.loads(actual['Target']['Input']) == payload
        deadline = time.monotonic() + 1200
        while True:
            native = json.loads(read(model.CURRENT)); packet = json.loads(read(canonical.CURRENT))
            if (native.get('collection_request_id') == request_id and packet.get('research') == native.get('replay')): break
            assert time.monotonic() < deadline, 'Combined scheduled publication missing; inspect execution before another collection'
            time.sleep(15)
        assert model.clock(native['collection_started_at']) >= when-timedelta(seconds=5)
        assert model.clock(native['source_generated_at']) >= model.clock(native['collection_started_at'])
        assert native['acquisition_mode'] == 'official_sec_collection'
        assert packet['canonical_replay'] != previous['canonical_replay']
        observed_at = datetime.now(timezone.utc)
        r.kv(observed_scheduled_collection={'source_generated_at': native['source_generated_at'],
             'generated_at': native['generated_at'], 'request_id': request_id, 'canonical_replay': packet['canonical_replay']})
        # Read each original artifact publicly and verify with reviewed local code.
        cache, inspected = {}, set()
        def public(key):
            inspected.add(key)
            immutable = bool(re.search(r'/[a-f0-9]{64}\.(?:json|py|bin\.gz)$', key))
            if immutable and key in cache: return cache[key]
            raw = read_public(key)
            if immutable: cache[key] = raw
            return raw
        run = packet['canonical_replay']['sha256']
        manifest, products = verify_current(public, run=run)
        assert manifest['research'] == native['replay']
        for key, product in products.items():
            current = json.loads(public(key))
            assert current.pop('canonical_replay') == packet['canonical_replay'] and current == product
            assert all(product[k] == v for k, v in model.PERMISSION.items())
        events, token = [], None
        while True:
            kwargs = {'logGroupName': '/aws/lambda/'+fn, 'startTime': int(when.timestamp()*1000)-5000,
                      'endTime': int(observed_at.timestamp()*1000)+5000,
                      'filterPattern': '"REPORT RequestId"', 'limit': 100}
            if token: kwargs['nextToken'] = token
            page = logs.filter_log_events(**kwargs); events.extend(page['events'])
            next_token = page.get('nextToken')
            if not next_token or next_token == token: break
            token = next_token
        assert len(events) == 1, 'Expected one scheduled execution report; inspect other executions before accepting'
        execution = runtime_report(events[0]['message'])
        current_schedule = scheduler.get_schedule(Name=schedule['Name'], GroupName='default')
        assert all(current_schedule[key] == schedule[key] for key in ('ScheduleExpression', 'State', 'Target'))
        assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == conf['CodeSha256']
        assert json.loads(read('data/ops/releases/'+fn+'.json'))['commit'] == expected
        proof = {'contract': 'holdings-canonical-scheduled-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
            'commit': expected, 'code_sha256': conf['CodeSha256'], 'collection_request_id': request_id,
            'canonical_replay': packet['canonical_replay'], 'research': native['replay'],
            'source_generated_at': native['source_generated_at'], 'research_generated_at': native['generated_at'],
            'runtime_execution': execution, 'workflow_run': run_id, 'source_requests': native['source_request_count'],
            'public_artifacts_replayed': len(inspected), 'complete_replay_verified': True,
            'funds': packet['funds_total'], 'manager_comparisons': packet['manager_comparison_count'],
            'scheduler_execution_verified': True, 'existing_schedule_unchanged': True, 'direct_collector_invokes': 0,
            'paid_ai_calls': 0, 'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0,
            'remaining': 'Downstream consumer migration and a recurring independent auditor remain required.'}
        key = 'data/holdings-canonical-scheduler-verification.json'
        s3.put_object(Bucket=bucket, Key=key, Body=model.encoded(proof), ContentType='application/json', CacheControl='no-store')
        assert json.loads(read_public(key)) == proof
        r.kv(acceptance_complete=True, proof=proof)


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Scheduled canonical acceptance failed. Read the report and existing invocation before scheduling again.')
        sys.exit(1)
