"""Inspect CapitalFlow's actual producer and retain its complete legacy publications."""
from pathlib import Path
import hashlib
import io
import json
import sys
import urllib.error
import urllib.request
import zipfile

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'aws/ops'))
from ops_report import report


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method='HEAD',
                headers={'User-Agent': 'justhodl-verify-release/1.0'}), timeout=25): return False
    except urllib.error.HTTPError as exc: return exc.code in (401, 403, 404)


def main():
    fn, bucket = 'justhodl-capital-flow', 'justhodl-dashboard-live'
    lam = boto3.client('lambda', region_name='us-east-1')
    s3 = boto3.client('s3', region_name='us-east-1')
    events = boto3.client('events', region_name='us-east-1')
    scheduler = boto3.client('scheduler', region_name='us-east-1')
    def raw(key):
        body = s3.get_object(Bucket=bucket, Key=key)['Body']
        try: value = body.read(64*1024*1024+1)
        finally: body.close()
        assert len(value) <= 64*1024*1024, 'Complete artifact exceeds bound'
        return value
    with report('ops_5890_capital_research_preflight') as r:
        cfg = lam.get_function_configuration(FunctionName=fn)
        r.kv(configuration={k: cfg.get(k) for k in ('FunctionName', 'Runtime', 'Handler', 'Timeout',
             'MemorySize', 'Role', 'Architectures', 'CodeSha256', 'State', 'LastUpdateStatus')})
        url = lam.get_function(FunctionName=fn)['Code']['Location']
        with urllib.request.urlopen(url, timeout=45) as response: archive = response.read(32*1024*1024+1)
        assert len(archive) <= 32*1024*1024
        with zipfile.ZipFile(io.BytesIO(archive)) as z: code = z.read('lambda_function.py')
        source = (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes()
        r.kv(runtime_source_sha256=hashlib.sha256(code).hexdigest(), repository_source_matches=source == code,
             runtime_source_bytes=len(code), engine_invocations=0)
        preserved = []
        for key in ('data/capital-flow.json', 'data/capital-flow-history.json'):
            body = raw(key); sha = hashlib.sha256(body).hexdigest()
            private = 'audit-private/20260909-originals/capital-research/'+sha+'.bin'
            try: s3.put_object(Bucket=bucket, Key=private, Body=body, IfNoneMatch='*', ContentType='application/octet-stream', CacheControl='no-store')
            except Exception as exc:
                if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', 'ConditionalRequestConflict'): raise
            assert raw(private) == body
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+private) and denied('https://justhodl.ai/'+private)
            packet = json.loads(body)
            item = {'key': key, 'sha256': sha, 'bytes': len(body), 'anonymous_denied': True,
                    'generated_at': packet.get('generated_at') or packet.get('as_of')}
            if key.endswith('capital-flow.json'):
                item.update(reported_sources=packet.get('sources'), ticker_rows=len(packet.get('by_ticker') or {}),
                            accumulating_rows=len(packet.get('accumulating') or []), distributing_rows=len(packet.get('distributing') or []))
            else: item['history_entries'] = len(packet.get('entries') or [])
            preserved.append(item)
        r.kv(whole_preceding_products=preserved)
        names = events.list_rule_names_by_target(TargetArn=cfg['FunctionArn'])
        assert not names.get('NextToken'), 'Inspect complete rule inventory before changes'
        found = []
        for name in names['RuleNames']:
            rule = events.describe_rule(Name=name)
            targets = events.list_targets_by_rule(Rule=name)
            assert not targets.get('NextToken')
            found.append({**{k: rule.get(k) for k in ('Name', 'ScheduleExpression', 'State')},
                          'matching_target_ids': [v['Id'] for v in targets['Targets'] if v['Arn'] == cfg['FunctionArn']]})
        r.kv(eventbridge_rules=found)
        found = []
        for page in scheduler.get_paginator('list_schedules').paginate():
            for item in page['Schedules']:
                if item.get('Target', {}).get('Arn') == cfg['FunctionArn']:
                    schedule = scheduler.get_schedule(Name=item['Name'], GroupName=item['GroupName'])
                    found.append({k: schedule.get(k) for k in ('Name', 'GroupName', 'ScheduleExpression', 'ScheduleExpressionTimezone', 'State', 'FlexibleTimeWindow')})
        r.kv(scheduler_targets=found, source_product_changed=False, notifications_sent=0, paid_ai_calls=0, private_account_reads=0, portfolio_writes=0)
        assert source == code, 'Runtime source differs: inspect before editing the producer'


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Capital research preflight failed; inspect the committed report before changing production.')
        sys.exit(1)
