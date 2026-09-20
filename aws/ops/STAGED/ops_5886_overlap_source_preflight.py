"""Inspect the current overlap producer and retain its whole legacy public product."""
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
    fn, bucket = 'justhodl-smart-money-cluster', 'justhodl-dashboard-live'
    lam = boto3.client('lambda', region_name='us-east-1')
    s3 = boto3.client('s3', region_name='us-east-1')
    events = boto3.client('events', region_name='us-east-1')
    scheduler = boto3.client('scheduler', region_name='us-east-1')
    with report('ops_5886_overlap_source_preflight') as r:
        cfg = lam.get_function_configuration(FunctionName=fn)
        r.kv(configuration={k: cfg.get(k) for k in ('FunctionName', 'Runtime', 'Handler', 'Timeout',
             'MemorySize', 'Role', 'Architectures', 'CodeSha256', 'State', 'LastUpdateStatus')})
        url = lam.get_function(FunctionName=fn)['Code']['Location']
        with urllib.request.urlopen(url, timeout=45) as response: archive = response.read(32*1024*1024+1)
        assert len(archive) <= 32*1024*1024
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            code = z.read('lambda_function.py')
        source = (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes()
        r.kv(runtime_source_sha256=hashlib.sha256(code).hexdigest(), repository_source_matches=source == code,
             runtime_source_bytes=len(code), engine_invocations=0)
        key = 'data/smart-money-clusters.json'
        obj = s3.get_object(Bucket=bucket, Key=key); body = obj['Body'].read(64*1024*1024+1)
        assert len(body) <= 64*1024*1024
        sha = hashlib.sha256(body).hexdigest(); private = 'audit-private/20260909-originals/holdings-overlap/'+sha+'.bin'
        try: s3.put_object(Bucket=bucket, Key=private, Body=body, IfNoneMatch='*', ContentType='application/octet-stream', CacheControl='no-store')
        except Exception as exc:
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', 'ConditionalRequestConflict'): raise
        assert s3.get_object(Bucket=bucket, Key=private)['Body'].read() == body
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+private) and denied('https://justhodl.ai/'+private)
        packet = json.loads(body)
        r.kv(preserved_source=key, preserved_sha256=sha, preserved_bytes=len(body), anonymous_denied=True,
             source_generated_at=packet.get('generated_at'), legacy_clusters=len(packet.get('clusters') or []))
        names = events.list_rule_names_by_target(TargetArn=cfg['FunctionArn'])
        assert not names.get('NextToken'), 'Unexpected rule pagination; inspect complete target inventory'
        for name in names['RuleNames']:
            rule = events.describe_rule(Name=name)
            targets = events.list_targets_by_rule(Rule=name)
            assert not targets.get('NextToken'), 'Unexpected target pagination'
            r.kv(eventbridge_rule={k: rule.get(k) for k in ('Name', 'ScheduleExpression', 'State')},
                 matching_target_ids=[v['Id'] for v in targets['Targets'] if v['Arn'] == cfg['FunctionArn']])
        found = []
        for page in scheduler.get_paginator('list_schedules').paginate():
            for item in page['Schedules']:
                if item.get('Target', {}).get('Arn') == cfg['FunctionArn']:
                    schedule = scheduler.get_schedule(Name=item['Name'], GroupName=item['GroupName'])
                    found.append({k: schedule.get(k) for k in ('Name', 'GroupName', 'ScheduleExpression', 'ScheduleExpressionTimezone', 'State', 'FlexibleTimeWindow')})
        r.kv(scheduler_targets=found, source_product_changed=False, notifications_sent=0, private_account_reads=0)


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Overlap source preflight failed; inspect the committed report before changing production.')
        sys.exit(1)
