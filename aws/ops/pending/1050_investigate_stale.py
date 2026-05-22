#!/usr/bin/env python3
"""ops 1050 — investigate the 2 stale-output Lambdas

The freshness monitor caught:
  - data/activist-13d.json — 48h stale (max 26h)  → written by justhodl-activist-13d
  - data/13f-price-divergence.json — 39h stale  → written by justhodl-13f-price-divergence

For each Lambda:
  1. Get config (last_modified, schedule, role)
  2. Get EventBridge rule state (ENABLED? what cron?)
  3. CW Invocations + Errors last 7 days
  4. Recent log tail for any errors
  5. Live invoke to see what happens
"""
import json, boto3, os, time
from datetime import datetime, timedelta, timezone
from botocore.config import Config

REGION = 'us-east-1'
cfg = Config(region_name=REGION, retries={'max_attempts': 5, 'mode': 'adaptive'}, read_timeout=900)
lam = boto3.client('lambda', config=cfg)
events = boto3.client('events', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

TARGETS = ['justhodl-activist-13d', 'justhodl-13f-price-divergence']

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'lambdas': {}}

for fn_name in TARGETS:
    print(f"\n{'='*60}")
    print(f"  INVESTIGATING: {fn_name}")
    print('='*60)
    info = {}
    
    # 1. Config
    try:
        cfg_l = lam.get_function_configuration(FunctionName=fn_name)
        info['config'] = {
            'last_modified': cfg_l.get('LastModified'),
            'memory': cfg_l.get('MemorySize'),
            'timeout': cfg_l.get('Timeout'),
            'role': cfg_l.get('Role'),
            'env_keys': sorted(list((cfg_l.get('Environment') or {}).get('Variables', {}).keys())),
        }
    except Exception as e:
        info['config'] = {'error': str(e)[:300]}
    
    # 2. EventBridge rules pointing to this Lambda
    rules_pointing = []
    try:
        for page in events.get_paginator('list_rule_names_by_target').paginate(
            TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn_name}"
        ):
            for rn in page.get('RuleNames', []):
                rd = events.describe_rule(Name=rn)
                rules_pointing.append({
                    'name': rn,
                    'state': rd.get('State'),
                    'expression': rd.get('ScheduleExpression'),
                })
    except Exception as e:
        rules_pointing = [{'error': str(e)[:200]}]
    info['eb_rules'] = rules_pointing
    
    # 3. CW Invocations + Errors last 7d
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    invs = errs = 0
    try:
        for m in ['Invocations', 'Errors']:
            r = cw.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName=m,
                Dimensions=[{'Name':'FunctionName','Value':fn_name}],
                StartTime=start, EndTime=end,
                Period=86400, Statistics=['Sum'],
            )
            total = sum(p['Sum'] for p in r.get('Datapoints',[]))
            if m == 'Invocations': invs = total
            else: errs = total
        info['cw_7d'] = {'invocations': int(invs), 'errors': int(errs),
                          'error_rate_pct': round(errs/invs*100, 1) if invs else 0}
    except Exception as e:
        info['cw_7d'] = {'error': str(e)[:200]}
    
    # 4. Recent log tail
    try:
        resp = logs.filter_log_events(
            logGroupName=f'/aws/lambda/{fn_name}',
            startTime=int(time.time()*1000) - 7*86400*1000,
            limit=40,
        )
        events_arr = resp.get('events', [])
        # Filter to errors or status messages
        info['log_tail'] = [
            {'ts': e.get('timestamp'), 'msg': e.get('message','').strip()[:300]}
            for e in events_arr[-15:]
        ]
        # Look for any ERROR/Exception/Traceback patterns
        info['has_errors_in_logs'] = any(
            ('ERROR' in e.get('message','') or 'Traceback' in e.get('message','') or 'Exception' in e.get('message',''))
            for e in events_arr
        )
    except Exception as e:
        info['log_tail'] = [{'error': str(e)[:200]}]
    
    # 5. Live invoke
    print(f"  Live invoking {fn_name}...")
    try:
        inv = lam.invoke(
            FunctionName=fn_name,
            InvocationType='RequestResponse',
            Payload=b'{}',
        )
        payload = inv['Payload'].read().decode()
        info['live_invoke'] = {
            'status': inv['StatusCode'],
            'function_error': inv.get('FunctionError', 'none'),
            'response_head': payload[:1200],
        }
        print(f"    status={inv['StatusCode']} fn_err={inv.get('FunctionError','none')}")
        print(f"    resp: {payload[:300]}")
    except Exception as e:
        info['live_invoke'] = {'error': str(e)[:300]}
    
    # 6. Check S3 output after invoke (did the invoke fix the staleness?)
    output_key = f"data/{fn_name.replace('justhodl-', '')}.json"
    try:
        head = s3.head_object(Bucket='justhodl-dashboard-live', Key=output_key)
        age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
        info['s3_after_invoke'] = {
            'key': output_key,
            'last_modified': head['LastModified'].isoformat(),
            'age_h': round(age_h, 2),
            'size': head['ContentLength'],
        }
    except Exception as e:
        info['s3_after_invoke'] = {'error': str(e)[:200]}
    
    report['lambdas'][fn_name] = info

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1050.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n{'='*60}\nDONE\n{'='*60}")
for fn_name, info in report['lambdas'].items():
    print(f"\n{fn_name}:")
    print(f"  EB rules: {info.get('eb_rules')}")
    print(f"  CW 7d: {info.get('cw_7d')}")
    print(f"  Live invoke: status={info.get('live_invoke',{}).get('status')} err={info.get('live_invoke',{}).get('function_error')}")
    print(f"  After invoke: {info.get('s3_after_invoke')}")
