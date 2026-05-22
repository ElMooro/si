#!/usr/bin/env python3
"""ops 1053 — final session scorecard + observability.html live verify

End-of-session report card. Verifies:
  1. observability.html reachable + key markers present
  2. data/_fleet-monitor.json fresh + healthy
  3. data/_freshness-monitor.json fresh + tuned correctly
  4. data/_freshness-manifest.json has the updates from 1052
  5. Both monitor Lambdas have recent successful invocations
  6. All 8 exp Lambdas have schedules + recent activity
"""
import json, boto3, os, time, urllib.request
from datetime import datetime, timedelta, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'sections': {}}

# 1. observability.html via temp Lambda
print("[1] Verifying observability.html via temp Lambda...")
TEMP_LAMBDA_CODE = '''
import urllib.request, json
def lambda_handler(event, context):
    try:
        r = urllib.request.urlopen("https://justhodl.ai/observability.html", timeout=15)
        body = r.read().decode("utf-8", errors="ignore")
        markers = {
            "title": "Fleet Observability" in body,
            "audit_scorecard": "Audit 2026-05-22 Scorecard" in body,
            "exp_table": "8 Exponential Engines" in body,
            "fleet_status_id": 'id="fleet-status"' in body,
            "stale_outputs": "Stale S3 Outputs" in body,
        }
        return {"http_code": r.status, "size": len(body),
                "markers": markers, "all_present": all(markers.values())}
    except Exception as e:
        return {"error": str(e)[:300]}
'''
import zipfile, io
zip_buf = io.BytesIO()
with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.writestr('lambda_function.py', TEMP_LAMBDA_CODE)
zip_buf.seek(0)

temp_name = f'ops-1053-verify-{int(time.time())}'
try:
    lam.create_function(
        FunctionName=temp_name,
        Runtime='python3.12',
        Role=f'arn:aws:iam::{ACCOUNT}:role/lambda-execution-role',
        Handler='lambda_function.lambda_handler',
        Code={'ZipFile': zip_buf.read()},
        Timeout=30,
        MemorySize=128,
    )
    # Wait for it to become active
    waiter = lam.get_waiter('function_active_v2')
    waiter.wait(FunctionName=temp_name, WaiterConfig={'MaxAttempts': 20, 'Delay': 1})
    
    inv = lam.invoke(FunctionName=temp_name, InvocationType='RequestResponse')
    body = inv['Payload'].read().decode()
    report['sections']['observability_html'] = json.loads(body)
    
    # Cleanup
    lam.delete_function(FunctionName=temp_name)
except Exception as e:
    report['sections']['observability_html'] = {'error': str(e)[:300]}
    try:
        lam.delete_function(FunctionName=temp_name)
    except Exception:
        pass

# 2. Fleet monitor freshness
print("[2] Fleet monitors state files...")
for key, name in [
    ('data/_fleet-monitor.json', 'fleet_monitor'),
    ('data/_freshness-monitor.json', 'freshness_monitor'),
    ('data/_freshness-manifest.json', 'freshness_manifest'),
]:
    try:
        obj = s3.get_object(Bucket='justhodl-dashboard-live', Key=key)
        d = json.loads(obj['Body'].read().decode())
        age_h = (datetime.now(timezone.utc) - obj['LastModified']).total_seconds() / 3600
        if name == 'freshness_manifest':
            report['sections'][name] = {
                'age_h': round(age_h, 2),
                'n_excludes': len(d.get('exclude_prefixes', [])),
                'n_overrides': len(d.get('key_overrides', {})),
                'overrides_keys': list(d.get('key_overrides', {}).keys()),
            }
        else:
            report['sections'][name] = {
                'age_h': round(age_h, 2),
                'version': d.get('version'),
                'generated_at': d.get('generated_at'),
                'n_tracked': d.get('n_keys_tracked') or d.get('n_lambdas_scanned'),
                'n_stale_or_alerts': d.get('n_stale') or d.get('n_alerts_raised'),
                'elapsed_s': d.get('elapsed_s'),
            }
    except Exception as e:
        report['sections'][name] = {'error': str(e)[:200]}

# 3. Monitor Lambda invocation counts last 1h
print("[3] Monitor Lambda activity last 1h...")
end = datetime.now(timezone.utc)
start = end - timedelta(hours=1)
for fn in ['justhodl-fleet-error-monitor', 'justhodl-fleet-freshness-monitor']:
    invs = errs = 0
    for m in ['Invocations', 'Errors']:
        try:
            r = cw.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName=m,
                Dimensions=[{'Name':'FunctionName','Value':fn}],
                StartTime=start, EndTime=end,
                Period=3600, Statistics=['Sum'],
            )
            total = sum(p['Sum'] for p in r.get('Datapoints',[]))
            if m == 'Invocations': invs = total
            else: errs = total
        except Exception:
            pass
    report['sections'][f'{fn}_1h'] = {'invocations': int(invs), 'errors': int(errs)}

# 4. 8 exp Lambdas have schedules + recent activity
print("[4] 8 exp Lambdas schedules + activity...")
exp_fleet = [
    ('#1', 'justhodl-premortem-engine'),
    ('#2', 'justhodl-signal-halflife'),
    ('#3', 'justhodl-causality-scanner'),
    ('#4', 'justhodl-behavior-mirror'),
    ('#5', 'justhodl-failure-library'),
    ('#6', 'justhodl-chart-vision'),
    ('#7', 'justhodl-convexity-scorer'),
    ('#8', 'justhodl-meta-improver'),
]
exp_status = []
end = datetime.now(timezone.utc)
start = end - timedelta(hours=24)
for idea, fn in exp_fleet:
    # CW invocations last 24h
    try:
        r = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name':'FunctionName','Value':fn}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        invs_24h = int(sum(p['Sum'] for p in r.get('Datapoints',[])))
    except Exception:
        invs_24h = -1
    
    # EB rules
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{fn}"
    rules = []
    try:
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=fn_arn):
            for rn in page.get('RuleNames', []):
                rd = events.describe_rule(Name=rn)
                rules.append({'name': rn, 'state': rd.get('State')})
    except Exception:
        pass
    
    # DLQ + X-Ray
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        has_dlq = bool((cfg.get('DeadLetterConfig') or {}).get('TargetArn'))
        has_xray = cfg.get('TracingConfig', {}).get('Mode') == 'Active'
    except Exception:
        has_dlq = has_xray = False
    
    exp_status.append({
        'idea': idea, 'lambda': fn,
        'invocations_24h': invs_24h,
        'rules': rules,
        'dlq': has_dlq, 'xray': has_xray,
    })

report['sections']['exp_status'] = exp_status

# 5. Final scorecard
def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict): return default
        cur = cur.get(k)
    return cur if cur is not None else default

obs = report['sections'].get('observability_html', {})
fm = report['sections'].get('fleet_monitor', {})
fr = report['sections'].get('freshness_monitor', {})
mn = report['sections'].get('freshness_manifest', {})

scorecard = {
    'observability_dashboard_live': obs.get('all_present', False),
    'fleet_monitor_state_age_h': fm.get('age_h'),
    'fleet_monitor_alerts': fm.get('n_stale_or_alerts'),
    'freshness_monitor_state_age_h': fr.get('age_h'),
    'freshness_monitor_stale': fr.get('n_stale_or_alerts'),
    'manifest_overrides': mn.get('n_overrides'),
    'manifest_excludes': mn.get('n_excludes'),
    'fleet_error_monitor_invocations_1h': report['sections'].get('justhodl-fleet-error-monitor_1h',{}).get('invocations'),
    'fleet_freshness_monitor_invocations_1h': report['sections'].get('justhodl-fleet-freshness-monitor_1h',{}).get('invocations'),
    'exp_lambdas_total': len(exp_status),
    'exp_lambdas_with_dlq_and_xray': sum(1 for e in exp_status if e.get('dlq') and e.get('xray')),
    'exp_lambdas_with_schedule': sum(1 for e in exp_status if e.get('rules')),
    'exp_lambdas_invoked_24h': sum(1 for e in exp_status if e.get('invocations_24h',0) > 0),
}
report['scorecard'] = scorecard

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1053.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print("\n" + "="*60)
print("  FINAL SESSION SCORECARD")
print("="*60)
print(json.dumps(scorecard, indent=2))
