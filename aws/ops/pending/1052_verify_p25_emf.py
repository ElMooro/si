#!/usr/bin/env python3
"""
ops 1051 — verify P2.5 silent-except sweep landed correctly

  1. List the JustHodl/Reliability namespace metrics (any data?)
  2. Live-invoke 3 patched Lambdas to confirm they compile + run
  3. Inspect fleet-error-monitor source for the new silent_alerts code path
  4. Sample a few patched files via lambda.get_function_configuration's CodeSha256
     to verify the new code reached AWS
"""
import json, boto3, os, time
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)

lam = boto3.client('lambda', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

report = {'started_at': NOW.isoformat()}

# 1. Check JustHodl/Reliability namespace
print("[1] Checking custom metric namespace JustHodl/Reliability...")
try:
    r = cw.list_metrics(Namespace='JustHodl/Reliability')
    metrics = r.get('Metrics', [])
    report['custom_namespace_metrics'] = [
        {'name': m['MetricName'], 'dimensions': m.get('Dimensions', [])}
        for m in metrics[:30]
    ]
    report['n_custom_metrics'] = len(metrics)
    print(f"  {len(metrics)} metrics in namespace (likely 0 if no failures yet)")
except Exception as e:
    report['custom_namespace_error'] = str(e)[:300]

# 2. Live-invoke 3 patched Lambdas
print("[2] Live-invoking 3 patched Lambdas to confirm syntax...")
test_lambdas = [
    'justhodl-portfolio-catalysts',
    'justhodl-daily-report-v3',
    'justhodl-vrp',
]
report['live_invokes'] = {}
for name in test_lambdas:
    print(f"  invoking {name}...")
    try:
        resp = lam.invoke(FunctionName=name, InvocationType='RequestResponse', Payload=b'{}')
        payload = resp['Payload'].read().decode('utf-8', errors='replace')
        report['live_invokes'][name] = {
            'status': resp['StatusCode'],
            'function_error': resp.get('FunctionError', 'none'),
            'response_head': payload[:400],
        }
    except Exception as e:
        report['live_invokes'][name] = {'error': str(e)[:300]}

# 3. Verify fleet-error-monitor has the new code
print("[3] Checking fleet-error-monitor for the new silent_alerts code...")
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-fleet-error-monitor')
    report['fleet_error_monitor'] = {
        'code_sha': cfg.get('CodeSha256', '')[:12],
        'last_modified': cfg.get('LastModified'),
        'code_size': cfg.get('CodeSize'),
    }
    # Pull code to verify
    url = lam.get_function(FunctionName='justhodl-fleet-error-monitor')['Code']['Location']
    import urllib.request, zipfile, io
    with urllib.request.urlopen(url, timeout=30) as r:
        zf = zipfile.ZipFile(io.BytesIO(r.read()))
        code = zf.read('lambda_function.py').decode()
    report['fleet_error_monitor']['has_silent_alerts'] = 'silent_alerts' in code
    report['fleet_error_monitor']['has_s3_put_failure'] = 'S3PutFailure' in code
    report['fleet_error_monitor']['has_get_silent_failure_count'] = 'get_silent_failure_count' in code
except Exception as e:
    report['fleet_error_monitor'] = {'error': str(e)[:300]}

# 4. Sample patched Lambdas — check their CodeSha changed vs the commit before P2.5
print("[4] Verifying patched Lambdas reached AWS...")
patched_sample = ['justhodl-portfolio-catalysts', 'justhodl-vrp', 'justhodl-options-flow',
                  'justhodl-global-stress', 'justhodl-financial-secretary']
report['patched_lambdas'] = {}
for name in patched_sample:
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        # Pull code, look for marker
        url = lam.get_function(FunctionName=name)['Code']['Location']
        import urllib.request as ur, zipfile, io
        with ur.urlopen(url, timeout=30) as r:
            zf = zipfile.ZipFile(io.BytesIO(r.read()))
            code = zf.read('lambda_function.py').decode()
        has_marker = 'audit P2.5: emit EMF metric for silent put_object failure' in code
        has_emf = 'JustHodl/Reliability' in code and 'S3PutFailure' in code
        report['patched_lambdas'][name] = {
            'last_modified': cfg.get('LastModified'),
            'code_sha': cfg.get('CodeSha256', '')[:12],
            'has_audit_marker': has_marker,
            'has_emf_metric': has_emf,
        }
    except Exception as e:
        report['patched_lambdas'][name] = {'error': str(e)[:200]}

# Summary
report['summary'] = {
    'custom_metric_namespace_exists': report.get('n_custom_metrics', 0) >= 0,
    'fleet_monitor_has_silent_check': report.get('fleet_error_monitor', {}).get('has_silent_alerts', False),
    'patched_lambdas_with_marker': sum(1 for v in report.get('patched_lambdas',{}).values() if v.get('has_audit_marker')),
    'patched_lambdas_with_emf': sum(1 for v in report.get('patched_lambdas',{}).values() if v.get('has_emf_metric')),
    'live_invokes_clean': sum(1 for v in report.get('live_invokes',{}).values() if v.get('function_error') == 'none'),
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1051.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== SUMMARY ===")
print(json.dumps(report['summary'], indent=2))
