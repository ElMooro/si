#!/usr/bin/env python3
"""
ops 1036 — VERIFY all audit P0 fixes landed

Confirms:
  1. autonomous-ai-schedule EB rule is DISABLED  (ops 1035)
  2. justhodl-8am EB rule is DISABLED  (ops 1035)
  3. justhodl-forced-selling-bounce CodeSha256 is NEW (redeployed)
     AND the deployed code contains the 'def to_number' fix.
  4. justhodl-portfolio-catalysts CodeSha256 is NEW
     AND deployed code contains 'earnings_buckets = {"T-0":' init.
  5. justhodl-nobrainer-rationale CodeSha256 is NEW
     AND deployed code contains the 'or 0' format defenders.

For each fixed Lambda, also invokes it once synchronously to confirm
the bug no longer reproduces. Captures the response status.
"""
import json, boto3, os, io, zipfile, urllib.request
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

report = {'started_at': NOW.isoformat(), 'checks': {}}

# ---- 1 & 2: EB rules disabled ----
for rule_name in ['autonomous-ai-schedule', 'justhodl-8am']:
    try:
        r = events.describe_rule(Name=rule_name)
        report['checks'][f'rule_{rule_name}'] = {
            'state': r.get('State'),
            'verdict': '✅ DISABLED' if r.get('State') == 'DISABLED' else '❌ still ENABLED',
        }
    except Exception as e:
        report['checks'][f'rule_{rule_name}'] = {'error': str(e)[:200]}

# ---- 3, 4, 5: Lambda redeploy + bug fix verification ----
EXPECTED_PATTERNS = {
    'justhodl-forced-selling-bounce': ['def to_number', 'dict-wrapped indicators'],
    'justhodl-portfolio-catalysts': ['earnings_buckets = {"T-0": []'],
    'justhodl-nobrainer-rationale': ['or 0:.0f}', 'or 0:.2f}'],
}

for lam_name, patterns in EXPECTED_PATTERNS.items():
    check = {'lambda': lam_name}
    try:
        cfg = lam.get_function_configuration(FunctionName=lam_name)
        check['code_sha'] = cfg['CodeSha256'][:12]
        check['last_modified'] = cfg['LastModified']
        
        # Download deployed code and scan for fix patterns
        code_url = lam.get_function(FunctionName=lam_name)['Code']['Location']
        with urllib.request.urlopen(code_url, timeout=30) as resp:
            zip_bytes = resp.read()
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        code_str = ''
        for name in zf.namelist():
            if name.endswith('.py'):
                code_str += zf.read(name).decode('utf-8', errors='replace')
        
        check['deployed_code_size'] = len(code_str)
        check['patterns_found'] = {p: (p in code_str) for p in patterns}
        check['has_redeploy_marker'] = 'audit-P0-redeploy: 2026-05-22' in code_str
        check['verdict'] = (
            '✅ FIX DEPLOYED' if all(check['patterns_found'].values()) and check['has_redeploy_marker']
            else '⚠️ FIX MISSING OR MARKER ABSENT'
        )
        
        # Invoke synchronously to test the bug doesn't reproduce
        # Only for portfolio-catalysts and forced-selling-bounce (no side effects)
        # nobrainer-rationale sends telegram which we don't want to fire
        if lam_name in ('justhodl-portfolio-catalysts', 'justhodl-forced-selling-bounce'):
            try:
                r = lam.invoke(FunctionName=lam_name, InvocationType='RequestResponse', LogType='Tail')
                payload = r['Payload'].read().decode('utf-8', errors='replace')
                check['invoke_status'] = r.get('StatusCode')
                check['invoke_function_error'] = r.get('FunctionError', 'none')
                check['invoke_response_head'] = payload[:300]
                if r.get('LogResult'):
                    import base64
                    log_tail = base64.b64decode(r['LogResult']).decode('utf-8', errors='replace')
                    # Check for the specific error patterns we're trying to fix
                    has_keyerror_t0 = "KeyError: 'T-0'" in log_tail
                    has_dictint_err = "'<' not supported between instances of 'dict' and 'int'" in log_tail
                    check['log_tail_has_bug'] = has_keyerror_t0 or has_dictint_err
                    check['log_tail_last_400'] = log_tail[-400:]
            except Exception as e:
                check['invoke_error'] = str(e)[:300]
        
        # Recent CW error metric
        end = NOW; start = end - timedelta(hours=1)
        try:
            m_inv = cw.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName='Invocations',
                Dimensions=[{'Name': 'FunctionName', 'Value': lam_name}],
                StartTime=start, EndTime=end, Period=3600, Statistics=['Sum'])
            m_err = cw.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName='Errors',
                Dimensions=[{'Name': 'FunctionName', 'Value': lam_name}],
                StartTime=start, EndTime=end, Period=3600, Statistics=['Sum'])
            check['cw_inv_last_1h'] = int(sum(p['Sum'] for p in m_inv['Datapoints']))
            check['cw_err_last_1h'] = int(sum(p['Sum'] for p in m_err['Datapoints']))
        except Exception: pass
        
    except Exception as e:
        check['error'] = str(e)[:300]
    
    report['checks'][f'lambda_{lam_name}'] = check

# Summary
report['summary'] = {
    'rules_disabled': sum(1 for k,v in report['checks'].items()
                          if k.startswith('rule_') and '✅' in v.get('verdict', '')),
    'lambdas_fixed': sum(1 for k,v in report['checks'].items()
                         if k.startswith('lambda_') and '✅' in v.get('verdict', '')),
    'lambdas_with_redeploy_marker': sum(1 for k,v in report['checks'].items()
                                         if k.startswith('lambda_') and v.get('has_redeploy_marker') is True),
}

# Write
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1036.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Console summary
print("="*70)
print("OPS 1036 — AUDIT P0 FIX VERIFICATION")
print("="*70)

for key, c in report['checks'].items():
    print(f"\n  {key}")
    if 'verdict' in c:
        print(f"    verdict: {c['verdict']}")
    if 'state' in c:
        print(f"    rule state: {c['state']}")
    if 'code_sha' in c:
        print(f"    code_sha: {c['code_sha']}  last_modified: {c['last_modified']}")
        print(f"    patterns: {c.get('patterns_found')}")
        print(f"    redeploy_marker: {c.get('has_redeploy_marker')}")
        if 'invoke_status' in c:
            print(f"    sync invoke: status={c['invoke_status']} FunctionError={c['invoke_function_error']}")
            print(f"    log_tail_has_bug: {c.get('log_tail_has_bug')}")
        if 'cw_inv_last_1h' in c:
            print(f"    last 1h: inv={c['cw_inv_last_1h']} err={c['cw_err_last_1h']}")

print()
print("="*70)
print("SUMMARY")
print("="*70)
for k, v in report['summary'].items():
    print(f"  {k}: {v}")
