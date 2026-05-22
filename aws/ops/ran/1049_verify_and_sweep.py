#!/usr/bin/env python3
"""ops 1049 — verify freshness monitor v1.1 + invoke 7 exp Lambdas not yet run

After deploy of fleet-freshness-monitor v1.1.0, invoke it and confirm it
reads the rules-based manifest correctly.

Also invoke the 7 exp Lambdas that haven't been invoked yet (per ops 1054
they all have s3_output.exists: false except #2 signal-halflife):
  #1 justhodl-premortem-engine
  #4 justhodl-behavior-mirror
  #5 justhodl-failure-library
  #3 justhodl-causality-scanner
  #7 justhodl-convexity-scorer
  #6 justhodl-chart-vision
  #8 justhodl-meta-improver

For each: live invoke + capture status + read S3 output if written.
"""
import json, boto3, os, time
from datetime import datetime, timezone
from botocore.config import Config

REGION = 'us-east-1'
cfg = Config(region_name=REGION, retries={'max_attempts': 5, 'mode': 'adaptive'},
             read_timeout=900, connect_timeout=10)
lam = boto3.client('lambda', config=cfg)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# ============================================================
# Part A: Verify freshness monitor v1.1.0
# ============================================================
print("[A] Freshness monitor v1.1.0 verify + invoke...")
try:
    cfg_lambda = lam.get_function_configuration(FunctionName='justhodl-fleet-freshness-monitor')
    report['freshness_lambda'] = {
        'last_modified': cfg_lambda.get('LastModified'),
        'code_sha': cfg_lambda.get('CodeSha256', '')[:16],
        'memory': cfg_lambda.get('MemorySize'),
        'timeout': cfg_lambda.get('Timeout'),
    }
except Exception as e:
    report['freshness_lambda'] = {'error': str(e)[:300]}

# Invoke
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-freshness-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    report['freshness_invoke'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response_head': payload[:800],
    }
    print(f"  status={inv['StatusCode']}  fn_err={inv.get('FunctionError','none')}")
except Exception as e:
    report['freshness_invoke'] = {'error': str(e)[:300]}

# Read run state
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-monitor.json')
    state = json.loads(obj['Body'].read().decode())
    report['freshness_state'] = {
        'version': state.get('version'),
        'n_keys_tracked': state.get('n_keys_tracked'),
        'n_stale': state.get('n_stale'),
        'n_fresh': state.get('n_fresh'),
        'n_alerts_raised': state.get('n_alerts_raised'),
        'elapsed_s': state.get('elapsed_s'),
        'top_10_stale': [
            {
                'key': r.get('key'),
                'age_h': r.get('age_h'),
                'max_h': r.get('max_age_h'),
                'ratio': round(r.get('age_h',0)/r.get('max_age_h',1), 1) if r.get('max_age_h') else None,
            }
            for r in state.get('stale_top_50', [])[:10]
        ],
    }
except Exception as e:
    report['freshness_state'] = {'error': str(e)[:200]}

# ============================================================
# Part B: Invoke the 7 unrun exp Lambdas
# ============================================================
print("\n[B] Invoking 7 exp Lambdas that haven't run yet...")
exp_lambdas = [
    ('#1', 'justhodl-premortem-engine', 'data/kill-theses.json'),
    ('#4', 'justhodl-behavior-mirror', 'data/khalid-behavior-mirror.json'),
    ('#5', 'justhodl-failure-library', 'data/failure-library.json'),
    ('#3', 'justhodl-causality-scanner', 'data/causality-scanner.json'),
    ('#7', 'justhodl-convexity-scorer', 'data/convexity-scores.json'),
    ('#6', 'justhodl-chart-vision', 'data/chart-vision.json'),
    ('#8', 'justhodl-meta-improver', 'data/meta-improver.json'),
]

report['exp_invokes'] = []
for idea, fn_name, output_key in exp_lambdas:
    print(f"  Invoking {idea} {fn_name}...")
    info = {'idea': idea, 'lambda': fn_name, 'expected_output_key': output_key}
    try:
        inv = lam.invoke(
            FunctionName=fn_name,
            InvocationType='RequestResponse',
            Payload=b'{}',
        )
        payload = inv['Payload'].read().decode()
        info['status'] = inv['StatusCode']
        info['function_error'] = inv.get('FunctionError', 'none')
        info['response_head'] = payload[:400]
        print(f"    status={inv['StatusCode']}  fn_err={inv.get('FunctionError','none')}  resp={payload[:120]}")
    except Exception as e:
        info['error'] = str(e)[:300]
        print(f"    ❌ {str(e)[:200]}")
    
    # Check expected output
    try:
        head = s3.head_object(Bucket='justhodl-dashboard-live', Key=output_key)
        info['s3_output'] = {
            'exists': True,
            'size': head['ContentLength'],
            'last_modified': head['LastModified'].isoformat(),
        }
    except Exception:
        # Try a few variants of the output key
        info['s3_output'] = {'exists': False, 'searched_key': output_key}
    report['exp_invokes'].append(info)

# Summary
exp_ok = sum(1 for r in report['exp_invokes'] if r.get('function_error') == 'none')
exp_failed = sum(1 for r in report['exp_invokes'] if r.get('function_error') == 'Unhandled')
exp_other = len(report['exp_invokes']) - exp_ok - exp_failed
report['summary'] = {
    'freshness_ok': report.get('freshness_invoke', {}).get('function_error') == 'none',
    'freshness_tracked': report.get('freshness_state', {}).get('n_keys_tracked'),
    'freshness_stale': report.get('freshness_state', {}).get('n_stale'),
    'exp_ok': exp_ok,
    'exp_failed': exp_failed,
    'exp_other': exp_other,
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1049.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== SUMMARY ===")
print(json.dumps(report['summary'], indent=2))
