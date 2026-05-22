#!/usr/bin/env python3
"""ops 1060 — verify which of the 5 new engines are LIVE on AWS + producing S3 output."""
import json, os, boto3
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
eb = boto3.client('events', region_name=REGION)

ENGINES = {
    'justhodl-carry-surface': 'data/carry-surface.json',
    'justhodl-engine-contribution': 'data/engine-contributions.json',
    'justhodl-cross-asset-confirm': 'data/cross-asset-confirm.json',
    'justhodl-earnings-linguistic': 'data/earnings-linguistic.json',
    'justhodl-engine-robustness': 'data/engine-robustness.json',
}

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'engines': {}}

for fn_name, out_key in ENGINES.items():
    info = {'name': fn_name, 'expected_output': out_key}
    
    # Lambda exists?
    try:
        cfg = lam.get_function_configuration(FunctionName=fn_name)
        info['lambda_exists'] = True
        info['lambda_memory'] = cfg.get('MemorySize')
        info['lambda_timeout'] = cfg.get('Timeout')
        info['lambda_last_modified'] = cfg.get('LastModified')
        info['lambda_description'] = (cfg.get('Description') or '')[:120]
        info['lambda_has_dlq'] = bool(cfg.get('DeadLetterConfig'))
        info['lambda_has_xray'] = cfg.get('TracingConfig', {}).get('Mode') == 'Active'
    except lam.exceptions.ResourceNotFoundException:
        info['lambda_exists'] = False
    except Exception as e:
        info['lambda_err'] = str(e)[:200]
    
    # S3 output exists?
    try:
        obj = s3.head_object(Bucket='justhodl-dashboard-live', Key=out_key)
        info['s3_exists'] = True
        info['s3_size'] = obj['ContentLength']
        info['s3_last_modified'] = obj['LastModified'].isoformat()
        # Age in hours
        age_h = (datetime.now(timezone.utc) - obj['LastModified']).total_seconds() / 3600
        info['s3_age_hours'] = round(age_h, 1)
    except Exception as e:
        info['s3_exists'] = False
        info['s3_err'] = str(e)[:120]
    
    # EB rule exists?
    if info.get('lambda_exists'):
        try:
            # Find rules invoking this Lambda
            rules_resp = eb.list_rule_names_by_target(TargetArn=cfg['FunctionArn'])
            rule_names = rules_resp.get('RuleNames', [])
            if rule_names:
                rule_info = []
                for rname in rule_names:
                    r = eb.describe_rule(Name=rname)
                    rule_info.append({
                        'name': rname,
                        'state': r.get('State'),
                        'expression': r.get('ScheduleExpression') or r.get('EventPattern'),
                    })
                info['eb_rules'] = rule_info
            else:
                info['eb_rules'] = []
        except Exception as e:
            info['eb_err'] = str(e)[:120]
    
    report['engines'][fn_name] = info

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1060.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Summary
print("="*80)
print(f"{'ENGINE':<35} {'LAMBDA':<8} {'DLQ':<5} {'XRAY':<5} {'EB':<5} {'S3':<6} {'AGE':<8}")
print("="*80)
for n, i in report['engines'].items():
    lambda_ok = '✅' if i.get('lambda_exists') else '❌'
    dlq = '✅' if i.get('lambda_has_dlq') else '—'
    xray = '✅' if i.get('lambda_has_xray') else '—'
    eb_ok = '✅' if i.get('eb_rules') else '—'
    s3_ok = '✅' if i.get('s3_exists') else '❌'
    age = f"{i.get('s3_age_hours','?')}h" if i.get('s3_age_hours') is not None else '—'
    print(f"{n:<35} {lambda_ok:<8} {dlq:<5} {xray:<5} {eb_ok:<5} {s3_ok:<6} {age:<8}")
print()
print("DETAIL:")
for n, i in report['engines'].items():
    if not i.get('lambda_exists') or not i.get('s3_exists'):
        print(f"\n  {n}:")
        for k, v in i.items():
            if k != 'name':
                print(f"    {k}: {v}")
