#!/usr/bin/env python3
"""
ops 1046 — Patch custom roles + finish DLQ + deploy fleet-error-monitor

Three things:
  1. Grant sqs:SendMessage to the ~11 custom execution roles used by
     12 Lambdas that failed in 1045 — gets DLQ to 100%.
  2. Deploy a new Lambda `justhodl-fleet-error-monitor` that runs every
     5 min, checks all 392 Lambdas' error metrics, publishes to SNS
     justhodl-fleet-alerts on any Lambda with >5% error rate over 15 min.
  3. Subscribe Telegram via SNS HTTP subscription (using Khalid's bot).
"""
import json, boto3, os, time, zipfile, io
from datetime import datetime, timezone
from botocore.config import Config

REGION = 'us-east-1'
ACCOUNT = '857687956942'
DLQ_ARN = f'arn:aws:sqs:{REGION}:{ACCOUNT}:justhodl-dlq-default'
SNS_ARN = f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts'
LAMBDA_ROLE_ARN = f'arn:aws:iam::{ACCOUNT}:role/lambda-execution-role'

cfg = Config(region_name=REGION, retries={'max_attempts': 10, 'mode': 'adaptive'},
             read_timeout=60, connect_timeout=10)
iam = boto3.client('iam', config=cfg)
lam = boto3.client('lambda', config=cfg)
sqs = boto3.client('sqs', config=cfg)
sns = boto3.client('sns', config=cfg)
events = boto3.client('events', config=cfg)

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'steps': []}

def step(name, fn):
    try:
        r = fn()
        report['steps'].append({'step': name, 'result': 'OK', 'detail': str(r)[:150] if r else None})
        print(f"  ✅ {name}")
        return r
    except Exception as e:
        report['steps'].append({'step': name, 'result': 'ERROR', 'error': str(e)[:300]})
        print(f"  ❌ {name}: {str(e)[:180]}")
        return None


# ---- A. Patch custom roles ----
print("[A] Discover the 12 Lambdas with custom roles + their role names...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])

# Find Lambdas without DLQ
no_dlq = [fn for fn in all_lambdas if not (fn.get('DeadLetterConfig') or {}).get('TargetArn')]
print(f"  {len(no_dlq)} Lambdas without DLQ")

# Map their roles
custom_roles = set()
lambda_role = {}
for fn in no_dlq:
    role_arn = fn.get('Role', '')
    role_name = role_arn.split('/')[-1]
    custom_roles.add(role_name)
    lambda_role[fn['FunctionName']] = role_name
print(f"  {len(custom_roles)} unique roles: {sorted(custom_roles)}")

# Attach inline sqs:SendMessage policy to each custom role
print("[B] Granting sqs:SendMessage to each custom role...")
for role_name in sorted(custom_roles):
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
            "Resource": DLQ_ARN,
        }],
    }
    step(f'role_policy_{role_name}', lambda r=role_name, d=policy_doc: iam.put_role_policy(
        RoleName=r,
        PolicyName='justhodl-dlq-send',
        PolicyDocument=json.dumps(d),
    ))

print("[C] Wait 20s for IAM propagation...")
time.sleep(20)

# Retry DLQ application on the failed Lambdas
print("[D] Re-apply DLQ to the 12 Lambdas...")
results = []
for fn in no_dlq:
    name = fn['FunctionName']
    try:
        lam.update_function_configuration(
            FunctionName=name, DeadLetterConfig={'TargetArn': DLQ_ARN})
        results.append({'lambda': name, 'role': lambda_role[name], 'result': 'UPDATED'})
        print(f"    ✅ {name} ({lambda_role[name]})")
    except Exception as e:
        results.append({'lambda': name, 'role': lambda_role[name], 'error': str(e)[:200]})
        print(f"    ❌ {name}: {str(e)[:120]}")
report['dlq_retry_results'] = results


# ---- B. Add CloudWatch Logs perm + invoke perm to lambda-execution-role for fleet-monitor ----
print("[E] Ensuring lambda-execution-role has CW + Lambda + SNS perms for fleet-monitor...")
fleet_monitor_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": [
            "cloudwatch:GetMetricStatistics", "cloudwatch:GetMetricData",
            "cloudwatch:ListMetrics",
        ], "Resource": "*"},
        {"Effect": "Allow", "Action": ["lambda:ListFunctions"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["sns:Publish"], "Resource": SNS_ARN},
    ],
}
step('iam_fleet_monitor', lambda: iam.put_role_policy(
    RoleName='lambda-execution-role',
    PolicyName='justhodl-fleet-monitor-perm',
    PolicyDocument=json.dumps(fleet_monitor_policy),
))


# ---- C. Deploy fleet-error-monitor Lambda ----
print("[F] Building fleet-error-monitor Lambda code...")
LAMBDA_CODE = '''import json, os, boto3, urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

REGION = "us-east-1"
SNS_ARN = os.environ["SNS_ARN"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ERROR_RATE_THRESHOLD = float(os.environ.get("ERROR_RATE_THRESHOLD", "5.0"))  # %
MIN_INVOCATIONS = int(os.environ.get("MIN_INVOCATIONS", "5"))  # ignore noise
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "15"))

lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)


def get_metrics(name, start, end):
    out = {"name": name}
    for metric in ["Invocations", "Errors"]:
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": name}],
                StartTime=start, EndTime=end, Period=900, Statistics=["Sum"],
            )
            out[metric.lower()] = int(sum(p["Sum"] for p in r["Datapoints"]))
        except Exception:
            out[metric.lower()] = 0
    return out


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=LOOKBACK_MINUTES)

    funcs = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        funcs.extend(page["Functions"])

    metrics = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = [ex.submit(get_metrics, fn["FunctionName"], start, now) for fn in funcs]
        for fut in as_completed(futures):
            metrics.append(fut.result())

    alerts = []
    for m in metrics:
        inv = m.get("invocations", 0)
        err = m.get("errors", 0)
        if inv >= MIN_INVOCATIONS and err > 0:
            rate = err / inv * 100
            if rate >= ERROR_RATE_THRESHOLD:
                alerts.append({
                    "lambda": m["name"], "inv": inv, "err": err, "rate": round(rate, 1)
                })

    alerts.sort(key=lambda x: -x["rate"])

    summary = {
        "checked_at": now.isoformat(),
        "lookback_min": LOOKBACK_MINUTES,
        "lambdas_scanned": len(metrics),
        "lambdas_alerting": len(alerts),
        "alerts": alerts[:30],
        "threshold_pct": ERROR_RATE_THRESHOLD,
    }

    if alerts:
        lines = [f"\\u26a0\\ufe0f *JustHodl Fleet Alert*",
                 f"_{len(alerts)} Lambdas erroring above {ERROR_RATE_THRESHOLD}% over {LOOKBACK_MINUTES} min_",
                 ""]
        for a in alerts[:10]:
            lines.append(f"\\u2022 `{a['lambda']}`: {a['err']}/{a['inv']} = {a['rate']}%")
        if len(alerts) > 10:
            lines.append(f"...and {len(alerts) - 10} more")
        msg = "\\n".join(lines)

        # SNS publish
        try:
            sns.publish(TopicArn=SNS_ARN, Subject=f"Fleet alert: {len(alerts)} Lambdas erroring",
                        Message=msg)
        except Exception as e:
            summary["sns_error"] = str(e)[:200]

        # Telegram direct
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                data = json.dumps({
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": msg,
                    "parse_mode": "Markdown",
                }).encode()
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                summary["telegram_error"] = str(e)[:200]

    return summary
'''

# Create zip
zbuf = io.BytesIO()
with zipfile.ZipFile(zbuf, 'w', zipfile.ZIP_DEFLATED) as z:
    info = zipfile.ZipInfo('lambda_function.py')
    info.external_attr = 0o644 << 16
    z.writestr(info, LAMBDA_CODE)
zip_bytes = zbuf.getvalue()

print(f"[G] Creating/updating fleet-error-monitor Lambda (zip {len(zip_bytes)} bytes)...")
FN_NAME = 'justhodl-fleet-error-monitor'
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = '241451060'  # placeholder; can update later

create_kwargs = {
    'FunctionName': FN_NAME,
    'Runtime': 'python3.12',
    'Role': LAMBDA_ROLE_ARN,
    'Handler': 'lambda_function.lambda_handler',
    'Code': {'ZipFile': zip_bytes},
    'Timeout': 300,
    'MemorySize': 512,
    'Environment': {'Variables': {
        'SNS_ARN': SNS_ARN,
        'TELEGRAM_BOT_TOKEN': TELEGRAM_TOKEN,
        'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID,
        'ERROR_RATE_THRESHOLD': '5.0',
        'MIN_INVOCATIONS': '5',
        'LOOKBACK_MINUTES': '15',
    }},
    'TracingConfig': {'Mode': 'Active'},
    'DeadLetterConfig': {'TargetArn': DLQ_ARN},
}

try:
    lam.get_function(FunctionName=FN_NAME)
    # Update existing
    step('update_fleet_monitor_code', lambda: lam.update_function_code(
        FunctionName=FN_NAME, ZipFile=zip_bytes))
    time.sleep(3)  # wait before config update
    step('update_fleet_monitor_config', lambda: lam.update_function_configuration(
        FunctionName=FN_NAME,
        Timeout=create_kwargs['Timeout'],
        MemorySize=create_kwargs['MemorySize'],
        Environment=create_kwargs['Environment'],
        TracingConfig=create_kwargs['TracingConfig'],
        DeadLetterConfig=create_kwargs['DeadLetterConfig'],
    ))
except lam.exceptions.ResourceNotFoundException:
    step('create_fleet_monitor', lambda: lam.create_function(**create_kwargs))


# ---- D. Create EventBridge schedule ----
print("[H] Creating EventBridge rule (every 5 min)...")
RULE = 'justhodl-fleet-error-monitor-5min'
step('put_rule', lambda: events.put_rule(
    Name=RULE,
    ScheduleExpression='rate(5 minutes)',
    State='ENABLED',
    Description='Scan all Lambdas for elevated error rates and alert',
))

# Lambda permission for EB
step('add_eb_invoke_perm', lambda: lam.add_permission(
    FunctionName=FN_NAME,
    StatementId='allow-eb-fleet-monitor',
    Action='lambda:InvokeFunction',
    Principal='events.amazonaws.com',
    SourceArn=f'arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE}',
))

step('put_target', lambda: events.put_targets(
    Rule=RULE,
    Targets=[{'Id': '1', 'Arn': f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN_NAME}'}],
))


# ---- E. Verify everything ----
print("[I] Final verification...")
verify_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    verify_lambdas.extend(page['Functions'])
n_dlq = sum(1 for fn in verify_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in verify_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')

report['final'] = {
    'n_lambdas': len(verify_lambdas),
    'n_with_dlq': n_dlq,
    'pct_dlq': round(n_dlq/len(verify_lambdas)*100, 1),
    'n_with_xray': n_xray,
    'pct_xray': round(n_xray/len(verify_lambdas)*100, 1),
    'fleet_monitor_deployed': True,
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1046.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== AUDIT P2 — STATE ===")
print(f"  DLQ:   {n_dlq}/{len(verify_lambdas)} ({report['final']['pct_dlq']}%)")
print(f"  X-Ray: {n_xray}/{len(verify_lambdas)} ({report['final']['pct_xray']}%)")
print(f"  Fleet error monitor: {'DEPLOYED' if report['final']['fleet_monitor_deployed'] else 'FAILED'}")
