#!/usr/bin/env python3
"""ops 1046 — verify fleet-error-monitor deployed + manually invoke once"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
iam = boto3.client('iam', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# 1. Lambda exists?
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-fleet-error-monitor')
    report['lambda'] = {
        'exists': True,
        'last_modified': cfg.get('LastModified'),
        'memory': cfg.get('MemorySize'),
        'timeout': cfg.get('Timeout'),
        'dlq': (cfg.get('DeadLetterConfig') or {}).get('TargetArn'),
        'xray': cfg.get('TracingConfig', {}).get('Mode'),
        'env_keys': sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys())),
        'role': cfg.get('Role'),
    }
except Exception as e:
    report['lambda'] = {'exists': False, 'error': str(e)[:300]}

# 2. EB rule?
try:
    r = events.describe_rule(Name='fleet-error-monitor-5min')
    report['eb_rule'] = {'state': r.get('State'), 'expression': r.get('ScheduleExpression')}
    targets = events.list_targets_by_rule(Rule='fleet-error-monitor-5min').get('Targets', [])
    report['eb_targets'] = [{'arn': t.get('Arn')} for t in targets]
except Exception as e:
    report['eb_rule'] = {'error': str(e)[:200]}

# 3. Ensure lambda role has needed permissions (sqs:GetQueueAttributes, sns:Publish, etc.)
# The lambda-execution-role already has CloudWatchLogs + S3 + SQS (we added) + SNS (we added).
# Need to verify it can also do cloudwatch:GetMetricStatistics + lambda:ListFunctions.
print("[3] Ensuring lambda-execution-role has fleet-monitor permissions...")
try:
    iam.put_role_policy(
        RoleName='lambda-execution-role',
        PolicyName='fleet-monitor-perms',
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:GetMetricStatistics",
                    "cloudwatch:ListMetrics",
                    "lambda:ListFunctions",
                    "lambda:GetFunctionConfiguration",
                    "logs:FilterLogEvents",
                    "logs:DescribeLogGroups",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                    "sns:Publish",
                ],
                "Resource": "*",
            }],
        }),
    )
    report['role_policy_attached'] = True
    print("  ✅ Role policy attached")
except Exception as e:
    report['role_policy_error'] = str(e)[:200]
    print(f"  ❌ {e}")

# 4. Wait briefly for IAM propagation
time.sleep(10)

# 5. Manual invoke
print("[5] Manual invoke...")
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-error-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    report['invoke'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response': payload[:1000],
    }
    print(f"  status={inv['StatusCode']}  fn_error={inv.get('FunctionError','none')}")
    print(f"  response: {payload[:500]}")
except Exception as e:
    report['invoke'] = {'error': str(e)[:300]}
    print(f"  ❌ {e}")

# 6. Read S3 output if invoke succeeded
print("[6] Reading data/_fleet-monitor.json...")
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/_fleet-monitor.json')
    state = json.loads(obj['Body'].read().decode())
    report['s3_state'] = {
        'n_lambdas_scanned': state.get('n_lambdas_scanned'),
        'n_alerts_raised': state.get('n_alerts_raised'),
        'dlq_status': state.get('dlq_status'),
        'elapsed_s': state.get('elapsed_s'),
        'thresholds': state.get('thresholds'),
        'alerts': state.get('alerts'),
    }
except Exception as e:
    report['s3_state'] = {'error': str(e)[:200]}

# 7. Tail recent logs
print("[7] Recent logs...")
try:
    resp = logs.filter_log_events(
        logGroupName='/aws/lambda/justhodl-fleet-error-monitor',
        startTime=int(time.time()*1000) - 600000,  # last 10 min
        limit=20,
    )
    report['recent_logs'] = [e['message'].strip()[:200] for e in resp.get('events', [])[-15:]]
except Exception as e:
    report['recent_logs'] = [f'log fetch error: {e}']

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1046.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str)[:3000])
