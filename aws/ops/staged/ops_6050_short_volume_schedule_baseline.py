"""Read actual scheduler bindings before replacing daily short-volume producers.

Read-only control-plane scan plus protected evidence retention. No invocation,
schedule mutation, account read, provider call or public packet write.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops'), str(ROOT / 'aws/ops/staged')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6046_short_volume_baseline as base
REQUEST = 'chatgpt-short-volume-schedules-6050'
STATUS = base.PRIVATE + 'requests/' + hashlib.sha256(REQUEST.encode()).hexdigest() + '.json'
NAMES = {'justhodl-finra-short': ('default', 'finra-short-sched'),
         'justhodl-short-pressure': ('default', 'short-pressure-sched')}


def schedule_row(actual, arn):
    if actual['Target']['Arn'] != arn:
        raise ValueError('Schedule target differs from exact native function')
    return {'name': actual['Name'], 'group': actual.get('GroupName', 'default'),
            'expression': actual['ScheduleExpression'], 'timezone': actual['ScheduleExpressionTimezone'],
            'state': actual['State'], 'flexible_time_window': actual['FlexibleTimeWindow'],
            'target_arn': arn, 'target_role_arn': actual['Target']['RoleArn'],
            'input_sha256': hashlib.sha256(actual['Target'].get('Input', '').encode()).hexdigest(),
            'retry_policy': actual['Target'].get('RetryPolicy'),
            'dead_letter_arn': actual['Target'].get('DeadLetterConfig', {}).get('Arn'),
            'start_date': actual.get('StartDate').isoformat() if actual.get('StartDate') else None,
            'end_date': actual.get('EndDate').isoformat() if actual.get('EndDate') else None}


def scan(scheduler, events, arns):
    schedules = {function: [] for function in arns}
    rules = {function: [] for function in arns}
    groups = []
    listed = 0
    for page in scheduler.get_paginator('list_schedule_groups').paginate():
        for group in page['ScheduleGroups']:
            groups.append(group['Name'])
            for chunk in scheduler.get_paginator('list_schedules').paginate(GroupName=group['Name']):
                for item in chunk.get('Schedules', []):
                    listed += 1
                    target = item.get('Target', {}).get('Arn', '')
                    for function, arn in arns.items():
                        if target == arn or target.startswith(arn + ':'):
                            schedules[function].append(schedule_row(scheduler.get_schedule(Name=item['Name'], GroupName=group['Name']), arn))
    for function, arn in arns.items():
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=arn):
            for name in page['RuleNames']:
                rule = events.describe_rule(Name=name)
                targets = events.list_targets_by_rule(Rule=name)['Targets']
                matched = [t for t in targets if t.get('Arn') == arn]
                if not matched:
                    raise ValueError('Reported EventBridge target missing')
                rules[function].append({'name': name, 'expression': rule.get('ScheduleExpression'),
                                        'state': rule['State'], 'target_arn': arn,
                                        'target_inputs_sha256': [hashlib.sha256(t.get('Input', '').encode()).hexdigest() for t in matched]})
        expected = NAMES[function]
        if not any((row['group'], row['name']) == expected for row in schedules[function]):
            raise ValueError('Known live scheduler absent from scan')
    return {'schedules': schedules, 'classic_default_bus_rules': rules,
            'scheduler_groups_scanned': sorted(groups), 'schedulers_scanned': listed,
            'other_invocation_paths_excluded_from_claim': True}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    with report('ops_6050_short_volume_schedule_baseline') as r:
        try:
            existing = base.retained.strict(base.read(s3, STATUS))
        except Exception as exc:
            if not base.retained.missing(exc):
                raise
            existing = None
        if existing:
            assert existing['status'] == 'complete', 'Inspect prior incomplete schedule scan'
            ref = existing['manifest']
            manifest = base.retained.strict(base.checked(s3, ref))
        else:
            raw = base.retained.encoded({'request_id': REQUEST, 'status': 'claimed'})
            s3.put_object(Bucket=base.BUCKET, Key=STATUS, Body=raw, CacheControl='no-store', IfNoneMatch='*')
            assert base.read(s3, STATUS) == raw
            lam = boto3.client('lambda', region_name='us-east-1')
            arns = {name: lam.get_function_configuration(FunctionName=name)['FunctionArn'] for name in NAMES}
            findings = scan(boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1'), arns)
            manifest = {'contract': 'short-volume-schedule-baseline.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                        'request_id': REQUEST, **findings}
            ref = base.protect(s3, base.retained.encoded(manifest))
            raw = base.retained.encoded({'request_id': REQUEST, 'status': 'complete', 'manifest': ref})
            s3.put_object(Bucket=base.BUCKET, Key=STATUS, Body=raw, CacheControl='no-store')
            assert base.read(s3, STATUS) == raw
        for key in (STATUS, ref['key']):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + base.BUCKET + '.s3.amazonaws.com/' + key)
        r.kv(manifest=ref, findings=manifest, schedule_mutations=0, engine_invocations=0,
             provider_requests=0, private_account_reads=0, notifications_sent=0, public_head_writes=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
