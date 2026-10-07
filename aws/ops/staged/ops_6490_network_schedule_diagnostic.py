"""Read only two named engine bindings and aggregate invocation metrics.

No Lambda invocation, packet/log/credential reads, or cloud mutation. Never
publish target inputs, roles, or unrelated schedule metadata.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json, sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
FUNCTIONS = ('justhodl-future-intelligence', 'justhodl-ticker-360')
PREFIX = 'arn:aws:lambda:us-east-1:857687956942:function:'


def pages(method, key, **args):
    token, seen = None, set()
    for _ in range(100):
        packet = method(**args, **({'NextToken': token} if token else {}))
        rows = packet.get(key)
        if not isinstance(rows, list):
            raise ValueError('invalid_inventory')
        yield from rows
        token = packet.get('NextToken')
        if not token:
            return
        if not isinstance(token, str) or token in seen:
            raise ValueError('incomplete_inventory')
        seen.add(token)
    raise ValueError('inventory_bound_exceeded')


def collect(events, scheduler, cloudwatch, now):
    result = {fn: {'classic': [], 'scheduler': [], 'metrics': {}} for fn in FUNCTIONS}
    for fn in FUNCTIONS:
        names = set()
        for suffix in ('', ':$LATEST', ':live'):
            names.update(pages(events.list_rule_names_by_target, 'RuleNames', TargetArn=PREFIX + fn + suffix, Limit=100))
        for name in sorted(names):
            rule = events.describe_rule(Name=name)
            matched = [t for t in pages(events.list_targets_by_rule, 'Targets', Rule=name, Limit=100)
                       if t.get('Arn', '').split(':function:')[-1].split(':')[0] == fn]
            result[fn]['classic'].append({'name': name, 'expression': rule.get('ScheduleExpression'),
                'state': rule.get('State'), 'matching_targets': len(matched),
                'qualifiers': [t['Arn'][len(PREFIX + fn):] or 'unqualified' for t in matched]})
    for row in pages(scheduler.list_schedules, 'Schedules', MaxResults=100):
        arn = (row.get('Target') or {}).get('Arn', '')
        fn = arn.split(':function:')[-1].split(':')[0]
        if fn not in result or not arn.startswith(PREFIX + fn):
            continue
        schedule = scheduler.get_schedule(Name=row['Name'], GroupName=row.get('GroupName', 'default'))
        result[fn]['scheduler'].append({'name': schedule['Name'], 'group': schedule.get('GroupName', 'default'),
            'expression': schedule.get('ScheduleExpression'), 'state': schedule.get('State'),
            'timezone': schedule.get('ScheduleExpressionTimezone'),
            'qualifier': arn[len(PREFIX + fn):] or 'unqualified'})
    for fn in FUNCTIONS:
        for metric, stat in (('Invocations', 'Sum'), ('Errors', 'Sum'), ('Duration', 'Maximum')):
            packet = cloudwatch.get_metric_statistics(Namespace='AWS/Lambda', MetricName=metric,
                Dimensions=[{'Name': 'FunctionName', 'Value': fn}], StartTime=now-timedelta(hours=24),
                EndTime=now, Period=3600, Statistics=[stat])
            result[fn]['metrics'][metric] = [{'at': row['Timestamp'].isoformat(), 'value': row.get(stat)}
                for row in sorted(packet.get('Datapoints', []), key=lambda x: x['Timestamp'])]
    return {'observed_at': now.isoformat(), 'functions': result,
        'scope': 'Classic unqualified/$LATEST/live target lookups and full target-filtered Scheduler census; other classic numbered/alias targets not enumerated. Empty metrics are unknown, not zero.',
        'native_invocations': 0, 'cloud_writes': 0, 'private_packet_reads': 0, 'application_log_reads': 0}


def main():
    import boto3
    from ops_report import report
    with report('ops_6490_network_schedule_diagnostic') as output:
        try:
            clients = [boto3.client(name, region_name='us-east-1') for name in ('events', 'scheduler', 'cloudwatch')]
            evidence = collect(*clients, datetime.now(timezone.utc))
        except Exception as exc:
            output.fail('Read-only diagnostic failed: ' + type(exc).__name__)
            raise RuntimeError('schedule_diagnostic_unavailable') from None
        output.log(json.dumps(evidence, sort_keys=True, separators=(',', ':')))


if __name__ == '__main__':
    main()
