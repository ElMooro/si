#!/usr/bin/env python3
"""Read-only guard: a code update must not introduce or retime a binding.

Called only for an already-existing function. Provisioning a new function is
separate. Reports contain no target Input, IAM roles, credentials or payloads.
"""
import json
import re
import sys
from datetime import datetime


class ScheduleMismatch(ValueError):
    pass


def _same_clock(left, right):
    def parse(value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ScheduleMismatch('scheduler_clock_invalid')
        return value
    return parse(left) == parse(right)


def _read(method, **kwargs):
    try:
        return method(**kwargs)
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') == 'ResourceNotFoundException':
            raise ScheduleMismatch('configured_binding_missing_use_original_reference') from None
        raise


def check(config, arn, events, scheduler):
    if not re.fullmatch(r'arn:aws:lambda:[a-z0-9-]+:\d{12}:function:[A-Za-z0-9_-]+', arn):
        raise ScheduleMismatch('invalid_function_identity')
    allowed = (arn, arn + ':$LATEST', arn + ':live')
    checked = []
    spec = config.get('schedule')
    if spec:
        rule = _read(events.describe_rule, Name=spec['rule_name'])
        if rule.get('Name') != spec['rule_name'] or rule.get('ScheduleExpression') != spec['cron']:
            raise ScheduleMismatch('classic_cadence_or_identity_differs')
        targets = []
        token = None
        seen = set()
        while True:
            page = events.list_targets_by_rule(Rule=spec['rule_name'], **({'NextToken': token} if token else {}))
            if not isinstance(page.get('Targets'), list):
                raise ScheduleMismatch('classic_target_inventory_invalid')
            targets.extend(page['Targets'])
            token = page.get('NextToken')
            if not token:
                break
            if not isinstance(token, str) or token in seen:
                raise ScheduleMismatch('classic_target_pagination_invalid')
            seen.add(token)
        matched = [t for t in targets if t.get('Arn') in allowed]
        if not matched:
            raise ScheduleMismatch('configured_rule_not_bound_to_function')
        checked.append('classic_existing_binding')
    spec = config.get('eventbridge_scheduler')
    if spec:
        current = _read(scheduler.get_schedule, Name=spec['schedule_name'], GroupName=spec.get('group_name', 'default'))
        if (current.get('Name') != spec['schedule_name']
                or current.get('GroupName', 'default') != spec.get('group_name', 'default')
                or current.get('ScheduleExpression') != spec['cron']
                or current.get('Target', {}).get('Arn') not in allowed):
            raise ScheduleMismatch('scheduler_cadence_or_binding_differs')
        fields = {'timezone': 'ScheduleExpressionTimezone', 'state': 'State',
                  'flexible_time_window': 'FlexibleTimeWindow', 'start_date': 'StartDate',
                  'end_date': 'EndDate', 'kms_key_arn': 'KmsKeyArn',
                  'action_after_completion': 'ActionAfterCompletion'}
        target_fields = {'role_arn': 'RoleArn', 'retry_policy': 'RetryPolicy',
                         'dead_letter_config': 'DeadLetterConfig'}
        for configured, live in fields.items():
            if configured not in spec:
                continue
            same = (_same_clock(spec[configured], current.get(live)) if configured in ('start_date', 'end_date')
                    else spec[configured] == current.get(live))
            if not same:
                raise ScheduleMismatch('scheduler_operating_settings_differ')
        for configured, live in target_fields.items():
            if configured in spec and spec[configured] != current['Target'].get(live):
                raise ScheduleMismatch('scheduler_target_settings_differ')
        if 'input' in spec:
            value = spec['input']
            expected = value if isinstance(value, str) else json.dumps(value, separators=(',', ':'))
            if expected != current['Target'].get('Input'):
                raise ScheduleMismatch('scheduler_input_differs')
        checked.append('scheduler_existing_binding')
    return {'phase': 'existing_schedule_guard', 'status': 'VERIFIED',
            'function': arn.split(':function:')[1], 'bindings_checked': checked,
            'schedule_writes': 0, 'native_invocations': 0}


def main():
    import boto3
    path, arn, region = sys.argv[1:]
    with open(path, encoding='utf-8') as stream:
        config = json.load(stream)
    print(json.dumps(check(config, arn, boto3.client('events', region_name=region),
                           boto3.client('scheduler', region_name=region)), sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Only our fixed diagnostic identifiers may reach the log.
        reason = str(exc) if isinstance(exc, ScheduleMismatch) else 'schedule_read_failed'
        print(json.dumps({'phase': 'existing_schedule_guard', 'status': 'FAILED',
                          'reason': reason,
                          'remedy': 'Match the existing binding or use its preserve-only Scheduler reference. Schedule migrations require their own reviewed operation.'}), file=sys.stderr)
        raise SystemExit(1) from None
