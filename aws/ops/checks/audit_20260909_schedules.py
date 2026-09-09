"""Consolidate only source-reviewed, equivalent Options Flow triggers.

Inputs remain in memory. The regular handler only distinguishes HTTP envelopes;
both an empty Scheduler input and an untransformed scheduled event take its same
quiet research refresh path. No schedule is deleted or rescheduled here.
"""
import copy
import json

FUNCTION = 'justhodl-options-flow'
ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
ROLE = 'arn:aws:iam::857687956942:role/justhodl-scheduler-role'
CADENCES = {
    'justhodl-options-flow-30m': 'cron(33 15 * * ? *)',
    'justhodl-options-flow-sched': 'cron(25 20 ? * MON-FRI *)',
}


def require(condition, code):
    if not condition:
        raise ValueError(code)


def input_kind(target):
    if 'InputPath' in target or 'InputTransformer' in target:
        return 'transformed'
    if 'Input' not in target:
        return 'absent'
    try:
        return 'empty_object' if json.loads(target['Input']) == {} else 'other'
    except (ValueError, TypeError):
        return 'invalid'


def retry_policy(target):
    # Documented defaults for both delivery services. Explicit overrides must
    # agree; consolidation must not silently discard stronger delivery settings.
    return {'MaximumRetryAttempts': 185, 'MaximumEventAgeInSeconds': 86400,
            **target.get('RetryPolicy', {})}


def guard(name, scheduler, rule, targets):
    expected = CADENCES[name]
    require(scheduler.get('Name') == name and scheduler.get('GroupName', 'default') == 'default', 'scheduler_identity')
    require(rule.get('Name') == name, 'rule_identity')
    require(scheduler.get('State') == 'ENABLED' and rule.get('State') in ('ENABLED', 'DISABLED'), 'trigger_state')
    require(scheduler.get('ScheduleExpression') == rule.get('ScheduleExpression') == expected, 'cadence_mismatch')
    require(scheduler.get('ScheduleExpressionTimezone', 'UTC') == 'UTC', 'timezone_mismatch')
    require(scheduler.get('FlexibleTimeWindow') == {'Mode': 'OFF'}, 'flexible_window')
    require(not any(scheduler.get(k) for k in ('StartDate', 'EndDate', 'KmsKeyArn')), 'lifecycle_or_encryption')
    require(scheduler.get('ActionAfterCompletion', 'NONE') == 'NONE', 'auto_delete')
    require(not rule.get('EventPattern') and not rule.get('RoleArn'), 'additional_rule_semantics')
    require(len(targets) == 1, 'unrelated_or_multiple_targets')
    st, et = scheduler.get('Target', {}), targets[0]
    require(st.get('Arn') == et.get('Arn') == ARN and st.get('RoleArn') == ROLE, 'target_identity')
    require(input_kind(st) == 'empty_object' and input_kind(et) in ('absent', 'empty_object'), 'input_semantics')
    require(set(st) <= {'Arn', 'RoleArn', 'Input', 'RetryPolicy', 'DeadLetterConfig'}, 'scheduler_target_options')
    require(set(et) <= {'Id', 'Arn', 'Input', 'RetryPolicy', 'DeadLetterConfig'}, 'event_target_options')
    require(retry_policy(st) == retry_policy(et), 'delivery_retry_mismatch')
    require(st.get('DeadLetterConfig', {}) == et.get('DeadLetterConfig', {}), 'delivery_dlq_mismatch')
    return {'name': name, 'expression': expected, 'timezone': 'UTC',
            'scheduler_state': scheduler['State'], 'rule_state': rule['State'],
            'scheduler_input_kind': input_kind(st), 'rule_input_kind': input_kind(et),
            'target_count': len(targets), 'delivery_policies_equal': True,
            'input_bodies_reported': 0}


def read(events, scheduler, name):
    targets = []; token = None; seen = set()
    while True:
        page = events.list_targets_by_rule(Rule=name, **({'NextToken': token} if token else {}))
        targets.extend(page.get('Targets', [])); token = page.get('NextToken')
        if not token:
            break
        require(token not in seen, 'repeated_target_page'); seen.add(token)
    return (scheduler.get_schedule(Name=name, GroupName='default'),
            events.describe_rule(Name=name), targets)


def stable(value):
    # AWS response metadata is transport information, not configuration.
    return [{k: v for k, v in part.items() if k != 'ResponseMetadata'} if isinstance(part, dict)
            else copy.deepcopy(part) for part in value]


def consolidate(events, scheduler, report):
    snapshots = {}
    report['checks'] = []
    for name in CADENCES:
        snapshot = read(events, scheduler, name)
        report['checks'].append(guard(name, *snapshot))
        snapshots[name] = snapshot
    report['changes'] = []
    for name, snapshot in snapshots.items():
        require(stable(read(events, scheduler, name)) == stable(snapshot), 'concurrent_configuration_change')
        if snapshot[1]['State'] == 'ENABLED':
            events.disable_rule(Name=name)
            report['changes'].append({'name': name, 'action': 'disable_duplicate_classic_rule',
                                      'rollback': 'enable_rule', 'schedule_deleted': False})
        after = read(events, scheduler, name)
        expected = copy.deepcopy(snapshot); expected[1]['State'] = 'DISABLED'
        require(stable(after) == stable(expected), 'disable_readback_mismatch')
    return report
