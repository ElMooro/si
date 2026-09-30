"""One bounded read-only observation of the existing ETF dependency schedules.
No producer invoke, environment values, provider requests, logs, or AWS writes.
Scheduler authorization errors stop the probe; never fall back on access denial.
"""
from pathlib import Path
import hashlib
import json
import sys
ROOT = Path(__file__).resolve().parents[3]
NAMES = ('justhodl-etf-global-desk', 'justhodl-etf-constituents', 'justhodl-flow-lookthrough')
OBJECTS = ('data/etf-desk-research.json', 'data/etf-holdings-research.json', 'data/holdings-lookthrough-research.json')


def error_code(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code', 'unknown')


def target_summary(target):
    out = {k: v for k, v in target.items() if k in ('Arn', 'Id', 'RoleArn', 'RetryPolicy', 'DeadLetterConfig')}
    value = target.get('Input')
    out['input_present'] = 'Input' in target
    out['input_bytes'] = len(value.encode('utf-8')) if isinstance(value, str) else None
    out['input_sha256'] = hashlib.sha256(value.encode('utf-8')).hexdigest() if isinstance(value, str) else None
    out['other_field_names'] = sorted(set(target) - set(out) - {'Input'})
    return out


def observe_schedule(scheduler, events, name):
    try:
        s = scheduler.get_schedule(Name=name, GroupName='default')
    except Exception as exc:
        code = error_code(exc)
        if code != 'ResourceNotFoundException':
            raise RuntimeError('scheduler:GetSchedule ' + name + ' failed: ' + code) from None
    else:
        return {'kind': 'scheduler', **{k: str(v) if k in ('CreationDate', 'LastModificationDate', 'StartDate', 'EndDate') else v for k, v in s.items() if k in ('Name', 'GroupName', 'Arn', 'ScheduleExpression', 'ScheduleExpressionTimezone', 'State', 'FlexibleTimeWindow', 'ActionAfterCompletion', 'KmsKeyArn', 'CreationDate', 'LastModificationDate', 'StartDate', 'EndDate', 'Description')}, 'Target': target_summary(s['Target'])}
    # A confirmed absence is distinct from denied Scheduler access.
    try:
        rule = events.describe_rule(Name=name)
        page = events.list_targets_by_rule(Rule=name)
    except Exception as exc:
        raise RuntimeError('events exact binding read ' + name + ' failed: ' + error_code(exc)) from None
    if page.get('NextToken'):
        raise RuntimeError('Unexpected multiple target pages for exact ETF binding')
    return {'kind': 'events', 'scheduler_exact_name': 'not_found',
            'rule': {k:v for k,v in rule.items() if k in ('Name','Arn','ScheduleExpression','State','Description','EventBusName','RoleArn','ManagedBy')},
            'event_pattern_present': bool(rule.get('EventPattern')),
            'targets': [target_summary(t) for t in page['Targets']]}


def main():
    import boto3
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    with report('ops_6380_etf_desk_phase_probe') as r:
        r.kv(scope='Three exact ETF dependency bindings, selected Lambda metadata and three public object headers only',
             native_invocations=0, provider_requests=0, aws_writes=0, environment_values_reported=0, private_original_reads=0)
        clients = {n:boto3.client(n, region_name='us-east-1') for n in ('scheduler', 'events', 'lambda', 's3')}
        for fn in NAMES:
            # Schedule authorization is checked first. A denial cannot trigger a fallback.
            binding = observe_schedule(clients['scheduler'], clients['events'], fn + '-daily')
            r.kv(function=fn, binding=binding)
            try:
                cfg = clients['lambda'].get_function_configuration(FunctionName=fn)
            except Exception as exc:
                raise RuntimeError('lambda:GetFunctionConfiguration ' + fn + ' failed: ' + error_code(exc)) from None
            r.kv(function=fn, runtime={k:cfg.get(k) for k in ('FunctionName','FunctionArn','Runtime','State','LastUpdateStatus','LastModified','Timeout','MemorySize','CodeSha256','RevisionId')})
        for key in OBJECTS:
            try:
                h = clients['s3'].head_object(Bucket='justhodl-dashboard-live', Key=key)
            except Exception as exc:
                raise RuntimeError('s3:HeadObject public ETF artifact failed: ' + error_code(exc)) from None
            r.kv(public_key=key, last_modified=str(h['LastModified']), bytes=h['ContentLength'], etag=h.get('ETag'))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
