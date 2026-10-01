"""Staged read-only publication diagnosis. No dispatch until exact-head review.
Only public output HEAD, function metrics and direct binding metadata are read.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import math
import os
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
FUNCTION = 'justhodl-industry-case'
ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
TARGETS = (ARN, ARN + ':$LATEST', ARN + ':live')
START = datetime(2026, 10, 1, 12, 55, tzinfo=timezone.utc)
MAX_CALLS = 33  # HEAD 1 + metrics 1 + rules 6 + descriptions 5 + targets 5 + schedules 10 + details 5
MAX_SECONDS = 90
ALLOWED = {('s3', 'head_object'), ('cloudwatch', 'get_metric_data'),
           ('events', 'list_rule_names_by_target'), ('events', 'describe_rule'),
           ('events', 'list_targets_by_rule'), ('scheduler', 'list_schedules'),
           ('scheduler', 'get_schedule')}


class Unavailable(Exception):
    pass


def require(ok):
    if not ok:
        raise Unavailable('invalid_response')


def name(value):
    require(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', value))
    return value


def clock(value):
    require(isinstance(value, datetime) and value.tzinfo is not None)
    return value.astimezone(timezone.utc)


def scalar(value):
    require(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 10**15)
    return value


def target(value):
    return isinstance(value, str) and (value == ARN or
        re.fullmatch(re.escape(ARN) + r':[A-Za-z0-9_$-]{1,128}', value) is not None)


def binding(value):
    expr = value.get('ScheduleExpression')
    state = value.get('State')
    require(state in ('ENABLED', 'DISABLED', 'ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS'))
    require(expr is None or (isinstance(expr, str) and len(expr) <= 256 and
        re.fullmatch(r'[A-Za-z0-9_?*,/(): .+\-]+', expr)))
    return {'state': state, 'expression': expr}


class Reads:
    def __init__(self, clients, now, timer=time.monotonic):
        self.end = clock(now).replace(second=0, microsecond=0)
        if self.end.date() != START.date() or self.end <= START:
            raise Unavailable('outside_approved_date_window')
        self.clients, self.timer = clients, timer
        self.deadline, self.calls = timer() + MAX_SECONDS, 0

    def call(self, service, operation, **kw):
        require((service, operation) in ALLOWED)
        if self.calls >= MAX_CALLS or self.timer() >= self.deadline:
            raise Unavailable('read_budget_exhausted')
        self.calls += 1
        try:
            result = getattr(self.clients[service], operation)(**kw)
        except Exception:
            raise Unavailable('aws_read_failed') from None
        require(isinstance(result, dict))
        return result

    def pages(self, service, operation, field, limit, **kw):
        rows, tokens = [], set()
        for _ in range(limit):
            try:
                reply = self.call(service, operation, **kw)
                batch = reply.get(field)
                require(isinstance(batch, list) and len(batch) <= 100)
                rows.extend(batch)
                token = reply.get('NextToken')
                if not token:
                    return rows, None
                require(isinstance(token, str) and len(token) <= 4096 and token not in tokens)
                tokens.add(token)
                kw['NextToken'] = token
            except Unavailable as exc:
                return rows, str(exc)
        return rows, 'pagination_limit'


def head(api):
    r = api.call('s3', 'head_object', Bucket='justhodl-dashboard-live', Key='data/industry-case.json')
    etag, encoding = r.get('ETag'), r.get('ContentEncoding')
    require(type(r.get('ContentLength')) is int)
    require(isinstance(etag, str) and re.fullmatch(r'"?[a-fA-F0-9]{32}(?:-[0-9]{1,8})?"?', etag))
    require(encoding in (None, 'gzip', 'identity', 'br'))
    return {'last_modified': clock(r.get('LastModified')).isoformat(),
            'bytes': scalar(r.get('ContentLength')), 'etag': etag, 'encoding': encoding,
            'body_read': False, 'semantic_freshness_verified': False}


def metrics(api):
    specs = {'invocations': ('Invocations', 'Sum'), 'errors': ('Errors', 'Sum'),
             'throttles': ('Throttles', 'Sum'), 'duration': ('Duration', 'Maximum')}
    queries = [{'Id': k, 'MetricStat': {'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': v[0],
        'Dimensions': [{'Name': 'FunctionName', 'Value': FUNCTION}]}, 'Period': 60, 'Stat': v[1]},
        'ReturnData': True} for k, v in specs.items()]
    r = api.call('cloudwatch', 'get_metric_data', MetricDataQueries=queries, StartTime=START,
                 EndTime=api.end, ScanBy='TimestampAscending', MaxDatapoints=3000)
    rows = r.get('MetricDataResults')
    require(isinstance(rows, list) and len(rows) <= 4)
    out = {k: {'status': 'unavailable', 'observed_value': None, 'reported_minutes': 0} for k in specs}
    seen = set()
    for row in rows:
        require(isinstance(row, dict))
        key = row.get('Id')
        require(key in specs and key not in seen)
        seen.add(key)
        ts, values = row.get('Timestamps'), row.get('Values')
        require(isinstance(ts, list) and isinstance(values, list) and len(ts) == len(values) <= 665)
        stamps = [clock(t) for t in ts]
        require(len(set(stamps)) == len(stamps) and all(START <= t < api.end and t.second == 0 and t.microsecond == 0 for t in stamps))
        vals = [scalar(v) for v in values]
        complete = row.get('StatusCode') == 'Complete' and not r.get('NextToken') and not r.get('Messages') and not row.get('Messages')
        out[key] = {'status': 'observed' if vals and complete else 'unavailable_or_partial',
            'observed_value': (max(vals) if key == 'duration' else sum(vals)) if vals else None,
            'reported_minutes': len(vals), 'expected_minutes': int((api.end - START).total_seconds() / 60),
            'api_complete': complete, 'all_minutes_reported': complete and len(vals) == int((api.end - START).total_seconds()/60),
            'unit': 'milliseconds' if key == 'duration' else 'count'}
    return {'start_inclusive': START.isoformat(), 'end_exclusive': api.end.isoformat(),
            'series': out, 'pagination_incomplete': bool(r.get('NextToken')),
            'limitation': 'Sparse or absent minutes are unknown, not zero. Ingestion may lag. Function aggregates do not identify code versions, triggers, completion, or successful writes.'}


def rules(api):
    found, issues = {}, []
    for arn in TARGETS:
        rows, error = api.pages('events', 'list_rule_names_by_target', 'RuleNames', 2,
                               TargetArn=arn, EventBusName='default', Limit=100)
        if error:
            issues.append(error)
        for row in rows:
            found.setdefault(name(row), set()).add(arn)
    if len(found) > 5:
        issues.append('detail_limit')
    details = []
    for rule in sorted(found)[:5]:
        try:
            r = api.call('events', 'describe_rule', Name=rule, EventBusName='default')
            require(r.get('Name') == rule)
            item = {'name': rule, **binding(r)}
            rows, error = api.pages('events', 'list_targets_by_rule', 'Targets', 1,
                                   Rule=rule, EventBusName='default', Limit=100)
            if error:
                issues.append(error)
            require(all(isinstance(t, dict) for t in rows))
            matched = [t['Arn'] for t in rows if target(t.get('Arn'))]
            item.update(confirmed_target_qualifiers=sorted({t[len(ARN):] or '(unqualified)' for t in matched}),
                        target_read_incomplete=bool(error), matching_target_observed=bool(matched))
            if not matched:
                issues.append('target_not_confirmed')
            details.append(item)
        except Unavailable as exc:
            issues.append(str(exc))
    return {'rules': details, 'lookup_matches': len(found), 'bounded_scope_incomplete': bool(issues),
            'issues': sorted(set(issues)), 'inventory_complete': False,
            'scope': 'Default bus; unqualified, $LATEST and live target lookups only. Other qualifiers, buses and indirect triggers unexamined. Empty results do not establish no binding.'}


def schedules(api):
    rows, error = api.pages('scheduler', 'list_schedules', 'Schedules', 10, MaxResults=100)
    issues = [error] if error else []
    require(all(isinstance(r, dict) and isinstance(r.get('Target'), dict) for r in rows))
    matches = {(name(r.get('GroupName')), name(r.get('Name'))) for r in rows if target(r['Target'].get('Arn'))}
    if len(matches) > 5:
        issues.append('detail_limit')
    details = []
    for group, schedule in sorted(matches)[:5]:
        try:
            r = api.call('scheduler', 'get_schedule', Name=schedule, GroupName=group)
            require(r.get('Name') == schedule and r.get('GroupName') == group)
            t = r.get('Target')
            require(isinstance(t, dict) and target(t.get('Arn')))
            zone = r.get('ScheduleExpressionTimezone')
            require(isinstance(zone, str) and re.fullmatch(r'[A-Za-z0-9_+./-]{1,80}', zone))
            details.append({'name': schedule, 'group': group, **binding(r), 'timezone': zone,
                            'target_qualifier': t['Arn'][len(ARN):] or '(unqualified)'})
        except Unavailable as exc:
            issues.append(str(exc))
    return {'schedules': details, 'matching_summaries': len(matches), 'bounded_scope_incomplete': bool(issues),
            'issues': sorted(set(issues)), 'inventory_complete': False,
            'scope': 'Regional direct Lambda targets across returned groups; universal AWS SDK targets and indirect invocations unexamined. No global absence claim.'}


def observe(api):
    result = {'function': FUNCTION, 'api_call_limit': MAX_CALLS, 'aws_mutations': 0, 'native_invocations': 0,
              'bodies_or_logs_read': 0, 'inventory_complete': False, 'read_errors': []}
    for key, fn in [('output_metadata', head), ('metrics', metrics), ('rules', rules), ('schedules', schedules)]:
        try:
            result[key] = fn(api)
        except Unavailable as exc:
            result[key] = {'status': 'unavailable', 'reason': str(exc)}
            result['read_errors'].append(key)
        except Exception:
            result[key] = {'status': 'unavailable', 'reason': 'invalid_response'}
            result['read_errors'].append(key)
    result['api_calls'] = api.calls
    result['bounded_reads_complete'] = not result['read_errors'] and not any(result[k].get('bounded_scope_incomplete') for k in ('rules', 'schedules')) and all(v.get('api_complete', False) and v.get('status') == 'observed' for v in result.get('metrics', {}).get('series', {}).values()) and bool(result.get('metrics', {}).get('series'))
    return result


def main():
    if not (os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REPOSITORY') == 'ElMooro/si'
            and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
            and os.environ.get('GITHUB_WORKFLOW') == 'Run ops script (direct)'
            and re.fullmatch(r'[a-f0-9]{40}', os.environ.get('GITHUB_SHA', ''))):
        raise SystemExit('reviewed_direct_runner_only')
    import boto3
    from botocore.config import Config
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    with report(Path(__file__).stem) as r:
        r.kv(probe_commit=os.environ['GITHUB_SHA'], probe_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        try:
            cfg = Config(connect_timeout=3, read_timeout=5, retries={'total_max_attempts': 1})
            clients = {s: boto3.client(s, region_name='us-east-1', config=cfg) for s in ('s3', 'cloudwatch', 'events', 'scheduler')}
            result = observe(Reads(clients, datetime.now(timezone.utc)))
            r.kv(**result)
            if not result['bounded_reads_complete']:
                r.fail('Bounded evidence incomplete; do not infer zero activity or absent bindings.')
                raise SystemExit(1)
        except Exception:
            r.fail('Read unavailable; exception details withheld.')
            raise SystemExit(1) from None


if __name__ == '__main__':
    main()
