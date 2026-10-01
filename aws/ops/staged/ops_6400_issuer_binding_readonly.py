"""Read-only exact-target binding diagnosis; AWS clients exist only on Actions."""
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
from ops_report import report

FUNCTION = 'justhodl-etf-issuer-holdings'
REGION = 'us-east-1'
ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
PAGE_SIZE = 20
MAX_PAGES = 3
MAX_REFERENCES = 20


class Stop(Exception):
    pass


def read(method, **kwargs):
    try:
        return method(**kwargs)
    except Exception as exc:
        code = getattr(exc, 'response', {}).get('Error', {}).get('Code', '')
        if code == 'ResourceNotFoundException':
            return None
        raise Stop('permission_denied' if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation') else 'read_failed') from None


def pages(method, key, **kwargs):
    token = None
    seen = set()
    for _ in range(MAX_PAGES):
        result = read(method, **kwargs, **({'NextToken': token} if token else {}))
        if result is None:
            raise Stop('listing_not_found')
        rows = result.get(key)
        if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
            raise Stop('listing_contract_invalid')
        yield rows
        token = result.get('NextToken')
        if not token:
            return
        if not isinstance(token, str) or token in seen:
            raise Stop('pagination_invalid')
        seen.add(token)
    raise Stop('pagination_bound_reached')


def references():
    refs = set()
    config = json.loads((ROOT / 'aws/lambdas' / FUNCTION / 'config.json').read_text(encoding='utf-8'))
    full = config.get('eventbridge_scheduler') or {}
    short = config.get('schedule') or {}
    if full.get('schedule_name'):
        refs.add((full.get('group_name', 'default'), full['schedule_name']))
    if isinstance(short, dict) and short.get('scheduler_name'):
        refs.add((short.get('group_name', 'default'), short['scheduler_name']))
    def walk(value):
        if isinstance(value, dict):
            if value.get('kind') == 'scheduler' and any(t.get('arn') in (ARN, ARN+':$LATEST', ARN+':live') for t in value.get('targets', []) if isinstance(t, dict)):
                refs.add((value.get('group', 'default'), value['name']))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(json.loads((ROOT / 'config/schedule-manifest.json').read_text(encoding='utf-8')))
    if len(refs) > MAX_REFERENCES:
        raise Stop('reference_bound_reached')
    return sorted(refs)


def inspect(r, lam, events, scheduler, refs):
    r.kv(search_scope='Classic default-bus rules by exact unqualified Lambda target ARN; Scheduler repository references plus NamePrefix='+FUNCTION,
         pagination_bound='3 pages x 20 items per listing; at most 20 explicit Scheduler references',
         absence_limit='Arbitrary Scheduler names and classic alias-only/custom-bus bindings are not exhaustively searched',
         repository_scheduler_references=len(refs))
    configuration = read(lam.get_function_configuration, FunctionName=FUNCTION)
    r.kv(lambda_exists=configuration is not None, lambda_state=(configuration or {}).get('State', 'unavailable'))
    names = []
    for batch in pages(events.list_rule_names_by_target, 'RuleNames', TargetArn=ARN, Limit=PAGE_SIZE):
        names.extend(batch)
    for name in sorted(set(names)):
        rule = read(events.describe_rule, Name=name)
        if rule is None:
            r.kv(binding_name=name, group='classic/default', target_match='unavailable', cadence='unavailable', timezone='UTC', state='not_found_during_read')
            continue
        match = False
        for batch in pages(events.list_targets_by_rule, 'Targets', Rule=name, Limit=PAGE_SIZE):
            match = match or any(t.get('Arn') == ARN for t in batch)
        r.kv(binding_name=name, group='classic/default', target_match=match,
             cadence=rule.get('ScheduleExpression'), timezone='UTC', state=rule.get('State'))
    found = set(refs)
    for batch in pages(scheduler.list_schedules, 'Schedules', NamePrefix=FUNCTION, MaxResults=PAGE_SIZE):
        for item in batch:
            found.add((item.get('GroupName', 'default'), item['Name']))
    for group, name in sorted(found):
        item = read(scheduler.get_schedule, GroupName=group, Name=name)
        if item is None:
            r.kv(binding_name=name, group=group, target_match='unavailable', cadence='unavailable', timezone='unavailable', state='not_found_during_read')
            continue
        r.kv(binding_name=name, group=group, target_match=item.get('Target', {}).get('Arn') in (ARN, ARN+':$LATEST', ARN+':live'),
             cadence=item.get('ScheduleExpression'), timezone=item.get('ScheduleExpressionTimezone', 'UTC'), state=item.get('State'))
    r.kv(search_completed=True, classic_rules_returned=len(set(names)), scheduler_names_inspected=len(found))


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise SystemExit('runner_only')
    import boto3
    from botocore.config import Config
    with report(Path(__file__).stem) as r:
        try:
            cfg = Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts': 1})
            clients = [boto3.client(service, region_name=REGION, config=cfg) for service in ('lambda', 'events', 'scheduler')]
            inspect(r, *clients, references())
        except Stop as exc:
            r.kv(search_completed=False, stop_reason=str(exc))
            r._failed = True
            raise SystemExit(1) from None
        except Exception:
            r.kv(search_completed=False, stop_reason='unexpected_read_failure_details_withheld')
            r._failed = True
            raise SystemExit(1) from None


if __name__ == '__main__':
    main()
