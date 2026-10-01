"""Runner-only, bounded technical metadata. No billing, metrics, logs or writes."""
import os
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
FUNCTIONS = ('justhodl-fleet-error-monitor', 'justhodl-health-monitor',
             'justhodl-cost-anomaly', 'justhodl-fleet-integrity')
REGION = 'us-east-1'
ARN_PREFIX = 'arn:aws:lambda:us-east-1:857687956942:function:'
SCHEDULERS = ('fleet-error-monitor-sched',)
MAX_CALLS = 33  # 4 configs + 4 exact-target lists + 12*(rule+targets) + 1 schedule
MAX_RULES = 12


class Stop(Exception):
    pass


class Reader:
    def __init__(self):
        self.calls = 0
        self.deadline = time.monotonic() + 300

    def read(self, method, **kwargs):
        if self.calls >= MAX_CALLS or time.monotonic() >= self.deadline:
            raise Stop('read_bound_reached')
        self.calls += 1
        try:
            return method(**kwargs)
        except Exception as exc:
            code = getattr(exc, 'response', {}).get('Error', {}).get('Code', '')
            if code == 'ResourceNotFoundException':
                return None
            if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation'):
                raise Stop('permission_denied') from None
            raise Stop('read_failed_details_withheld') from None


def inspect(lam, events, scheduler, out, reader):
    arns = {ARN_PREFIX + name: name for name in FUNCTIONS}
    rules = set()
    incomplete = False
    for name in FUNCTIONS:
        item = reader.read(lam.get_function_configuration, FunctionName=name)
        item = item or {}
        out.kv(function=name, exists=bool(item), code_sha256=item.get('CodeSha256'),
               runtime=item.get('Runtime'), memory_mb=item.get('MemorySize'),
               timeout_seconds=item.get('Timeout'), state=item.get('State'))
        listing = reader.read(events.list_rule_names_by_target,
                              TargetArn=ARN_PREFIX + name, Limit=100)
        if listing is None:
            raise Stop('listing_unavailable')
        names = listing.get('RuleNames')
        if not isinstance(names, list) or len(names) > 100:
            raise Stop('listing_contract_invalid')
        incomplete = incomplete or bool(listing.get('NextToken'))
        rules.update(names)
        if len(rules) > MAX_RULES:
            raise Stop('rule_bound_reached')
    for name in sorted(rules):
        rule = reader.read(events.describe_rule, Name=name)
        targets = reader.read(events.list_targets_by_rule, Rule=name, Limit=100)
        if rule is None or targets is None:
            incomplete = True
            out.kv(kind='events', binding=name, status='changed_during_read')
            continue
        rows = targets.get('Targets')
        if not isinstance(rows, list) or len(rows) > 100:
            raise Stop('targets_contract_invalid')
        incomplete = incomplete or bool(targets.get('NextToken'))
        matches = [arns[t.get('Arn')] for t in rows if t.get('Arn') in arns]
        out.kv(kind='events', binding=name, state=rule.get('State'),
               cadence=rule.get('ScheduleExpression'), exact_function_matches=matches,
               target_count=len(rows), targets_complete=not bool(targets.get('NextToken')))
    for name in SCHEDULERS:
        item = reader.read(scheduler.get_schedule, Name=name, GroupName='default')
        if item is None:
            out.kv(kind='scheduler', binding=name, exists=False)
            continue
        target = item.get('Target') or {}
        retry = target.get('RetryPolicy') or {}
        out.kv(kind='scheduler', binding=name, exists=True, state=item.get('State'),
               cadence=item.get('ScheduleExpression'), timezone=item.get('ScheduleExpressionTimezone'),
               exact_function_match=arns.get(target.get('Arn')),
               max_retries=retry.get('MaximumRetryAttempts'),
               max_event_age_seconds=retry.get('MaximumEventAgeInSeconds'))
    out.kv(completed=True, api_calls=reader.calls, pagination_incomplete=incomplete,
           scope='Four named functions; default-bus exact unqualified targets; one recorded default-group Scheduler. Alias/custom-bus/arbitrary-name bindings unsearched.',
           aws_writes=0, metric_queries=0, billing_reads=0, log_reads=0)


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise SystemExit('runner_only')
    import boto3
    from botocore.config import Config
    from ops_report import report

    def deadline(*_):
        raise Stop('time_bound_reached')

    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(300)
    reader = Reader()
    with report(Path(__file__).stem) as out:
        try:
            cfg = Config(connect_timeout=3, read_timeout=5, retries={'total_max_attempts': 1})
            clients = [boto3.client(service, region_name=REGION, config=cfg)
                       for service in ('lambda', 'events', 'scheduler')]
            inspect(*clients, out, reader)
        except Stop as exc:
            out.kv(completed=False, api_calls=reader.calls, stop_reason=str(exc))
            out._failed = True
            raise SystemExit(1) from None
        except Exception:
            out.kv(completed=False, api_calls=reader.calls, stop_reason='unexpected_failure_details_withheld')
            out._failed = True
            raise SystemExit(1) from None
        finally:
            signal.alarm(0)


if __name__ == '__main__':
    main()
