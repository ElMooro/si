"""Install the authorized weekday post-close schedule after source deployment.

Only this newly named Scheduler is created/enabled. Existing bindings are never
retimed or removed. Reuse the existing Scheduler execution role, with no IAM
writes or producer invocation. A durable conditional journal retains the prior
absence and rollback identity; an ambiguous write is not automatically repeated.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops'), str(ROOT / 'scripts')]
from ops_6490_network_schedule_diagnostic import pages
from scheduler_payload import WRITABLE

FUNCTION = 'justhodl-master-ranker'
ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
BUCKET = 'justhodl-dashboard-live'
KEY = 'data/ops/master-ranker-schedule/ops_6494.json'
SPEC = json.loads((ROOT / 'config/master-ranker-schedule.json').read_text(encoding='utf-8'))['eventbridge_scheduler']
NAME = SPEC['schedule_name']
GROUP = SPEC['group_name']


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def missing(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404', 'ResourceNotFoundException')


def read_journal(s3):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)['Body'].read())
    except Exception as exc:
        if missing(exc):
            return None
        raise


def desired(state='ENABLED'):
    return {'Name': NAME, 'GroupName': GROUP, 'ScheduleExpression': SPEC['cron'],
            'ScheduleExpressionTimezone': SPEC['timezone'], 'State': state,
            'FlexibleTimeWindow': SPEC['flexible_time_window'], 'Description': SPEC['description'],
            'Target': {'Arn': ARN, 'RoleArn': SPEC['role_arn'], 'Input': '{}', 'RetryPolicy': SPEC['retry_policy']}}


def shape(schedule):
    return {key: value for key, value in schedule.items() if key in WRITABLE and key != 'ActionAfterCompletion'}


def targets_ranker(target):
    arn = target.get('Arn', '')
    if arn == ARN or arn.startswith(ARN + ':'):
        return True
    if arn.endswith(':aws-sdk:lambda:invoke'):
        try:
            function = json.loads(target.get('Input', '{}')).get('FunctionName', '')
        except (ValueError, AttributeError):
            raise ValueError('unreadable_universal_lambda_target') from None
        return function == FUNCTION or function.startswith(FUNCTION + ':') or function == ARN or function.startswith(ARN + ':')
    return False


def bindings(events, scheduler):
    """Complete direct-target census, including numbered versions/custom buses."""
    found = []
    for bus in pages(events.list_event_buses, 'EventBuses', Limit=100):
        for rule in pages(events.list_rules, 'Rules', EventBusName=bus['Name'], Limit=100):
            for target in pages(events.list_targets_by_rule, 'Targets', Rule=rule['Name'], EventBusName=bus['Name'], Limit=100):
                if targets_ranker(target):
                    found.append({'kind': 'classic', 'bus': bus['Name'], 'name': rule['Name'], 'state': rule.get('State')})
    for row in pages(scheduler.list_schedules, 'Schedules', MaxResults=100):
        target = row.get('Target') or {}
        if target.get('Arn', '').endswith(':aws-sdk:lambda:invoke'):
            target = scheduler.get_schedule(Name=row['Name'], GroupName=row.get('GroupName', 'default'))['Target']
        if targets_ranker(target):
            found.append({'kind': 'scheduler', 'group': row.get('GroupName', 'default'), 'name': row['Name'], 'state': row.get('State')})
    return found


def release_guard(lam, s3):
    proof = {}
    for fn in (FUNCTION, 'justhodl-event-coordinator'):
        receipt = json.loads(s3.get_object(Bucket=BUCKET, Key='data/ops/releases/' + fn + '.json')['Body'].read())
        config = lam.get_function_configuration(FunctionName=fn)
        source_sha = hashlib.sha256((ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py').read_bytes()).hexdigest()
        if (receipt.get('function') != fn or receipt.get('verified') is not True
                or receipt.get('source', {}).get('lambda_function.py', {}).get('sha256') != source_sha
                or receipt.get('code_sha256') != config.get('CodeSha256')
                or config.get('State') != 'Active' or config.get('LastUpdateStatus') != 'Successful'):
            raise ValueError('required_source_release_not_verified')
        proof[fn] = {'source_sha256': source_sha, 'code_sha256': config['CodeSha256'],
                     'revision_id': config['RevisionId'], 'commit': receipt['commit']}
    return proof


def fanout_guard(lam, s3):
    """Reject indirect automatic invokes from either current fan-out manifest."""
    cfg = lam.get_function_configuration(FunctionName='justhodl-scheduler')
    key = cfg.get('Environment', {}).get('Variables', {}).get('FANOUT_MANIFEST_KEY', 'config/fanout-manifest.json')
    if key not in ('config/fanout-manifest.json', 'config/schedule-manifest.json'):
        raise ValueError('unreviewed_fanout_manifest_location')
    proof = {}
    for path in dict.fromkeys((key, 'config/schedule-manifest.json')):
        try:
            response = s3.get_object(Bucket=BUCKET, Key=path)
        except Exception as exc:
            if not missing(exc):
                raise
            proof[path] = {'missing': True}
            continue
        raw = response['Body'].read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError('fanout_manifest_exceeds_bound')
        packet = json.loads(raw)
        ticks = packet.get('ticks', {})
        if not isinstance(ticks, dict):
            raise ValueError('fanout_manifest_invalid')
        disabled = packet.get('disabled') or []
        if not isinstance(disabled, list):
            raise ValueError('fanout_disabled_invalid')
        for functions in ticks.values():
            if not isinstance(functions, list) or any(not isinstance(fn, str) for fn in functions):
                raise ValueError('fanout_members_invalid')
            for fn in functions:
                if fn not in disabled and targets_ranker({'Arn': fn if fn.startswith('arn:') else ARN.replace(FUNCTION, fn)}):
                    raise ValueError('master_ranker_has_fanout_trigger')
        proof[path] = {'sha256': hashlib.sha256(raw).hexdigest(), 'active_ranker_memberships': 0}
    return proof


def check_current(scheduler, state):
    current = scheduler.get_schedule(Name=NAME, GroupName=GROUP)
    if shape(current) != desired(state) or current.get('ActionAfterCompletion', 'NONE') != 'NONE':
        raise ValueError('schedule_changed_or_unexpected')
    return current


def execute(lam, s3, events, scheduler, now):
    proof = release_guard(lam, s3)
    fanout = fanout_guard(lam, s3)
    existing = read_journal(s3)
    if existing:
        if existing.get('status') != 'verified' or existing.get('desired_sha256') != digest(desired()):
            raise ValueError('prior_attempt_requires_review_no_automatic_retry')
        check_current(scheduler, 'ENABLED')
        if bindings(events, scheduler) != [{'kind': 'scheduler', 'group': GROUP, 'name': NAME, 'state': 'ENABLED'}]:
            raise ValueError('additional_binding_detected')
        return {'status': 'verified_existing', 'schedule': desired(), 'native_invocations': 0}
    if bindings(events, scheduler):
        raise ValueError('existing_binding_requires_separate_review')
    try:
        scheduler.get_schedule(Name=NAME, GroupName=GROUP)
    except Exception as exc:
        if not missing(exc):
            raise
    else:
        raise ValueError('schedule_name_already_owned')
    # The same role already operates the reviewed Ticker 360 Scheduler.
    reference = scheduler.get_schedule(Name='justhodl-ticker-360-schedule', GroupName='default')
    if (reference.get('State') != 'ENABLED' or reference.get('Target', {}).get('RoleArn') != SPEC['role_arn']
            or reference.get('Target', {}).get('Arn') != ARN.replace(FUNCTION, 'justhodl-ticker-360')):
        raise ValueError('existing_scheduler_role_reference_differs')
    journal = {'schema': 'master-ranker-schedule-migration.v1', 'status': 'reserved', 'observed_at': now.isoformat(),
               'before': {'matching_bindings': [], 'named_schedule': None}, 'release': proof, 'fanout': fanout,
               'desired': desired(), 'desired_sha256': digest(desired()),
               'rollback': 'Delete only this newly created schedule if its full writable configuration and CreationDate still match this journal. No prior schedule to restore.'}
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=canonical(journal), ContentType='application/json', IfNoneMatch='*')
    def save(status, **extra):
        journal.update(status=status, **extra)
        s3.put_object(Bucket=BUCKET, Key=KEY, Body=canonical(journal), ContentType='application/json')
    created = None
    try:
        if release_guard(lam, s3) != proof or fanout_guard(lam, s3) != fanout or bindings(events, scheduler):
            raise ValueError('prerequisites_changed_before_creation')
        save('create_requested')
        scheduler.create_schedule(**desired('DISABLED'), ClientToken=digest({'op': KEY, 'action': 'create'}))
        created = check_current(scheduler, 'DISABLED')
        save('created_disabled', created_at=str(created['CreationDate']))
        if release_guard(lam, s3) != proof or fanout_guard(lam, s3) != fanout:
            raise ValueError('release_changed_before_enable')
        if bindings(events, scheduler) != [{'kind': 'scheduler', 'group': GROUP, 'name': NAME, 'state': 'DISABLED'}]:
            raise ValueError('additional_binding_before_enable')
        check_current(scheduler, 'DISABLED')
        save('enable_requested')
        scheduler.update_schedule(**desired(), ClientToken=digest({'op': KEY, 'action': 'enable'}))
        current = check_current(scheduler, 'ENABLED')
        if current['CreationDate'] != created['CreationDate']:
            raise ValueError('schedule_identity_changed')
        if bindings(events, scheduler) != [{'kind': 'scheduler', 'group': GROUP, 'name': NAME, 'state': 'ENABLED'}]:
            raise ValueError('additional_binding_after_enable')
        save('verified', verified_at=datetime.now(timezone.utc).isoformat())
    except Exception:
        rollback = 'not_attempted_creation_outcome_unknown' if created is None else 'withheld_changed_control'
        if created is not None:
            try:
                current = scheduler.get_schedule(Name=NAME, GroupName=GROUP)
                if (current.get('CreationDate') == created['CreationDate']
                        and shape(current) in (desired(), desired('DISABLED'))
                        and current.get('ActionAfterCompletion', 'NONE') == 'NONE'):
                    scheduler.delete_schedule(Name=NAME, GroupName=GROUP, ClientToken=digest({'op': KEY, 'action': 'rollback'}))
                    rollback = 'removed_only_created_schedule'
            except Exception:
                rollback = 'rollback_outcome_unknown'
        try:
            save('failed_requires_review', rollback_result=rollback)
        except Exception:
            pass
        raise RuntimeError('migration_stopped_no_automatic_retry; ' + rollback) from None
    return {'status': 'verified', 'observed_at': now.isoformat(), 'schedule': desired(),
            'release': proof, 'fanout': fanout, 'journal_key': KEY, 'native_invocations': 0,
            'scope': 'One weekday timer, no event-coordinator kicks. AWS retry delivery remains at-least-once; natural execution is pending.'}


def main():
    import boto3
    from ops_report import report
    with report('ops_6494_master_ranker_weekday_close') as output:
        try:
            result = execute(*(boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')),
                             datetime.now(timezone.utc))
        except Exception as exc:
            output.fail('Schedule migration stopped: ' + (str(exc) if isinstance(exc, (ValueError, RuntimeError)) else type(exc).__name__))
            raise RuntimeError('master_ranker_schedule_not_verified') from None
        output.log(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
